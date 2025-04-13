package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
)

// syncData synchronizes data between file and database
func syncData(ctx context.Context, db *sql.DB, config Config, fileRecords []DataRecord) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("transaction start error: %w", err)
	}
	defer tx.Rollback()

	switch config.Sync.SyncMode {
	case "overwrite":
		err = syncOverwrite(ctx, tx, config, fileRecords)
	case "diff":
		err = syncDiff(ctx, tx, config, fileRecords)
	default:
		return fmt.Errorf("unknown sync mode: %s", config.Sync.SyncMode)
	}

	if err != nil {
		return fmt.Errorf("sync process error: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("transaction commit error: %w", err)
	}

	return nil
}

// syncOverwrite performs complete overwrite synchronization
func syncOverwrite(ctx context.Context, tx *sql.Tx, config Config, fileRecords []DataRecord) error {
	// 1. Delete existing data (TRUNCATE or DELETE)
	// Note: TRUNCATE may not be rollback-able in some DBs
	// _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s", config.Sync.TableName))
	_, err := tx.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", config.Sync.TableName)) // TRUNCATE may be faster in some DBs
	if err != nil {
		return fmt.Errorf("error deleting data from table '%s': %w", config.Sync.TableName, err)
	}
	log.Printf("Deleted existing data from table '%s'.", config.Sync.TableName)

	// 2. Insert all file data
	if len(fileRecords) == 0 {
		log.Println("No data to insert.")
		return nil
	}

	// Build INSERT statement (using Bulk Insert for efficiency)
	valueStrings := make([]string, 0, len(fileRecords))
	valueArgs := make([]any, 0, len(fileRecords)*len(config.Sync.Columns))
	placeholders := make([]string, len(config.Sync.Columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	placeholderStr := fmt.Sprintf("(%s)", strings.Join(placeholders, ","))

	for _, record := range fileRecords {
		valueStrings = append(valueStrings, placeholderStr)
		for _, col := range config.Sync.Columns {
			valueArgs = append(valueArgs, record[col])
		}
	}

	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		config.Sync.TableName,
		strings.Join(config.Sync.Columns, ","),
		strings.Join(valueStrings, ","))

	_, err = tx.ExecContext(ctx, stmt, valueArgs...)
	if err != nil {
		return fmt.Errorf("data insertion error: %w", err)
	}
	log.Printf("Inserted %d records.", len(fileRecords))

	return nil
}

// syncDiff performs differential synchronization
func syncDiff(ctx context.Context, tx *sql.Tx, config Config, fileRecords []DataRecord) error {
	// Implementation is more complex
	// 1. Get current data from DB (primary key and target columns)
	dbRecords, err := getCurrentDBData(ctx, tx, config)
	if err != nil {
		return fmt.Errorf("DB data retrieval error: %w", err)
	}

	// 2. Compare file data with DB data
	//    - Data only in file -> INSERT target
	//    - Data only in DB -> DELETE target (if config.Sync.DeleteNotInFile is true)
	//    - Data in both but different content -> UPDATE target
	//    - Data in both with same content -> Do nothing
	toInsert, toUpdate, toDelete := diffData(config, fileRecords, dbRecords)

	log.Printf("Difference detection result: Insert %d, Update %d, Delete %d", len(toInsert), len(toUpdate), len(toDelete))

	// 3. INSERT processing
	if len(toInsert) > 0 {
		// Same implementation as in syncOverwrite's INSERT part
		err = bulkInsert(ctx, tx, config, toInsert)
		if err != nil {
			return fmt.Errorf("INSERT error: %w", err)
		}
		log.Printf("Inserted %d records.", len(toInsert))
	}

	// 4. UPDATE processing
	if len(toUpdate) > 0 {
		// Execute individual UPDATEs or try Bulk Update
		err = bulkUpdate(ctx, tx, config, toUpdate)
		if err != nil {
			return fmt.Errorf("UPDATE error: %w", err)
		}
		log.Printf("Updated %d records.", len(toUpdate))
	}

	// 5. DELETE processing
	if len(toDelete) > 0 && config.Sync.DeleteNotInFile {
		// Execute DELETE statement (using IN clause)
		err = bulkDelete(ctx, tx, config, toDelete)
		if err != nil {
			return fmt.Errorf("DELETE error: %w", err)
		}
		log.Printf("Deleted %d records.", len(toDelete))
	}

	return nil
}

// getCurrentDBData retrieves current data from database (for differential sync)
func getCurrentDBData(ctx context.Context, tx *sql.Tx, config Config) (map[string]DataRecord, error) {
	// SELECT <primaryKey>, <columns...> FROM <tableName>
	query := fmt.Sprintf("SELECT %s FROM %s",
		strings.Join(append([]string{config.Sync.PrimaryKey}, config.Sync.Columns...), ","), // Include primary key
		config.Sync.TableName)

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query execution error (%s): %w", query, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("column name retrieval error: %w", err)
	}

	dbData := make(map[string]DataRecord) // Map with primary key as key
	vals := make([]any, len(cols))
	scanArgs := make([]any, len(cols))
	for i := range vals {
		scanArgs[i] = &vals[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, fmt.Errorf("row data scan error: %w", err)
		}
		record := make(DataRecord)
		var pkValue string
		for i, colName := range cols {
			// Values from DB might be []byte or specific types, convert to string
			val := vals[i]
			var strVal string
			if b, ok := val.([]byte); ok {
				strVal = string(b)
			} else if val != nil {
				strVal = fmt.Sprintf("%v", val) // Handle other types
			} // NULL might be handled as empty string or separately

			record[colName] = strVal
			if colName == config.Sync.PrimaryKey {
				pkValue = strVal
			}
		}
		if pkValue == "" {
			// Skip or error if primary key can't be obtained
			log.Printf("Warning: Found record with empty primary key. Skipping. Record: %v", record)
			continue
		}
		dbData[pkValue] = record
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("row processing error: %w", err)
	}

	return dbData, nil
}

// diffData compares file data with DB data (for differential sync)
func diffData(
	config Config,
	fileRecords []DataRecord,
	dbRecords map[string]DataRecord,
) (toInsert []DataRecord, toUpdate []DataRecord, toDelete []DataRecord) {

	fileKeys := make(map[string]bool)

	for _, fileRecord := range fileRecords {
		pkValue := fileRecord[config.Sync.PrimaryKey]
		if pkValue == "" {
			log.Printf("Warning: Found record with empty primary key in file. Skipping: %v", fileRecord)
			continue
		}
		fileKeys[pkValue] = true

		dbRecord, exists := dbRecords[pkValue]
		if !exists {
			// Only in file -> Add
			toInsert = append(toInsert, fileRecord)
		} else {
			// In both -> Compare content
			isDiff := false
			for _, col := range config.Sync.Columns {
				// Check if file and DB values differ (consider type and NULL handling)
				if fileRecord[col] != dbRecord[col] {
					isDiff = true
					break
				}
			}
			if isDiff {
				// Content differs -> Update
				toUpdate = append(toUpdate, fileRecord)
			}
			// If content is same, do nothing
		}
	}

	// Check for data only in DB -> Delete targets
	if config.Sync.DeleteNotInFile {
		for pkValue, dbRecord := range dbRecords {
			if !fileKeys[pkValue] {
				toDelete = append(toDelete, dbRecord) // Delete target is DB record
			}
		}
	}

	return
}

// bulkInsert performs bulk insertion of records
func bulkInsert(ctx context.Context, tx *sql.Tx, config Config, records []DataRecord) error {
	if len(records) == 0 {
		return nil
	}
	valueStrings := make([]string, 0, len(records))
	valueArgs := make([]any, 0, len(records)*len(config.Sync.Columns))
	placeholders := make([]string, len(config.Sync.Columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	placeholderStr := fmt.Sprintf("(%s)", strings.Join(placeholders, ","))

	for _, record := range records {
		valueStrings = append(valueStrings, placeholderStr)
		for _, col := range config.Sync.Columns {
			valueArgs = append(valueArgs, record[col])
		}
	}
	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		config.Sync.TableName,
		strings.Join(config.Sync.Columns, ","),
		strings.Join(valueStrings, ","))
	_, err := tx.ExecContext(ctx, stmt, valueArgs...)
	return err
}

// bulkUpdate performs updates for multiple records
func bulkUpdate(ctx context.Context, tx *sql.Tx, config Config, records []DataRecord) error {
	if len(records) == 0 {
		return nil
	}
	// Simple example: Update one by one (inefficient)
	updateCols := make([]string, 0, len(config.Sync.Columns))
	for _, col := range config.Sync.Columns {
		if col != config.Sync.PrimaryKey { // Don't include primary key in SET clause
			updateCols = append(updateCols, fmt.Sprintf("%s = ?", col))
		}
	}
	if len(updateCols) == 0 {
		log.Println("No columns to update (primary key only).")
		return nil
	}

	stmtSQL := fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?",
		config.Sync.TableName,
		strings.Join(updateCols, ", "),
		config.Sync.PrimaryKey)

	stmt, err := tx.PrepareContext(ctx, stmtSQL)
	if err != nil {
		return fmt.Errorf("UPDATE preparation error: %w", err)
	}
	defer stmt.Close()

	for _, record := range records {
		args := make([]any, 0, len(config.Sync.Columns))
		for _, col := range config.Sync.Columns {
			if col != config.Sync.PrimaryKey {
				args = append(args, record[col])
			}
		}
		args = append(args, record[config.Sync.PrimaryKey]) // Primary key for WHERE clause
		_, err = stmt.ExecContext(ctx, args...)
		if err != nil {
			// Good to log which record caused the error
			return fmt.Errorf("UPDATE execution error (PK: %s): %w", record[config.Sync.PrimaryKey], err)
		}
	}
	return nil
}

// bulkDelete performs deletion of multiple records
func bulkDelete(ctx context.Context, tx *sql.Tx, config Config, records []DataRecord) error {
	if len(records) == 0 {
		return nil
	}
	pkValues := make([]any, 0, len(records))
	placeholders := make([]string, 0, len(records))
	for _, record := range records {
		pkValues = append(pkValues, record[config.Sync.PrimaryKey])
		placeholders = append(placeholders, "?")
	}

	stmt := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)",
		config.Sync.TableName,
		config.Sync.PrimaryKey,
		strings.Join(placeholders, ","))

	_, err := tx.ExecContext(ctx, stmt, pkValues...)
	return err
}
