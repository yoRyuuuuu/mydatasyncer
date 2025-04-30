package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"
)

// SyncOperation represents a single database synchronization operation
type SyncOperation struct {
	OperationType string      // Operation type: "INSERT", "UPDATE", or "DELETE"
	Records       []DataRecord // Records affected by this operation
	SQL          string      // SQL statement to be executed
}

// SyncPreview represents a comprehensive preview of all synchronization operations
// that would be performed during actual execution. This is used in dry-run mode
// to show the user what changes would be made without actually making them.
type SyncPreview struct {
	Operations   []SyncOperation // List of all operations that would be performed
	TotalChanges int            // Total number of records that would be affected
}

// syncData synchronizes data between file and database
func syncData(ctx context.Context, db *sql.DB, config Config, fileRecords []DataRecord) error {
	// If DryRun is enabled, generate and display preview
	if config.Sync.DryRun {
		preview, err := previewSync(ctx, db, config, fileRecords)
		if err != nil {
			return fmt.Errorf("preview generation error: %w", err)
		}
		displaySyncPreview(preview)
		return nil
	}

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

// previewSync generates a detailed preview of all database operations that would be performed
// during synchronization without actually executing them. It analyzes the source data and
// current database state to determine necessary INSERT, UPDATE, and DELETE operations.
//
// For overwrite mode, it previews:
// - A single DELETE operation to clear the table
// - INSERT operations for all records from the source file
//
// For differential mode, it previews:
// - INSERT operations for new records
// - UPDATE operations for modified records
// - DELETE operations for records not in source (if deleteNotInFile is true)
func previewSync(ctx context.Context, db *sql.DB, config Config, fileRecords []DataRecord) (*SyncPreview, error) {
	preview := &SyncPreview{
		Operations: make([]SyncOperation, 0),
	}

	if config.Sync.SyncMode == "overwrite" {
		// Overwrite mode preview
		deleteOp := SyncOperation{
			OperationType: "DELETE",
			SQL:          fmt.Sprintf("DELETE FROM %s", config.Sync.TableName),
		}
		preview.Operations = append(preview.Operations, deleteOp)

		if len(fileRecords) > 0 {
			columns := strings.Join(config.Sync.Columns, ",")
			placeholders := make([]string, len(config.Sync.Columns))
			for i := range placeholders {
				placeholders[i] = "?"
			}
			insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
				config.Sync.TableName,
				columns,
				strings.Join(placeholders, ","))

			insertOp := SyncOperation{
				OperationType: "INSERT",
				Records:      fileRecords,
				SQL:          insertSQL,
			}
			preview.Operations = append(preview.Operations, insertOp)
		}
		preview.TotalChanges = len(fileRecords) + 1 // +1 for DELETE operation
	} else {
		// Diff mode preview
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			return nil, fmt.Errorf("preview transaction start error: %w", err)
		}
		defer tx.Rollback()

		dbRecords, err := getCurrentDBData(ctx, tx, config)
		if err != nil {
			return nil, fmt.Errorf("DB data retrieval error: %w", err)
		}

		toInsert, toUpdate, toDelete := diffData(config, fileRecords, dbRecords)

		if len(toInsert) > 0 {
			columns := strings.Join(config.Sync.Columns, ",")
			placeholders := make([]string, len(config.Sync.Columns))
			for i := range placeholders {
				placeholders[i] = "?"
			}
			insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
				config.Sync.TableName,
				columns,
				strings.Join(placeholders, ","))

			insertOp := SyncOperation{
				OperationType: "INSERT",
				Records:      toInsert,
				SQL:          insertSQL,
			}
			preview.Operations = append(preview.Operations, insertOp)
		}

		if len(toUpdate) > 0 {
			updateCols := make([]string, 0)
			for _, col := range config.Sync.Columns {
				if col != config.Sync.PrimaryKey && !slices.Contains(config.Sync.ImmutableColumns, col) {
					updateCols = append(updateCols, fmt.Sprintf("%s = ?", col))
				}
			}
			updateSQL := fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?",
				config.Sync.TableName,
				strings.Join(updateCols, ", "),
				config.Sync.PrimaryKey)

			updateOp := SyncOperation{
				OperationType: "UPDATE",
				Records:      toUpdate,
				SQL:          updateSQL,
			}
			preview.Operations = append(preview.Operations, updateOp)
		}

		if len(toDelete) > 0 && config.Sync.DeleteNotInFile {
			deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE %s IN (?)",
				config.Sync.TableName,
				config.Sync.PrimaryKey)

			deleteOp := SyncOperation{
				OperationType: "DELETE",
				Records:      toDelete,
				SQL:          deleteSQL,
			}
			preview.Operations = append(preview.Operations, deleteOp)
		}

		preview.TotalChanges = len(toInsert) + len(toUpdate) + len(toDelete)
	}

	return preview, nil
}

// displaySyncPreview formats and displays the synchronization preview in a human-readable format.
// It shows:
// - Total number of changes that would be made
// - SQL statements that would be executed
// - Sample of affected records (up to 5 per operation)
// - Count of additional records beyond the sample
func displaySyncPreview(preview *SyncPreview) {
	fmt.Printf("=== Sync Preview ===\n")
	fmt.Printf("Total changes: %d\n\n", preview.TotalChanges)

	for _, op := range preview.Operations {
		switch op.OperationType {
		case "INSERT":
			fmt.Printf("[INSERT Operations]\n")
			fmt.Printf("- Records to insert: %d\n", len(op.Records))
			if len(op.Records) > 0 {
				fmt.Printf("  Sample records:\n")
				for i, record := range op.Records {
					if i < 5 {
						fmt.Printf("  %d. id: %s, ", i+1, record["id"])
						if name, ok := record["name"]; ok {
							fmt.Printf("name: %s, ", name)
						}
						if price, ok := record["price"]; ok {
							fmt.Printf("price: %s", price)
						}
						fmt.Printf("\n")
					}
				}
				if len(op.Records) > 5 {
					fmt.Printf("  ...and %d more\n", len(op.Records)-5)
				}
			}
			fmt.Printf("\n")

		case "UPDATE":
			fmt.Printf("[UPDATE Operations]\n")
			fmt.Printf("- Records to update: %d\n", len(op.Records))
			if len(op.Records) > 0 {
				fmt.Printf("  Sample records:\n")
				for i, record := range op.Records {
					if i < 5 {
						// Find the primary key value
						var pkValue string
						for k, v := range record {
							if !strings.HasPrefix(k, "old_") {
								pkValue = v
								break
							}
						}
						fmt.Printf("  %d. Record ID: %s\n", i+1, pkValue)

						// Display only changed values
						fmt.Printf("     Changes:\n")
						hasChanges := false
						if name, oldName := record["name"], record["old_name"]; name != oldName {
							fmt.Printf("     - name: %s -> %s\n", oldName, name)
							hasChanges = true
						}
						if price, oldPrice := record["price"], record["old_price"]; price != oldPrice {
							fmt.Printf("     - price: %s -> %s\n", oldPrice, price)
							hasChanges = true
						}
						if !hasChanges {
							fmt.Printf("     - No changes in displayed fields\n")
						}
					}
				}
				if len(op.Records) > 5 {
					fmt.Printf("  ...and %d more\n", len(op.Records)-5)
				}
			}
			fmt.Printf("\n")

		case "DELETE":
			fmt.Printf("[DELETE Operations]\n")
			fmt.Printf("- Records to delete: %d\n", len(op.Records))
			if len(op.Records) > 0 {
				fmt.Printf("  Sample records:\n")
				for i, record := range op.Records {
					if i < 5 {
						fmt.Printf("  %d. id: %s, ", i+1, record["id"])
						if name, ok := record["name"]; ok {
							fmt.Printf("name: %s, ", name)
						}
						if price, ok := record["price"]; ok {
							fmt.Printf("price: %s", price)
						}
						fmt.Printf("\n")
					}
				}
				if len(op.Records) > 5 {
					fmt.Printf("  ...and %d more\n", len(op.Records)-5)
				}
			}
			fmt.Printf("\n")
		}
	}

	fmt.Printf("SQL Statements:\n")
	for _, op := range preview.Operations {
		fmt.Printf("- %s: %s\n", op.OperationType, op.SQL)
	}
	fmt.Printf("=== End Preview ===\n")
}

// syncOverwrite performs complete overwrite synchronization
func syncOverwrite(ctx context.Context, tx *sql.Tx, config Config, fileRecords []DataRecord) error {
	// 1. Delete existing data (DELETE)
	// Note: Using DELETE instead of TRUNCATE to ensure transaction safety
	_, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s", config.Sync.TableName))
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
			diffCols := make(map[string]bool)
			for _, col := range config.Sync.Columns {
				// Check if file and DB values differ (consider type and NULL handling)
				if fileRecord[col] != dbRecord[col] {
					isDiff = true
					diffCols[col] = true
				}
			}
			if isDiff {
				// Content differs -> Update
				// Create a merged record containing both old and new values
				mergedRecord := make(DataRecord)
				for k, v := range fileRecord {
					if k == config.Sync.PrimaryKey || diffCols[k] {
						mergedRecord[k] = v // new value
						if oldVal, ok := dbRecord[k]; ok {
							mergedRecord["old_"+k] = oldVal // old value
						}
					}
				}
				toUpdate = append(toUpdate, mergedRecord)
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

	// Calculate total number of columns (source columns + timestamp columns)
	totalCols := len(config.Sync.Columns) + len(config.Sync.TimestampColumns)

	// Prepare placeholders
	valueStrings := make([]string, 0, len(records))
	valueArgs := make([]any, 0, len(records)*totalCols)
	placeholders := make([]string, totalCols)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	placeholderStr := fmt.Sprintf("(%s)", strings.Join(placeholders, ","))

	now := time.Now()
	for _, record := range records {
		valueStrings = append(valueStrings, placeholderStr)

		// Add values from source data
		for _, col := range config.Sync.Columns {
			valueArgs = append(valueArgs, record[col])
		}

		// Add current timestamp for all timestamp columns
		for range config.Sync.TimestampColumns {
			valueArgs = append(valueArgs, now)
		}
	}

	// Prepare column names by concatenating source and timestamp columns
	columns := slices.Clone(config.Sync.Columns)
	columns = append(columns, config.Sync.TimestampColumns...)

	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		config.Sync.TableName,
		strings.Join(columns, ","),
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
	// Prepare update columns
	updateCols := make([]string, 0, len(config.Sync.Columns)+len(config.Sync.TimestampColumns))

	// Add columns from source data
	for _, col := range config.Sync.Columns {
		// Don't include primary key or immutable columns in SET clause
		if col != config.Sync.PrimaryKey && !slices.Contains(config.Sync.ImmutableColumns, col) {
			updateCols = append(updateCols, fmt.Sprintf("%s = ?", col))
		}
	}

	// Add timestamp columns (except immutable ones)
	for _, tsCol := range config.Sync.TimestampColumns {
		if tsCol != config.Sync.PrimaryKey && !slices.Contains(config.Sync.Columns, tsCol) && !slices.Contains(config.Sync.ImmutableColumns, tsCol) {
			updateCols = append(updateCols, fmt.Sprintf("%s = ?", tsCol))
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
		now := time.Now()
		args := make([]any, 0, len(config.Sync.Columns)+len(config.Sync.TimestampColumns))

		// Add values from source data
		for _, col := range config.Sync.Columns {
			if col != config.Sync.PrimaryKey && !slices.Contains(config.Sync.ImmutableColumns, col) {
				args = append(args, record[col])
			}
		}

		// Add current timestamp for timestamp columns (except immutable ones)
		for _, tsCol := range config.Sync.TimestampColumns {
			if tsCol != config.Sync.PrimaryKey && !slices.Contains(config.Sync.Columns, tsCol) && !slices.Contains(config.Sync.ImmutableColumns, tsCol) {
				args = append(args, now)
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
