package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"
)

// UpdateOperation represents a single update operation with before and after states
type UpdateOperation struct {
	Before DataRecord
	After  DataRecord
}

// ExecutionPlan represents the planned operations for data synchronization
type ExecutionPlan struct {
	SyncMode         string
	TableName        string
	FileRecordCount  int
	DbRecordCount    int
	InsertOperations []DataRecord
	UpdateOperations []UpdateOperation
	DeleteOperations []DataRecord
	AffectedColumns  []string
	TimestampColumns []string
	ImmutableColumns []string // Added for dry-run display
}

// String returns a human-readable representation of the execution plan
func (p *ExecutionPlan) String() string {
	var buf bytes.Buffer
	now := time.Now()

	buf.WriteString("[DRY-RUN Mode] Execution Plan\n")
	buf.WriteString("----------------------------------------------------\n")
	buf.WriteString(fmt.Sprintf("Execution Summary:\n"))
	buf.WriteString(fmt.Sprintf("- Sync Mode: %s\n", p.SyncMode))
	buf.WriteString(fmt.Sprintf("- Target Table: %s\n", p.TableName))
	buf.WriteString(fmt.Sprintf("- Records in File: %d\n", p.FileRecordCount))
	buf.WriteString(fmt.Sprintf("- Records in Database: %d\n", p.DbRecordCount))
	buf.WriteString("\nPlanned Operations:\n")

	if len(p.DeleteOperations) > 0 {
		buf.WriteString(fmt.Sprintf("\n1. DELETE Operations (%d records)\n", len(p.DeleteOperations)))
		buf.WriteString("----------------------------------------------------\n")
		for i, record := range p.DeleteOperations {
			buf.WriteString(fmt.Sprintf("Record %d:\n", i+1))
			for col, val := range record {
				buf.WriteString(fmt.Sprintf("   %s: %v\n", col, val))
			}
			buf.WriteString("\n")
		}
	}

	if len(p.InsertOperations) > 0 {
		buf.WriteString(fmt.Sprintf("\n2. INSERT Operations (%d records)\n", len(p.InsertOperations)))
		buf.WriteString("----------------------------------------------------\n")
		buf.WriteString(fmt.Sprintf("Affected columns: %v\n\n", p.AffectedColumns))
		for i, record := range p.InsertOperations {
			buf.WriteString(fmt.Sprintf("Record %d:\n", i+1))
			for _, col := range p.AffectedColumns {
				buf.WriteString(fmt.Sprintf("   %s: %v\n", col, record[col]))
			}
			// Show timestamp values that will be set
			for _, tsCol := range p.TimestampColumns {
				buf.WriteString(fmt.Sprintf("   %s: %v (will be set)\n", tsCol, now.Format(time.RFC3339)))
			}
			buf.WriteString("\n")
		}
	}

	if len(p.UpdateOperations) > 0 {
		buf.WriteString(fmt.Sprintf("\n3. UPDATE Operations (%d records)\n", len(p.UpdateOperations)))
		buf.WriteString("----------------------------------------------------\n")

		// Filter out immutable columns from affected columns for UPDATE
		var updateableColumns []string
		for _, col := range p.AffectedColumns {
			isImmutable := false
			for _, immutableCol := range p.ImmutableColumns {
				if col == immutableCol {
					isImmutable = true
					break
				}
			}
			if !isImmutable {
				updateableColumns = append(updateableColumns, col)
			}
		}

		buf.WriteString(fmt.Sprintf("Affected columns: %v\n", updateableColumns))
		if len(p.ImmutableColumns) > 0 {
			buf.WriteString(fmt.Sprintf("Immutable columns (will not be updated): %v\n", p.ImmutableColumns))
		}
		buf.WriteString("\n")

		for i, update := range p.UpdateOperations {
			buf.WriteString(fmt.Sprintf("Record %d:\n", i+1))
			// Display values for updateable columns
			for _, col := range updateableColumns {
				oldVal := update.Before[col]
				newVal := update.After[col]
				if oldVal != newVal {
					buf.WriteString(fmt.Sprintf("   %s: %v -> %v\n", col, oldVal, newVal))
				} else {
					buf.WriteString(fmt.Sprintf("   %s: %v (unchanged)\n", col, oldVal))
				}
			}
			// Display immutable columns with a note
			for _, col := range p.ImmutableColumns {
				if val, exists := update.After[col]; exists {
					buf.WriteString(fmt.Sprintf("   %s: %v (immutable)\n", col, val))
				}
			}
			// Show timestamp values that will be set
			for _, tsCol := range p.TimestampColumns {
				// Check if the timestamp column is not immutable
				isImmutable := false
				for _, immutableCol := range p.ImmutableColumns {
					if tsCol == immutableCol {
						isImmutable = true
						break
					}
				}
				if !isImmutable {
					buf.WriteString(fmt.Sprintf("   %s: %v (will be set)\n", tsCol, now.Format(time.RFC3339)))
				}
			}
			buf.WriteString("\n")
		}
	}

	return buf.String()
}

// generateExecutionPlan creates an execution plan for the sync operation
func generateExecutionPlan(ctx context.Context, tx *sql.Tx, config Config, fileRecords []DataRecord) (*ExecutionPlan, error) {
	plan := &ExecutionPlan{
		SyncMode:         config.Sync.SyncMode,
		TableName:        config.Sync.TableName,
		FileRecordCount:  len(fileRecords),
		AffectedColumns:  config.Sync.Columns,
		TimestampColumns: config.Sync.TimestampColumns,
		ImmutableColumns: config.Sync.ImmutableColumns,
	}

	switch config.Sync.SyncMode {
	case "overwrite":
		// For overwrite mode, all records will be deleted and reinserted
		dbRecords, err := getCurrentDBData(ctx, tx, config)
		if err != nil {
			return nil, fmt.Errorf("error getting current DB data: %w", err)
		}
		plan.DbRecordCount = len(dbRecords)
		plan.DeleteOperations = make([]DataRecord, 0, len(dbRecords))
		for _, record := range dbRecords {
			plan.DeleteOperations = append(plan.DeleteOperations, record)
		}
		plan.InsertOperations = fileRecords

	case "diff":
		// For diff mode, calculate the actual differences
		dbRecords, err := getCurrentDBData(ctx, tx, config)
		if err != nil {
			return nil, fmt.Errorf("error getting current DB data: %w", err)
		}
		plan.DbRecordCount = len(dbRecords)

		// Use existing diffData function to calculate differences
		toInsert, toUpdate, toDelete := diffData(config, fileRecords, dbRecords)
		plan.InsertOperations = toInsert
		plan.UpdateOperations = toUpdate
		if config.Sync.DeleteNotInFile {
			plan.DeleteOperations = toDelete
		}

	default:
		return nil, fmt.Errorf("unknown sync mode: %s", config.Sync.SyncMode)
	}

	return plan, nil
}

// syncData synchronizes data between file and database
func syncData(ctx context.Context, db *sql.DB, config Config, fileRecords []DataRecord) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("transaction start error: %w", err)
	}
	defer tx.Rollback()

	// For dry-run mode, generate and display execution plan
	if config.DryRun {
		plan, err := generateExecutionPlan(ctx, tx, config, fileRecords)
		if err != nil {
			return fmt.Errorf("error generating execution plan: %w", err)
		}
		log.Print(plan.String())
		return nil
	}

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
		// Convert UpdateOperation to DataRecord for bulkUpdate
		updateRecords := make([]DataRecord, len(toUpdate))
		for i, update := range toUpdate {
			updateRecords[i] = update.After
		}
		// Execute individual UPDATEs or try Bulk Update
		err = bulkUpdate(ctx, tx, config, updateRecords)
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
) (toInsert []DataRecord, toUpdate []UpdateOperation, toDelete []DataRecord) {

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
				toUpdate = append(toUpdate, UpdateOperation{
					Before: dbRecord,
					After:  fileRecord,
				})
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
