package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	// "github.com/pkg/errors" // Removed as per user feedback
)

// PrimaryKey represents a type-preserving primary key structure that maintains
// both the original typed value and its string representation for safe comparison
// and SQL operations.
//
// Example usage:
//
//	pk1 := NewPrimaryKey(123)
//	pk2 := NewPrimaryKey("123")
//	if pk1.Equal(pk2) {
//	    // Handle equal primary keys
//	}
type PrimaryKey struct {
	Value any    // Original typed value from the data source
	Str   string // String representation for display and SQL operations
}

// NewPrimaryKey creates a new PrimaryKey with type preservation and validation
func NewPrimaryKey(value any) PrimaryKey {
	if value == nil {
		return PrimaryKey{Value: nil, Str: ""}
	}
	return PrimaryKey{
		Value: value,
		Str:   convertValueToString(value),
	}
}

// Equal compares two PrimaryKey values for equality using type-safe comparison
func (pk PrimaryKey) Equal(other PrimaryKey) bool {
	// Type-safe comparison using reflection for complex types
	return reflect.DeepEqual(pk.Value, other.Value) || pk.Str == other.Str
}

// String returns the string representation of the PrimaryKey
func (pk PrimaryKey) String() string {
	return pk.Str
}

// convertValueToString converts any value to string for comparison
// Uses fast type assertions for common types, fallback to reflection for others
func convertValueToString(val any) string {
	if val == nil {
		return ""
	}

	// Fast path: Use type assertions for common types
	switch v := val.(type) {
	case string:
		return v
	case bool:
		return strconv.FormatBool(v)
	case int:
		return strconv.Itoa(v)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float32:
		f := float64(v)
		if f == float64(int64(f)) {
			return strconv.FormatInt(int64(f), 10)
		}
		return strconv.FormatFloat(f, 'g', -1, 32)
	case float64:
		if v == float64(int64(v)) {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'g', -1, 64)
	case time.Time:
		return v.Format(time.RFC3339)
	}

	// Slow path: Use reflection for other types
	rv := reflect.ValueOf(val)
	switch rv.Kind() {
	case reflect.String:
		return val.(string)
	case reflect.Bool:
		return strconv.FormatBool(val.(bool))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		f := rv.Float()
		if f == float64(int64(f)) {
			return strconv.FormatInt(int64(f), 10)
		}
		return strconv.FormatFloat(f, 'g', -1, 64)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// UpdateOperation represents a single update operation with before and after states
type UpdateOperation struct {
	Before DataRecord
	After  DataRecord
}

// DiffOperations represents the operations to be performed during differential synchronization
type DiffOperations struct {
	ToInsert []DataRecord
	ToUpdate []UpdateOperation
	ToDelete []DataRecord
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
	AffectedColumns  []string // These will be the columns actually present in both CSV header and DB
	TimestampColumns []string
	ImmutableColumns []string
	PrimaryKey       string // Added to know which column is PK for display
}

// String returns a human-readable representation of the execution plan
func (p *ExecutionPlan) String() string {
	var buf bytes.Buffer
	now := time.Now()

	buf.WriteString("[DRY-RUN Mode] Execution Plan\n")
	buf.WriteString("----------------------------------------------------\n")
	buf.WriteString("Execution Summary:\n")
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

// getTableColumns retrieves the column names of a given table
func getTableColumns(ctx context.Context, tx *sql.Tx, tableName string) ([]string, error) {
	// Query to get column names, specific to MySQL. For other DBs, this might need adjustment.
	// Using `information_schema.columns` is a standard way.
	// Use parameterized query to prevent SQL injection
	query := "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
	rows, err := tx.QueryContext(ctx, query, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns for table %s: %w", tableName, err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("failed to scan column name for table %s: %w", tableName, err)
		}
		columns = append(columns, columnName)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows for table %s columns: %w", tableName, err)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns found for table %s or table does not exist", tableName)
	}
	return columns, nil
}

// findCommonColumns returns the intersection of CSV headers and DB table columns
func findCommonColumns(csvHeaders []string, dbTableColumns []string) []string {
	var commonColumns []string
	for _, csvHeader := range csvHeaders {
		if slices.Contains(dbTableColumns, csvHeader) {
			if !slices.Contains(commonColumns, csvHeader) {
				commonColumns = append(commonColumns, csvHeader)
			}
		}
	}
	return commonColumns
}

// filterColumnsByConfig filters common columns using the config.Sync.Columns specification
func filterColumnsByConfig(commonColumns []string, configSyncColumns []string) []string {
	if len(configSyncColumns) == 0 {
		return commonColumns
	}

	var filteredColumns []string
	for _, col := range commonColumns {
		if slices.Contains(configSyncColumns, col) {
			filteredColumns = append(filteredColumns, col)
		}
	}
	return filteredColumns
}

// validatePrimaryKeyInColumns ensures the primary key is included in the sync columns
func validatePrimaryKeyInColumns(actualSyncColumns []string, pkName string) error {
	if pkName != "" && !slices.Contains(actualSyncColumns, pkName) {
		return fmt.Errorf("configured primary key '%s' is not among the final actual sync columns: %v. It must be present in CSV header, DB table, and config.Sync.Columns (if specified)", pkName, actualSyncColumns)
	}
	return nil
}

// determineActualSyncColumns determines the columns to be synced.
// If configSyncColumns (config.Sync.Columns) is provided, it acts as a filter:
// actual columns will be the intersection of csvHeaders, dbTableColumns, and configSyncColumns.
// If configSyncColumns is empty, actual columns will be the intersection of csvHeaders and dbTableColumns.
func determineActualSyncColumns(csvHeaders []string, dbTableColumns []string, configSyncColumns []string, pkName string) ([]string, error) {
	if len(csvHeaders) == 0 {
		return nil, fmt.Errorf("CSV header is empty, cannot determine sync columns")
	}

	// Step 1: Find intersection of CSV headers and DB table columns
	commonColumns := findCommonColumns(csvHeaders, dbTableColumns)
	if len(commonColumns) == 0 {
		return nil, fmt.Errorf("no matching columns found between CSV header (%v) and DB table columns (%v)", csvHeaders, dbTableColumns)
	}

	// Step 2: Apply config.Sync.Columns filter if specified
	actualSyncColumns := filterColumnsByConfig(commonColumns, configSyncColumns)
	if len(actualSyncColumns) == 0 {
		return nil, fmt.Errorf("no matching columns after filtering with config.Sync.Columns (%v). Intersection of CSV headers (%v) and DB columns (%v) was (%v), but none of these were in config.Sync.Columns", configSyncColumns, csvHeaders, dbTableColumns, commonColumns)
	}

	// Step 3: Validate primary key presence
	if err := validatePrimaryKeyInColumns(actualSyncColumns, pkName); err != nil {
		return nil, err
	}

	return actualSyncColumns, nil
}

// generateExecutionPlan creates an execution plan for the sync operation
func generateExecutionPlan(ctx context.Context, tx *sql.Tx, config Config, fileRecords []DataRecord, actualSyncCols []string) (*ExecutionPlan, error) {
	plan := &ExecutionPlan{
		SyncMode:         config.Sync.SyncMode,
		TableName:        config.Sync.TableName,
		FileRecordCount:  len(fileRecords),
		AffectedColumns:  actualSyncCols, // Use actual sync columns
		TimestampColumns: config.Sync.TimestampColumns,
		ImmutableColumns: config.Sync.ImmutableColumns,
		PrimaryKey:       config.Sync.PrimaryKey,
	}

	switch config.Sync.SyncMode {
	case "overwrite":
		// For overwrite mode, all records will be deleted and reinserted
		dbRecords, _, err := getCurrentDBData(ctx, tx, config, actualSyncCols) // Pass actualSyncCols
		if err != nil {
			return nil, fmt.Errorf("error getting current DB data for overwrite plan: %w", err)
		}
		plan.DbRecordCount = len(dbRecords)
		plan.DeleteOperations = make([]DataRecord, 0, len(dbRecords))
		for _, record := range dbRecords {
			plan.DeleteOperations = append(plan.DeleteOperations, record)
		}
		plan.InsertOperations = fileRecords

	case "diff":
		// For diff mode, calculate the actual differences
		if config.Sync.PrimaryKey == "" {
			return nil, fmt.Errorf("primary key is required for diff sync mode but is not configured")
		}
		if !slices.Contains(actualSyncCols, config.Sync.PrimaryKey) {
			return nil, fmt.Errorf("primary key '%s' (from config) is not present in the actual columns to be synced (%v) based on CSV headers and DB schema. Diff mode cannot proceed", config.Sync.PrimaryKey, actualSyncCols)
		}

		dbRecords, _, err := getCurrentDBData(ctx, tx, config, actualSyncCols) // Pass actualSyncCols
		if err != nil {
			return nil, fmt.Errorf("error getting current DB data for diff plan: %w", err)
		}
		plan.DbRecordCount = len(dbRecords)

		// Use existing diffData function to calculate differences
		toInsert, toUpdate, toDelete := diffData(config, fileRecords, dbRecords, actualSyncCols) // Pass actualSyncCols
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
//
// TRANSACTION BOUNDARY: Single-table synchronization uses one dedicated transaction per table.
// Transaction scope: Load data → Sync operations → Commit/Rollback
// If any operation fails, only this table's changes are rolled back.
func syncData(ctx context.Context, db *sql.DB, config Config, fileRecords []DataRecord) error {
	// Early return only for diff mode without deleteNotInFile
	if len(fileRecords) == 0 {
		if config.Sync.SyncMode == "diff" && !config.Sync.DeleteNotInFile {
			log.Println("No records loaded from file. Nothing to sync.")
			return nil
		}
		// Log the intention for overwrite or diff+deleteNotInFile modes
		if config.Sync.SyncMode == "overwrite" {
			log.Println("File is empty. In overwrite mode, all existing data will be deleted.")
		} else if config.Sync.SyncMode == "diff" && config.Sync.DeleteNotInFile {
			log.Println("File is empty. In diff mode with deleteNotInFile, all existing data may be deleted.")
		}
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("transaction start error: %w", err)
	}
	defer tx.Rollback() // Rollback on error or if commit fails

	// Determine actual columns to sync
	var actualSyncColumns []string
	if len(fileRecords) > 0 {
		// Normal case: get headers from file records
		fileHeaders := make([]string, 0, len(fileRecords[0]))
		for k := range fileRecords[0] {
			fileHeaders = append(fileHeaders, k)
		}
		slices.Sort(fileHeaders) // Ensure consistent order

		dbTableCols, err := getTableColumns(ctx, tx, config.Sync.TableName)
		if err != nil {
			return fmt.Errorf("failed to get database table columns: %w", err)
		}

		actualSyncColumns, err = determineActualSyncColumns(fileHeaders, dbTableCols, config.Sync.Columns, config.Sync.PrimaryKey)
		if err != nil {
			return fmt.Errorf("failed to determine actual columns for synchronization: %w", err)
		}
	} else {
		// Empty file case: use all DB columns for overwrite or diff+deleteNotInFile
		dbTableCols, err := getTableColumns(ctx, tx, config.Sync.TableName)
		if err != nil {
			return fmt.Errorf("failed to get database table columns: %w", err)
		}
		actualSyncColumns = dbTableCols

		// Primary key validation for diff mode
		if config.Sync.SyncMode == "diff" && config.Sync.PrimaryKey == "" {
			return fmt.Errorf("primary key must be configured for diff mode with deleteNotInFile when file is empty")
		}
	}

	log.Printf("Actual columns to be synced: %v", actualSyncColumns)

	// For dry-run mode, generate and display execution plan
	if config.DryRun {
		plan, err := generateExecutionPlan(ctx, tx, config, fileRecords, actualSyncColumns) // Pass actualSyncCols
		if err != nil {
			return fmt.Errorf("error generating execution plan: %w", err)
		}
		log.Print(plan.String())
		return nil // Dry run ends here
	}

	switch config.Sync.SyncMode {
	case "overwrite":
		err = syncOverwrite(ctx, tx, config, fileRecords, actualSyncColumns) // Pass actualSyncCols
	case "diff":
		err = syncDiff(ctx, tx, config, fileRecords, actualSyncColumns) // Pass actualSyncCols
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
func syncOverwrite(ctx context.Context, tx *sql.Tx, config Config, fileRecords []DataRecord, actualSyncCols []string) error {
	// 1. Delete existing data (DELETE)
	_, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s", config.Sync.TableName))
	if err != nil {
		return fmt.Errorf("error deleting data from table '%s': %w", config.Sync.TableName, err)
	}
	log.Printf("Deleted existing data from table '%s'.", config.Sync.TableName)

	// 2. Insert all file data
	if len(fileRecords) == 0 {
		log.Println("No data to insert from file (file was empty or only header).")
		return nil
	}
	if len(actualSyncCols) == 0 {
		return fmt.Errorf("no columns determined for sync, cannot insert data")
	}

	err = bulkInsert(ctx, tx, config, fileRecords, actualSyncCols)
	if err != nil {
		return fmt.Errorf("data insertion error: %w", err)
	}
	log.Printf("Inserted %d records using columns: %v.", len(fileRecords), actualSyncCols)

	return nil
}

// validateDiffSyncRequirements validates the requirements for differential synchronization
func validateDiffSyncRequirements(config Config, actualSyncCols []string) error {
	if config.Sync.PrimaryKey == "" {
		return fmt.Errorf("primary key is required for diff sync mode")
	}
	if !slices.Contains(actualSyncCols, config.Sync.PrimaryKey) {
		return fmt.Errorf("primary key '%s' is not among the actual sync columns '%v', diff cannot proceed", config.Sync.PrimaryKey, actualSyncCols)
	}
	return nil
}

// executeSyncOperations executes the planned sync operations
func executeSyncOperations(ctx context.Context, tx *sql.Tx, config Config, operations DiffOperations, actualSyncCols []string) error {
	// INSERT processing
	if len(operations.ToInsert) > 0 {
		err := bulkInsert(ctx, tx, config, operations.ToInsert, actualSyncCols)
		if err != nil {
			return fmt.Errorf("INSERT error: %w", err)
		}
		log.Printf("Inserted %d records.", len(operations.ToInsert))
	}

	// UPDATE processing
	if len(operations.ToUpdate) > 0 {
		updateRecords := make([]DataRecord, len(operations.ToUpdate))
		for i, op := range operations.ToUpdate {
			updateRecords[i] = op.After
		}
		err := bulkUpdate(ctx, tx, config, updateRecords, actualSyncCols)
		if err != nil {
			return fmt.Errorf("UPDATE error: %w", err)
		}
		log.Printf("Updated %d records.", len(operations.ToUpdate))
	}

	// DELETE processing
	if len(operations.ToDelete) > 0 && config.Sync.DeleteNotInFile {
		err := bulkDelete(ctx, tx, config, operations.ToDelete)
		if err != nil {
			return fmt.Errorf("DELETE error: %w", err)
		}
		log.Printf("Deleted %d records.", len(operations.ToDelete))
	}

	return nil
}

// syncDiff performs differential synchronization
func syncDiff(ctx context.Context, tx *sql.Tx, config Config, fileRecords []DataRecord, actualSyncCols []string) error {
	// Validate requirements for differential sync
	if err := validateDiffSyncRequirements(config, actualSyncCols); err != nil {
		return err
	}

	// Get current data from DB
	dbRecords, _, err := getCurrentDBData(ctx, tx, config, actualSyncCols)
	if err != nil {
		return fmt.Errorf("DB data retrieval error: %w", err)
	}

	// Compare file data with DB data
	toInsert, toUpdate, toDelete := diffData(config, fileRecords, dbRecords, actualSyncCols)
	operations := DiffOperations{
		ToInsert: toInsert,
		ToUpdate: toUpdate,
		ToDelete: toDelete,
	}
	log.Printf("Difference detection result: Insert %d, Update %d, Delete %d",
		len(operations.ToInsert), len(operations.ToUpdate), len(operations.ToDelete))

	// Execute the planned operations
	return executeSyncOperations(ctx, tx, config, operations, actualSyncCols)
}

// getCurrentDBData retrieves current data from database (for differential sync)
// It now uses actualSyncCols to determine which columns to SELECT.
// The Primary Key must be part of actualSyncCols for diff to work.
func getCurrentDBData(ctx context.Context, tx *sql.Tx, config Config, actualSyncCols []string) (map[string]DataRecord, map[string]PrimaryKey, error) {
	if len(actualSyncCols) == 0 {
		return nil, nil, fmt.Errorf("no columns specified to fetch from database")
	}

	selectCols := slices.Clone(actualSyncCols)
	// Ensure PK is in selectCols if it's configured and not already present.
	// This is vital for mapping records.
	if config.Sync.PrimaryKey != "" && !slices.Contains(selectCols, config.Sync.PrimaryKey) {
		// This situation implies that the PK configured in yml is not in the CSV header
		// or not in the DB table, which should have been caught by determineActualSyncColumns.
		// If determineActualSyncColumns ensures PK is present if it's a valid sync col, this append might be redundant
		// or indicate a logic gap. For safety, we ensure it's selected if configured.
		// However, if PK is not in actualSyncCols, it means it wasn't a common column.
		// This function should only select columns that are in actualSyncCols.
		// The check for PK presence in actualSyncCols should be done *before* calling this.
		// So, if PK is not in actualSyncCols here, it's a problem.
		return nil, nil, fmt.Errorf("primary key '%s' is configured but not in actual sync columns %v; cannot fetch DB data correctly for diff", config.Sync.PrimaryKey, actualSyncCols)
	}

	query := fmt.Sprintf("SELECT %s FROM %s",
		strings.Join(selectCols, ","), // Use selectCols which is a clone of actualSyncCols
		config.Sync.TableName)

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, fmt.Errorf("query execution error (%s): %w", query, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, fmt.Errorf("column name retrieval error: %w", err)
	}

	dbData := make(map[string]DataRecord) // Map with primary key string as key
	pkMap := make(map[string]PrimaryKey)  // Map to store PrimaryKey objects
	vals := make([]any, len(cols))
	scanArgs := make([]any, len(cols))
	for i := range vals {
		scanArgs[i] = &vals[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, nil, fmt.Errorf("row data scan error: %w", err)
		}
		record := make(DataRecord)
		var pkValue any
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
				// For PrimaryKey, use the string representation to ensure consistency
				pkValue = strVal
			}
		}
		if pkValue == nil {
			// Skip or error if primary key can't be obtained
			log.Printf("Warning: Found record with nil primary key value. Skipping. Record: %v", record)
			continue
		}
		pk := NewPrimaryKey(pkValue)
		if pk.Str == "" {
			log.Printf("Warning: Found record with empty primary key after string conversion. Skipping. Record: %v", record)
			continue
		}
		dbData[pk.Str] = record
		pkMap[pk.Str] = pk
	}
	if err = rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("row processing error: %w", err)
	}

	return dbData, pkMap, nil
}

// extractPrimaryKeyValue extracts and validates primary key value from a record
func extractPrimaryKeyValue(record DataRecord, primaryKey string) (PrimaryKey, bool) {
	pkValue, pkExists := record[primaryKey]
	if !pkExists || pkValue == nil {
		return PrimaryKey{}, false
	}
	pk := NewPrimaryKey(pkValue)
	if pk.Str == "" {
		return PrimaryKey{}, false
	}
	return pk, true
}

// compareRecords compares two records and returns true if they differ
func compareRecords(fileRecord, dbRecord DataRecord, actualSyncCols []string, primaryKey string) bool {
	for _, col := range actualSyncCols {
		if col == primaryKey {
			continue // Don't compare PK value itself for diff content
		}
		fileVal, fileColExists := fileRecord[col]
		dbVal, dbColExists := dbRecord[col]

		if !fileColExists && dbColExists {
			return true
		} else if fileColExists && !dbColExists {
			return true
		} else if fileColExists && dbColExists {
			fileStr := convertValueToString(fileVal)
			if fileStr != dbVal {
				return true
			}
		}
	}
	return false
}

// processFileRecords processes file records and determines insert/update operations
func processFileRecords(fileRecords []DataRecord, dbRecords map[string]DataRecord, config Config, actualSyncCols []string) ([]DataRecord, []UpdateOperation, map[string]bool) {
	var toInsert []DataRecord
	var toUpdate []UpdateOperation
	fileKeys := make(map[string]bool)

	for _, fileRecord := range fileRecords {
		pk, isValid := extractPrimaryKeyValue(fileRecord, config.Sync.PrimaryKey)
		if !isValid {
			log.Printf("Warning: Record in file is missing primary key '%s' value or key itself. Skipping: %v", config.Sync.PrimaryKey, fileRecord)
			continue
		}
		fileKeys[pk.Str] = true

		dbRecord, existsInDB := dbRecords[pk.Str]
		if !existsInDB {
			toInsert = append(toInsert, fileRecord)
		} else if compareRecords(fileRecord, dbRecord, actualSyncCols, config.Sync.PrimaryKey) {
			toUpdate = append(toUpdate, UpdateOperation{
				Before: dbRecord,
				After:  fileRecord,
			})
		}
	}

	return toInsert, toUpdate, fileKeys
}

// findRecordsToDelete identifies records that need to be deleted
func findRecordsToDelete(dbRecords map[string]DataRecord, fileKeys map[string]bool, deleteNotInFile bool) []DataRecord {
	if !deleteNotInFile {
		return nil
	}

	var toDelete []DataRecord
	for pkStr, dbRecord := range dbRecords {
		if !fileKeys[pkStr] {
			toDelete = append(toDelete, dbRecord)
		}
	}
	return toDelete
}

// diffData compares file data with DB data (for differential sync)
// It now uses actualSyncCols to determine which columns to compare.
func diffData(
	config Config,
	fileRecords []DataRecord,
	dbRecords map[string]DataRecord,
	actualSyncCols []string,
) (toInsert []DataRecord, toUpdate []UpdateOperation, toDelete []DataRecord) {

	if config.Sync.PrimaryKey == "" {
		log.Println("Error: Primary key not configured, cannot perform diff.") // Should be caught earlier
		return
	}

	// Process file records to determine insert/update operations
	toInsert, toUpdate, fileKeys := processFileRecords(fileRecords, dbRecords, config, actualSyncCols)

	// Identify records to delete
	toDelete = findRecordsToDelete(dbRecords, fileKeys, config.Sync.DeleteNotInFile)

	return
}

// bulkInsert performs bulk insertion of records using actualSyncCols
func bulkInsert(ctx context.Context, tx *sql.Tx, config Config, records []DataRecord, actualSyncCols []string) error {
	if len(records) == 0 {
		return nil
	}
	if len(actualSyncCols) == 0 {
		return fmt.Errorf("no columns specified for insert")
	}

	// Columns for INSERT statement are actualSyncCols + timestamp columns not already in actualSyncCols
	insertStatementCols := slices.Clone(actualSyncCols)
	activeTimestampCols := []string{} // Timestamp columns that are not part of actualSyncCols but need to be set
	for _, tsCol := range config.Sync.TimestampColumns {
		if !slices.Contains(insertStatementCols, tsCol) {
			insertStatementCols = append(insertStatementCols, tsCol)
			activeTimestampCols = append(activeTimestampCols, tsCol)
		}
	}

	valueStrings := make([]string, 0, len(records))
	valueArgs := make([]any, 0, len(records)*len(insertStatementCols))
	placeholders := make([]string, len(insertStatementCols))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	placeholderStr := fmt.Sprintf("(%s)", strings.Join(placeholders, ","))

	now := time.Now()
	for _, record := range records {
		valueStrings = append(valueStrings, placeholderStr)
		for _, col := range actualSyncCols { // Iterate actualSyncCols for record values
			valueArgs = append(valueArgs, record[col])
		}
		for range activeTimestampCols { // Add values for the additionally active timestamp columns
			valueArgs = append(valueArgs, now)
		}
	}

	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		config.Sync.TableName,
		strings.Join(insertStatementCols, ","),
		strings.Join(valueStrings, ","))

	_, err := tx.ExecContext(ctx, stmt, valueArgs...)
	return err
}

// bulkUpdate performs updates for multiple records using actualSyncCols
func bulkUpdate(ctx context.Context, tx *sql.Tx, config Config, records []DataRecord, actualSyncCols []string) error {
	if len(records) == 0 {
		return nil
	}
	if config.Sync.PrimaryKey == "" {
		return fmt.Errorf("primary key not specified for update")
	}

	setClauses := []string{}
	// Determine columns to include in SET clause
	// These are actualSyncCols excluding PK and immutable columns
	updatableRecordCols := []string{} // Columns from record to use in SET
	for _, col := range actualSyncCols {
		if col != config.Sync.PrimaryKey && !slices.Contains(config.Sync.ImmutableColumns, col) {
			setClauses = append(setClauses, fmt.Sprintf("%s = ?", col))
			updatableRecordCols = append(updatableRecordCols, col)
		}
	}

	activeTimestampSetCols := []string{} // Timestamp columns to SET that are not in actualSyncCols
	for _, tsCol := range config.Sync.TimestampColumns {
		// Add to SET if it's a timestamp column, not immutable, and not already handled via actualSyncCols
		if !slices.Contains(config.Sync.ImmutableColumns, tsCol) && !slices.Contains(actualSyncCols, tsCol) {
			setClauses = append(setClauses, fmt.Sprintf("%s = ?", tsCol))
			activeTimestampSetCols = append(activeTimestampSetCols, tsCol)
		}
	}

	if len(setClauses) == 0 {
		log.Println("No columns to update after excluding primary key and immutable columns.")
		return nil
	}

	stmtSQL := fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?",
		config.Sync.TableName,
		strings.Join(setClauses, ", "),
		config.Sync.PrimaryKey)

	stmt, err := tx.PrepareContext(ctx, stmtSQL)
	if err != nil {
		return fmt.Errorf("UPDATE preparation error: %w", err)
	}
	defer stmt.Close()

	now := time.Now()
	for _, record := range records {
		args := make([]any, 0, len(updatableRecordCols)+len(activeTimestampSetCols)+1)
		for _, col := range updatableRecordCols {
			args = append(args, record[col])
		}
		for range activeTimestampSetCols {
			args = append(args, now)
		}
		args = append(args, record[config.Sync.PrimaryKey]) // PK for WHERE

		_, err = stmt.ExecContext(ctx, args...)
		if err != nil {
			return fmt.Errorf("UPDATE execution error (PK: %s): %w", record[config.Sync.PrimaryKey], err)
		}
	}
	return nil
}

// bulkDelete performs deletion of multiple records
// This function does not need actualSyncCols as it only uses the Primary Key.
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

// syncMultipleTablesData synchronizes data for multiple tables with dependency order
//
// TRANSACTION BOUNDARY: This function implements a single global transaction for all tables.
// All tables are synchronized within one shared transaction to ensure ACID properties
// and maintain referential integrity across related tables with foreign key relationships.
//
// Transaction Flow:
//  1. Load all data (outside transaction to minimize lock time)
//  2. Calculate dependency order (outside transaction)
//  3. Begin single transaction for all sync operations
//  4. Execute all table syncs within the transaction
//  5. Commit or rollback based on success/failure
//
// Error Handling: If ANY table sync fails, the ENTIRE multi-table operation is rolled back.
// This all-or-nothing approach prevents partial sync states that could leave the database
// in an inconsistent condition.
func syncMultipleTablesData(ctx context.Context, db *sql.DB, config Config) error {
	if !IsMultiTableConfig(config) {
		return fmt.Errorf("config does not contain multi-table configuration")
	}

	// 1. Load data from all files (OUTSIDE TRANSACTION)
	// Note: For very large datasets, consider implementing streaming/batching to reduce memory usage
	multiLoader := NewMultiTableLoader(config.Tables)
	if err := multiLoader.ValidateFilePaths(); err != nil {
		return fmt.Errorf("multi-table file validation error: %w", err)
	}

	allData, err := multiLoader.LoadAll()
	if err != nil {
		return fmt.Errorf("multi-table data loading error: %w", err)
	}

	log.Printf("Loaded data from %d table files", len(allData))
	for tableName, records := range allData {
		log.Printf("Table '%s': %d records", tableName, len(records))
	}

	// 2. Determine synchronization order based on dependencies (OUTSIDE TRANSACTION)
	insertOrder, deleteOrder, err := GetSyncOrder(config.Tables)
	if err != nil {
		return fmt.Errorf("dependency order calculation error: %w", err)
	}

	log.Printf("Synchronization order - Insert: %v, Delete: %v", insertOrder, deleteOrder)

	// 3. Start SINGLE GLOBAL TRANSACTION for all table synchronizations
	// This ensures all-or-nothing semantics across all related tables
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("transaction start error: %w", err)
	}
	// Automatic rollback on ANY error via defer - ensures cleanup if commit fails or panic occurs
	defer tx.Rollback()

	// 4. For dry-run mode, generate and display execution plan
	if config.DryRun {
		err = generateMultiTableExecutionPlan(ctx, db, tx, config, allData, insertOrder, deleteOrder)
		if err != nil {
			return fmt.Errorf("error generating multi-table execution plan: %w", err)
		}
		return nil // Dry run ends here
	}

	// 5. Execute synchronization in dependency order
	err = executeMultiTableSync(ctx, tx, config, allData, insertOrder, deleteOrder)
	if err != nil {
		return fmt.Errorf("multi-table sync execution error: %w", err)
	}

	// 6. Commit transaction - only if ALL table syncs succeeded
	// If commit fails, defer tx.Rollback() will handle cleanup
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("transaction commit error: %w", err)
	}

	return nil
}

// generateMultiTableExecutionPlan creates and displays execution plan for multiple tables
func generateMultiTableExecutionPlan(ctx context.Context, db *sql.DB, tx *sql.Tx, config Config, allData MultiTableData, insertOrder []string, deleteOrder []string) error {
	log.Println("[DRY-RUN Mode] Multi-Table Execution Plan")
	log.Println("====================================================")

	// Show sync order
	log.Printf("Insert/Update Order (parent→child): %v", insertOrder)
	log.Printf("Delete Order (child→parent): %v", deleteOrder)
	log.Println()

	// Generate individual execution plans for each table
	for i, tableName := range insertOrder {
		log.Printf("[%d] Table: %s", i+1, tableName)
		log.Println("----------------------------------------------------")

		tableConfig, err := GetTableConfig(config.Tables, tableName)
		if err != nil {
			return fmt.Errorf("table config not found for '%s': %w", tableName, err)
		}

		// Create single-table config for compatibility with existing syncData function
		singleConfig := Config{
			DB:     config.DB,
			DryRun: true,
			Sync: SyncConfig{
				FilePath:         tableConfig.FilePath,
				TableName:        tableConfig.Name,
				Columns:          tableConfig.Columns,
				TimestampColumns: tableConfig.TimestampColumns,
				ImmutableColumns: tableConfig.ImmutableColumns,
				PrimaryKey:       tableConfig.PrimaryKey,
				SyncMode:         tableConfig.SyncMode,
				DeleteNotInFile:  tableConfig.DeleteNotInFile,
			},
		}

		// Get table data
		tableData, exists := allData[tableName]
		if !exists {
			log.Printf("No data loaded for table '%s'\n", tableName)
			continue
		}

		// Use existing syncData function for planning (in dry-run mode)
		err = syncData(ctx, db, singleConfig, tableData)
		if err != nil {
			return fmt.Errorf("execution plan generation error for table '%s': %w", tableName, err)
		}
		log.Println()
	}

	return nil
}

// executeMultiTableSync executes synchronization for multiple tables in dependency order
func executeMultiTableSync(ctx context.Context, tx *sql.Tx, config Config, allData MultiTableData, insertOrder []string, deleteOrder []string) error {
	// Phase 1: Delete operations in reverse dependency order (child→parent)
	for _, tableName := range deleteOrder {
		tableConfig, err := GetTableConfig(config.Tables, tableName)
		if err != nil {
			return fmt.Errorf("table config not found for '%s': %w", tableName, err)
		}

		// Skip deletion if deleteNotInFile is false
		if !tableConfig.DeleteNotInFile {
			continue
		}

		log.Printf("Processing deletes for table '%s'", tableName)
		err = executeSingleTableSync(ctx, tx, config, tableName, allData[tableName], "delete")
		if err != nil {
			return fmt.Errorf("delete phase error for table '%s': %w", tableName, err)
		}
	}

	// Phase 2: Insert/Update operations in dependency order (parent→child)
	for _, tableName := range insertOrder {
		log.Printf("Processing inserts/updates for table '%s'", tableName)
		err := executeSingleTableSync(ctx, tx, config, tableName, allData[tableName], "insert_update")
		if err != nil {
			return fmt.Errorf("insert/update phase error for table '%s': %w", tableName, err)
		}
	}

	return nil
}

// executeSingleTableSync executes synchronization for a single table within the transaction
func executeSingleTableSync(ctx context.Context, tx *sql.Tx, config Config, tableName string, tableData []DataRecord, phase string) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}
	if tableName == "" {
		return fmt.Errorf("table name is empty")
	}

	tableConfig, err := GetTableConfig(config.Tables, tableName)
	if err != nil {
		return fmt.Errorf("table config not found for '%s': %w", tableName, err)
	}

	// Create single-table config for compatibility with existing functions
	singleConfig := Config{
		DB:     config.DB,
		DryRun: false, // We're in execution mode
		Sync: SyncConfig{
			FilePath:         tableConfig.FilePath,
			TableName:        tableConfig.Name,
			Columns:          tableConfig.Columns,
			TimestampColumns: tableConfig.TimestampColumns,
			ImmutableColumns: tableConfig.ImmutableColumns,
			PrimaryKey:       tableConfig.PrimaryKey,
			SyncMode:         tableConfig.SyncMode,
			DeleteNotInFile:  tableConfig.DeleteNotInFile,
		},
	}

	// Determine actual columns to sync
	var actualSyncColumns []string
	if len(tableData) > 0 {
		// Normal case: get headers from file records
		fileHeaders := make([]string, 0, len(tableData[0]))
		for k := range tableData[0] {
			fileHeaders = append(fileHeaders, k)
		}
		slices.Sort(fileHeaders) // Ensure consistent order

		dbTableCols, err := getTableColumns(ctx, tx, singleConfig.Sync.TableName)
		if err != nil {
			return fmt.Errorf("failed to get database table columns for '%s': %w", tableName, err)
		}

		actualSyncColumns, err = determineActualSyncColumns(fileHeaders, dbTableCols, singleConfig.Sync.Columns, singleConfig.Sync.PrimaryKey)
		if err != nil {
			return fmt.Errorf("failed to determine actual columns for table '%s': %w", tableName, err)
		}
	} else {
		// Empty data case: use DB columns
		dbTableCols, err := getTableColumns(ctx, tx, singleConfig.Sync.TableName)
		if err != nil {
			return fmt.Errorf("failed to get database table columns for '%s': %w", tableName, err)
		}
		actualSyncColumns = dbTableCols
	}

	// Execute phase-specific operations
	switch phase {
	case "delete":
		// Only execute delete phase for diff mode with deleteNotInFile
		if singleConfig.Sync.SyncMode == "diff" && singleConfig.Sync.DeleteNotInFile {
			return executeDeletePhase(ctx, tx, singleConfig, tableData, actualSyncColumns)
		}
		return nil // Skip delete for other modes
	case "insert_update":
		return executeInsertUpdatePhase(ctx, tx, singleConfig, tableData, actualSyncColumns)
	default:
		return fmt.Errorf("unknown sync phase: %s", phase)
	}
}

// executeDeletePhase handles delete operations for a single table in multi-table sync
func executeDeletePhase(ctx context.Context, tx *sql.Tx, config Config, tableData []DataRecord, actualSyncColumns []string) error {
	if config.Sync.SyncMode != "diff" {
		return nil // Only diff mode supports delete operations
	}

	// Validate requirements for differential sync
	if err := validateDiffSyncRequirements(config, actualSyncColumns); err != nil {
		return fmt.Errorf("delete phase validation error: %w", err)
	}

	// Get current data from DB
	dbRecords, _, err := getCurrentDBData(ctx, tx, config, actualSyncColumns)
	if err != nil {
		return fmt.Errorf("DB data retrieval error: %w", err)
	}

	// Find records to delete (records in DB but not in file)
	fileKeys := make(map[string]bool)
	for _, fileRecord := range tableData {
		pk, isValid := extractPrimaryKeyValue(fileRecord, config.Sync.PrimaryKey)
		if isValid {
			fileKeys[pk.Str] = true
		}
	}

	var toDelete []DataRecord
	for pkStr, dbRecord := range dbRecords {
		if !fileKeys[pkStr] {
			toDelete = append(toDelete, dbRecord)
		}
	}

	// Execute delete operations
	if len(toDelete) > 0 {
		err = bulkDelete(ctx, tx, config, toDelete)
		if err != nil {
			return fmt.Errorf("delete execution error: %w", err)
		}
		log.Printf("Table '%s': Deleted %d records", config.Sync.TableName, len(toDelete))
	}

	return nil
}

// executeInsertUpdatePhase handles insert and update operations for a single table in multi-table sync
func executeInsertUpdatePhase(ctx context.Context, tx *sql.Tx, config Config, tableData []DataRecord, actualSyncColumns []string) error {
	switch config.Sync.SyncMode {
	case "overwrite":
		return executeOverwritePhase(ctx, tx, config, tableData, actualSyncColumns)
	case "diff":
		return executeDiffInsertUpdatePhase(ctx, tx, config, tableData, actualSyncColumns)
	default:
		return fmt.Errorf("unknown sync mode: %s", config.Sync.SyncMode)
	}
}

// executeOverwritePhase handles overwrite mode operations (delete existing + insert all)
func executeOverwritePhase(ctx context.Context, tx *sql.Tx, config Config, tableData []DataRecord, actualSyncColumns []string) error {
	// In overwrite mode for multi-table sync, we delete ALL existing data first
	// This ensures a complete refresh of the table data
	_, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s", config.Sync.TableName))
	if err != nil {
		return fmt.Errorf("error deleting all data from table '%s': %w", config.Sync.TableName, err)
	}
	log.Printf("Table '%s': Deleted all existing data for overwrite", config.Sync.TableName)

	// Insert all file records
	if len(tableData) > 0 {
		err := bulkInsert(ctx, tx, config, tableData, actualSyncColumns)
		if err != nil {
			return fmt.Errorf("insert execution error: %w", err)
		}
		log.Printf("Table '%s': Inserted %d records", config.Sync.TableName, len(tableData))
	}

	return nil
}

// executeDiffInsertUpdatePhase handles differential insert and update operations
func executeDiffInsertUpdatePhase(ctx context.Context, tx *sql.Tx, config Config, tableData []DataRecord, actualSyncColumns []string) error {
	// Validate requirements for differential sync
	if err := validateDiffSyncRequirements(config, actualSyncColumns); err != nil {
		return err
	}

	// Get current data from DB
	dbRecords, _, err := getCurrentDBData(ctx, tx, config, actualSyncColumns)
	if err != nil {
		return fmt.Errorf("DB data retrieval error: %w", err)
	}

	// Compare file data with DB data to find insert/update operations
	toInsert, toUpdate, _ := diffData(config, tableData, dbRecords, actualSyncColumns)

	// Execute insert operations
	if len(toInsert) > 0 {
		err = bulkInsert(ctx, tx, config, toInsert, actualSyncColumns)
		if err != nil {
			return fmt.Errorf("insert execution error: %w", err)
		}
		log.Printf("Table '%s': Inserted %d records", config.Sync.TableName, len(toInsert))
	}

	// Execute update operations
	if len(toUpdate) > 0 {
		updateRecords := make([]DataRecord, len(toUpdate))
		for i, op := range toUpdate {
			updateRecords[i] = op.After
		}
		err = bulkUpdate(ctx, tx, config, updateRecords, actualSyncColumns)
		if err != nil {
			return fmt.Errorf("update execution error: %w", err)
		}
		log.Printf("Table '%s': Updated %d records", config.Sync.TableName, len(toUpdate))
	}

	return nil
}
