package helpers

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// DataValidator provides utilities for validating data in tests
type DataValidator struct {
	t  *testing.T
	db *sql.DB
}

// NewDataValidator creates a new data validator
func NewDataValidator(t *testing.T, db *sql.DB) *DataValidator {
	return &DataValidator{
		t:  t,
		db: db,
	}
}

// ValidateRowCount checks if the table has the expected number of rows
func (dv *DataValidator) ValidateRowCount(tableName string, expectedCount int) {
	dv.t.Helper()
	
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	var actualCount int
	err := dv.db.QueryRow(query).Scan(&actualCount)
	if err != nil {
		dv.t.Fatalf("Failed to count rows in table %s: %v", tableName, err)
	}
	
	if actualCount != expectedCount {
		dv.t.Errorf("Row count mismatch in table %s: expected %d, got %d", 
			tableName, expectedCount, actualCount)
	}
}

// ValidateTableExists checks if a table exists in the database
func (dv *DataValidator) ValidateTableExists(tableName string) {
	dv.t.Helper()
	
	query := `SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
			  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`
	var count int
	err := dv.db.QueryRow(query, tableName).Scan(&count)
	if err != nil {
		dv.t.Fatalf("Failed to check table existence for %s: %v", tableName, err)
	}
	
	if count == 0 {
		dv.t.Errorf("Table %s does not exist", tableName)
	}
}

// ValidateTableNotExists checks if a table does not exist in the database
func (dv *DataValidator) ValidateTableNotExists(tableName string) {
	dv.t.Helper()
	
	query := `SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
			  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`
	var count int
	err := dv.db.QueryRow(query, tableName).Scan(&count)
	if err != nil {
		dv.t.Fatalf("Failed to check table existence for %s: %v", tableName, err)
	}
	
	if count > 0 {
		dv.t.Errorf("Table %s should not exist", tableName)
	}
}

// ValidateColumnExists checks if a column exists in a table
func (dv *DataValidator) ValidateColumnExists(tableName, columnName string) {
	dv.t.Helper()
	
	query := `SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
			  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?`
	var count int
	err := dv.db.QueryRow(query, tableName, columnName).Scan(&count)
	if err != nil {
		dv.t.Fatalf("Failed to check column existence for %s.%s: %v", tableName, columnName, err)
	}
	
	if count == 0 {
		dv.t.Errorf("Column %s does not exist in table %s", columnName, tableName)
	}
}

// ValidateRecordExists checks if a record with the given primary key exists
func (dv *DataValidator) ValidateRecordExists(tableName, primaryKey, value string) {
	dv.t.Helper()
	
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = ?", tableName, primaryKey)
	var count int
	err := dv.db.QueryRow(query, value).Scan(&count)
	if err != nil {
		dv.t.Fatalf("Failed to check record existence in %s: %v", tableName, err)
	}
	
	if count == 0 {
		dv.t.Errorf("Record with %s=%s does not exist in table %s", primaryKey, value, tableName)
	}
}

// ValidateRecordNotExists checks if a record with the given primary key does not exist
func (dv *DataValidator) ValidateRecordNotExists(tableName, primaryKey, value string) {
	dv.t.Helper()
	
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = ?", tableName, primaryKey)
	var count int
	err := dv.db.QueryRow(query, value).Scan(&count)
	if err != nil {
		dv.t.Fatalf("Failed to check record non-existence in %s: %v", tableName, err)
	}
	
	if count > 0 {
		dv.t.Errorf("Record with %s=%s should not exist in table %s", primaryKey, value, tableName)
	}
}

// ValidateRecordValue checks if a specific field has the expected value
func (dv *DataValidator) ValidateRecordValue(tableName, primaryKey, primaryValue, column, expectedValue string) {
	dv.t.Helper()
	
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", column, tableName, primaryKey)
	var actualValue sql.NullString
	err := dv.db.QueryRow(query, primaryValue).Scan(&actualValue)
	if err != nil {
		if err == sql.ErrNoRows {
			dv.t.Errorf("Record with %s=%s not found in table %s", primaryKey, primaryValue, tableName)
			return
		}
		dv.t.Fatalf("Failed to query record value in %s: %v", tableName, err)
	}
	
	actual := ""
	if actualValue.Valid {
		actual = actualValue.String
	}
	
	if actual != expectedValue {
		dv.t.Errorf("Value mismatch in table %s, column %s for %s=%s: expected '%s', got '%s'",
			tableName, column, primaryKey, primaryValue, expectedValue, actual)
	}
}

// ValidateRecordValues checks multiple field values for a record
func (dv *DataValidator) ValidateRecordValues(tableName, primaryKey, primaryValue string, expectedValues map[string]string) {
	dv.t.Helper()
	
	for column, expectedValue := range expectedValues {
		dv.ValidateRecordValue(tableName, primaryKey, primaryValue, column, expectedValue)
	}
}

// ValidateDataIntegrity checks referential integrity between tables
func (dv *DataValidator) ValidateDataIntegrity(childTable, childColumn, parentTable, parentColumn string) {
	dv.t.Helper()
	
	query := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s c 
		LEFT JOIN %s p ON c.%s = p.%s 
		WHERE c.%s IS NOT NULL AND p.%s IS NULL`,
		childTable, parentTable, childColumn, parentColumn, childColumn, parentColumn)
	
	var orphanCount int
	err := dv.db.QueryRow(query).Scan(&orphanCount)
	if err != nil {
		dv.t.Fatalf("Failed to check data integrity between %s and %s: %v", childTable, parentTable, err)
	}
	
	if orphanCount > 0 {
		dv.t.Errorf("Data integrity violation: %d orphan records found in %s.%s referencing %s.%s",
			orphanCount, childTable, childColumn, parentTable, parentColumn)
	}
}

// ValidateUniqueConstraint checks if values in a column are unique
func (dv *DataValidator) ValidateUniqueConstraint(tableName, columnName string) {
	dv.t.Helper()
	
	query := fmt.Sprintf(`
		SELECT %s, COUNT(*) as cnt FROM %s 
		WHERE %s IS NOT NULL 
		GROUP BY %s 
		HAVING COUNT(*) > 1`,
		columnName, tableName, columnName, columnName)
	
	rows, err := dv.db.Query(query)
	if err != nil {
		dv.t.Fatalf("Failed to check unique constraint for %s.%s: %v", tableName, columnName, err)
	}
	defer rows.Close()
	
	var duplicates []string
	for rows.Next() {
		var value string
		var count int
		if err := rows.Scan(&value, &count); err != nil {
			dv.t.Fatalf("Failed to scan duplicate values: %v", err)
		}
		duplicates = append(duplicates, fmt.Sprintf("%s (appears %d times)", value, count))
	}
	
	if len(duplicates) > 0 {
		dv.t.Errorf("Unique constraint violation in %s.%s: %v", tableName, columnName, duplicates)
	}
}

// ValidateDataRange checks if numeric values are within the expected range
func (dv *DataValidator) ValidateDataRange(tableName, columnName string, minValue, maxValue float64) {
	dv.t.Helper()
	
	query := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s WHERE %s IS NOT NULL",
		columnName, columnName, tableName, columnName)
	
	var actualMin, actualMax sql.NullFloat64
	err := dv.db.QueryRow(query).Scan(&actualMin, &actualMax)
	if err != nil {
		dv.t.Fatalf("Failed to check data range for %s.%s: %v", tableName, columnName, err)
	}
	
	if actualMin.Valid && actualMin.Float64 < minValue {
		dv.t.Errorf("Minimum value violation in %s.%s: expected >= %f, got %f",
			tableName, columnName, minValue, actualMin.Float64)
	}
	
	if actualMax.Valid && actualMax.Float64 > maxValue {
		dv.t.Errorf("Maximum value violation in %s.%s: expected <= %f, got %f",
			tableName, columnName, maxValue, actualMax.Float64)
	}
}

// ValidateNullConstraint checks if a column properly handles NULL values
func (dv *DataValidator) ValidateNullConstraint(tableName, columnName string, allowNull bool) {
	dv.t.Helper()
	
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s IS NULL", tableName, columnName)
	var nullCount int
	err := dv.db.QueryRow(query).Scan(&nullCount)
	if err != nil {
		dv.t.Fatalf("Failed to check null constraint for %s.%s: %v", tableName, columnName, err)
	}
	
	if !allowNull && nullCount > 0 {
		dv.t.Errorf("NULL constraint violation in %s.%s: found %d NULL values when none allowed",
			tableName, columnName, nullCount)
	}
}

// ValidateTimestampColumns checks if timestamp columns are properly set
func (dv *DataValidator) ValidateTimestampColumns(tableName string, createdAtCol, updatedAtCol string) {
	dv.t.Helper()
	
	// Check that created_at and updated_at are not null for all records
	if createdAtCol != "" {
		dv.ValidateNullConstraint(tableName, createdAtCol, false)
	}
	
	if updatedAtCol != "" {
		dv.ValidateNullConstraint(tableName, updatedAtCol, false)
	}
	
	// Check that updated_at >= created_at
	if createdAtCol != "" && updatedAtCol != "" {
		query := fmt.Sprintf(`
			SELECT COUNT(*) FROM %s 
			WHERE %s IS NOT NULL AND %s IS NOT NULL AND %s < %s`,
			tableName, createdAtCol, updatedAtCol, updatedAtCol, createdAtCol)
		
		var invalidCount int
		err := dv.db.QueryRow(query).Scan(&invalidCount)
		if err != nil {
			dv.t.Fatalf("Failed to validate timestamp ordering: %v", err)
		}
		
		if invalidCount > 0 {
			dv.t.Errorf("Timestamp ordering violation: %d records have updated_at < created_at", invalidCount)
		}
	}
}

// GetAllRecords retrieves all records from a table for detailed comparison
func (dv *DataValidator) GetAllRecords(tableName string, columns []string) []map[string]interface{} {
	dv.t.Helper()
	
	columnStr := "*"
	if len(columns) > 0 {
		columnStr = strings.Join(columns, ", ")
	}
	
	query := fmt.Sprintf("SELECT %s FROM %s ORDER BY 1", columnStr, tableName)
	rows, err := dv.db.Query(query)
	if err != nil {
		dv.t.Fatalf("Failed to query all records from %s: %v", tableName, err)
	}
	defer rows.Close()
	
	// Get column information
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		dv.t.Fatalf("Failed to get column types: %v", err)
	}
	
	var results []map[string]interface{}
	for rows.Next() {
		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columnTypes))
		valuePointers := make([]interface{}, len(columnTypes))
		
		for i := range values {
			valuePointers[i] = &values[i]
		}
		
		if err := rows.Scan(valuePointers...); err != nil {
			dv.t.Fatalf("Failed to scan row: %v", err)
		}
		
		// Convert to map
		record := make(map[string]interface{})
		for i, colType := range columnTypes {
			colName := colType.Name()
			record[colName] = values[i]
		}
		
		results = append(results, record)
	}
	
	return results
}

// CompareRecords compares two sets of records and reports differences
func (dv *DataValidator) CompareRecords(expected, actual []map[string]interface{}, primaryKey string) {
	dv.t.Helper()
	
	if len(expected) != len(actual) {
		dv.t.Errorf("Record count mismatch: expected %d, got %d", len(expected), len(actual))
	}
	
	// Create maps for easier comparison
	expectedMap := make(map[string]map[string]interface{})
	actualMap := make(map[string]map[string]interface{})
	
	for _, record := range expected {
		if pkValue, ok := record[primaryKey]; ok {
			key := fmt.Sprintf("%v", pkValue)
			expectedMap[key] = record
		}
	}
	
	for _, record := range actual {
		if pkValue, ok := record[primaryKey]; ok {
			key := fmt.Sprintf("%v", pkValue)
			actualMap[key] = record
		}
	}
	
	// Check for missing and extra records
	for key := range expectedMap {
		if _, found := actualMap[key]; !found {
			dv.t.Errorf("Missing record with %s=%s", primaryKey, key)
		}
	}
	
	for key := range actualMap {
		if _, found := expectedMap[key]; !found {
			dv.t.Errorf("Unexpected record with %s=%s", primaryKey, key)
		}
	}
	
	// Compare matching records
	for key, expectedRecord := range expectedMap {
		if actualRecord, found := actualMap[key]; found {
			dv.compareRecordFields(expectedRecord, actualRecord, primaryKey, key)
		}
	}
}

// compareRecordFields compares fields of two records
func (dv *DataValidator) compareRecordFields(expected, actual map[string]interface{}, primaryKey, primaryValue string) {
	for field, expectedValue := range expected {
		actualValue, exists := actual[field]
		if !exists {
			dv.t.Errorf("Missing field %s in record %s=%s", field, primaryKey, primaryValue)
			continue
		}
		
		if !dv.valuesEqual(expectedValue, actualValue) {
			dv.t.Errorf("Field mismatch in record %s=%s, field %s: expected %v, got %v",
				primaryKey, primaryValue, field, expectedValue, actualValue)
		}
	}
}

// valuesEqual compares two values, handling different types appropriately
func (dv *DataValidator) valuesEqual(expected, actual interface{}) bool {
	// Handle nil values
	if expected == nil && actual == nil {
		return true
	}
	if expected == nil || actual == nil {
		return false
	}
	
	// Convert byte slices to strings for comparison
	if expectedBytes, ok := expected.([]byte); ok {
		expected = string(expectedBytes)
	}
	if actualBytes, ok := actual.([]byte); ok {
		actual = string(actualBytes)
	}
	
	// Use reflection for deep comparison
	return reflect.DeepEqual(expected, actual)
}

// ValidateDataConsistency performs comprehensive data consistency checks
func (dv *DataValidator) ValidateDataConsistency(tableName string, expectations map[string]interface{}) {
	dv.t.Helper()
	
	for checkType, params := range expectations {
		switch checkType {
		case "rowCount":
			if count, ok := params.(int); ok {
				dv.ValidateRowCount(tableName, count)
			}
			
		case "uniqueColumns":
			if columns, ok := params.([]string); ok {
				for _, column := range columns {
					dv.ValidateUniqueConstraint(tableName, column)
				}
			}
			
		case "notNullColumns":
			if columns, ok := params.([]string); ok {
				for _, column := range columns {
					dv.ValidateNullConstraint(tableName, column, false)
				}
			}
			
		default:
			dv.t.Logf("Unknown consistency check type: %s", checkType)
		}
	}
}