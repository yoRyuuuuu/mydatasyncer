package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	e2eTestDBDSN                   = "user:password@tcp(127.0.0.1:3306)/testdb?parseTime=true"
	e2eTestTableName               = "test_table"
	e2eTestConfigSmallCSV          = "testdata/config_small.yml"
	e2eTestDataSmallCSV            = "testdata/small_data.csv"
	e2eTestConfigCSVDiff           = "testdata/e2e_config_csv_diff.yml"
	e2eTestDataCSVDiff             = "testdata/e2e_data_csv_diff.csv"
	e2eTestConfigJSONOverwrite     = "testdata/e2e_config_json_overwrite.yml"
	e2eTestDataJSONOverwrite       = "testdata/e2e_data_overwrite.json"
	e2eTestConfigJSONDiff          = "testdata/e2e_config_json_diff.yml"
	e2eTestDataJSONDiff            = "testdata/e2e_data_diff.json"
	e2eTestDataJSONEmptyArray      = "testdata/e2e_data_empty_array.json"
	e2eTestConfigJSONEmptyArray    = "testdata/e2e_config_json_empty_array.yml"
	e2eTestDataJSONMissingKey      = "testdata/e2e_data_missing_key.json"
	e2eTestConfigJSONMissingKey    = "testdata/e2e_config_json_missing_key.yml"
	e2eTestDataJSONInvalidFormat   = "testdata/e2e_data_invalid_format.json"
	e2eTestConfigJSONInvalidFormat = "testdata/e2e_config_json_invalid_format.yml"
	e2eTestConfigDataTypes         = "testdata/e2e_config_data_types.yml"
	e2eTestDataTypes               = "testdata/e2e_data_types.json"
)

// TestRecord is a helper struct for verifying data in E2E tests
type TestRecord struct {
	ID    int
	Name  string
	Email string
}

// DataTypesTestRecord is a helper struct for verifying data type conversion in E2E tests
type DataTypesTestRecord struct {
	ID               int
	StringCol        string
	BoolTrueCol      bool
	BoolFalseCol     bool
	IntCol           int
	FloatCol         float64
	LargeIntCol      int64
	ZeroCol          int
	NegativeIntCol   int
	NegativeFloatCol float64
	WholeNumberFloat float64
	NullCol          *string
	RFC3339Time      time.Time
}

// e2eSetupTestDB connects to the database, drops the test table if it exists, and creates it for E2E tests.
func e2eSetupTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("mysql", e2eTestDBDSN)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", e2eTestTableName))
	if err != nil {
		t.Fatalf("Failed to drop test table: %v", err)
	}

	createTableSQL := fmt.Sprintf(`
CREATE TABLE %s (
id INT PRIMARY KEY,
name VARCHAR(255),
email VARCHAR(255)
);`, e2eTestTableName)
	_, err = db.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	return db
}

// e2eSetupDataTypesTestDB creates a table specifically for data type conversion testing.
func e2eSetupDataTypesTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("mysql", e2eTestDBDSN)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	_, err = db.Exec("DROP TABLE IF EXISTS data_types_test")
	if err != nil {
		t.Fatalf("Failed to drop data types test table: %v", err)
	}

	createTableSQL := `
CREATE TABLE data_types_test (
	id INT PRIMARY KEY,
	string_col VARCHAR(255),
	bool_true_col BOOLEAN,
	bool_false_col BOOLEAN,
	int_col INT,
	float_col DOUBLE,
	large_int_col BIGINT,
	zero_col INT,
	negative_int_col INT,
	negative_float_col DOUBLE,
	whole_number_float DOUBLE,
	null_col VARCHAR(50),
	rfc3339_time DATETIME,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);`
	_, err = db.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create data types test table: %v", err)
	}
	return db
}

// e2eTearDownTestDB drops the test table for E2E tests.
func e2eTearDownTestDB(t *testing.T, db *sql.DB) {
	if db == nil {
		return
	}
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", e2eTestTableName))
	if err != nil {
		// Log error but don't fail the test, as this is cleanup
		log.Printf("Failed to drop test table during teardown: %v", err)
	}
	db.Close()
}

// e2eVerifyDBState queries the database and compares the records with the expected records.
func e2eVerifyDBState(t *testing.T, db *sql.DB, expectedRecords []TestRecord) {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf("SELECT id, name, email FROM %s ORDER BY id ASC", e2eTestTableName))
	if err != nil {
		t.Fatalf("Failed to query data from test table: %v", err)
	}
	defer rows.Close()

	var actualRecords []TestRecord
	for rows.Next() {
		var r TestRecord
		if err := rows.Scan(&r.ID, &r.Name, &r.Email); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		actualRecords = append(actualRecords, r)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("Error during rows iteration: %v", err)
	}

	if len(actualRecords) != len(expectedRecords) {
		t.Fatalf("Expected %d records, but got %d. Actual: %+v", len(expectedRecords), len(actualRecords), actualRecords)
	}

	for i, expected := range expectedRecords {
		actual := actualRecords[i]
		if actual.ID != expected.ID || actual.Name != expected.Name || actual.Email != expected.Email {
			t.Errorf("Record mismatch at index %d. Expected: %+v, Got: %+v", i, expected, actual)
		}
	}
}

// e2eInsertInitialData inserts initial data into the test table.
func e2eInsertInitialData(t *testing.T, db *sql.DB, records []TestRecord) {
	t.Helper()
	stmt, err := db.Prepare(fmt.Sprintf("INSERT INTO %s (id, name, email) VALUES (?, ?, ?)", e2eTestTableName))
	if err != nil {
		t.Fatalf("Failed to prepare insert statement: %v", err)
	}
	defer stmt.Close()

	for _, r := range records {
		_, err := stmt.Exec(r.ID, r.Name, r.Email)
		if err != nil {
			t.Fatalf("Failed to insert initial data %+v: %v", r, err)
		}
	}
}

// checkTestFileExists checks if a given test file exists and fails the test if not.
func checkTestFileExists(t *testing.T, filePath string) {
	t.Helper()
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Fatalf("Test file %s does not exist. Please create it first.", filePath)
	}
}

func TestE2ESyncSmallDataCSV(t *testing.T) {
	checkTestFileExists(t, e2eTestConfigSmallCSV)
	checkTestFileExists(t, e2eTestDataSmallCSV)

	db := e2eSetupTestDB(t)
	defer e2eTearDownTestDB(t, db)

	err := RunApp(e2eTestConfigSmallCSV, false)
	if err != nil {
		t.Fatalf("RunApp failed: %v", err)
	}

	expectedRecords := []TestRecord{
		{ID: 1, Name: "John Doe", Email: "john.doe@example.com"},
		{ID: 2, Name: "Jane Smith", Email: "jane.smith@example.com"},
		{ID: 3, Name: "Alice Johnson", Email: "alice.johnson@example.com"},
	}
	e2eVerifyDBState(t, db, expectedRecords)
}

func TestE2ESyncCSV_Diff_WithDeletes(t *testing.T) {
	checkTestFileExists(t, e2eTestConfigCSVDiff)
	checkTestFileExists(t, e2eTestDataCSVDiff)

	db := e2eSetupTestDB(t)
	defer e2eTearDownTestDB(t, db)

	// Initial data in DB
	initialDBData := []TestRecord{
		{ID: 402, Name: "CSV Diff User Old", Email: "csv.diff.old@example.com"},       // This will be updated
		{ID: 403, Name: "CSV Diff User Delete", Email: "csv.diff.delete@example.com"}, // This will be deleted
	}
	e2eInsertInitialData(t, db, initialDBData)

	err := RunApp(e2eTestConfigCSVDiff, false)
	if err != nil {
		t.Fatalf("RunApp with CSV diff failed: %v", err)
	}

	// Expected data after diff sync
	// Record 401 is new.
	// Record 402 is updated.
	// Record 403 is deleted.
	expectedRecords := []TestRecord{
		{ID: 401, Name: "CSV Diff User New", Email: "csv.diff.new@example.com"},
		{ID: 402, Name: "CSV Diff User Updated", Email: "csv.diff.updated.new@example.com"},
	}
	e2eVerifyDBState(t, db, expectedRecords)
}

func TestE2ESyncJSON_Overwrite(t *testing.T) {
	checkTestFileExists(t, e2eTestConfigJSONOverwrite)
	checkTestFileExists(t, e2eTestDataJSONOverwrite)

	db := e2eSetupTestDB(t)
	defer e2eTearDownTestDB(t, db)

	// Insert some initial data to ensure overwrite works
	initialData := []TestRecord{
		{ID: 99, Name: "Old User", Email: "old.user@example.com"},
	}
	e2eInsertInitialData(t, db, initialData)

	err := RunApp(e2eTestConfigJSONOverwrite, false)
	if err != nil {
		t.Fatalf("RunApp with JSON overwrite failed: %v", err)
	}

	expectedRecords := []TestRecord{
		{ID: 101, Name: "Json User One", Email: "json.one@example.com"},
		{ID: 102, Name: "Json User Two", Email: "json.two@example.com"},
	}
	e2eVerifyDBState(t, db, expectedRecords)
}

func TestE2ESyncJSON_Diff_WithDeletes(t *testing.T) {
	checkTestFileExists(t, e2eTestConfigJSONDiff)
	checkTestFileExists(t, e2eTestDataJSONDiff)

	db := e2eSetupTestDB(t)
	defer e2eTearDownTestDB(t, db)

	// Initial data in DB
	initialDBData := []TestRecord{
		{ID: 202, Name: "Json Diff User Old", Email: "json.diff.old@example.com"},       // This will be updated
		{ID: 203, Name: "Json Diff User Delete", Email: "json.diff.delete@example.com"}, // This will be deleted
	}
	e2eInsertInitialData(t, db, initialDBData)

	err := RunApp(e2eTestConfigJSONDiff, false)
	if err != nil {
		t.Fatalf("RunApp with JSON diff failed: %v", err)
	}

	// Expected data after diff sync
	// Record 201 is new.
	// Record 202 is updated.
	// Record 203 is deleted.
	expectedRecords := []TestRecord{
		{ID: 201, Name: "Json Diff User New", Email: "json.diff.new@example.com"},
		{ID: 202, Name: "Json Diff User Updated", Email: "json.diff.updated.new@example.com"},
	}
	e2eVerifyDBState(t, db, expectedRecords)
}

func TestE2ESyncJSON_EmptyArray(t *testing.T) {
	checkTestFileExists(t, e2eTestConfigJSONEmptyArray)
	checkTestFileExists(t, e2eTestDataJSONEmptyArray)

	db := e2eSetupTestDB(t)
	defer e2eTearDownTestDB(t, db)

	// Insert some initial data to ensure overwrite with empty clears the table
	initialData := []TestRecord{
		{ID: 301, Name: "To Be Deleted", Email: "delete@example.com"},
	}
	e2eInsertInitialData(t, db, initialData)

	err := RunApp(e2eTestConfigJSONEmptyArray, false)
	if err != nil {
		t.Fatalf("RunApp with empty JSON array failed: %v", err)
	}

	e2eVerifyDBState(t, db, []TestRecord{}) // Expect no records
}

func TestE2ESyncJSON_Error_MissingKey(t *testing.T) {
	checkTestFileExists(t, e2eTestConfigJSONMissingKey)
	checkTestFileExists(t, e2eTestDataJSONMissingKey)

	db := e2eSetupTestDB(t)
	defer e2eTearDownTestDB(t, db)

	runErr := RunApp(e2eTestConfigJSONMissingKey, false)
	if runErr == nil {
		t.Fatalf("RunApp expected an error due to missing key, but got nil")
	}
}

func TestE2ESyncJSON_Error_InvalidFormat(t *testing.T) {
	checkTestFileExists(t, e2eTestConfigJSONInvalidFormat)
	checkTestFileExists(t, e2eTestDataJSONInvalidFormat)

	db := e2eSetupTestDB(t)
	defer e2eTearDownTestDB(t, db)

	runErr := RunApp(e2eTestConfigJSONInvalidFormat, false)
	if runErr == nil {
		t.Fatalf("RunApp expected an error due to invalid JSON format, but got nil")
	}
}

// e2eTearDownDataTypesTestDB drops the data types test table for E2E tests.
func e2eTearDownDataTypesTestDB(_ *testing.T, db *sql.DB) {
	if db == nil {
		return
	}
	_, err := db.Exec("DROP TABLE IF EXISTS data_types_test")
	if err != nil {
		// Log error but don't fail the test, as this is cleanup
		log.Printf("Failed to drop data types test table during teardown: %v", err)
	}
	db.Close()
}

// e2eVerifyDataTypesDBState queries the database and compares the data type conversion records.
func e2eVerifyDataTypesDBState(t *testing.T, db *sql.DB, expectedRecords []DataTypesTestRecord) {
	t.Helper()
	query := `SELECT id, string_col, bool_true_col, bool_false_col, int_col, float_col,
	          large_int_col, zero_col, negative_int_col, negative_float_col,
	          whole_number_float, null_col, rfc3339_time
	          FROM data_types_test ORDER BY id ASC`

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Failed to query data from data types test table: %v", err)
	}
	defer rows.Close()

	var actualRecords []DataTypesTestRecord
	for rows.Next() {
		var r DataTypesTestRecord
		err := rows.Scan(&r.ID, &r.StringCol, &r.BoolTrueCol, &r.BoolFalseCol,
			&r.IntCol, &r.FloatCol, &r.LargeIntCol, &r.ZeroCol,
			&r.NegativeIntCol, &r.NegativeFloatCol, &r.WholeNumberFloat,
			&r.NullCol, &r.RFC3339Time)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		actualRecords = append(actualRecords, r)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("Error during rows iteration: %v", err)
	}

	if len(actualRecords) != len(expectedRecords) {
		t.Fatalf("Expected %d records, but got %d. Actual: %+v", len(expectedRecords), len(actualRecords), actualRecords)
	}

	for i, expected := range expectedRecords {
		actual := actualRecords[i]
		if actual.ID != expected.ID {
			t.Errorf("ID mismatch at index %d. Expected: %d, Got: %d", i, expected.ID, actual.ID)
		}
		if actual.StringCol != expected.StringCol {
			t.Errorf("StringCol mismatch at index %d. Expected: '%s', Got: '%s'", i, expected.StringCol, actual.StringCol)
		}
		if actual.BoolTrueCol != expected.BoolTrueCol {
			t.Errorf("BoolTrueCol mismatch at index %d. Expected: %t, Got: %t", i, expected.BoolTrueCol, actual.BoolTrueCol)
		}
		if actual.BoolFalseCol != expected.BoolFalseCol {
			t.Errorf("BoolFalseCol mismatch at index %d. Expected: %t, Got: %t", i, expected.BoolFalseCol, actual.BoolFalseCol)
		}
		if actual.IntCol != expected.IntCol {
			t.Errorf("IntCol mismatch at index %d. Expected: %d, Got: %d", i, expected.IntCol, actual.IntCol)
		}
		if actual.FloatCol != expected.FloatCol {
			t.Errorf("FloatCol mismatch at index %d. Expected: %f, Got: %f", i, expected.FloatCol, actual.FloatCol)
		}
		if actual.LargeIntCol != expected.LargeIntCol {
			t.Errorf("LargeIntCol mismatch at index %d. Expected: %d, Got: %d", i, expected.LargeIntCol, actual.LargeIntCol)
		}
		if actual.ZeroCol != expected.ZeroCol {
			t.Errorf("ZeroCol mismatch at index %d. Expected: %d, Got: %d", i, expected.ZeroCol, actual.ZeroCol)
		}
		if actual.NegativeIntCol != expected.NegativeIntCol {
			t.Errorf("NegativeIntCol mismatch at index %d. Expected: %d, Got: %d", i, expected.NegativeIntCol, actual.NegativeIntCol)
		}
		if actual.NegativeFloatCol != expected.NegativeFloatCol {
			t.Errorf("NegativeFloatCol mismatch at index %d. Expected: %f, Got: %f", i, expected.NegativeFloatCol, actual.NegativeFloatCol)
		}
		if actual.WholeNumberFloat != expected.WholeNumberFloat {
			t.Errorf("WholeNumberFloat mismatch at index %d. Expected: %f, Got: %f", i, expected.WholeNumberFloat, actual.WholeNumberFloat)
		}
		if (actual.NullCol == nil) != (expected.NullCol == nil) || (actual.NullCol != nil && expected.NullCol != nil && *actual.NullCol != *expected.NullCol) {
			actualStr := "nil"
			expectedStr := "nil"
			if actual.NullCol != nil {
				actualStr = *actual.NullCol
			}
			if expected.NullCol != nil {
				expectedStr = *expected.NullCol
			}
			t.Errorf("NullCol mismatch at index %d. Expected: %s, Got: %s", i, expectedStr, actualStr)
		}
		if !actual.RFC3339Time.Equal(expected.RFC3339Time) {
			t.Errorf("RFC3339Time mismatch at index %d. Expected: %s, Got: %s", i, expected.RFC3339Time, actual.RFC3339Time)
		}
	}
}

func TestE2ESyncJSON_DataTypes(t *testing.T) {
	checkTestFileExists(t, e2eTestConfigDataTypes)
	checkTestFileExists(t, e2eTestDataTypes)

	db := e2eSetupDataTypesTestDB(t)
	defer e2eTearDownDataTypesTestDB(t, db)

	err := RunApp(e2eTestConfigDataTypes, false)
	if err != nil {
		t.Fatalf("RunApp with data types JSON failed: %v", err)
	}

	expectedRecords := []DataTypesTestRecord{
		{
			ID:               1,
			StringCol:        "Hello World",
			BoolTrueCol:      true,
			BoolFalseCol:     false,
			IntCol:           42,
			FloatCol:         3.14159,
			LargeIntCol:      9007199254740000,
			ZeroCol:          0,
			NegativeIntCol:   -123,
			NegativeFloatCol: -99.99,
			WholeNumberFloat: 100,
			NullCol:          nil,
			RFC3339Time:      time.Date(2023, 12, 25, 15, 30, 45, 0, time.FixedZone("+09:00", 9*3600)),
		},
		{
			ID:               2,
			StringCol:        "JSON Test",
			BoolTrueCol:      false,
			BoolFalseCol:     true,
			IntCol:           0,
			FloatCol:         0.001,
			LargeIntCol:      1,
			ZeroCol:          999,
			NegativeIntCol:   -1,
			NegativeFloatCol: -0.5,
			WholeNumberFloat: 42,
			NullCol:          nil,
			RFC3339Time:      time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			ID:               3,
			StringCol:        "",
			BoolTrueCol:      true,
			BoolFalseCol:     false,
			IntCol:           2147483647,
			FloatCol:         1.7976931348623157e+308,
			LargeIntCol:      -9007199254740000,
			ZeroCol:          0,
			NegativeIntCol:   -2147483648,
			NegativeFloatCol: -1.7976931348623157e+308,
			WholeNumberFloat: 0,
			NullCol:          nil,
			RFC3339Time:      time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}
	e2eVerifyDataTypesDBState(t, db, expectedRecords)
}

func TestRunAppErrorHandling(t *testing.T) {
	t.Run("invalid config validation error", func(t *testing.T) {
		tempDir := t.TempDir()
		configFile := filepath.Join(tempDir, "invalid_config.yml")

		// Create a dummy CSV file
		dummyCSV := filepath.Join(tempDir, "test.csv")
		err := os.WriteFile(dummyCSV, []byte("id,name\n1,test"), 0644)
		if err != nil {
			t.Fatalf("Failed to create dummy CSV file: %v", err)
		}

		// Create a config that will pass parsing but fail validation
		invalidConfigYAML := `
db:
  dsn: ""  # Empty DSN should cause validation error
sync:
  filePath: "` + dummyCSV + `"
  tableName: "test_table"
  syncMode: "diff"
  primaryKey: "id"
  columns: ["id", "name"]
  timestampColumns: []
  immutableColumns: []
  deleteNotInFile: false
`
		err = os.WriteFile(configFile, []byte(invalidConfigYAML), 0644)
		if err != nil {
			t.Fatalf("Failed to create test config file: %v", err)
		}

		err = RunApp(configFile, false)
		if err == nil {
			t.Error("Expected error for invalid config")
		}
		if !strings.Contains(err.Error(), "configuration error") {
			t.Errorf("Expected configuration error, got: %v", err)
		}
	})

	t.Run("invalid database DSN error", func(t *testing.T) {
		tempDir := t.TempDir()
		configFile := filepath.Join(tempDir, "invalid_dsn.yml")

		// Create a dummy CSV file
		dummyCSV := filepath.Join(tempDir, "test.csv")
		err := os.WriteFile(dummyCSV, []byte("id,name\n1,test"), 0644)
		if err != nil {
			t.Fatalf("Failed to create dummy CSV file: %v", err)
		}

		invalidDSNConfig := `
db:
  dsn: "invalid:dsn:format"  # Invalid DSN format
sync:
  filePath: "` + dummyCSV + `"
  tableName: "test_table"
  syncMode: "overwrite"
  primaryKey: "id"
  columns: ["id", "name"]
  timestampColumns: []
  immutableColumns: []
  deleteNotInFile: false
`
		err = os.WriteFile(configFile, []byte(invalidDSNConfig), 0644)
		if err != nil {
			t.Fatalf("Failed to create test config file: %v", err)
		}

		err = RunApp(configFile, false)
		if err == nil {
			t.Error("Expected error for invalid DSN")
		}
		if !strings.Contains(err.Error(), "database connection error") &&
			!strings.Contains(err.Error(), "database connectivity error") {
			t.Errorf("Expected database connection/connectivity error, got: %v", err)
		}
	})

	t.Run("non-existent file error", func(t *testing.T) {
		tempDir := t.TempDir()
		configFile := filepath.Join(tempDir, "nonexistent_file.yml")

		nonExistentFileConfig := `
db:
  dsn: "user:password@tcp(127.0.0.1:3306)/testdb"
sync:
  filePath: "non_existent_file.csv"  # File that doesn't exist
  tableName: "test_table"
  syncMode: "overwrite"
  primaryKey: "id"
  columns: ["id", "name"]
  timestampColumns: []
  immutableColumns: []
  deleteNotInFile: false
`
		err := os.WriteFile(configFile, []byte(nonExistentFileConfig), 0644)
		if err != nil {
			t.Fatalf("Failed to create test config file: %v", err)
		}

		err = RunApp(configFile, false)
		if err == nil {
			t.Error("Expected error for non-existent file")
		}
		if !strings.Contains(err.Error(), "file reading error") {
			t.Errorf("Expected file reading error, got: %v", err)
		}
	})

	t.Run("dry run mode logs correctly", func(t *testing.T) {
		tempDir := t.TempDir()
		configFile := filepath.Join(tempDir, "dry_run_config.yml")
		dataFile := filepath.Join(tempDir, "test_data.csv")

		// Create valid config with all required fields
		validConfig := `
db:
  dsn: "user:password@tcp(127.0.0.1:3306)/testdb"
sync:
  filePath: "` + dataFile + `"
  tableName: "test_table"
  syncMode: "overwrite"
  primaryKey: "id"
  columns: ["id", "name", "email"]
  timestampColumns: []
  immutableColumns: []
  deleteNotInFile: false
`
		err := os.WriteFile(configFile, []byte(validConfig), 0644)
		if err != nil {
			t.Fatalf("Failed to create test config file: %v", err)
		}

		// Create test CSV file
		csvContent := `id,name,email
1,Test User,test@example.com
2,Another User,another@example.com`
		err = os.WriteFile(dataFile, []byte(csvContent), 0644)
		if err != nil {
			t.Fatalf("Failed to create test data file: %v", err)
		}

		// This should fail on database connection but we're testing the dry-run logging
		err = RunApp(configFile, true)
		// Expected to fail due to invalid DB connection, but that's ok for this test
		// We're mainly testing that dry-run mode doesn't panic and logs correctly
		if err != nil && !strings.Contains(err.Error(), "database") {
			t.Errorf("Unexpected error type: %v", err)
		}
	})

	t.Run("invalid file format error", func(t *testing.T) {
		tempDir := t.TempDir()
		configFile := filepath.Join(tempDir, "invalid_format.yml")
		dataFile := filepath.Join(tempDir, "invalid.txt") // Unsupported file extension

		validConfig := `
db:
  dsn: "user:password@tcp(127.0.0.1:3306)/testdb"
sync:
  filePath: "` + dataFile + `"
  tableName: "test_table"
  syncMode: "overwrite"
  primaryKey: "id"
  columns: ["id", "name"]
  timestampColumns: []
  immutableColumns: []
  deleteNotInFile: false
`
		err := os.WriteFile(configFile, []byte(validConfig), 0644)
		if err != nil {
			t.Fatalf("Failed to create test config file: %v", err)
		}

		// Create file with unsupported extension
		err = os.WriteFile(dataFile, []byte("some content"), 0644)
		if err != nil {
			t.Fatalf("Failed to create test data file: %v", err)
		}

		err = RunApp(configFile, false)
		if err == nil {
			t.Error("Expected error for unsupported file format")
		}
		if !strings.Contains(err.Error(), "file reading error") {
			t.Errorf("Expected file reading error, got: %v", err)
		}
	})
}

func TestLoadDataFromFileErrorHandling(t *testing.T) {
	t.Run("unsupported file extension", func(t *testing.T) {
		config := &Config{
			Sync: SyncConfig{
				FilePath: "test.txt", // Unsupported extension
				Columns:  []string{"id", "name"},
			},
		}

		_, err := loadDataFromFile(config)
		if err == nil {
			t.Error("Expected error for unsupported file extension")
		}
		if !strings.Contains(err.Error(), "error creating loader") {
			t.Errorf("Expected loader creation error, got: %v", err)
		}
	})

	t.Run("non-existent file", func(t *testing.T) {
		config := &Config{
			Sync: SyncConfig{
				FilePath: "non_existent_file.csv",
				Columns:  []string{"id", "name"},
			},
		}

		_, err := loadDataFromFile(config)
		if err == nil {
			t.Error("Expected error for non-existent file")
		}
	})
}

// Multi-table test helper structs
type CategoryTestRecord struct {
	ID          int
	Name        string
	Description string
}

type ProductTestRecord struct {
	ID         int
	Name       string
	CategoryID int
	Price      float64
}

type OrderTestRecord struct {
	ID         int
	CustomerID int
	Total      float64
	Status     string
}

type OrderItemTestRecord struct {
	ID        int
	OrderID   int
	ProductID int
	Quantity  int
	UnitPrice float64
}

// e2eSetupMultiTableTestDB creates tables for multi-table E2E tests
func e2eSetupMultiTableTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("mysql", e2eTestDBDSN)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// Drop tables in reverse dependency order (child → parent)
	// Based on foreign key dependencies in create_business_tables.sql:
	tablesToDrop := []string{
		"inventory_logs",  // References: orders, products, warehouses
		"order_items",     // References: orders, products, warehouses
		"orders",          // References: customers
		"products",        // References: categories
		"customers",       // Independent
		"warehouses",      // Independent
		"categories",      // Independent
	}
	for _, tableName := range tablesToDrop {
		_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
		if err != nil {
			t.Fatalf("Failed to drop table %s: %v", tableName, err)
		}
	}

	// Create tables in dependency order (parent → child)
	createCategoriesSQL := `
CREATE TABLE categories (
	id INT PRIMARY KEY,
	name VARCHAR(255) NOT NULL,
	description TEXT
);`
	_, err = db.Exec(createCategoriesSQL)
	if err != nil {
		t.Fatalf("Failed to create categories table: %v", err)
	}

	createProductsSQL := `
CREATE TABLE products (
	id INT PRIMARY KEY,
	name VARCHAR(255) NOT NULL,
	category_id INT NOT NULL,
	price DECIMAL(10,2) NOT NULL,
	FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
);`
	_, err = db.Exec(createProductsSQL)
	if err != nil {
		t.Fatalf("Failed to create products table: %v", err)
	}

	createOrdersSQL := `
CREATE TABLE orders (
	id INT PRIMARY KEY,
	customer_id INT NOT NULL,
	total DECIMAL(10,2) NOT NULL,
	status VARCHAR(50) NOT NULL
);`
	_, err = db.Exec(createOrdersSQL)
	if err != nil {
		t.Fatalf("Failed to create orders table: %v", err)
	}

	createOrderItemsSQL := `
CREATE TABLE order_items (
	id INT PRIMARY KEY,
	order_id INT NOT NULL,
	product_id INT NOT NULL,
	quantity INT NOT NULL,
	unit_price DECIMAL(10,2) NOT NULL,
	FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
	FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
);`
	_, err = db.Exec(createOrderItemsSQL)
	if err != nil {
		t.Fatalf("Failed to create order_items table: %v", err)
	}

	return db
}

// e2eTearDownMultiTableTestDB drops all multi-table test tables
func e2eTearDownMultiTableTestDB(_ *testing.T, db *sql.DB) {
	if db == nil {
		return
	}

	// Drop tables in reverse dependency order (child → parent)
	// Based on foreign key dependencies in create_business_tables.sql:
	// inventory_logs → products, warehouses, orders
	// order_items → orders, products, warehouses  
	// orders → customers
	// products → categories
	tablesToDrop := []string{
		"inventory_logs",  // References: orders, products, warehouses
		"order_items",     // References: orders, products, warehouses
		"orders",          // References: customers
		"products",        // References: categories
		"customers",       // Independent
		"warehouses",      // Independent
		"categories",      // Independent
	}
	
	for _, tableName := range tablesToDrop {
		_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
		if err != nil {
			log.Printf("Failed to drop table %s during teardown: %v", tableName, err)
		}
	}
	db.Close()
}

// Helper functions to verify multi-table database state
func e2eVerifyCategoriesDBState(t *testing.T, db *sql.DB, expectedRecords []CategoryTestRecord) {
	t.Helper()
	rows, err := db.Query("SELECT id, name, description FROM categories ORDER BY id ASC")
	if err != nil {
		t.Fatalf("Failed to query categories: %v", err)
	}
	defer rows.Close()

	var actualRecords []CategoryTestRecord
	for rows.Next() {
		var r CategoryTestRecord
		if err := rows.Scan(&r.ID, &r.Name, &r.Description); err != nil {
			t.Fatalf("Failed to scan category row: %v", err)
		}
		actualRecords = append(actualRecords, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Error during rows iteration: %v", err)
	}

	if len(actualRecords) != len(expectedRecords) {
		t.Fatalf("Categories: Expected %d records, but got %d. Actual: %+v", len(expectedRecords), len(actualRecords), actualRecords)
	}

	for i, expected := range expectedRecords {
		actual := actualRecords[i]
		if actual.ID != expected.ID || actual.Name != expected.Name || actual.Description != expected.Description {
			t.Errorf("Category record mismatch at index %d. Expected: %+v, Got: %+v", i, expected, actual)
		}
	}
}

func e2eVerifyProductsDBState(t *testing.T, db *sql.DB, expectedRecords []ProductTestRecord) {
	t.Helper()
	rows, err := db.Query("SELECT id, name, category_id, price FROM products ORDER BY id ASC")
	if err != nil {
		t.Fatalf("Failed to query products: %v", err)
	}
	defer rows.Close()

	var actualRecords []ProductTestRecord
	for rows.Next() {
		var r ProductTestRecord
		if err := rows.Scan(&r.ID, &r.Name, &r.CategoryID, &r.Price); err != nil {
			t.Fatalf("Failed to scan product row: %v", err)
		}
		actualRecords = append(actualRecords, r)
	}

	if len(actualRecords) != len(expectedRecords) {
		t.Fatalf("Products: Expected %d records, but got %d. Actual: %+v", len(expectedRecords), len(actualRecords), actualRecords)
	}

	for i, expected := range expectedRecords {
		actual := actualRecords[i]
		if actual.ID != expected.ID || actual.Name != expected.Name || actual.CategoryID != expected.CategoryID || actual.Price != expected.Price {
			t.Errorf("Product record mismatch at index %d. Expected: %+v, Got: %+v", i, expected, actual)
		}
	}
}

func e2eVerifyOrdersDBState(t *testing.T, db *sql.DB, expectedRecords []OrderTestRecord) {
	t.Helper()
	rows, err := db.Query("SELECT id, customer_id, total, status FROM orders ORDER BY id ASC")
	if err != nil {
		t.Fatalf("Failed to query orders: %v", err)
	}
	defer rows.Close()

	var actualRecords []OrderTestRecord
	for rows.Next() {
		var r OrderTestRecord
		if err := rows.Scan(&r.ID, &r.CustomerID, &r.Total, &r.Status); err != nil {
			t.Fatalf("Failed to scan order row: %v", err)
		}
		actualRecords = append(actualRecords, r)
	}

	if len(actualRecords) != len(expectedRecords) {
		t.Fatalf("Orders: Expected %d records, but got %d. Actual: %+v", len(expectedRecords), len(actualRecords), actualRecords)
	}

	for i, expected := range expectedRecords {
		actual := actualRecords[i]
		if actual.ID != expected.ID || actual.CustomerID != expected.CustomerID || actual.Total != expected.Total || actual.Status != expected.Status {
			t.Errorf("Order record mismatch at index %d. Expected: %+v, Got: %+v", i, expected, actual)
		}
	}
}

func e2eVerifyOrderItemsDBState(t *testing.T, db *sql.DB, expectedRecords []OrderItemTestRecord) {
	t.Helper()
	rows, err := db.Query("SELECT id, order_id, product_id, quantity, unit_price FROM order_items ORDER BY id ASC")
	if err != nil {
		t.Fatalf("Failed to query order_items: %v", err)
	}
	defer rows.Close()

	var actualRecords []OrderItemTestRecord
	for rows.Next() {
		var r OrderItemTestRecord
		if err := rows.Scan(&r.ID, &r.OrderID, &r.ProductID, &r.Quantity, &r.UnitPrice); err != nil {
			t.Fatalf("Failed to scan order_item row: %v", err)
		}
		actualRecords = append(actualRecords, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Error during rows iteration: %v", err)
	}

	if len(actualRecords) != len(expectedRecords) {
		t.Fatalf("Order Items: Expected %d records, but got %d. Actual: %+v", len(expectedRecords), len(actualRecords), actualRecords)
	}

	for i, expected := range expectedRecords {
		actual := actualRecords[i]
		if actual.ID != expected.ID || actual.OrderID != expected.OrderID || actual.ProductID != expected.ProductID || actual.Quantity != expected.Quantity || actual.UnitPrice != expected.UnitPrice {
			t.Errorf("Order Item record mismatch at index %d. Expected: %+v, Got: %+v", i, expected, actual)
		}
	}
}

// E2E Multi-table Tests
func TestE2EMultiTableSync_Initial(t *testing.T) {
	checkTestFileExists(t, "testdata/e2e_multi_table_config.yml")
	checkTestFileExists(t, "testdata/e2e_categories.json")
	checkTestFileExists(t, "testdata/e2e_products.csv")
	checkTestFileExists(t, "testdata/e2e_orders.json")
	checkTestFileExists(t, "testdata/e2e_order_items.csv")

	db := e2eSetupMultiTableTestDB(t)
	defer e2eTearDownMultiTableTestDB(t, db)

	err := RunApp("testdata/e2e_multi_table_config.yml", false)
	if err != nil {
		t.Fatalf("RunApp failed for multi-table sync: %v", err)
	}

	// Verify all tables have expected data
	expectedCategories := []CategoryTestRecord{
		{ID: 1, Name: "Electronics", Description: "Electronic products"},
		{ID: 2, Name: "Books", Description: "Books and literature"},
	}
	e2eVerifyCategoriesDBState(t, db, expectedCategories)

	expectedProducts := []ProductTestRecord{
		{ID: 1, Name: "Laptop", CategoryID: 1, Price: 1299.99},
		{ID: 2, Name: "Smartphone", CategoryID: 1, Price: 799.99},
		{ID: 3, Name: "Programming Book", CategoryID: 2, Price: 49.99},
	}
	e2eVerifyProductsDBState(t, db, expectedProducts)

	expectedOrders := []OrderTestRecord{
		{ID: 1, CustomerID: 101, Total: 1349.98, Status: "completed"},
		{ID: 2, CustomerID: 102, Total: 49.99, Status: "pending"},
	}
	e2eVerifyOrdersDBState(t, db, expectedOrders)

	expectedOrderItems := []OrderItemTestRecord{
		{ID: 1, OrderID: 1, ProductID: 1, Quantity: 1, UnitPrice: 1299.99},
		{ID: 2, OrderID: 1, ProductID: 2, Quantity: 1, UnitPrice: 799.99},
		{ID: 3, OrderID: 2, ProductID: 3, Quantity: 1, UnitPrice: 49.99},
	}
	e2eVerifyOrderItemsDBState(t, db, expectedOrderItems)
}

func TestE2EMultiTableSync_Overwrite(t *testing.T) {
	checkTestFileExists(t, "testdata/e2e_multi_table_overwrite_config.yml")
	checkTestFileExists(t, "testdata/e2e_categories_updated.json")
	checkTestFileExists(t, "testdata/e2e_products_updated.csv")

	db := e2eSetupMultiTableTestDB(t)
	defer e2eTearDownMultiTableTestDB(t, db)

	// Insert some initial data to verify overwrite works
	_, err := db.Exec("INSERT INTO categories (id, name, description) VALUES (1, 'Old Category', 'Old description')")
	if err != nil {
		t.Fatalf("Failed to insert initial categories: %v", err)
	}
	_, err = db.Exec("INSERT INTO products (id, name, category_id, price) VALUES (1, 'Old Product', 1, 99.99)")
	if err != nil {
		t.Fatalf("Failed to insert initial products: %v", err)
	}

	err = RunApp("testdata/e2e_multi_table_overwrite_config.yml", false)
	if err != nil {
		t.Fatalf("RunApp failed for multi-table overwrite sync: %v", err)
	}

	// Verify data has been completely replaced
	expectedCategories := []CategoryTestRecord{
		{ID: 3, Name: "Home & Garden", Description: "Home and garden products"},
		{ID: 4, Name: "Sports", Description: "Sports equipment"},
	}
	e2eVerifyCategoriesDBState(t, db, expectedCategories)

	expectedProducts := []ProductTestRecord{
		{ID: 1, Name: "Garden Tools", CategoryID: 3, Price: 89.99},
		{ID: 2, Name: "Tennis Racket", CategoryID: 4, Price: 149.99},
	}
	e2eVerifyProductsDBState(t, db, expectedProducts)
}

func TestE2EMultiTableSync_Diff(t *testing.T) {
	checkTestFileExists(t, "testdata/e2e_multi_table_diff_config.yml")
	checkTestFileExists(t, "testdata/e2e_categories_diff.json")
	checkTestFileExists(t, "testdata/e2e_products_diff.csv")

	db := e2eSetupMultiTableTestDB(t)
	defer e2eTearDownMultiTableTestDB(t, db)

	// Insert initial data to test diff operations
	// Categories: Keep 1 (update), delete 2, add 3
	_, err := db.Exec("INSERT INTO categories (id, name, description) VALUES (1, 'Electronics', 'Electronic products'), (2, 'Books', 'Books and literature')")
	if err != nil {
		t.Fatalf("Failed to insert initial categories: %v", err)
	}

	// Products: Keep 1 (update), delete 2 and 3, add 4
	_, err = db.Exec("INSERT INTO products (id, name, category_id, price) VALUES (1, 'Laptop', 1, 1299.99), (2, 'Smartphone', 1, 799.99), (3, 'Programming Book', 2, 49.99)")
	if err != nil {
		t.Fatalf("Failed to insert initial products: %v", err)
	}

	err = RunApp("testdata/e2e_multi_table_diff_config.yml", false)
	if err != nil {
		t.Fatalf("RunApp failed for multi-table diff sync: %v", err)
	}

	// Verify diff operations: updates, inserts, and deletes
	expectedCategories := []CategoryTestRecord{
		{ID: 1, Name: "Electronics", Description: "Updated: Electronic products and gadgets"}, // Updated
		{ID: 3, Name: "Home & Garden", Description: "Home and garden products"},               // Inserted
		// ID 2 should be deleted
	}
	e2eVerifyCategoriesDBState(t, db, expectedCategories)

	expectedProducts := []ProductTestRecord{
		{ID: 1, Name: "Gaming Laptop", CategoryID: 1, Price: 1599.99}, // Updated
		{ID: 4, Name: "Garden Hose", CategoryID: 3, Price: 29.99},     // Inserted
		// ID 2 and 3 should be deleted
	}
	e2eVerifyProductsDBState(t, db, expectedProducts)
}

func TestE2EMultiTableSync_DryRun(t *testing.T) {
	checkTestFileExists(t, "testdata/e2e_multi_table_config.yml")

	db := e2eSetupMultiTableTestDB(t)
	defer e2eTearDownMultiTableTestDB(t, db)

	// Insert some existing data to see what the dry run would do
	_, err := db.Exec("INSERT INTO categories (id, name, description) VALUES (99, 'Existing Category', 'Will be deleted in dry run')")
	if err != nil {
		t.Fatalf("Failed to insert initial test data: %v", err)
	}

	// Run in dry-run mode
	err = RunApp("testdata/e2e_multi_table_config.yml", true)
	if err != nil {
		t.Fatalf("RunApp failed for multi-table dry run: %v", err)
	}

	// Verify that no actual changes were made (original data should still exist)
	rows, err := db.Query("SELECT COUNT(*) FROM categories WHERE id = 99")
	if err != nil {
		t.Fatalf("Failed to query test data: %v", err)
	}
	defer rows.Close()

	var count int
	if rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			t.Fatalf("Failed to scan count: %v", err)
		}
	}

	if count != 1 {
		t.Errorf("Expected existing data to remain unchanged in dry-run mode, but category was modified")
	}

	// Verify that new data was not inserted
	rows2, err := db.Query("SELECT COUNT(*) FROM categories WHERE id IN (1, 2)")
	if err != nil {
		t.Fatalf("Failed to query for new data: %v", err)
	}
	defer rows2.Close()

	var newCount int
	if rows2.Next() {
		err = rows2.Scan(&newCount)
		if err != nil {
			t.Fatalf("Failed to scan new count: %v", err)
		}
	}

	if newCount != 0 {
		t.Errorf("Expected no new data to be inserted in dry-run mode, but found %d new records", newCount)
	}
}
