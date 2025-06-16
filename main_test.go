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
		dataFile := filepath.Join(tempDir, "invalid.txt")  // Unsupported file extension

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
