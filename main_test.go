package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

const (
	e2eTestDBDSN                   = "user:password@tcp(127.0.0.1:3306)/testdb?parseTime=true"
	e2eTestTableName               = "test_table"
	e2eTestConfigSmallCSV          = "testdata/config_small.yml"
	e2eTestDataSmallCSV            = "testdata/small_data.csv"
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
)

// TestRecord is a helper struct for verifying data in E2E tests
type TestRecord struct {
	ID    int
	Name  string
	Email string
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
