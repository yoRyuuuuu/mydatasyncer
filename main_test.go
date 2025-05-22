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
	e2eTestDBDSN        = "user:password@tcp(127.0.0.1:3306)/testdb?parseTime=true"
	e2eTestTableName    = "test_table"
	e2eTestConfigSmall  = "testdata/config_small.yml"
	e2eTestDataSmallCSV = "testdata/small_data.csv"
)

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

func TestE2ESyncSmallData(t *testing.T) {
	// Check if dependent files exist
	if _, err := os.Stat(e2eTestConfigSmall); os.IsNotExist(err) {
		t.Fatalf("Test config file %s does not exist. Please create it first.", e2eTestConfigSmall)
	}
	if _, err := os.Stat(e2eTestDataSmallCSV); os.IsNotExist(err) {
		t.Fatalf("Test data file %s does not exist. Please create it first.", e2eTestDataSmallCSV)
	}

	db := e2eSetupTestDB(t)
	defer e2eTearDownTestDB(t, db)

	// Run the application
	err := RunApp(e2eTestConfigSmall, false)
	if err != nil {
		t.Fatalf("RunApp failed: %v", err)
	}

	// Verify data in the database
	rows, err := db.Query(fmt.Sprintf("SELECT id, name, email FROM %s ORDER BY id ASC", e2eTestTableName))
	if err != nil {
		t.Fatalf("Failed to query data from test table: %v", err)
	}
	defer rows.Close()

	type TestRecord struct {
		ID    int
		Name  string
		Email string
	}

	expectedRecords := []TestRecord{
		{ID: 1, Name: "John Doe", Email: "john.doe@example.com"},
		{ID: 2, Name: "Jane Smith", Email: "jane.smith@example.com"},
		{ID: 3, Name: "Alice Johnson", Email: "alice.johnson@example.com"},
	}

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
		t.Fatalf("Expected %d records, but got %d", len(expectedRecords), len(actualRecords))
	}

	for i, expected := range expectedRecords {
		actual := actualRecords[i]
		if actual.ID != expected.ID || actual.Name != expected.Name || actual.Email != expected.Email {
			t.Errorf("Record mismatch at index %d. Expected: %+v, Got: %+v", i, expected, actual)
		}
	}
}
