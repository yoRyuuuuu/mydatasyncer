package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"sort" 
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/go-cmp/cmp"
)

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()

	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("MYSQL_PORT")
	if port == "" {
		port = "3306"
	}
	user := os.Getenv("MYSQL_USER")
	if user == "" {
		user = "root"
	}
	password := os.Getenv("MYSQL_PASSWORD")
	if password == "" {
		password = "root"
	}

	rootDSN := fmt.Sprintf("%s:%s@tcp(%s:%s)/", user, password, host, port)
	db, err := sql.Open("mysql", rootDSN)
	if err != nil {
		t.Fatalf("Failed to open root database: %v", err)
	}

	dbname := os.Getenv("MYSQL_DATABASE")
	if dbname == "" {
		dbname = "test_db"
	}

	_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	if err != nil {
		t.Fatalf("Failed to drop test database: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s", dbname))
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	db.Close()
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", user, password, host, port, dbname)
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Failed to open test database: %v", err)
	}

	_, err = db.Exec(`
CREATE TABLE IF NOT EXISTS test_table (
id VARCHAR(255) PRIMARY KEY,
name VARCHAR(255),
value VARCHAR(255),
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)
`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	return db
}

func cleanupTestData(t *testing.T, db *sql.DB) {
	t.Helper()

	_, err := db.Exec("TRUNCATE TABLE test_table")
	if err != nil {
		t.Fatalf("Failed to clean test table: %v", err)
	}
}

func createTestConfig() Config {
	return Config{
		Sync: SyncConfig{
			TableName:        "test_table",
			PrimaryKey:       "id",
			Columns:          []string{"id", "name", "value"},
			TimestampColumns: []string{"created_at", "updated_at"},
			SyncMode:         "diff",
			DeleteNotInFile:  true,
		},
	}
}

func TestSyncOverwrite(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := createTestConfig()
	config.Sync.SyncMode = "overwrite"

	t.Run("sync to empty database", func(t *testing.T) {
		cleanupTestData(t, db)

		fileRecords := []DataRecord{
			{"id": "1", "name": "test1", "value": "value1"},
			{"id": "2", "name": "test2", "value": "value2"},
		}

		err := syncData(context.Background(), db, config, fileRecords)
		if err != nil {
			t.Fatalf("Failed to sync data: %v", err)
		}

		rows, err := db.Query("SELECT id, name, value FROM test_table ORDER BY id")
		if err != nil {
			t.Fatalf("Failed to query results: %v", err)
		}
		defer rows.Close()

		var result []DataRecord
		for rows.Next() {
			var id, name, value string
			if err := rows.Scan(&id, &name, &value); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			result = append(result, DataRecord{"id": id, "name": name, "value": value})
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("Error during row iteration: %v", err)
		}

		if diff := cmp.Diff(result, fileRecords); diff != "" {
			t.Errorf("Sync result mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("overwrite existing data", func(t *testing.T) {
		cleanupTestData(t, db)

		_, err := db.Exec("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", "1", "old1", "old_value1")
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}

		fileRecords := []DataRecord{
			{"id": "1", "name": "new1", "value": "new_value1"},
			{"id": "2", "name": "new2", "value": "new_value2"},
		}

		err = syncData(context.Background(), db, config, fileRecords)
		if err != nil {
			t.Fatalf("Failed to sync data: %v", err)
		}

		rows, err := db.Query("SELECT id, name, value FROM test_table ORDER BY id")
		if err != nil {
			t.Fatalf("Failed to query results: %v", err)
		}
		defer rows.Close()

		var result []DataRecord
		for rows.Next() {
			var id, name, value string
			if err := rows.Scan(&id, &name, &value); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			result = append(result, DataRecord{"id": id, "name": name, "value": value})
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("Error during row iteration: %v", err)
		}

		if diff := cmp.Diff(result, fileRecords); diff != "" {
			t.Errorf("Sync result mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("overwrite with empty config.Sync.Columns (CSV header dictates)", func(t *testing.T) {
		cleanupTestData(t, db)
		localConfig := createTestConfig()
		localConfig.Sync.Columns = []string{} // Empty
		localConfig.Sync.SyncMode = "overwrite"

		// Insert some initial data that should be wiped
		_, err := db.Exec("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
			"0", "initial_data", "initial_value")
		if err != nil {
			t.Fatalf("Failed to insert initial test data: %v", err)
		}

		// File has an extra column not in DB.
		fileRecords := []DataRecord{
			{"id": "1", "name": "file_name1", "value": "file_value1", "extra_csv_col": "ignore_this"},
			{"id": "2", "name": "file_name2", "value": "file_value2", "extra_csv_col": "ignore_this_too"},
		}

		err = syncData(context.Background(), db, localConfig, fileRecords)
		if err != nil {
			t.Fatalf("Failed to sync data: %v", err)
		}

		// Verify: DB should only contain fileRecords, considering only common columns (id, name, value).
		expectedDB := []DataRecord{
			{"id": "1", "name": "file_name1", "value": "file_value1"},
			{"id": "2", "name": "file_name2", "value": "file_value2"},
		}

		rows, err := db.Query("SELECT id, name, value FROM test_table ORDER BY id")
		if err != nil {
			t.Fatalf("Failed to query results: %v", err)
		}
		defer rows.Close()

		var result []DataRecord
		for rows.Next() {
			var id, name, value string
			if err := rows.Scan(&id, &name, &value); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			result = append(result, DataRecord{"id": id, "name": name, "value": value})
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("Error during row iteration: %v", err)
		}

		if diff := cmp.Diff(expectedDB, result); diff != "" {
			t.Errorf("Sync result mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("overwrite with config.Sync.Columns filtering", func(t *testing.T) {
		cleanupTestData(t, db)
		localConfig := createTestConfig()
		localConfig.Sync.Columns = []string{"id", "name"} // Only sync id and name
		localConfig.Sync.SyncMode = "overwrite"

		// Insert some initial data that should be wiped
		_, err := db.Exec("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
			"0", "initial_data", "initial_value")
		if err != nil {
			t.Fatalf("Failed to insert initial test data: %v", err)
		}

		// File has id, name, value. "value" should be ignored during insert.
		fileRecords := []DataRecord{
			{"id": "1", "name": "file_name1", "value": "file_value1_ignored"},
			{"id": "2", "name": "file_name2", "value": "file_value2_ignored"},
		}

		err = syncData(context.Background(), db, localConfig, fileRecords)
		if err != nil {
			t.Fatalf("Failed to sync data: %v", err)
		}

		// Verify: DB should contain records with "id", "name" from file.
		// "value" column should be NULL/default as it was not part of sync columns.
		expectedDB := []DataRecord{
			{"id": "1", "name": "file_name1", "value": ""}, // value will be empty string if DB column is nullable string and not set
			{"id": "2", "name": "file_name2", "value": ""},
		}

		rows, err := db.Query("SELECT id, name, value FROM test_table ORDER BY id")
		if err != nil {
			t.Fatalf("Failed to query results: %v", err)
		}
		defer rows.Close()

		var result []DataRecord
		for rows.Next() {
			var idRes, nameRes string
			var valueRes sql.NullString
			if err := rows.Scan(&idRes, &nameRes, &valueRes); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			record := DataRecord{"id": idRes, "name": nameRes}
			if valueRes.Valid {
				record["value"] = valueRes.String
			} else {
				record["value"] = ""
			}
			result = append(result, record)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("Error during row iteration: %v", err)
		}

		if diff := cmp.Diff(expectedDB, result); diff != "" {
			t.Errorf("Sync result mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestSyncDiff(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := createTestConfig()
	config.Sync.SyncMode = "diff"

	t.Run("diff sync to empty database", func(t *testing.T) {
		cleanupTestData(t, db)

		fileRecords := []DataRecord{
			{"id": "1", "name": "test1", "value": "value1"},
			{"id": "2", "name": "test2", "value": "value2"},
		}

		err := syncData(context.Background(), db, config, fileRecords)
		if err != nil {
			t.Fatalf("Failed to sync data: %v", err)
		}

		rows, err := db.Query("SELECT id, name, value FROM test_table ORDER BY id")
		if err != nil {
			t.Fatalf("Failed to query results: %v", err)
		}
		defer rows.Close()

		var result []DataRecord
		for rows.Next() {
			var id, name, value string
			if err := rows.Scan(&id, &name, &value); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			result = append(result, DataRecord{"id": id, "name": name, "value": value})
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("Error during row iteration: %v", err)
		}

		if diff := cmp.Diff(result, fileRecords); diff != "" {
			t.Errorf("Sync result mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("diff sync with updates and deletes", func(t *testing.T) {
		cleanupTestData(t, db)

		_, err := db.Exec("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)",
			"1", "old1", "old_value1",
			"2", "old2", "old_value2",
			"3", "old3", "old_value3")
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}

		fileRecords := []DataRecord{
			{"id": "1", "name": "new1", "value": "new_value1"},
			{"id": "2", "name": "old2", "value": "old_value2"},
			{"id": "4", "name": "test4", "value": "value4"},
		}

		err = syncData(context.Background(), db, config, fileRecords)
		if err != nil {
			t.Fatalf("Failed to sync data: %v", err)
		}

		rows, err := db.Query("SELECT id, name, value FROM test_table ORDER BY id")
		if err != nil {
			t.Fatalf("Failed to query results: %v", err)
		}
		defer rows.Close()

		var result []DataRecord
		for rows.Next() {
			var id, name, value string
			if err := rows.Scan(&id, &name, &value); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			result = append(result, DataRecord{"id": id, "name": name, "value": value})
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("Error during row iteration: %v", err)
		}

		if diff := cmp.Diff(result, fileRecords); diff != "" {
			t.Errorf("Sync result mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("diff sync with immutable columns", func(t *testing.T) {
		db := setupTestDB(t) // db is already set up for TestSyncDiff
		defer db.Close()

		cleanupTestData(t, db)

		_, err := db.Exec("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
			"1", "original_name", "original_value")
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}

		localConfig := createTestConfig() // Use a local config for this subtest
		localConfig.Sync.SyncMode = "diff"
		localConfig.Sync.ImmutableColumns = []string{"name"}

		fileRecords := []DataRecord{
			{"id": "1", "name": "new_name", "value": "new_value"},
		}

		err = syncData(context.Background(), db, localConfig, fileRecords)
		if err != nil {
			t.Fatalf("Failed to sync data: %v", err)
		}

		var name, value string
		err = db.QueryRow("SELECT name, value FROM test_table WHERE id = ?", "1").Scan(&name, &value)
		if err != nil {
			t.Fatalf("Failed to query result: %v", err)
		}

		if name != "original_name" {
			t.Errorf("name was updated despite being immutable. Expected 'original_name', got '%s'", name)
		}
		if value != "new_value" {
			t.Errorf("value was not updated. Expected 'new_value', got '%s'", value)
		}
	})

	t.Run("diff sync with empty config.Sync.Columns (CSV header dictates)", func(t *testing.T) {
		cleanupTestData(t, db)
		localConfig := createTestConfig()
		localConfig.Sync.Columns = []string{} // Empty, so CSV header and DB cols determine sync
		localConfig.Sync.SyncMode = "diff"

		// DB has id, name, value
		_, err := db.Exec("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
			"1", "db_name1", "db_value1")
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}

		fileRecords := []DataRecord{
			{"id": "1", "name": "file_name1", "value": "file_value1", "extra_csv_col": "ignore_this"},
			{"id": "2", "name": "file_name2", "value": "file_value2", "extra_csv_col": "ignore_this_too"},
		}

		err = syncData(context.Background(), db, localConfig, fileRecords)
		if err != nil {
			t.Fatalf("Failed to sync data: %v", err)
		}

		expectedDB := []DataRecord{
			{"id": "1", "name": "file_name1", "value": "file_value1"},
			{"id": "2", "name": "file_name2", "value": "file_value2"},
		}

		rows, err := db.Query("SELECT id, name, value FROM test_table ORDER BY id")
		if err != nil {
			t.Fatalf("Failed to query results: %v", err)
		}
		defer rows.Close()

		var result []DataRecord
		for rows.Next() {
			var id, name, value string
			if err := rows.Scan(&id, &name, &value); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			result = append(result, DataRecord{"id": id, "name": name, "value": value})
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("Error during row iteration: %v", err)
		}

		if diff := cmp.Diff(expectedDB, result); diff != "" {
			t.Errorf("Sync result mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("diff sync with config.Sync.Columns filtering", func(t *testing.T) {
		cleanupTestData(t, db)
		localConfig := createTestConfig()
		localConfig.Sync.Columns = []string{"id", "name"}
		localConfig.Sync.SyncMode = "diff"

		_, err := db.Exec("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
			"1", "db_name1", "db_value1")
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}

		fileRecords := []DataRecord{
			{"id": "1", "name": "file_name1", "value": "file_value1"},
			{"id": "2", "name": "file_name2", "value": "file_value2"},
		}

		err = syncData(context.Background(), db, localConfig, fileRecords)
		if err != nil {
			t.Fatalf("Failed to sync data: %v", err)
		}

		expectedDB := []DataRecord{
			{"id": "1", "name": "file_name1", "value": "db_value1"},
			{"id": "2", "name": "file_name2", "value": ""},
		}

		rows, err := db.Query("SELECT id, name, value FROM test_table ORDER BY id")
		if err != nil {
			t.Fatalf("Failed to query results: %v", err)
		}
		defer rows.Close()

		var result []DataRecord
		for rows.Next() {
			var idRes, nameRes string
			var valueRes sql.NullString
			if err := rows.Scan(&idRes, &nameRes, &valueRes); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			record := DataRecord{"id": idRes, "name": nameRes}
			if valueRes.Valid {
				record["value"] = valueRes.String
			} else {
				record["value"] = ""
			}
			result = append(result, record)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("Error during row iteration: %v", err)
		}

		if diff := cmp.Diff(expectedDB, result); diff != "" {
			t.Errorf("Sync result mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestDiffData(t *testing.T) {
	config := createTestConfig()

	fileRecords := []DataRecord{
		{"id": "1", "name": "new1", "value": "new_value1"},
		{"id": "2", "name": "test2", "value": "value2"},
		{"id": "4", "name": "test4", "value": "value4"},
	}

	dbRecords := map[string]DataRecord{
		"1": {"id": "1", "name": "old1", "value": "old_value1"},
		"2": {"id": "2", "name": "test2", "value": "value2"},
		"3": {"id": "3", "name": "test3", "value": "value3"},
	}

	actualSyncCols := config.Sync.Columns
	toInsert, toUpdate, toDelete := diffData(config, fileRecords, dbRecords, actualSyncCols)

	expectedInsert := []DataRecord{{"id": "4", "name": "test4", "value": "value4"}}
	expectedUpdate := []UpdateOperation{
		{
			Before: DataRecord{"id": "1", "name": "old1", "value": "old_value1"},
			After:  DataRecord{"id": "1", "name": "new1", "value": "new_value1"},
		},
	}
	expectedDelete := []DataRecord{{"id": "3", "name": "test3", "value": "value3"}}

	if diff := cmp.Diff(toInsert, expectedInsert); diff != "" {
		t.Errorf("Insert mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(toUpdate, expectedUpdate); diff != "" {
		t.Errorf("Update mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(toDelete, expectedDelete); diff != "" {
		t.Errorf("Delete mismatch (-want +got):\n%s", diff)
	}
}

func TestDiffDataErrorCases(t *testing.T) {
	t.Run("empty primary key returns empty results", func(t *testing.T) {
		config := createTestConfig()
		config.Sync.PrimaryKey = "" // Empty primary key

		fileRecords := []DataRecord{
			{"id": "1", "name": "test1", "value": "value1"},
		}

		dbRecords := map[string]DataRecord{
			"1": {"id": "1", "name": "test1", "value": "value1"},
		}

		actualSyncCols := config.Sync.Columns
		toInsert, toUpdate, toDelete := diffData(config, fileRecords, dbRecords, actualSyncCols)

		// Should return empty slices when primary key is empty
		if len(toInsert) != 0 {
			t.Errorf("Expected empty insert slice, got %d items", len(toInsert))
		}
		if len(toUpdate) != 0 {
			t.Errorf("Expected empty update slice, got %d items", len(toUpdate))
		}
		if len(toDelete) != 0 {
			t.Errorf("Expected empty delete slice, got %d items", len(toDelete))
		}
	})
}

func TestSyncDataEmptyFile(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	t.Run("diff mode without deleteNotInFile with empty file", func(t *testing.T) {
		cleanupTestData(t, db)
		config := createTestConfig()
		config.Sync.SyncMode = "diff"
		config.Sync.DeleteNotInFile = false

		// Insert some initial data
		_, err := db.Exec("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", "1", "test1", "value1")
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}

		// Empty file records
		fileRecords := []DataRecord{}

		err = syncData(context.Background(), db, config, fileRecords)
		if err != nil {
			t.Fatalf("syncData failed: %v", err)
		}

		// Data should remain unchanged
		rows, err := db.Query("SELECT COUNT(*) FROM test_table")
		if err != nil {
			t.Fatalf("Failed to query count: %v", err)
		}
		defer rows.Close()

		var count int
		if rows.Next() {
			if err := rows.Scan(&count); err != nil {
				t.Fatalf("Failed to scan count: %v", err)
			}
		}

		if count != 1 {
			t.Errorf("Expected 1 record to remain, got %d", count)
		}
	})

	t.Run("diff mode with deleteNotInFile and empty file", func(t *testing.T) {
		cleanupTestData(t, db)
		config := createTestConfig()
		config.Sync.SyncMode = "diff"
		config.Sync.DeleteNotInFile = true

		// Insert some initial data
		_, err := db.Exec("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", "1", "test1", "value1")
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}

		// Empty file records
		fileRecords := []DataRecord{}

		err = syncData(context.Background(), db, config, fileRecords)
		if err != nil {
			t.Fatalf("syncData failed: %v", err)
		}

		// All data should be deleted
		rows, err := db.Query("SELECT COUNT(*) FROM test_table")
		if err != nil {
			t.Fatalf("Failed to query count: %v", err)
		}
		defer rows.Close()

		var count int
		if rows.Next() {
			if err := rows.Scan(&count); err != nil {
				t.Fatalf("Failed to scan count: %v", err)
			}
		}

		if count != 0 {
			t.Errorf("Expected 0 records after delete, got %d", count)
		}
	})

	t.Run("overwrite mode with empty file", func(t *testing.T) {
		cleanupTestData(t, db)
		config := createTestConfig()
		config.Sync.SyncMode = "overwrite"

		// Insert some initial data
		_, err := db.Exec("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", "1", "test1", "value1")
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}

		// Empty file records
		fileRecords := []DataRecord{}

		err = syncData(context.Background(), db, config, fileRecords)
		if err != nil {
			t.Fatalf("syncData failed: %v", err)
		}

		// All data should be deleted in overwrite mode
		rows, err := db.Query("SELECT COUNT(*) FROM test_table")
		if err != nil {
			t.Fatalf("Failed to query count: %v", err)
		}
		defer rows.Close()

		var count int
		if rows.Next() {
			if err := rows.Scan(&count); err != nil {
				t.Fatalf("Failed to scan count: %v", err)
			}
		}

		if count != 0 {
			t.Errorf("Expected 0 records after overwrite, got %d", count)
		}
	})
}

func TestDryRunOverwriteMode(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cleanupTestData(t, db)

	_, err := db.Exec("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?), (?, ?, ?)",
		"1", "old1", "old_value1",
		"2", "old2", "old_value2")
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	config := createTestConfig()
	config.DryRun = true
	config.Sync.SyncMode = "overwrite"

	fileRecords := []DataRecord{
		{"id": "3", "name": "new3", "value": "new_value3"},
		{"id": "4", "name": "new4", "value": "new_value4"},
	}

	err = syncData(context.Background(), db, config, fileRecords)
	if err != nil {
		t.Fatalf("Failed to execute dry run: %v", err)
	}

	rows, err := db.Query("SELECT id, name, value FROM test_table ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query results: %v", err)
	}
	defer rows.Close()

	var result []DataRecord
	for rows.Next() {
		var id, name, value string
		if err := rows.Scan(&id, &name, &value); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		result = append(result, DataRecord{"id": id, "name": name, "value": value})
	}

	expectedRecords := []DataRecord{
		{"id": "1", "name": "old1", "value": "old_value1"},
		{"id": "2", "name": "old2", "value": "old_value2"},
	}

	if diff := cmp.Diff(result, expectedRecords); diff != "" {
		t.Errorf("Database was modified in dry-run mode (-want +got):\n%s", diff)
	}
}

func TestDryRunDiffMode(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cleanupTestData(t, db)

	_, err := db.Exec("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)",
		"1", "old1", "old_value1",
		"2", "old2", "old_value2",
		"3", "old3", "old_value3")
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	config := createTestConfig()
	config.DryRun = true
	config.Sync.SyncMode = "diff"

	fileRecords := []DataRecord{
		{"id": "1", "name": "new1", "value": "new_value1"},
		{"id": "4", "name": "new4", "value": "new_value4"},
		{"id": "2", "name": "old2", "value": "old_value2"},
	}

	err = syncData(context.Background(), db, config, fileRecords)
	if err != nil {
		t.Fatalf("Failed to execute dry run: %v", err)
	}

	rows, err := db.Query("SELECT id, name, value FROM test_table ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query results: %v", err)
	}
	defer rows.Close()

	var result []DataRecord
	for rows.Next() {
		var id, name, value string
		if err := rows.Scan(&id, &name, &value); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		result = append(result, DataRecord{"id": id, "name": name, "value": value})
	}

	expectedRecords := []DataRecord{
		{"id": "1", "name": "old1", "value": "old_value1"},
		{"id": "2", "name": "old2", "value": "old_value2"},
		{"id": "3", "name": "old3", "value": "old_value3"},
	}

	if diff := cmp.Diff(result, expectedRecords); diff != "" {
		t.Errorf("Database was modified in dry-run mode (-want +got):\n%s", diff)
	}
}

func TestExecutionPlanString(t *testing.T) {
	plan := &ExecutionPlan{
		SyncMode:        "diff",
		TableName:       "test_table",
		FileRecordCount: 3,
		DbRecordCount:   3,
		InsertOperations: []DataRecord{
			{"id": "4", "name": "new4", "value": "new_value4"},
		},
		UpdateOperations: []UpdateOperation{
			{
				Before: DataRecord{"id": "1", "name": "old1", "value": "old_value1"},
				After:  DataRecord{"id": "1", "name": "new1", "value": "new_value1"},
			},
		},
		DeleteOperations: []DataRecord{
			{"id": "3", "name": "old3", "value": "old_value3"},
		},
		AffectedColumns:  []string{"id", "name", "value"},
		TimestampColumns: []string{"created_at", "updated_at"},
		ImmutableColumns: []string{"created_at"},
	}

	output := plan.String()

	expectedSections := []string{
		"[DRY-RUN Mode] Execution Plan",
		"Sync Mode: diff",
		"Target Table: test_table",
		"Records in File: 3",
		"Records in Database: 3",
		"DELETE Operations (1 records)",
		"INSERT Operations (1 records)",
		"UPDATE Operations (1 records)",
		"Immutable columns (will not be updated): [created_at]",
	}

	for _, section := range expectedSections {
		if !strings.Contains(output, section) {
			t.Errorf("ExecutionPlan.String() output missing section: %s", section)
		}
	}

	expectedDetails := []string{
		"id: 4",
		"name: new4",
		"value: new_value4",
		"name: old1 -> new1",
		"value: old_value1 -> new_value1",
		"id: 3",
		"name: old3",
		"value: old_value3",
	}

	for _, detail := range expectedDetails {
		if !strings.Contains(output, detail) {
			t.Errorf("ExecutionPlan.String() output missing detail: %s", detail)
		}
	}
}

// TestDetermineActualSyncColumns should be a top-level function
func TestPrimaryKey(t *testing.T) {
	t.Run("NewPrimaryKey with different types", func(t *testing.T) {
		tests := []struct {
			name     string
			value    any
			expected string
		}{
			{"nil value", nil, ""},
			{"string value", "test123", "test123"},
			{"int value", 42, "42"},
			{"float value", 3.14, "3.14"},
			{"bool true", true, "true"},
			{"bool false", false, "false"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				pk := NewPrimaryKey(tt.value)
				if pk.Str != tt.expected {
					t.Errorf("Expected string %q, got %q", tt.expected, pk.Str)
				}
				if !reflect.DeepEqual(pk.Value, tt.value) {
					t.Errorf("Expected value %v, got %v", tt.value, pk.Value)
				}
			})
		}
	})

	t.Run("PrimaryKey.Equal", func(t *testing.T) {
		tests := []struct {
			name     string
			pk1      PrimaryKey
			pk2      PrimaryKey
			expected bool
		}{
			{
				"same int values",
				NewPrimaryKey(123),
				NewPrimaryKey(123),
				true,
			},
			{
				"int and string with same value",
				NewPrimaryKey(123),
				NewPrimaryKey("123"),
				true,
			},
			{
				"different values",
				NewPrimaryKey(123),
				NewPrimaryKey(456),
				false,
			},
			{
				"both nil",
				NewPrimaryKey(nil),
				NewPrimaryKey(nil),
				true,
			},
			{
				"nil and non-nil",
				NewPrimaryKey(nil),
				NewPrimaryKey("test"),
				false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := tt.pk1.Equal(tt.pk2)
				if result != tt.expected {
					t.Errorf("Expected %v, got %v", tt.expected, result)
				}
			})
		}
	})

	t.Run("PrimaryKey.String", func(t *testing.T) {
		pk := NewPrimaryKey(123)
		if pk.String() != "123" {
			t.Errorf("Expected '123', got %q", pk.String())
		}

		pkNil := NewPrimaryKey(nil)
		if pkNil.String() != "" {
			t.Errorf("Expected empty string for nil value, got %q", pkNil.String())
		}
	})
}

func TestConvertValueToString(t *testing.T) {
	t.Run("basic types", func(t *testing.T) {
		tests := []struct {
			name     string
			value    any
			expected string
		}{
			{"nil", nil, ""},
			{"string", "hello", "hello"},
			{"bool true", true, "true"},
			{"bool false", false, "false"},
			{"int", 42, "42"},
			{"int8", int8(8), "8"},
			{"int16", int16(16), "16"},
			{"int32", int32(32), "32"},
			{"int64", int64(64), "64"},
			{"uint", uint(42), "42"},
			{"uint8", uint8(8), "8"},
			{"uint16", uint16(16), "16"},
			{"uint32", uint32(32), "32"},
			{"uint64", uint64(64), "64"},
			{"float32", float32(3.14), "3.14"},
			{"float64", 3.14159, "3.14159"},
			{"float64 whole number", 100.0, "100"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := convertValueToString(tt.value)
				if result != tt.expected {
					t.Errorf("Expected %q, got %q", tt.expected, result)
				}
			})
		}
	})

	t.Run("time type", func(t *testing.T) {
		testTime := time.Date(2023, 12, 25, 15, 30, 45, 0, time.UTC)
		result := convertValueToString(testTime)
		expected := "2023-12-25T15:30:45Z"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})

	t.Run("reflection fallback", func(t *testing.T) {
		type CustomType struct {
			Value string
		}
		custom := CustomType{Value: "test"}
		result := convertValueToString(custom)
		if !strings.Contains(result, "test") {
			t.Errorf("Expected result to contain 'test', got %q", result)
		}
	})

	t.Run("complex types through reflection", func(t *testing.T) {
		// Test reflection path for various types
		tests := []struct {
			name     string
			value    any
			expected string
		}{
			{"slice", []int{1, 2, 3}, "[1 2 3]"},
			{"map", map[string]int{"a": 1}, "map[a:1]"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := convertValueToString(tt.value)
				if result != tt.expected {
					t.Errorf("Expected %q, got %q", tt.expected, result)
				}
			})
		}
	})

	t.Run("default case fallback", func(t *testing.T) {
		// Test types that don't match any specific case and fall through to default
		tests := []struct {
			name          string
			value         any
			expectContains string
		}{
			{"pointer to string", func() *string { s := "test"; return &s }(), "0x"},
			{"channel", make(chan int), "0x"},
			{"function", func() int { return 42 }, "0x"},
			{"struct", struct{ Name string }{Name: "test"}, "test"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := convertValueToString(tt.value)
				if !strings.Contains(result, tt.expectContains) {
					t.Errorf("Expected result to contain %q, got %q", tt.expectContains, result)
				}
			})
		}
	})
}

func TestDetermineActualSyncColumns(t *testing.T) {
	tests := []struct {
		name                string
		csvHeaders          []string
		dbTableColumns      []string
		configSyncColumns   []string
		pkName              string
		expectedSyncColumns []string
		expectedError       string
	}{
		{
			name:                "config.Sync.Columns empty - perfect match",
			csvHeaders:          []string{"id", "name", "value"},
			dbTableColumns:      []string{"id", "name", "value", "created_at"},
			configSyncColumns:   []string{},
			pkName:              "id",
			expectedSyncColumns: []string{"id", "name", "value"},
			expectedError:       "",
		},
		{
			name:                "config.Sync.Columns empty - CSV has extra cols",
			csvHeaders:          []string{"id", "name", "value", "extra_csv_col"},
			dbTableColumns:      []string{"id", "name", "value"},
			configSyncColumns:   []string{},
			pkName:              "id",
			expectedSyncColumns: []string{"id", "name", "value"},
			expectedError:       "",
		},
		{
			name:                "config.Sync.Columns empty - DB has extra cols",
			csvHeaders:          []string{"id", "name", "value"},
			dbTableColumns:      []string{"id", "name", "value", "extra_db_col"},
			configSyncColumns:   []string{},
			pkName:              "id",
			expectedSyncColumns: []string{"id", "name", "value"},
			expectedError:       "",
		},
		{
			name:                "config.Sync.Columns empty - no common columns",
			csvHeaders:          []string{"col1", "col2"},
			dbTableColumns:      []string{"col3", "col4"},
			configSyncColumns:   []string{},
			pkName:              "id",
			expectedSyncColumns: nil,
			expectedError:       "no matching columns found between CSV header",
		},
		{
			name:                "config.Sync.Columns empty - PK not in common columns",
			csvHeaders:          []string{"name", "value"},
			dbTableColumns:      []string{"id", "name", "value"},
			configSyncColumns:   []string{},
			pkName:              "id",
			expectedSyncColumns: nil,
			expectedError:       "configured primary key 'id' is not among the final actual sync columns",
		},
		{
			name:                "config.Sync.Columns empty - PK specified but not in DB",
			csvHeaders:          []string{"id", "name"},
			dbTableColumns:      []string{"name", "value"},
			configSyncColumns:   []string{},
			pkName:              "id",
			expectedSyncColumns: nil,
			expectedError:       "configured primary key 'id' is not among the final actual sync columns",
		},
		{
			name:                "config.Sync.Columns specified - perfect match",
			csvHeaders:          []string{"id", "name", "value"},
			dbTableColumns:      []string{"id", "name", "value", "created_at"},
			configSyncColumns:   []string{"id", "name", "value"},
			pkName:              "id",
			expectedSyncColumns: []string{"id", "name", "value"},
			expectedError:       "",
		},
		{
			name:                "config.Sync.Columns specified - filters CSV/DB common columns",
			csvHeaders:          []string{"id", "name", "value", "extra_csv_col"},
			dbTableColumns:      []string{"id", "name", "value", "extra_db_col"},
			configSyncColumns:   []string{"id", "name"},
			pkName:              "id",
			expectedSyncColumns: []string{"id", "name"},
			expectedError:       "",
		},
		{
			name:                "config.Sync.Columns specified - one col not in CSV",
			csvHeaders:          []string{"id", "name"},
			dbTableColumns:      []string{"id", "name", "value"},
			configSyncColumns:   []string{"id", "name", "value"},
			pkName:              "id",
			expectedSyncColumns: []string{"id", "name"},
			expectedError:       "",
		},
		{
			name:                "config.Sync.Columns specified - one col not in DB",
			csvHeaders:          []string{"id", "name", "value"},
			dbTableColumns:      []string{"id", "name"},
			configSyncColumns:   []string{"id", "name", "value"},
			pkName:              "id",
			expectedSyncColumns: []string{"id", "name"},
			expectedError:       "",
		},
		{
			name:                "config.Sync.Columns specified - no resulting common columns",
			csvHeaders:          []string{"id", "name"},
			dbTableColumns:      []string{"id", "name"},
			configSyncColumns:   []string{"value1", "value2"},
			pkName:              "id",
			expectedSyncColumns: nil,
			expectedError:       "no matching columns after filtering with config.Sync.Columns",
		},
		{
			name:                "config.Sync.Columns specified - PK in config but not in CSV after filter",
			csvHeaders:          []string{"id", "name", "value"},
			dbTableColumns:      []string{"id", "name", "value"},
			configSyncColumns:   []string{"name", "value"},
			pkName:              "id",
			expectedSyncColumns: nil,
			expectedError:       "configured primary key 'id' is not among the final actual sync columns",
		},
		{
			name:                "CSV header empty",
			csvHeaders:          []string{},
			dbTableColumns:      []string{"id", "name", "value"},
			configSyncColumns:   []string{"id", "name"},
			pkName:              "id",
			expectedSyncColumns: nil,
			expectedError:       "CSV header is empty",
		},
		{
			name:                "No PK specified - config.Sync.Columns empty",
			csvHeaders:          []string{"colA", "colB"},
			dbTableColumns:      []string{"colA", "colB", "colC"},
			configSyncColumns:   []string{},
			pkName:              "",
			expectedSyncColumns: []string{"colA", "colB"},
			expectedError:       "",
		},
		{
			name:                "No PK specified - config.Sync.Columns present",
			csvHeaders:          []string{"colA", "colB", "colD"},
			dbTableColumns:      []string{"colA", "colB", "colC"},
			configSyncColumns:   []string{"colA", "colB"},
			pkName:              "",
			expectedSyncColumns: []string{"colA", "colB"},
			expectedError:       "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := determineActualSyncColumns(tt.csvHeaders, tt.dbTableColumns, tt.configSyncColumns, tt.pkName)

			if tt.expectedError != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.expectedError)
				}
				if !strings.Contains(err.Error(), tt.expectedError) {
					t.Errorf("expected error string %q to be part of actual error %q", tt.expectedError, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got: %v", err)
				}
				sort.Strings(actual)
				sort.Strings(tt.expectedSyncColumns)
				if diff := cmp.Diff(actual, tt.expectedSyncColumns); diff != "" {
					t.Errorf("Sync columns mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestValidateDiffSyncRequirements(t *testing.T) {
	t.Run("valid diff sync config", func(t *testing.T) {
		config := createTestConfig()
		config.Sync.SyncMode = "diff"
		config.Sync.PrimaryKey = "id"
		syncCols := []string{"id", "name", "value"}
		
		err := validateDiffSyncRequirements(config, syncCols)
		if err != nil {
			t.Errorf("Expected no error for valid diff sync config, got: %v", err)
		}
	})

	t.Run("diff sync without primary key", func(t *testing.T) {
		config := createTestConfig()
		config.Sync.SyncMode = "diff"
		config.Sync.PrimaryKey = ""
		syncCols := []string{"name", "value"}
		
		err := validateDiffSyncRequirements(config, syncCols)
		if err == nil {
			t.Error("Expected error for diff sync without primary key")
		}
		if !strings.Contains(err.Error(), "primary key") {
			t.Errorf("Expected error message to contain 'primary key', got: %v", err)
		}
	})

	t.Run("primary key not in sync columns", func(t *testing.T) {
		config := createTestConfig()
		config.Sync.SyncMode = "diff"
		config.Sync.PrimaryKey = "id"
		syncCols := []string{"name", "value"} // id not included
		
		err := validateDiffSyncRequirements(config, syncCols)
		if err == nil {
			t.Error("Expected error when primary key not in sync columns")
		}
		if !strings.Contains(err.Error(), "not among the actual sync columns") {
			t.Errorf("Expected error message about sync columns, got: %v", err)
		}
	})
}

func TestValidatePrimaryKeyInColumns(t *testing.T) {
	t.Run("primary key in columns", func(t *testing.T) {
		columns := []string{"id", "name", "value"}
		primaryKey := "id"
		
		err := validatePrimaryKeyInColumns(columns, primaryKey)
		if err != nil {
			t.Errorf("Expected no error when primary key is in columns, got: %v", err)
		}
	})

	t.Run("primary key not in columns", func(t *testing.T) {
		columns := []string{"name", "value"}
		primaryKey := "id"
		
		err := validatePrimaryKeyInColumns(columns, primaryKey)
		if err == nil {
			t.Error("Expected error when primary key is not in columns")
		}
		if !strings.Contains(err.Error(), "not among the final actual sync columns") {
			t.Errorf("Expected specific error message, got: %v", err)
		}
	})

	t.Run("empty primary key", func(t *testing.T) {
		columns := []string{"name", "value"}
		primaryKey := ""
		
		err := validatePrimaryKeyInColumns(columns, primaryKey)
		if err != nil {
			t.Errorf("Expected no error for empty primary key, got: %v", err)
		}
	})
}

func TestGetTableColumns(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	t.Run("get columns from existing table", func(t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}
		defer tx.Rollback()

		columns, err := getTableColumns(context.Background(), tx, "test_table")
		if err != nil {
			t.Fatalf("Failed to get table columns: %v", err)
		}

		expectedColumns := []string{"id", "name", "value", "created_at", "updated_at"}
		sort.Strings(columns)
		sort.Strings(expectedColumns)

		if diff := cmp.Diff(expectedColumns, columns); diff != "" {
			t.Errorf("Column mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("get columns from non-existent table", func(t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}
		defer tx.Rollback()

		_, err = getTableColumns(context.Background(), tx, "non_existent_table")
		if err == nil {
			t.Error("Expected error for non-existent table")
		}
	})
}

func TestFindCommonColumns(t *testing.T) {
	tests := []struct {
		name     string
		headers  []string
		dbCols   []string
		expected []string
	}{
		{
			name:     "perfect match",
			headers:  []string{"id", "name", "value"},
			dbCols:   []string{"id", "name", "value"},
			expected: []string{"id", "name", "value"},
		},
		{
			name:     "headers have extra columns",
			headers:  []string{"id", "name", "value", "extra"},
			dbCols:   []string{"id", "name", "value"},
			expected: []string{"id", "name", "value"},
		},
		{
			name:     "db has extra columns",
			headers:  []string{"id", "name", "value"},
			dbCols:   []string{"id", "name", "value", "created_at"},
			expected: []string{"id", "name", "value"},
		},
		{
			name:     "no common columns",
			headers:  []string{"col1", "col2"},
			dbCols:   []string{"col3", "col4"},
			expected: nil,
		},
		{
			name:     "partial overlap",
			headers:  []string{"id", "name", "extra1"},
			dbCols:   []string{"id", "value", "extra2"},
			expected: []string{"id"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := findCommonColumns(tt.headers, tt.dbCols)
			if tt.expected != nil {
				sort.Strings(result)
				sort.Strings(tt.expected)
			}

			if diff := cmp.Diff(tt.expected, result); diff != "" {
				t.Errorf("Common columns mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestFilterColumnsByConfig(t *testing.T) {
	tests := []struct {
		name           string
		commonColumns  []string
		configColumns  []string
		expected       []string
	}{
		{
			name:           "empty config columns - return all common",
			commonColumns:  []string{"id", "name", "value"},
			configColumns:  []string{},
			expected:       []string{"id", "name", "value"},
		},
		{
			name:           "config filters to subset",
			commonColumns:  []string{"id", "name", "value"},
			configColumns:  []string{"id", "name"},
			expected:       []string{"id", "name"},
		},
		{
			name:           "config includes non-existent columns",
			commonColumns:  []string{"id", "name"},
			configColumns:  []string{"id", "name", "non_existent"},
			expected:       []string{"id", "name"},
		},
		{
			name:           "no overlap between config and common",
			commonColumns:  []string{"id", "name"},
			configColumns:  []string{"value", "other"},
			expected:       nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterColumnsByConfig(tt.commonColumns, tt.configColumns)
			if tt.expected != nil {
				sort.Strings(result)
				sort.Strings(tt.expected)
			}

			if diff := cmp.Diff(tt.expected, result); diff != "" {
				t.Errorf("Filtered columns mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestExtractPrimaryKeyValue(t *testing.T) {
	t.Run("extract existing key", func(t *testing.T) {
		record := DataRecord{
			"id":    "123",
			"name":  "test",
			"value": "test_value",
		}
		
		pk, exists := extractPrimaryKeyValue(record, "id")
		if !exists {
			t.Error("Expected key to exist")
		}
		if pk.Str != "123" {
			t.Errorf("Expected %q, got %q", "123", pk.Str)
		}
	})

	t.Run("extract non-existent key", func(t *testing.T) {
		record := DataRecord{
			"name":  "test",
			"value": "test_value",
		}
		
		_, exists := extractPrimaryKeyValue(record, "id")
		if exists {
			t.Error("Expected key to not exist")
		}
	})

	t.Run("extract nil value", func(t *testing.T) {
		record := DataRecord{
			"id":   nil,
			"name": "test",
		}
		
		_, exists := extractPrimaryKeyValue(record, "id")
		if exists {
			t.Error("Expected nil value to be treated as non-existent")
		}
	})
}

func TestCompareRecords(t *testing.T) {
	syncColumns := []string{"id", "name", "value"}
	primaryKey := "id"

	t.Run("identical records", func(t *testing.T) {
		record1 := DataRecord{"id": "1", "name": "test", "value": "value1"}
		record2 := DataRecord{"id": "1", "name": "test", "value": "value1"}
		
		result := compareRecords(record1, record2, syncColumns, primaryKey)
		if result {
			t.Error("Expected false for identical records (no difference)")
		}
	})

	t.Run("different records", func(t *testing.T) {
		record1 := DataRecord{"id": "1", "name": "test", "value": "value1"}
		record2 := DataRecord{"id": "1", "name": "test", "value": "value2"}
		
		result := compareRecords(record1, record2, syncColumns, primaryKey)
		if !result {
			t.Error("Expected true for different records")
		}
	})

	t.Run("records with extra columns ignored", func(t *testing.T) {
		record1 := DataRecord{"id": "1", "name": "test", "value": "value1", "extra": "ignore1"}
		record2 := DataRecord{"id": "1", "name": "test", "value": "value1", "extra": "ignore2"}
		
		result := compareRecords(record1, record2, syncColumns, primaryKey)
		if result {
			t.Error("Expected false when only non-sync columns differ")
		}
	})

	t.Run("primary key ignored in comparison", func(t *testing.T) {
		record1 := DataRecord{"id": "1", "name": "test", "value": "value1"}
		record2 := DataRecord{"id": "2", "name": "test", "value": "value1"}
		
		result := compareRecords(record1, record2, syncColumns, primaryKey)
		if result {
			t.Error("Expected false when only primary key differs (should be ignored)")
		}
	})
}
