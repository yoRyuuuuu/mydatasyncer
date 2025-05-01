package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

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
	// amazonq-ignore-next-line
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
		db := setupTestDB(t)
		defer db.Close()

		cleanupTestData(t, db)

		_, err := db.Exec("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
			"1", "original_name", "original_value")
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}

		config := createTestConfig()
		config.Sync.SyncMode = "diff"
		config.Sync.ImmutableColumns = []string{"name"}

		fileRecords := []DataRecord{
			{"id": "1", "name": "new_name", "value": "new_value"},
		}

		err = syncData(context.Background(), db, config, fileRecords)
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

	toInsert, toUpdate, toDelete := diffData(config, fileRecords, dbRecords)

	expectedInsert := []DataRecord{{"id": "4", "name": "test4", "value": "value4"}}
	expectedUpdate := []DataRecord{{"id": "1", "name": "new1", "value": "new_value1"}}
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
