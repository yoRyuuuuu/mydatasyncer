package infrastructure

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
)

// TestDBHelper provides database operations for tests
type TestDBHelper struct {
	env *TestEnvironment
}

// NewTestDBHelper creates a new database helper for the given environment
func NewTestDBHelper(env *TestEnvironment) *TestDBHelper {
	return &TestDBHelper{env: env}
}

// CreateTable creates a table with the given schema
func (helper *TestDBHelper) CreateTable(tableName, schema string) error {
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", tableName, schema)
	_, err := helper.env.DB.Exec(query)
	return err
}

// CreateStandardTestTable creates the standard test table used in most tests
func (helper *TestDBHelper) CreateStandardTestTable() error {
	schema := `
		id VARCHAR(255) PRIMARY KEY,
		name VARCHAR(255),
		value VARCHAR(255),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	`
	return helper.CreateTable("test_table", schema)
}

// CreateMultiTableSchema creates the multi-table schema for dependency tests
func (helper *TestDBHelper) CreateMultiTableSchema() error {
	// Create categories table (parent)
	categoriesSchema := `
		id INT PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		description TEXT
	`
	if err := helper.CreateTable("categories", categoriesSchema); err != nil {
		return fmt.Errorf("failed to create categories table: %w", err)
	}

	// Create products table (depends on categories)
	productsSchema := `
		id INT PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		category_id INT NOT NULL,
		price DECIMAL(10,2) NOT NULL,
		FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
	`
	if err := helper.CreateTable("products", productsSchema); err != nil {
		return fmt.Errorf("failed to create products table: %w", err)
	}

	// Create orders table (independent)
	ordersSchema := `
		id INT PRIMARY KEY,
		customer_id INT NOT NULL,
		total DECIMAL(10,2) NOT NULL,
		status VARCHAR(50) NOT NULL
	`
	if err := helper.CreateTable("orders", ordersSchema); err != nil {
		return fmt.Errorf("failed to create orders table: %w", err)
	}

	// Create order_items table (depends on both orders and products)
	orderItemsSchema := `
		id INT PRIMARY KEY,
		order_id INT NOT NULL,
		product_id INT NOT NULL,
		quantity INT NOT NULL,
		unit_price DECIMAL(10,2) NOT NULL,
		FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
		FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
	`
	if err := helper.CreateTable("order_items", orderItemsSchema); err != nil {
		return fmt.Errorf("failed to create order_items table: %w", err)
	}

	return nil
}

// CreateDataTypesTestTable creates a table for testing various data types
func (helper *TestDBHelper) CreateDataTypesTestTable() error {
	schema := `
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
	`
	return helper.CreateTable("data_types_test", schema)
}

// DropTable drops the specified table
func (helper *TestDBHelper) DropTable(tableName string) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	_, err := helper.env.DB.Exec(query)
	return err
}

// DropAllTables drops all test tables in the correct order (respecting foreign keys)
func (helper *TestDBHelper) DropAllTables() error {
	// Drop in reverse dependency order
	tables := []string{"order_items", "orders", "products", "categories", "data_types_test", "test_table"}

	for _, table := range tables {
		if err := helper.DropTable(table); err != nil {
			return fmt.Errorf("failed to drop table %s: %w", table, err)
		}
	}
	return nil
}

// TruncateTable removes all data from the specified table
func (helper *TestDBHelper) TruncateTable(tableName string) error {
	query := fmt.Sprintf("TRUNCATE TABLE %s", tableName)
	_, err := helper.env.DB.Exec(query)
	return err
}

// InsertTestData inserts test data into the specified table
func (helper *TestDBHelper) InsertTestData(tableName string, data []map[string]interface{}) error {
	if len(data) == 0 {
		return nil
	}

	// Get column names from first record
	var columns []string
	for col := range data[0] {
		columns = append(columns, col)
	}

	// Prepare insert statement
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		joinStrings(columns, ","),
		joinStrings(placeholders, ","))

	stmt, err := helper.env.DB.Prepare(query)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	// Insert each record
	for _, record := range data {
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			values[i] = record[col]
		}

		if _, err := stmt.Exec(values...); err != nil {
			return fmt.Errorf("failed to insert record: %w", err)
		}
	}

	return nil
}

// GetRowCount returns the number of rows in the specified table
func (helper *TestDBHelper) GetRowCount(tableName string) (int, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	var count int
	err := helper.env.DB.QueryRow(query).Scan(&count)
	return count, err
}

// TableExists checks if the specified table exists
func (helper *TestDBHelper) TableExists(tableName string) (bool, error) {
	query := `SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
			  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`
	var count int
	err := helper.env.DB.QueryRow(query, tableName).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// ExecuteInTransaction executes a function within a database transaction
func (helper *TestDBHelper) ExecuteInTransaction(fn func(*sql.Tx) error) error {
	tx, err := helper.env.DB.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r)
		}
	}()

	if err := fn(tx); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("transaction error: %v, rollback error: %v", err, rollbackErr)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// WaitForTable waits for a table to exist (useful for async operations)
func (helper *TestDBHelper) WaitForTable(tableName string, maxAttempts int) error {
	for i := 0; i < maxAttempts; i++ {
		exists, err := helper.TableExists(tableName)
		if err != nil {
			return err
		}
		if exists {
			return nil
		}
		// Wait a bit before next attempt
		if i < maxAttempts-1 {
			// In a real implementation, we'd use time.Sleep
			// For tests, we might want a configurable delay
		}
	}
	return fmt.Errorf("table %s did not exist after %d attempts", tableName, maxAttempts)
}

// Helper function to join strings
func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	if len(strs) == 1 {
		return strs[0]
	}

	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}
	return result
}

// GetIsolatedTestDB creates an isolated test database for a specific test
// This is a convenience function that wraps the environment manager
func GetIsolatedTestDB(t *testing.T, testName string) (*TestEnvironment, *TestDBHelper) {
	t.Helper()

	// Create or get shared environment manager
	envManager := getOrCreateEnvManager()

	// Create isolated environment
	env, err := envManager.CreateEnvironment(testName)
	if err != nil {
		t.Fatalf("Failed to create isolated test environment: %v", err)
	}

	// Register cleanup with test
	t.Cleanup(func() {
		if err := envManager.CleanupEnvironment(env.ID); err != nil {
			t.Logf("Warning: failed to cleanup test environment %s: %v", env.ID, err)
		}
	})

	helper := NewTestDBHelper(env)
	return env, helper
}

// Singleton environment manager for tests
var globalEnvManager *TestEnvManager

func getOrCreateEnvManager() *TestEnvManager {
	if globalEnvManager == nil {
		globalEnvManager = NewTestEnvManager()
	}
	return globalEnvManager
}
