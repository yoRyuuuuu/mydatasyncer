package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	t.Run("empty config path uses default", func(t *testing.T) {
		// Move to a temporary directory where mydatasyncer.yml doesn't exist
		tempDir := t.TempDir()
		originalWd, _ := os.Getwd()
		defer os.Chdir(originalWd)
		os.Chdir(tempDir)
		
		// This will try to load "mydatasyncer.yml" which doesn't exist in temp dir
		// Should fall back to default config
		cfg := LoadConfig("")
		
		// Should get default config
		defaultCfg := NewDefaultConfig()
		if cfg.DB.DSN != defaultCfg.DB.DSN {
			t.Errorf("Expected default DSN %q, got %q", defaultCfg.DB.DSN, cfg.DB.DSN)
		}
		if cfg.Sync.FilePath != defaultCfg.Sync.FilePath {
			t.Errorf("Expected default FilePath %q, got %q", defaultCfg.Sync.FilePath, cfg.Sync.FilePath)
		}
	})

	t.Run("non-existent file falls back to default", func(t *testing.T) {
		cfg := LoadConfig("non_existent_file.yml")
		
		// Should get default config
		defaultCfg := NewDefaultConfig()
		if cfg.DB.DSN != defaultCfg.DB.DSN {
			t.Errorf("Expected default DSN %q, got %q", defaultCfg.DB.DSN, cfg.DB.DSN)
		}
		if cfg.Sync.FilePath != defaultCfg.Sync.FilePath {
			t.Errorf("Expected default FilePath %q, got %q", defaultCfg.Sync.FilePath, cfg.Sync.FilePath)
		}
	})

	t.Run("invalid YAML falls back to default", func(t *testing.T) {
		// Create a temporary file with invalid YAML
		tempDir := t.TempDir()
		tempFile := filepath.Join(tempDir, "invalid.yml")
		
		invalidYAML := `
invalid: yaml: content:
  - missing proper structure
  malformed: [
`
		err := os.WriteFile(tempFile, []byte(invalidYAML), 0644)
		if err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		cfg := LoadConfig(tempFile)
		
		// Should fall back to default config
		defaultCfg := NewDefaultConfig()
		if cfg.DB.DSN != defaultCfg.DB.DSN {
			t.Errorf("Expected default DSN %q, got %q", defaultCfg.DB.DSN, cfg.DB.DSN)
		}
		if cfg.Sync.FilePath != defaultCfg.Sync.FilePath {
			t.Errorf("Expected default FilePath %q, got %q", defaultCfg.Sync.FilePath, cfg.Sync.FilePath)
		}
	})

	t.Run("valid config file is loaded", func(t *testing.T) {
		tempDir := t.TempDir()
		tempFile := filepath.Join(tempDir, "valid.yml")
		
		validYAML := `
db:
  dsn: "test:password@tcp(localhost:3306)/testdb"

sync:
  filePath: "test.csv"
  tableName: "test_table"
  primaryKey: "id" 
  syncMode: "diff"
  columns: ["id", "name", "email"]
`
		err := os.WriteFile(tempFile, []byte(validYAML), 0644)
		if err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		cfg := LoadConfig(tempFile)
		
		if cfg.DB.DSN != "test:password@tcp(localhost:3306)/testdb" {
			t.Errorf("Expected DSN from file, got %q", cfg.DB.DSN)
		}
		if cfg.Sync.FilePath != "test.csv" {
			t.Errorf("Expected file path from file, got %q", cfg.Sync.FilePath)
		}
		if cfg.Sync.TableName != "test_table" {
			t.Errorf("Expected table name from file, got %q", cfg.Sync.TableName)
		}
		if cfg.Sync.PrimaryKey != "id" {
			t.Errorf("Expected primary key from file, got %q", cfg.Sync.PrimaryKey)
		}
		if cfg.Sync.SyncMode != "diff" {
			t.Errorf("Expected sync mode from file, got %q", cfg.Sync.SyncMode)
		}
	})

	t.Run("file permission error falls back to default", func(t *testing.T) {
		if os.Getuid() == 0 {
			t.Skip("Skipping permission test when running as root")
		}

		tempDir := t.TempDir()
		tempFile := filepath.Join(tempDir, "no_permission.yml")
		
		// Create file then remove read permission
		err := os.WriteFile(tempFile, []byte("test: content"), 0644)
		if err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		
		err = os.Chmod(tempFile, 0000) // No permissions
		if err != nil {
			t.Fatalf("Failed to change file permissions: %v", err)
		}
		defer os.Chmod(tempFile, 0644) // Restore permissions for cleanup

		cfg := LoadConfig(tempFile)
		
		// Should fall back to default config
		defaultCfg := NewDefaultConfig()
		if cfg.DB.DSN != defaultCfg.DB.DSN {
			t.Errorf("Expected default DSN %q, got %q", defaultCfg.DB.DSN, cfg.DB.DSN)
		}
		if cfg.Sync.FilePath != defaultCfg.Sync.FilePath {
			t.Errorf("Expected default FilePath %q, got %q", defaultCfg.Sync.FilePath, cfg.Sync.FilePath)
		}
	})
}

func TestSetDefaultsIfNeeded(t *testing.T) {
	t.Run("empty config gets all defaults", func(t *testing.T) {
		cfg := Config{}
		setDefaultsIfNeeded(&cfg)
		
		defaultCfg := NewDefaultConfig()
		
		// DSN should remain empty (not set to default)
		if cfg.DB.DSN != "" {
			t.Errorf("Expected empty DSN, got %q", cfg.DB.DSN)
		}
		if cfg.Sync.FilePath != defaultCfg.Sync.FilePath {
			t.Errorf("Expected default FilePath, got %q", cfg.Sync.FilePath)
		}
		if cfg.Sync.TableName != defaultCfg.Sync.TableName {
			t.Errorf("Expected default TableName, got %q", cfg.Sync.TableName)
		}
		if cfg.Sync.PrimaryKey != defaultCfg.Sync.PrimaryKey {
			t.Errorf("Expected default PrimaryKey, got %q", cfg.Sync.PrimaryKey)
		}
		if cfg.Sync.SyncMode != defaultCfg.Sync.SyncMode {
			t.Errorf("Expected default SyncMode, got %q", cfg.Sync.SyncMode)
		}
	})

	t.Run("partially filled config keeps existing values", func(t *testing.T) {
		cfg := Config{
			DB: DBConfig{
				DSN: "custom:dsn@tcp(localhost:3306)/customdb",
			},
			Sync: SyncConfig{
				FilePath:  "custom.csv",
				TableName: "custom_table",
				// PrimaryKey and SyncMode are empty, should get defaults
			},
		}
		
		setDefaultsIfNeeded(&cfg)
		
		defaultCfg := NewDefaultConfig()
		
		// Custom values should be preserved (DSN doesn't get overwritten to default)
		if cfg.DB.DSN != "custom:dsn@tcp(localhost:3306)/customdb" {
			t.Errorf("Custom DSN was overwritten: %q", cfg.DB.DSN)
		}
		if cfg.Sync.FilePath != "custom.csv" {
			t.Errorf("Custom FilePath was overwritten: %q", cfg.Sync.FilePath)
		}
		if cfg.Sync.TableName != "custom_table" {
			t.Errorf("Custom TableName was overwritten: %q", cfg.Sync.TableName)
		}
		
		// Empty values should get defaults
		if cfg.Sync.PrimaryKey != defaultCfg.Sync.PrimaryKey {
			t.Errorf("Expected default PrimaryKey for empty value, got %q", cfg.Sync.PrimaryKey)
		}
		if cfg.Sync.SyncMode != defaultCfg.Sync.SyncMode {
			t.Errorf("Expected default SyncMode for empty value, got %q", cfg.Sync.SyncMode)
		}
	})

	t.Run("empty columns slice gets defaults", func(t *testing.T) {
		cfg := Config{
			Sync: SyncConfig{
				Columns: []string{}, // Empty slice
			},
		}
		
		setDefaultsIfNeeded(&cfg)
		
		defaultCfg := NewDefaultConfig()
		if len(cfg.Sync.Columns) != len(defaultCfg.Sync.Columns) {
			t.Errorf("Expected default columns length %d, got %d", len(defaultCfg.Sync.Columns), len(cfg.Sync.Columns))
		}
	})

	t.Run("existing columns are preserved", func(t *testing.T) {
		customColumns := []string{"custom_col1", "custom_col2"}
		cfg := Config{
			Sync: SyncConfig{
				Columns: customColumns,
			},
		}
		
		setDefaultsIfNeeded(&cfg)
		
		if len(cfg.Sync.Columns) != len(customColumns) {
			t.Errorf("Custom columns were overwritten")
		}
		for i, col := range customColumns {
			if cfg.Sync.Columns[i] != col {
				t.Errorf("Custom column %d was changed from %q to %q", i, col, cfg.Sync.Columns[i])
			}
		}
	})
}

func TestValidateConfig(t *testing.T) {
	t.Run("valid config passes validation", func(t *testing.T) {
		cfg := Config{
			DB: DBConfig{
				DSN: "user:pass@tcp(localhost:3306)/db",
			},
			Sync: SyncConfig{
				FilePath:   "data.csv",
				TableName:  "test_table",
				PrimaryKey: "id",
				SyncMode:   "diff",
			},
		}
		
		err := ValidateConfig(cfg)
		if err != nil {
			t.Errorf("Expected no error for valid config, got: %v", err)
		}
	})

	t.Run("empty DSN fails validation", func(t *testing.T) {
		cfg := Config{
			DB: DBConfig{
				DSN: "", // Empty DSN
			},
			Sync: SyncConfig{
				FilePath:   "data.csv",
				TableName:  "test_table",
				PrimaryKey: "id",
				SyncMode:   "diff",
			},
		}
		
		err := ValidateConfig(cfg)
		if err == nil {
			t.Error("Expected error for empty DSN")
		}
		if !strings.Contains(err.Error(), "DSN is required") {
			t.Errorf("Expected DSN error message, got: %v", err)
		}
	})

	t.Run("empty file path fails validation", func(t *testing.T) {
		cfg := Config{
			DB: DBConfig{
				DSN: "user:pass@tcp(localhost:3306)/db",
			},
			Sync: SyncConfig{
				FilePath:   "", // Empty file path
				TableName:  "test_table",
				PrimaryKey: "id",
				SyncMode:   "diff",
			},
		}
		
		err := ValidateConfig(cfg)
		if err == nil {
			t.Error("Expected error for empty file path")
		}
		if !strings.Contains(err.Error(), "file path is required") {
			t.Errorf("Expected file path error message, got: %v", err)
		}
	})

	t.Run("empty table name fails validation", func(t *testing.T) {
		cfg := Config{
			DB: DBConfig{
				DSN: "user:pass@tcp(localhost:3306)/db",
			},
			Sync: SyncConfig{
				FilePath:   "data.csv",
				TableName:  "", // Empty table name
				PrimaryKey: "id",
				SyncMode:   "diff",
			},
		}
		
		err := ValidateConfig(cfg)
		if err == nil {
			t.Error("Expected error for empty table name")
		}
		if !strings.Contains(err.Error(), "table name is required") {
			t.Errorf("Expected table name error message, got: %v", err)
		}
	})

	t.Run("invalid sync mode fails validation", func(t *testing.T) {
		cfg := Config{
			DB: DBConfig{
				DSN: "user:pass@tcp(localhost:3306)/db",
			},
			Sync: SyncConfig{
				FilePath:   "data.csv",
				TableName:  "test_table",
				PrimaryKey: "id",
				SyncMode:   "invalid_mode", // Invalid sync mode
			},
		}
		
		err := ValidateConfig(cfg)
		if err == nil {
			t.Error("Expected error for invalid sync mode")
		}
		if !strings.Contains(err.Error(), "sync mode must be either") {
			t.Errorf("Expected sync mode error message, got: %v", err)
		}
	})

	t.Run("diff mode without primary key fails validation", func(t *testing.T) {
		cfg := Config{
			DB: DBConfig{
				DSN: "user:pass@tcp(localhost:3306)/db",
			},
			Sync: SyncConfig{
				FilePath:   "data.csv",
				TableName:  "test_table",
				PrimaryKey: "", // Empty primary key with diff mode
				SyncMode:   "diff",
			},
		}
		
		err := ValidateConfig(cfg)
		if err == nil {
			t.Error("Expected error for diff mode without primary key")
		}
		if !strings.Contains(err.Error(), "primary key is required for diff") {
			t.Errorf("Expected primary key error message, got: %v", err)
		}
	})

	t.Run("overwrite mode without primary key passes validation", func(t *testing.T) {
		cfg := Config{
			DB: DBConfig{
				DSN: "user:pass@tcp(localhost:3306)/db",
			},
			Sync: SyncConfig{
				FilePath:   "data.csv",
				TableName:  "test_table",
				PrimaryKey: "", // Empty primary key but overwrite mode
				SyncMode:   "overwrite",
			},
		}
		
		err := ValidateConfig(cfg)
		if err != nil {
			t.Errorf("Expected no error for overwrite mode without primary key, got: %v", err)
		}
	})
}

// Multi-table configuration tests
func TestValidateMultiTableConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		wantErr     bool
		errContains string
	}{
		{
			name: "Valid multi-table config with dependencies",
			config: Config{
				DB: DBConfig{DSN: "user:pass@tcp(localhost:3306)/db"},
				Tables: []TableSyncConfig{
					{
						Name:         "users",
						FilePath:     "./users.csv",
						PrimaryKey:   "id",
						SyncMode:     "diff",
						Dependencies: []string{},
					},
					{
						Name:         "orders",
						FilePath:     "./orders.csv",
						PrimaryKey:   "id",
						SyncMode:     "diff",
						Dependencies: []string{"users"},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "Empty tables array",
			config: Config{
				DB:     DBConfig{DSN: "user:pass@tcp(localhost:3306)/db"},
				Tables: []TableSyncConfig{},
				Sync:   SyncConfig{
					FilePath:  "", // Empty to bypass single table validation
					TableName: "", // Empty to bypass single table validation
				},
			},
			wantErr:     true,
			errContains: "at least one table configuration is required",
		},
		{
			name: "Circular dependency",
			config: Config{
				DB: DBConfig{DSN: "user:pass@tcp(localhost:3306)/db"},
				Tables: []TableSyncConfig{
					{
						Name:         "users",
						FilePath:     "./users.csv",
						PrimaryKey:   "id",
						SyncMode:     "diff",
						Dependencies: []string{"orders"},
					},
					{
						Name:         "orders",
						FilePath:     "./orders.csv",
						PrimaryKey:   "id",
						SyncMode:     "diff",
						Dependencies: []string{"users"},
					},
				},
			},
			wantErr:     true,
			errContains: "circular dependency",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConfig(tt.config)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateConfig() expected error but got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("ValidateConfig() error = %v, want error containing %v", err, tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateConfig() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestGetSyncOrder(t *testing.T) {
	tests := []struct {
		name                string
		tables              []TableSyncConfig
		expectedInsertOrder []string
		expectedDeleteOrder []string
		wantErr             bool
		errContains         string
	}{
		{
			name: "Simple linear dependency",
			tables: []TableSyncConfig{
				{Name: "users", Dependencies: []string{}},
				{Name: "orders", Dependencies: []string{"users"}},
				{Name: "order_items", Dependencies: []string{"orders"}},
			},
			expectedInsertOrder: []string{"users", "orders", "order_items"},
			expectedDeleteOrder: []string{"order_items", "orders", "users"},
			wantErr:             false,
		},
		{
			name: "Multiple dependencies",
			tables: []TableSyncConfig{
				{Name: "users", Dependencies: []string{}},
				{Name: "categories", Dependencies: []string{}},
				{Name: "products", Dependencies: []string{"categories"}},
				{Name: "orders", Dependencies: []string{"users"}},
				{Name: "order_items", Dependencies: []string{"orders", "products"}},
			},
			expectedInsertOrder: []string{"users", "categories", "orders", "products", "order_items"},
			expectedDeleteOrder: []string{"order_items", "products", "orders", "categories", "users"},
			wantErr:             false,
		},
		{
			name:        "No tables",
			tables:      []TableSyncConfig{},
			wantErr:     true,
			errContains: "no tables provided",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			insertOrder, deleteOrder, err := GetSyncOrder(tt.tables)

			if tt.wantErr {
				if err == nil {
					t.Errorf("GetSyncOrder() expected error but got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("GetSyncOrder() error = %v, want error containing %v", err, tt.errContains)
				}
				return
			}

			if err != nil {
				t.Errorf("GetSyncOrder() unexpected error = %v", err)
				return
			}

			// Check insert order matches expectation
			if !slicesEqual(insertOrder, tt.expectedInsertOrder) {
				t.Errorf("GetSyncOrder() insertOrder = %v, want %v", insertOrder, tt.expectedInsertOrder)
			}

			// Check delete order matches expectation  
			if !slicesEqual(deleteOrder, tt.expectedDeleteOrder) {
				t.Errorf("GetSyncOrder() deleteOrder = %v, want %v", deleteOrder, tt.expectedDeleteOrder)
			}
		})
	}
}

func TestIsMultiTableConfig(t *testing.T) {
	tests := []struct {
		name   string
		config Config
		want   bool
	}{
		{
			name: "Multi-table config",
			config: Config{
				Tables: []TableSyncConfig{{Name: "users"}},
			},
			want: true,
		},
		{
			name: "Single table config",
			config: Config{
				Sync: SyncConfig{TableName: "users"},
			},
			want: false,
		},
		{
			name:   "Empty config",
			config: Config{},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsMultiTableConfig(tt.config)
			if got != tt.want {
				t.Errorf("IsMultiTableConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Helper function for slice comparison
func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}