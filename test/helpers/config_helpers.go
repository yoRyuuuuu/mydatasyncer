package helpers

import (
	"fmt"
	"github.com/goccy/go-yaml"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ConfigHelper provides utilities for creating test configurations
type ConfigHelper struct {
	t       *testing.T
	tempDir string
}

// NewConfigHelper creates a new config helper
func NewConfigHelper(t *testing.T) *ConfigHelper {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "config_test_")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	return &ConfigHelper{
		t:       t,
		tempDir: tempDir,
	}
}

// TestDBConfig represents database configuration for tests
type TestDBConfig struct {
	DSN string `yaml:"dsn"`
}

// TestSyncConfig represents synchronization configuration for tests
type TestSyncConfig struct {
	FilePath         string           `yaml:"filePath"`
	TableName        string           `yaml:"tableName"`
	Columns          []string         `yaml:"columns,omitempty"`
	PrimaryKey       string           `yaml:"primaryKey"`
	SyncMode         string           `yaml:"syncMode"`
	DeleteNotInFile  bool             `yaml:"deleteNotInFile,omitempty"`
	Timestamps       *TimestampConfig `yaml:"timestamps,omitempty"`
	ImmutableColumns []string         `yaml:"immutableColumns,omitempty"`
}

// TimestampConfig represents timestamp column configuration
type TimestampConfig struct {
	CreatedAt string `yaml:"createdAt,omitempty"`
	UpdatedAt string `yaml:"updatedAt,omitempty"`
}

// MultiTableConfig represents multi-table synchronization configuration
type MultiTableConfig struct {
	Tables []TestSyncConfig `yaml:"tables"`
}

// TestConfig represents the complete test configuration
type TestConfig struct {
	DB         TestDBConfig      `yaml:"db"`
	Sync       *TestSyncConfig   `yaml:"sync,omitempty"`
	MultiTable *MultiTableConfig `yaml:"multiTable,omitempty"`
}

// CreateBasicConfig creates a basic configuration for tests
func (ch *ConfigHelper) CreateBasicConfig() *TestConfig {
	return &TestConfig{
		DB: TestDBConfig{
			DSN: "test:test@tcp(localhost:3306)/testdb?parseTime=true",
		},
		Sync: &TestSyncConfig{
			FilePath:        "test.csv",
			TableName:       "test_table",
			PrimaryKey:      "id",
			SyncMode:        "diff",
			DeleteNotInFile: false,
		},
	}
}

// CreateOverwriteModeConfig creates a configuration for overwrite mode testing
func (ch *ConfigHelper) CreateOverwriteModeConfig() *TestConfig {
	config := ch.CreateBasicConfig()
	config.Sync.SyncMode = "overwrite"
	return config
}

// CreateDiffModeConfig creates a configuration for differential mode testing
func (ch *ConfigHelper) CreateDiffModeConfig() *TestConfig {
	config := ch.CreateBasicConfig()
	config.Sync.SyncMode = "diff"
	config.Sync.DeleteNotInFile = true
	return config
}

// CreateTimestampConfig creates a configuration with timestamp columns
func (ch *ConfigHelper) CreateTimestampConfig() *TestConfig {
	config := ch.CreateBasicConfig()
	config.Sync.Columns = []string{"id", "name", "value", "created_at", "updated_at"}
	config.Sync.Timestamps = &TimestampConfig{
		CreatedAt: "created_at",
		UpdatedAt: "updated_at",
	}
	config.Sync.ImmutableColumns = []string{"created_at"}
	return config
}

// CreateMultiTableConfig creates a configuration for multi-table testing
func (ch *ConfigHelper) CreateMultiTableConfig() *TestConfig {
	return &TestConfig{
		DB: TestDBConfig{
			DSN: "test:test@tcp(localhost:3306)/testdb?parseTime=true",
		},
		MultiTable: &MultiTableConfig{
			Tables: []TestSyncConfig{
				{
					FilePath:   "categories.json",
					TableName:  "categories",
					PrimaryKey: "id",
					SyncMode:   "diff",
					Columns:    []string{"id", "name", "description"},
				},
				{
					FilePath:   "products.csv",
					TableName:  "products",
					PrimaryKey: "id",
					SyncMode:   "diff",
					Columns:    []string{"id", "name", "category_id", "price"},
				},
			},
		},
	}
}

// CreateLargeDatasetConfig creates a configuration for large dataset testing
func (ch *ConfigHelper) CreateLargeDatasetConfig(datasetSize string) *TestConfig {
	config := ch.CreateBasicConfig()
	config.Sync.FilePath = fmt.Sprintf("large_dataset_%s.csv", datasetSize)
	config.Sync.TableName = "large_test_table"
	return config
}

// CreateDataTypesConfig creates a configuration for data types testing
func (ch *ConfigHelper) CreateDataTypesConfig() *TestConfig {
	config := ch.CreateBasicConfig()
	config.Sync.TableName = "data_types_test"
	config.Sync.Columns = []string{
		"id", "string_col", "bool_true_col", "bool_false_col",
		"int_col", "float_col", "large_int_col", "zero_col",
		"negative_int_col", "negative_float_col", "whole_number_float",
		"null_col", "rfc3339_time",
	}
	return config
}

// CreatePerformanceTestConfig creates a configuration for performance testing
func (ch *ConfigHelper) CreatePerformanceTestConfig(recordCount int) *TestConfig {
	config := ch.CreateBasicConfig()
	config.Sync.FilePath = fmt.Sprintf("performance_test_%d_records.csv", recordCount)
	config.Sync.TableName = "performance_test_table"
	return config
}

// SaveConfigToFile saves a configuration to a YAML file
func (ch *ConfigHelper) SaveConfigToFile(config *TestConfig, filename string) string {
	ch.t.Helper()

	filePath := filepath.Join(ch.tempDir, filename)

	// Create directory if needed
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		ch.t.Fatalf("Failed to create directory %s: %v", dir, err)
	}

	data, err := yaml.Marshal(config)
	if err != nil {
		ch.t.Fatalf("Failed to marshal config: %v", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		ch.t.Fatalf("Failed to write config file %s: %v", filePath, err)
	}

	return filePath
}

// CreateConfigWithCustomDB creates a configuration with custom database settings
func (ch *ConfigHelper) CreateConfigWithCustomDB(dsn string) *TestConfig {
	config := ch.CreateBasicConfig()
	config.DB.DSN = dsn
	return config
}

// CreateConfigWithColumns creates a configuration with specific columns
func (ch *ConfigHelper) CreateConfigWithColumns(columns []string) *TestConfig {
	config := ch.CreateBasicConfig()
	config.Sync.Columns = columns
	return config
}

// CreateInvalidConfig creates an intentionally invalid configuration for error testing
func (ch *ConfigHelper) CreateInvalidConfig(invalidationType string) *TestConfig {
	switch invalidationType {
	case "missing_primary_key":
		config := ch.CreateBasicConfig()
		config.Sync.PrimaryKey = ""
		return config

	case "invalid_sync_mode":
		config := ch.CreateBasicConfig()
		config.Sync.SyncMode = "invalid_mode"
		return config

	case "missing_file_path":
		config := ch.CreateBasicConfig()
		config.Sync.FilePath = ""
		return config

	case "missing_table_name":
		config := ch.CreateBasicConfig()
		config.Sync.TableName = ""
		return config

	default:
		ch.t.Fatalf("Unknown invalidation type: %s", invalidationType)
		return nil
	}
}

// CreateConfigForFileFormat creates a configuration for specific file format testing
func (ch *ConfigHelper) CreateConfigForFileFormat(format string) *TestConfig {
	config := ch.CreateBasicConfig()

	switch format {
	case "csv":
		config.Sync.FilePath = "test_data.csv"
	case "json":
		config.Sync.FilePath = "test_data.json"
	default:
		ch.t.Fatalf("Unsupported file format: %s", format)
	}

	return config
}

// CreateConfigWithTimeout creates a configuration with custom timeout settings
func (ch *ConfigHelper) CreateConfigWithTimeout(timeout time.Duration) *TestConfig {
	config := ch.CreateBasicConfig()
	// Add timeout configuration if supported by the actual config structure
	return config
}

// CreateStressTestConfig creates a configuration for stress testing
func (ch *ConfigHelper) CreateStressTestConfig() *TestConfig {
	config := ch.CreateBasicConfig()
	config.Sync.FilePath = "stress_test_data.csv"
	config.Sync.TableName = "stress_test_table"
	config.Sync.SyncMode = "diff"
	config.Sync.DeleteNotInFile = true
	return config
}

// CreateCircularDependencyConfig creates a configuration that would cause circular dependencies
func (ch *ConfigHelper) CreateCircularDependencyConfig() *TestConfig {
	return &TestConfig{
		DB: TestDBConfig{
			DSN: "test:test@tcp(localhost:3306)/testdb?parseTime=true",
		},
		MultiTable: &MultiTableConfig{
			Tables: []TestSyncConfig{
				{
					FilePath:   "table_a.csv",
					TableName:  "table_a",
					PrimaryKey: "id",
					SyncMode:   "diff",
				},
				{
					FilePath:   "table_b.csv",
					TableName:  "table_b",
					PrimaryKey: "id",
					SyncMode:   "diff",
				},
			},
		},
	}
}

// UpdateConfigFilePath updates the file path in a configuration
func (ch *ConfigHelper) UpdateConfigFilePath(config *TestConfig, newFilePath string) {
	if config.Sync != nil {
		config.Sync.FilePath = newFilePath
	}
}

// UpdateConfigTableName updates the table name in a configuration
func (ch *ConfigHelper) UpdateConfigTableName(config *TestConfig, newTableName string) {
	if config.Sync != nil {
		config.Sync.TableName = newTableName
	}
}

// GetTempDir returns the temporary directory for config files
func (ch *ConfigHelper) GetTempDir() string {
	return ch.tempDir
}

// CreateConfigVariation creates a configuration variation for parameterized tests
func (ch *ConfigHelper) CreateConfigVariation(baseName string, variations map[string]interface{}) *TestConfig {
	config := ch.CreateBasicConfig()

	for key, value := range variations {
		switch key {
		case "syncMode":
			config.Sync.SyncMode = value.(string)
		case "deleteNotInFile":
			config.Sync.DeleteNotInFile = value.(bool)
		case "tableName":
			config.Sync.TableName = value.(string)
		case "filePath":
			config.Sync.FilePath = value.(string)
		default:
			ch.t.Logf("Unknown configuration key: %s", key)
		}
	}

	return config
}

// ValidateConfig performs basic validation on a configuration
func (ch *ConfigHelper) ValidateConfig(config *TestConfig) []string {
	var errors []string

	if config.DB.DSN == "" {
		errors = append(errors, "Database DSN is required")
	}

	if config.Sync != nil {
		if config.Sync.FilePath == "" {
			errors = append(errors, "File path is required")
		}
		if config.Sync.TableName == "" {
			errors = append(errors, "Table name is required")
		}
		if config.Sync.PrimaryKey == "" {
			errors = append(errors, "Primary key is required")
		}
		if config.Sync.SyncMode != "diff" && config.Sync.SyncMode != "overwrite" {
			errors = append(errors, "Sync mode must be 'diff' or 'overwrite'")
		}
	}

	return errors
}

// GenerateRandomConfigName generates a random configuration file name
func (ch *ConfigHelper) GenerateRandomConfigName() string {
	timestamp := time.Now().UnixNano()
	return fmt.Sprintf("test_config_%d.yml", timestamp)
}
