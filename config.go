package main

import (
	"fmt"
	"os"

	"github.com/goccy/go-yaml"
)

// DBConfig represents database connection settings
type DBConfig struct {
	DSN string `yaml:"dsn"` // Data Source Name (example: "user:password@tcp(127.0.0.1:3306)/dbname")
}

// SyncConfig represents data synchronization settings
type SyncConfig struct {
	FilePath         string   `yaml:"filePath"`         // Input file path
	TableName        string   `yaml:"tableName"`        // Target table name
	Columns          []string `yaml:"columns"`          // DB column names corresponding to file columns (order is important)
	TimestampColumns []string `yaml:"timestampColumns"` // Column names to set current timestamp on insert/update
	ImmutableColumns []string `yaml:"immutableColumns"` // Column names that should not be updated in diff mode
	PrimaryKey       string   `yaml:"primaryKey"`       // Primary key column name (required for differential update)
	SyncMode         string   `yaml:"syncMode"`         // "overwrite" or "diff" (differential)
	DeleteNotInFile  bool     `yaml:"deleteNotInFile"`  // Whether to delete records not in file when using diff mode
}

// Config represents configuration information
type Config struct {
	DB     DBConfig   `yaml:"db"`
	Sync   SyncConfig `yaml:"sync"`
	DryRun bool       `yaml:"dryRun"` // Enable dry-run mode
}

// NewDefaultConfig returns a Config struct with default values
func NewDefaultConfig() Config {
	return Config{
		DB: DBConfig{
			DSN: "", // DSN must be provided in config file
		},
		Sync: SyncConfig{
			FilePath:         "./testdata.csv",
			TableName:        "products",
			Columns:          []string{"id", "name", "price"}, // Match CSV column order
			PrimaryKey:       "id",
			SyncMode:         "diff", // "overwrite" or "diff"
			DeleteNotInFile:  true,
			TimestampColumns: []string{}, // Default to empty slice
			ImmutableColumns: []string{}, // Default to empty slice
		},
	}
}

// LoadConfig loads configuration from file specified by configPath
// Falls back to default configuration if the file doesn't exist or contains errors
func LoadConfig(configPath string) Config {
	// If no config path is provided, use the default
	if configPath == "" {
		configPath = "mydatasyncer.yml"
	}

	// Check for the config file
	data, err := os.ReadFile(configPath)

	// If config file not found or error reading, use default configuration
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("Config file '%s' not found. Using default configuration.\n", configPath)
		} else {
			fmt.Printf("Warning: Error reading config file %s: %v\n", configPath, err)
			fmt.Println("Using default configuration")
		}
		return NewDefaultConfig()
	}

	fmt.Printf("Using config file: %s\n", configPath)

	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		fmt.Printf("Warning: Could not parse config file %s: %v\n", configPath, err)
		fmt.Println("Using default configuration")
		return NewDefaultConfig()
	}

	// Set default values for fields not specified in the config file
	setDefaultsIfNeeded(&cfg)

	return cfg
}

// setDefaultsIfNeeded sets default values for fields that are not specified in the config file
func setDefaultsIfNeeded(cfg *Config) {
	defaultCfg := NewDefaultConfig()

	// DB defaults - DSN is not set to default, it must be provided

	// Sync defaults
	if cfg.Sync.FilePath == "" {
		cfg.Sync.FilePath = defaultCfg.Sync.FilePath
	}
	if cfg.Sync.TableName == "" {
		cfg.Sync.TableName = defaultCfg.Sync.TableName
	}
	if len(cfg.Sync.Columns) == 0 {
		cfg.Sync.Columns = defaultCfg.Sync.Columns
	}
	if cfg.Sync.PrimaryKey == "" {
		cfg.Sync.PrimaryKey = defaultCfg.Sync.PrimaryKey
	}
	if cfg.Sync.SyncMode == "" {
		cfg.Sync.SyncMode = defaultCfg.Sync.SyncMode
	}

	// Note: DryRun is a bool, so it will default to false if not specified in the config
}

// ValidateConfig checks if the configuration has all required values
func ValidateConfig(cfg Config) error {
	// Check DB configuration
	if cfg.DB.DSN == "" {
		return fmt.Errorf("database DSN is required")
	}

	// Check Sync configuration
	if cfg.Sync.FilePath == "" {
		return fmt.Errorf("sync file path is required")
	}
	if cfg.Sync.TableName == "" {
		return fmt.Errorf("table name is required")
	}
	// Note: If Columns are not specified in config, they will be derived from the CSV header.
	if cfg.Sync.SyncMode != "overwrite" && cfg.Sync.SyncMode != "diff" {
		return fmt.Errorf("sync mode must be either 'overwrite' or 'diff'")
	}
	if cfg.Sync.SyncMode == "diff" && cfg.Sync.PrimaryKey == "" {
		return fmt.Errorf("primary key is required for diff sync mode")
	}

	return nil
}
