package main

// Config represents configuration information
type Config struct {
	DB struct {
		DSN string // Data Source Name (example: "user:password@tcp(127.0.0.1:3306)/dbname")
	}
	Sync struct {
		FilePath        string   // Input file path
		TableName       string   // Target table name
		Columns         []string // DB column names corresponding to file columns (order is important)
		PrimaryKey      string   // Primary key column name (required for differential update)
		SyncMode        string   // "overwrite" or "diff" (differential)
		DeleteNotInFile bool     // Whether to delete records not in file when using diff mode
	}
}

// NewDefaultConfig returns a Config struct with default values
func NewDefaultConfig() Config {
	return Config{
		DB: struct{ DSN string }{DSN: "user:password@tcp(127.0.0.1:3306)/testdb?parseTime=true"},
		Sync: struct {
			FilePath        string
			TableName       string
			Columns         []string
			PrimaryKey      string
			SyncMode        string
			DeleteNotInFile bool
		}{
			FilePath:        "./testdata.csv",
			TableName:       "products",
			Columns:         []string{"id", "name", "price"}, // Match CSV column order
			PrimaryKey:      "id",
			SyncMode:        "diff", // "overwrite" or "diff"
			DeleteNotInFile: true,
		},
	}
}

// LoadConfig loads configuration from settings file
// Currently a stub implementation that returns default values
func LoadConfig() Config {
	// TODO: Replace with actual implementation using viper or similar library
	return NewDefaultConfig()
}

// Future extension points:
// - Command line argument overrides
// - Environment variable loading
// - Configuration file formats (YAML, JSON, TOML)
