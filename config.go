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

// SyncConfig represents data synchronization settings (legacy single table config)
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

// TableSyncConfig represents synchronization settings for a single table
type TableSyncConfig struct {
	Name             string   `yaml:"name"`             // Target table name
	FilePath         string   `yaml:"filePath"`         // Input file path
	Columns          []string `yaml:"columns"`          // DB column names corresponding to file columns (order is important)
	TimestampColumns []string `yaml:"timestampColumns"` // Column names to set current timestamp on insert/update
	ImmutableColumns []string `yaml:"immutableColumns"` // Column names that should not be updated in diff mode
	PrimaryKey       string   `yaml:"primaryKey"`       // Primary key column name (required for differential update)
	SyncMode         string   `yaml:"syncMode"`         // "overwrite" or "diff" (differential)
	DeleteNotInFile  bool     `yaml:"deleteNotInFile"`  // Whether to delete records not in file when using diff mode
	Dependencies     []string `yaml:"dependencies"`     // List of table names this table depends on (foreign key parents)
}

// Config represents configuration information
type Config struct {
	DB     DBConfig          `yaml:"db"`
	Sync   SyncConfig        `yaml:"sync"`                      // Legacy single table sync config (for backward compatibility)
	Tables []TableSyncConfig `yaml:"tables,omitempty"`          // Multi-table sync config
	DryRun bool              `yaml:"dryRun"`                    // Enable dry-run mode
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

	// Check if using multi-table sync or legacy single table sync
	if len(cfg.Tables) > 0 || (cfg.Sync.FilePath == "" && cfg.Sync.TableName == "") {
		// Multi-table sync validation (either has Tables array or empty Sync config)
		return validateMultiTableConfig(cfg)
	} else {
		// Legacy single table sync validation
		return validateSingleTableConfig(cfg)
	}
}

// validateSingleTableConfig validates legacy single table configuration
func validateSingleTableConfig(cfg Config) error {
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

// validateMultiTableConfig validates multi-table configuration
func validateMultiTableConfig(cfg Config) error {
	if len(cfg.Tables) == 0 {
		return fmt.Errorf("at least one table configuration is required in tables array")
	}

	// First pass: collect all table names and validate basic fields
	tableNames := make(map[string]bool)
	for i, table := range cfg.Tables {
		// Check required fields
		if table.Name == "" {
			return fmt.Errorf("table[%d]: table name is required", i)
		}
		if table.FilePath == "" {
			return fmt.Errorf("table[%d] (%s): file path is required", i, table.Name)
		}
		if table.SyncMode != "overwrite" && table.SyncMode != "diff" {
			return fmt.Errorf("table[%d] (%s): sync mode must be either 'overwrite' or 'diff'", i, table.Name)
		}
		if table.SyncMode == "diff" && table.PrimaryKey == "" {
			return fmt.Errorf("table[%d] (%s): primary key is required for diff sync mode", i, table.Name)
		}

		// Check for duplicate table names
		if tableNames[table.Name] {
			return fmt.Errorf("table[%d]: duplicate table name '%s'", i, table.Name)
		}
		tableNames[table.Name] = true
	}

	// Second pass: validate dependencies exist in the configuration
	for i, table := range cfg.Tables {
		for _, dep := range table.Dependencies {
			if !tableNames[dep] {
				return fmt.Errorf("table[%d] (%s): dependency '%s' not found in configuration", i, table.Name, dep)
			}
		}
	}

	// Check for circular dependencies
	if err := validateNoCycles(cfg.Tables); err != nil {
		return fmt.Errorf("circular dependency detected: %w", err)
	}

	return nil
}

// validateNoCycles performs topological sort to detect circular dependencies
func validateNoCycles(tables []TableSyncConfig) error {
	// Build adjacency list for dependency graph
	graph := make(map[string][]string)
	inDegree := make(map[string]int)
	
	// Initialize all tables
	for _, table := range tables {
		graph[table.Name] = []string{}
		inDegree[table.Name] = 0
	}
	
	// Build dependency edges
	for _, table := range tables {
		for _, dep := range table.Dependencies {
			graph[dep] = append(graph[dep], table.Name)
			inDegree[table.Name]++
		}
	}
	
	// Kahn's algorithm for cycle detection
	queue := []string{}
	for tableName, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, tableName)
		}
	}
	
	processed := 0
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		processed++
		
		for _, neighbor := range graph[current] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}
	
	if processed != len(tables) {
		return fmt.Errorf("circular dependency exists in table configuration")
	}
	
	return nil
}

// GetSyncOrder determines the order of table synchronization based on dependencies
// Returns two slices: insertOrder (parent->child) and deleteOrder (child->parent)
func GetSyncOrder(tables []TableSyncConfig) (insertOrder []string, deleteOrder []string, err error) {
	if len(tables) == 0 {
		return nil, nil, fmt.Errorf("no tables provided")
	}

	// Build adjacency list for dependency graph
	graph := make(map[string][]string)
	inDegree := make(map[string]int)
	
	// Initialize all tables
	for _, table := range tables {
		graph[table.Name] = []string{}
		inDegree[table.Name] = 0
	}
	
	// Build dependency edges (parent -> child)
	for _, table := range tables {
		for _, dep := range table.Dependencies {
			graph[dep] = append(graph[dep], table.Name)
			inDegree[table.Name]++
		}
	}
	
	// Kahn's algorithm for topological sorting
	queue := []string{}
	for tableName, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, tableName)
		}
	}
	
	var sortedOrder []string
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		sortedOrder = append(sortedOrder, current)
		
		for _, neighbor := range graph[current] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}
	
	// Check if all tables were processed (no cycles)
	if len(sortedOrder) != len(tables) {
		return nil, nil, fmt.Errorf("circular dependency detected, cannot determine sync order")
	}
	
	// Insert order: parent -> child (same as topological order)
	insertOrder = make([]string, len(sortedOrder))
	copy(insertOrder, sortedOrder)
	
	// Delete order: child -> parent (reverse of topological order)
	deleteOrder = make([]string, len(sortedOrder))
	for i, table := range sortedOrder {
		deleteOrder[len(sortedOrder)-1-i] = table
	}
	
	return insertOrder, deleteOrder, nil
}

// GetTableConfig returns the configuration for a specific table by name
func GetTableConfig(tables []TableSyncConfig, tableName string) (*TableSyncConfig, error) {
	for _, table := range tables {
		if table.Name == tableName {
			return &table, nil
		}
	}
	return nil, fmt.Errorf("table configuration not found for '%s'", tableName)
}

// IsMultiTableConfig returns true if the config uses multi-table configuration
func IsMultiTableConfig(cfg Config) bool {
	return len(cfg.Tables) > 0
}
