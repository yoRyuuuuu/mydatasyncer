package infrastructure

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// TestEnvironment represents an isolated test environment
type TestEnvironment struct {
	ID       string
	DB       *sql.DB
	TempDir  string
	Metrics  *TestMetrics
	cleanup  []func() error
	mutex    sync.RWMutex
}

// TestEnvManager manages multiple isolated test environments
type TestEnvManager struct {
	environments sync.Map // map[string]*TestEnvironment
	baseConfig   DBConfig
	dbTemplate   string
	rootDB       *sql.DB
	mutex        sync.RWMutex
}

// DBConfig represents database configuration for tests
type DBConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	RootDSN  string
}

// TestMetrics holds performance and execution metrics for tests
type TestMetrics struct {
	StartTime       time.Time
	EndTime         time.Time
	Duration        time.Duration
	PeakMemoryMB    uint64
	CPUUsagePercent float64
	DBConnections   int
	QueryCount      int64
	ErrorCount      int
}

// NewTestEnvManager creates a new test environment manager
func NewTestEnvManager() *TestEnvManager {
	config := getTestDBConfig()
	
	// Connect to root database for creating/dropping test databases
	rootDB, err := sql.Open("mysql", config.RootDSN)
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to root database: %v", err))
	}
	
	return &TestEnvManager{
		baseConfig: config,
		dbTemplate: "test_env_%s_%d",
		rootDB:     rootDB,
	}
}

// CreateEnvironment creates a new isolated test environment
func (tem *TestEnvManager) CreateEnvironment(testName string) (*TestEnvironment, error) {
	tem.mutex.Lock()
	defer tem.mutex.Unlock()
	
	// Generate unique environment ID
	envID := fmt.Sprintf(tem.dbTemplate, testName, time.Now().UnixNano())
	
	// Create dedicated database
	db, err := tem.createIsolatedDB(envID)
	if err != nil {
		return nil, fmt.Errorf("failed to create isolated DB: %w", err)
	}
	
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", fmt.Sprintf("test_%s_", testName))
	if err != nil {
		db.Close()
		tem.dropDatabase(envID)
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}
	
	env := &TestEnvironment{
		ID:      envID,
		DB:      db,
		TempDir: tempDir,
		Metrics: NewTestMetrics(),
		cleanup: []func() error{},
	}
	
	// Register cleanup functions
	env.RegisterCleanup(func() error { return os.RemoveAll(tempDir) })
	env.RegisterCleanup(func() error { return tem.dropDatabase(envID) })
	env.RegisterCleanup(func() error { return db.Close() })
	
	tem.environments.Store(envID, env)
	return env, nil
}

// createIsolatedDB creates a new database and returns a connection to it
func (tem *TestEnvManager) createIsolatedDB(dbName string) (*sql.DB, error) {
	// Create database
	_, err := tem.rootDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
	if err != nil {
		return nil, fmt.Errorf("failed to create database %s: %w", dbName, err)
	}
	
	// Create DSN for the new database
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		tem.baseConfig.User,
		tem.baseConfig.Password,
		tem.baseConfig.Host,
		tem.baseConfig.Port,
		dbName)
	
	// Connect to the new database
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		tem.dropDatabase(dbName)
		return nil, fmt.Errorf("failed to connect to database %s: %w", dbName, err)
	}
	
	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		tem.dropDatabase(dbName)
		return nil, fmt.Errorf("failed to ping database %s: %w", dbName, err)
	}
	
	return db, nil
}

// dropDatabase drops the specified database
func (tem *TestEnvManager) dropDatabase(dbName string) error {
	_, err := tem.rootDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
	return err
}

// RegisterCleanup registers a cleanup function to be called when the environment is destroyed
func (env *TestEnvironment) RegisterCleanup(fn func() error) {
	env.mutex.Lock()
	defer env.mutex.Unlock()
	env.cleanup = append(env.cleanup, fn)
}

// Cleanup performs all registered cleanup operations
func (env *TestEnvironment) Cleanup() error {
	env.mutex.Lock()
	defer env.mutex.Unlock()
	
	var errors []string
	// Execute cleanup functions in reverse order (LIFO)
	for i := len(env.cleanup) - 1; i >= 0; i-- {
		if err := env.cleanup[i](); err != nil {
			errors = append(errors, err.Error())
		}
	}
	
	if len(errors) > 0 {
		return fmt.Errorf("cleanup errors: %v", errors)
	}
	return nil
}

// GetEnvironment retrieves an existing test environment
func (tem *TestEnvManager) GetEnvironment(envID string) (*TestEnvironment, bool) {
	value, ok := tem.environments.Load(envID)
	if !ok {
		return nil, false
	}
	return value.(*TestEnvironment), true
}

// CleanupEnvironment removes and cleans up a specific environment
func (tem *TestEnvManager) CleanupEnvironment(envID string) error {
	value, ok := tem.environments.Load(envID)
	if !ok {
		return fmt.Errorf("environment %s not found", envID)
	}
	
	env := value.(*TestEnvironment)
	err := env.Cleanup()
	tem.environments.Delete(envID)
	return err
}

// CleanupAll removes and cleans up all environments
func (tem *TestEnvManager) CleanupAll() error {
	var errors []string
	
	tem.environments.Range(func(key, value interface{}) bool {
		envID := key.(string)
		env := value.(*TestEnvironment)
		
		if err := env.Cleanup(); err != nil {
			errors = append(errors, fmt.Sprintf("env %s: %v", envID, err))
		}
		
		tem.environments.Delete(envID)
		return true
	})
	
	if len(errors) > 0 {
		return fmt.Errorf("cleanup errors: %v", errors)
	}
	return nil
}

// Close closes the test environment manager and all its resources
func (tem *TestEnvManager) Close() error {
	// Cleanup all environments first
	if err := tem.CleanupAll(); err != nil {
		// Log error but continue with closing root DB
		fmt.Printf("Warning: failed to cleanup all environments: %v\n", err)
	}
	
	// Close root database connection
	if tem.rootDB != nil {
		return tem.rootDB.Close()
	}
	return nil
}

// NewTestMetrics creates a new TestMetrics instance
func NewTestMetrics() *TestMetrics {
	return &TestMetrics{
		StartTime: time.Now(),
	}
}

// getTestDBConfig retrieves database configuration from environment variables
func getTestDBConfig() DBConfig {
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
	
	return DBConfig{
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
		RootDSN:  rootDSN,
	}
}