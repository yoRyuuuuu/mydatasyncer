package main

import (
	"context"
	"database/sql"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// Create a context with timeout for the entire process
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// 1. Load configuration
	config := LoadConfig() // Function defined in config.go

	// Validate configuration
	if err := ValidateConfig(config); err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	// 2. Database connection (using database/sql or GORM)
	db, err := sql.Open("mysql", config.DB.DSN)
	if err != nil {
		log.Fatalf("Database connection error: %v", err)
	}
	defer db.Close()
	err = db.PingContext(ctx)
	if err != nil {
		log.Fatalf("Database connectivity error: %v", err)
	}
	log.Println("Database connection successful")

	// 3. File loading and parsing
	records, err := loadDataFromFile(config.Sync.FilePath, config.Sync.Columns)
	if err != nil {
		log.Fatalf("File reading error: %v", err)
	}
	log.Printf("Loaded %d records from file.", len(records))

	// 4. Synchronization process - now uses the function from dbsync.go
	err = syncData(ctx, db, config, records)
	if err != nil {
		log.Fatalf("Data synchronization error: %v", err)
	}

	log.Println("Data synchronization completed successfully.")
}

// loadDataFromFile loads data from file using the integrated loader functionality
func loadDataFromFile(filePath string, columns []string) ([]DataRecord, error) {
	// Get appropriate loader automatically
	dataLoader := GetLoader(filePath)

	// Configure CSV loader options if applicable
	if csvLoader, ok := dataLoader.(*CSVLoader); ok {
		csvLoader.WithHeader(true) // File has header row
		// csvLoader.WithDelimiter('\t')  // For tab-delimited files
	}

	// Execute file loading
	return dataLoader.Load(columns)
}
