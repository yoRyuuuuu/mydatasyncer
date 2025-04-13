package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql" // MySQL driver import (change according to your DB)
	// "github.com/spf13/viper" // For loading configuration files
	// "gorm.io/driver/mysql" // GORM driver
	// "gorm.io/gorm"         // GORM
)

// DataRecord represents one record of data loaded from file
// A map with column names as keys
type DataRecord map[string]string

// Loader is the basic interface for data loaders
type Loader interface {
	// Load loads data according to specified column definitions
	Load(columns []string) ([]DataRecord, error)
}

// CSVLoader loads data from CSV files
type CSVLoader struct {
	Delimiter rune   // CSV delimiter character
	HasHeader bool   // Whether file has a header row
	FilePath  string // Path to file to be loaded
}

// NewCSVLoader creates a new CSV loader instance
func NewCSVLoader(filePath string) *CSVLoader {
	return &CSVLoader{
		Delimiter: ',',   // Default is comma delimiter
		HasHeader: false, // Default is no header
		FilePath:  filePath,
	}
}

// WithDelimiter returns a CSV loader with the specified delimiter
func (l *CSVLoader) WithDelimiter(delimiter rune) *CSVLoader {
	l.Delimiter = delimiter
	return l
}

// WithHeader returns a CSV loader with header setting
func (l *CSVLoader) WithHeader(hasHeader bool) *CSVLoader {
	l.HasHeader = hasHeader
	return l
}

// Load loads data from CSV file
func (l *CSVLoader) Load(columns []string) ([]DataRecord, error) {
	file, err := os.Open(l.FilePath)
	if err != nil {
		return nil, fmt.Errorf("cannot open file '%s': %w", l.FilePath, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = l.Delimiter

	// Skip header row if applicable
	if l.HasHeader {
		_, err = reader.Read()
		if err != nil {
			return nil, fmt.Errorf("header row reading error: %w", err)
		}
	}

	csvRows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("CSV data reading error: %w", err)
	}

	var records []DataRecord
	for i, row := range csvRows {
		if len(row) != len(columns) {
			return nil, fmt.Errorf("line %d: column count (%d) does not match configuration (%d)", i+1, len(row), len(columns))
		}
		record := make(DataRecord)
		for j, colName := range columns {
			record[colName] = row[j]
		}
		records = append(records, record)
	}
	return records, nil
}

// GetLoader creates a loader instance for the specified file path
// Returns appropriate loader based on file extension
func GetLoader(filePath string) Loader {
	// Currently only supports CSV
	// TODO: Add support for other formats (Excel, JSON, XML, etc.) in the future
	return NewCSVLoader(filePath)
}

func main() {
	// Create a context with timeout for the entire process
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// 1. Load configuration
	config := LoadConfig() // Function defined in config.go

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
