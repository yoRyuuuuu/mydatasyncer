package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// CustomUsage prints a custom formatted usage message
func CustomUsage() {
	fmt.Fprintf(os.Stdout, `mydatasyncer - Database Synchronization Tool

Usage:
  mydatasyncer [options]

Options:
`)
	flag.PrintDefaults()
	fmt.Fprintf(os.Stdout, `
Examples:
  Basic usage:
    $ mydatasyncer -config ./config.yml

  Preview changes:
    $ mydatasyncer -config ./config.yml -dry-run
`)
}

func main() {
	// Set custom usage function
	flag.Usage = CustomUsage

	// Define command-line flags with detailed descriptions
	configPath := flag.String("config", "", `Path to the configuration file
	Default: mydatasyncer.yml in the current directory
	YAML format configuration file containing database connection details and column mappings`)

	dryRun := flag.Bool("dry-run", false, `Execute in dry-run mode
	Preview changes that would be made without applying them to the database
	Use this to verify changes before actual synchronization`)

	flag.Parse()

	if err := RunApp(*configPath, *dryRun); err != nil {
		log.Fatalf("Application error: %v", err)
	}
}

func RunApp(configPath string, dryRun bool) error {
	// Create a context with timeout for the entire process
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// 1. Load configuration
	config := LoadConfig(configPath)
	config.DryRun = dryRun // Set dry-run mode from command line flag

	if dryRun {
		log.Println("Running in DRY-RUN mode - No changes will be applied to the database")
	}

	if err := ValidateConfig(config); err != nil {
		// Check if it's a DependencyError for enhanced error reporting
		if depErr, ok := err.(*DependencyError); ok {
			log.Printf("%s", depErr.GetDetailedErrorMessage())
			return fmt.Errorf("configuration validation failed")
		}
		return fmt.Errorf("configuration error: %w", err)
	}

	// 2. Database connection
	db, err := sql.Open("mysql", config.DB.DSN)
	if err != nil {
		return fmt.Errorf("database connection error: %w", err)
	}
	defer db.Close()
	err = db.PingContext(ctx)
	if err != nil {
		return fmt.Errorf("database connectivity error: %w", err)
	}
	log.Println("Database connection successful")

	// 3. Check configuration type and execute appropriate synchronization
	if IsMultiTableConfig(config) {
		// Multi-table synchronization
		err = syncMultipleTablesData(ctx, db, config)
		if err != nil {
			return fmt.Errorf("multi-table data synchronization error: %w", err)
		}
	} else {
		// Legacy single table synchronization
		records, err := loadDataFromFile(&config)
		if err != nil {
			return fmt.Errorf("file reading error: %w", err)
		}
		log.Printf("Loaded %d records from file.", len(records))

		// 🚨 STRICT PRIMARY KEY VALIDATION - Always enforced for data safety
		if config.Sync.SyncMode == "diff" && config.Sync.PrimaryKey != "" {
			validator := NewPrimaryKeyValidator()
			validationResult, err := validator.ValidateAllRecords(records, config.Sync.PrimaryKey)
			if err != nil {
				// Report detailed validation failure
				validator.ReportValidationFailure(validationResult)
				return fmt.Errorf("primary key validation failed: %w", err)
			}
		}

		err = syncData(ctx, db, config, records)
		if err != nil {
			return fmt.Errorf("data synchronization error: %w", err)
		}
	}

	log.Println("Data synchronization completed successfully.")
	return nil
}

// loadDataFromFile loads data from file using the integrated loader functionality
func loadDataFromFile(config *Config) ([]DataRecord, error) {
	dataLoader, err := GetLoader(config.Sync.FilePath)
	if err != nil {
		return nil, fmt.Errorf("error creating loader for %s: %w", config.Sync.FilePath, err)
	}
	return dataLoader.Load(config.Sync.Columns)
}
