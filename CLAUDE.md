# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

mydatasyncer is a data synchronization utility that synchronizes data from CSV and JSON files to MySQL databases. It supports two sync modes:

- **Overwrite mode**: Completely replaces all existing data in the target table
- **Differential mode**: Updates only changed records, adds new records, and optionally deletes records not in the source file

## Architecture

The codebase follows a modular design:

- `main.go`: Entry point, command-line parsing, and application flow orchestration
- `config.go`: Configuration management using YAML files with sensible defaults
- `loader.go`: File format abstraction layer (supports CSV and JSON formats)
- `dbsync.go`: Core synchronization logic including transaction management and bulk operations
- `compose.yml`: Docker setup for MySQL development environment

Key architectural patterns:
- Transaction-based operations ensure data integrity (all-or-nothing approach)
- Bulk operations for performance with large datasets
- Column mapping between file headers and database schema
- Automatic column detection for both CSV and JSON formats
- Type-preserving JSON value conversion to maintain data integrity
- Dry-run mode for safe change preview
- Immutable column protection to preserve system metadata

## Development Commands

### Building and Running
```bash
# Build the application
go build

# Run with default config (mydatasyncer.yml)
./mydatasyncer

# Run with custom config
./mydatasyncer --config config.yml

# Preview changes without applying them
./mydatasyncer --dry-run
```

### Testing
```bash
# Run all tests
go test ./...

# Run tests with coverage
go test -cover ./...

# Run specific test files
go test -v -run TestCSVLoader
go test -v -run TestJSONLoader
go test -v -run TestConvertJSONValueToString
go test -v -run TestE2ESyncJSON

# Run single test case
go test -v -run "TestJSONLoader_Load_Success/precise_type_conversion_handling"
```

### Development Environment
```bash
# Start MySQL database for development
docker compose up -d

# Stop development environment
docker compose down
```

## Key Implementation Details

### Configuration System
- YAML-based configuration with fallback to sensible defaults
- Supports timestamp columns (created_at, updated_at) with automatic management
- Immutable columns prevent accidental modification of system fields
- Primary key requirement for differential sync mode

### Data Processing Pipeline
1. Load and validate configuration
2. Establish database connection with transaction support
3. Load data from file using appropriate loader (CSV or JSON, auto-detected by file extension)
4. Determine actual sync columns (intersection of file headers, DB schema, and config)
5. Execute sync operations (overwrite or differential) within transaction
6. Commit or rollback based on success/failure

### Column Mapping Logic
The system automatically determines which columns to sync based on:
- File headers (CSV headers or JSON object keys from first record)
- Database table schema
- Optional configuration filter (`sync.columns`)
- Primary key requirements for differential mode

### File Format Support
- **CSV**: Headers read from first row, values used as-is
- **JSON**: Must be an array of objects. Column names auto-detected from first object's keys if not specified in config. Values converted to strings with type preservation (integers, floats, booleans, null handled appropriately)

This ensures robust handling of schema mismatches and provides clear error messages.

## Branch Structure
- `main`: Main development branch
- `feature/*`: Feature development branches (current: `feature/json-loader`)

## Code Style
- Think and write comments in English (following project conventions)
- Go standard formatting and naming conventions
