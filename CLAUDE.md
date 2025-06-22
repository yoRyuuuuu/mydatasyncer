# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

mydatasyncer is a data synchronization utility that synchronizes data from CSV and JSON files to MySQL databases. It supports two sync modes:

- **Overwrite mode**: Completely replaces all existing data in the target table
- **Differential mode**: Updates only changed records, adds new records, and optionally deletes records not in the source file

Key features:
- **Multi-table synchronization**: Supports synchronizing multiple tables with dependency-aware ordering
- **Parent-child relationships**: Automatic sync order determination based on foreign key dependencies
- **Circular dependency detection**: Validates table dependency graphs to prevent infinite loops

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
- Dependency graph construction with topological sorting for multi-table sync ordering

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

### Code Linting and Formatting
```bash
# Format code before committing
go fmt ./...

# Run linter (requires golangci-lint)
golangci-lint run

# Install golangci-lint (if not installed)
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
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
- Supports both single-table sync (legacy `sync` config) and multi-table sync (`tables` array)
- Dependency-aware table ordering with foreign key relationship support
- Circular dependency detection with detailed error reporting
- Supports timestamp columns (created_at, updated_at) with automatic management
- Immutable columns prevent accidental modification of system fields
- Primary key requirement for differential sync mode

### Data Processing Pipeline
1. Load and validate configuration (includes dependency graph validation for multi-table sync)
2. Establish database connection with transaction support
3. For multi-table sync: determine sync order using topological sort (insert: parent→child, delete: child→parent)
4. Load data from file using appropriate loader (CSV or JSON, auto-detected by file extension)
5. Determine actual sync columns (intersection of file headers, DB schema, and config)
6. Execute sync operations (overwrite or differential) within transaction
7. Commit or rollback based on success/failure

### Transaction Boundaries and Data Integrity

**Single-Table Synchronization:**
- Each table sync operation runs within its own dedicated transaction
- Transaction scope: Load data → Sync operations → Commit/Rollback
- Failure in any step triggers automatic rollback for that table only

**Multi-Table Synchronization:**
- **Single Global Transaction**: All tables are synchronized within one shared transaction boundary
- **All-or-Nothing Approach**: If any single table sync fails, the entire multi-table operation is rolled back
- **Transaction Scope**: Data loading (outside transaction) → Single transaction for all sync operations → Global commit/rollback
- **Dependency-Aware Processing**: 
  - Delete operations: Child tables → Parent tables (to avoid foreign key violations)
  - Insert/Update operations: Parent tables → Child tables (to satisfy foreign key constraints)

**Design Rationale:**
- Ensures ACID properties across related tables with foreign key relationships
- Maintains referential integrity throughout the entire synchronization process
- Prevents partial sync states that could leave the database in an inconsistent condition
- Simplifies rollback logic and error recovery

**Performance Considerations:**
- Single transaction may hold locks longer for multi-table operations
- Default 5-minute timeout prevents indefinite lock situations
- Bulk operations optimize performance within transaction boundaries
- Memory usage is managed by loading all data before transaction begins

**Error Handling:**
- Automatic rollback via `defer tx.Rollback()` ensures cleanup on any failure
- Detailed error reporting includes which table and operation failed
- Dry-run mode allows preview of all changes before committing
- Transaction isolation level (default: REPEATABLE READ) provides consistent data view

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

## Code Style
- Think and write comments in English (following project conventions)
- Go standard formatting and naming conventions
- Use `go fmt ./...` to format code before committing
- Run `golangci-lint run` to check for linting issues (configuration in .golangci.yaml)

## important-instruction-reminders
Do what has been asked; nothing more, nothing less.
NEVER create files unless they're absolutely necessary for achieving your goal.
ALWAYS prefer editing an existing file to creating a new one.
NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.
