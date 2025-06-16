# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

mydatasyncer is a Go database synchronization utility that efficiently syncs data from CSV/JSON files to relational databases. It supports two modes: complete overwrite and differential synchronization with smart change detection.

## Development Commands

```bash
# Build and test
go build                         # Build binary
go install                       # Install to GOPATH/bin
go test ./...                   # Run all tests
go test -cover ./...            # Run with coverage (current: 87.2%)
go test -v ./...                # Verbose test output
go test -v -run TestCSVLoader   # Run specific test
go test -count=1 ./...          # Run tests without cache

# Development environment
docker compose up -d            # Start MySQL test database
docker compose down             # Stop test database

# Run the tool
./mydatasyncer --config config.yml           # Use specific config
./mydatasyncer --config config.yml --dry-run # Preview changes only
./mydatasyncer                                # Use mydatasyncer.yml
```

## Architecture

### Core Components
- **`main.go`**: CLI orchestration and argument parsing
- **`config.go`**: YAML configuration management with validation
- **`dbsync.go`**: Database synchronization engine with transaction support
- **`loader.go`**: File format abstraction (CSV/JSON auto-detection)

### Key Design Patterns
- **Interface-based file loading**: `Loader` interface abstracts CSV/JSON handling
- **Type-safe primary keys**: `PrimaryKey` struct handles mixed data types consistently
- **Transaction-wrapped operations**: All database operations use transactions for integrity
- **Bulk operations**: Efficient INSERT/UPDATE/DELETE for large datasets

### Synchronization Modes
- **Overwrite**: Complete table replacement
- **Differential**: Smart sync that only updates changed records, adds new ones, optionally deletes missing ones

## Configuration Structure

Configuration is YAML-based with these key sections:
```yaml
db:
  dsn: "required database connection string"
sync:
  filePath: "path to CSV/JSON file"
  tableName: "target database table"
  columns: ["column", "list"]
  primaryKey: "primary key column"
  syncMode: "diff" | "overwrite"
  deleteNotInFile: true/false
  timestampColumns: ["auto-managed timestamp columns"]
  immutableColumns: ["protected columns"]
```

## Testing

The project has comprehensive test coverage (87.2%) with:
- **Unit tests**: Individual component testing
- **Integration tests**: End-to-end database operations using Docker MySQL
- **Edge case testing**: Invalid data, malformed configs, connection failures
- **Test data**: Extensive `testdata/` directory with various scenarios

Test files mirror main components: `*_test.go` files contain tests for corresponding modules.

## Key Dependencies

- `github.com/go-sql-driver/mysql` - MySQL database driver
- `github.com/goccy/go-yaml` - YAML configuration parsing
- `github.com/google/go-cmp` - Deep comparison utilities for testing

## Development Notes

- Always use the Docker environment for consistent database testing
- The tool requires explicit DSN configuration for security (no defaults)
- Dry-run mode is available for safe change preview with detailed execution plans
- All database operations are transaction-wrapped with automatic rollback on errors
- File format detection is automatic (CSV/JSON) based on file extension
- Primary key handling supports mixed data types (string/int) with type-safe comparisons
- Timestamp columns are auto-managed if configured (created_at/updated_at)
- Immutable columns are protected from updates during synchronization
- Bulk operations are used for efficient large dataset handling
- Comprehensive test coverage (87.2%) with edge case and error scenario testing