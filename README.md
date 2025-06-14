The mydatasyncer is a utility for synchronizing data from various file formats to a relational database. It provides flexible synchronization options including full overwrite and differential update modes.

## Features

- **Two sync modes**:
  - **Overwrite mode**: Completely replaces all existing data in the target table
  - **Differential mode**: Updates only changed records, adds new records, and optionally deletes records not in the source file
- **Bulk operations**: Efficiently handles large datasets using bulk insert/update/delete operations
- **Transaction support**: All operations are wrapped in a database transaction to ensure data integrity
- **Simple configuration**: Easy to define target tables, columns, and primary keys
- **Multiple format support**: Supports CSV and JSON formats with automatic format detection
- **Comprehensive testing**: 87.2% test coverage with robust error handling and edge case testing
- **Enhanced security**: Requires explicit database configuration to prevent accidental connections

## Installation

### Prerequisites

- Go 1.16 or later
- MySQL or compatible database (configurable for other database systems)

### Installing with go install

The simplest way to install mydatasyncer is to use `go install`:

```bash
go install github.com/yoRyuuuuu/mydatasyncer@latest
```

This will install the latest version of mydatasyncer in your Go bin directory, which should be in your PATH.

### Building from Source

Alternatively, you can build from source:

```bash
git clone https://github.com/yoRyuuuuu/mydatasyncer.git
cd mydatasyncer
go build
```
## Usage

### Basic Usage

```bash
mydatasyncer --config config.yml
```

### Dry Run Mode

The Dry Run mode allows you to preview synchronization changes without actually modifying the database.

#### Overview

Key benefits of the Dry Run feature:

- **Safe Change Preview**: Review synchronization results without affecting the actual database
- **Detailed Execution Plan**: Check the number of records to be inserted, updated, or deleted, and which columns will be affected
- **Transaction Protection**: Changes are automatically rolled back in Dry Run mode, ensuring data integrity
- **Dual Mode Support**: Available in both overwrite and diff modes

#### Usage

1. Command Line Execution:
```bash
# Basic usage
mydatasyncer --config config.yml --dry-run

# Without config path (uses default mydatasyncer.yml)
mydatasyncer --dry-run
```

2. Configuration File:
```yaml
# mydatasyncer.yml
db:
  dsn: "user:password@tcp(127.0.0.1:3306)/testdb?parseTime=true"  # Required: must be explicitly configured
sync:
  filePath: "./testdata.csv"
  tableName: "products"
  columns:
    - id
    - name
    - price
  primaryKey: "id"
  syncMode: "diff"
  deleteNotInFile: true
```

#### Output Format

The Dry Run mode displays an execution plan in the following format:

```
[DRY-RUN MODE] Execution Plan for Table {table_name}
----------------------------------------------------
Summary:
- Sync Mode: {overwrite|diff}
- Records in File: X
- Records in Database: Y

Planned Operations:
1. Delete Operations
   - Records to Delete: N
   - Affected Primary Keys: [key list]

2. Insert Operations
   - Records to Insert: M
   - Affected Columns: [column1, column2, ...]

3. Update Operations
   - Records to Update: P
   - Affected Columns: [column1, column2, ...]

Timestamps:
- Timestamp Columns to Update: [created_at, updated_at]
```

#### Important Notes

1. **Transaction Control**
   - All operations in Dry Run mode are automatically rolled back
   - No actual database changes are made during execution

2. **Timestamp Columns**
   - Timestamp columns (created_at, updated_at) are included in the execution plan
   - Actual values will be set automatically during real execution

3. **Performance**
   - Minimal performance overhead even with large datasets
   - Efficient execution plan generation with minimal database impact

4. **Error Handling**
   - Configuration and connection errors are detected before execution
   - Error messages are displayed in a clear, understandable format


### Basic Usage

Run the synchronization with:

```bash
mydatasyncer
```

By default, it will use the built-in configuration. You can customize the behavior through a configuration file.

### Configuration

The configuration file uses YAML format with the following structure:

```yaml
# Database connection settings
db:
  dsn: "user:password@tcp(127.0.0.1:3306)/testdb?parseTime=true"  # Required: must be explicitly configured

# Synchronization settings
sync:
  # Input file path (CSV or JSON format - auto-detected by file extension)
  filePath: "./testdata.csv"

  # Target table name
  tableName: "products"

  # Columns to synchronize
  columns:
    - id          # Primary key
    - name        # Regular column
    - price       # Regular column
    - created_at  # Timestamp column
    - updated_at  # Timestamp column

  # Primary key specification (required)
  primaryKey: "id"

  # Synchronization mode
  # - "diff": Differential sync mode. Updates existing records, inserts new ones
  # - "overwrite": Overwrite mode. Deletes all existing data and replaces with file contents
  syncMode: "diff"

  # Delete records not in file (only effective in diff mode)
  deleteNotInFile: true

  # Timestamp column auto-update settings
  timestamps:
    createdAt: "created_at"  # Updated only when creating new records
    updatedAt: "updated_at"  # Updated every time records are modified

  # Immutable columns (excluded from synchronization)
  immutableColumns:
    - "created_at"  # Creation timestamp cannot be modified
```

### Sync Mode Details

#### Differential Mode (diff)
- Matches existing records using primary key
- Updates only changed records
- Inserts new records
- Deletes records not in file if `deleteNotInFile: true`
- Executes within a transaction, rolls back all changes on error

#### Overwrite Mode (overwrite)
- Deletes all existing data in the table
- Completely replaces with file contents
- Faster but existing data is completely lost
- Ignores `deleteNotInFile` setting

### Using Timestamp and Immutable Columns

1. Timestamp Columns
   - `created_at`: Updated with current timestamp only for new records
   - `updated_at`: Updated with current timestamp on every record modification
   - Timestamp columns must be included in the `columns` list

2. Immutable Columns
   - Specified in `immutableColumns`
   - Values once set will not be modified during synchronization
   - Typically used to protect metadata or system management fields

### Usage Examples

1. Product Master Differential Sync
```yaml
sync:
  filePath: "./products.csv"
  tableName: "products"
  columns:
    - product_id
    - name
    - price
    - category
    - created_at
    - updated_at
  primaryKey: "product_id"
  syncMode: "diff"
  deleteNotInFile: false  # Don't delete products
  timestamps:
    createdAt: "created_at"
    updatedAt: "updated_at"
```

2. User Data Complete Sync
```yaml
sync:
  filePath: "./users.csv"
  tableName: "users"
  columns:
    - user_id
    - email
    - name
    - status
    - created_at
  primaryKey: "user_id"
  syncMode: "overwrite"  # Complete replacement
  immutableColumns:
    - "created_at"  # Preserve creation timestamp
```

3. Inventory Differential Update (with deletions)
```yaml
sync:
  filePath: "./inventory.csv"
  tableName: "inventory"
  columns:
    - item_id
    - quantity
    - last_checked
    - updated_at
  primaryKey: "item_id"
  syncMode: "diff"
  deleteNotInFile: true  # Remove out-of-stock items
  timestamps:
    updatedAt: "updated_at"
```

## Using with Docker

A Docker Compose setup is included for easy development and testing:

```bash
# Start the MySQL database
docker compose up -d

# Build and run the application
go build
./mydatasyncer
```

The provided `compose.yml` sets up a MySQL database and initializes it with sample tables.

## Project Structure

- `main.go`: Entry point and application flow orchestration
- `dbsync.go`: Core database synchronization operations with transaction support
- `config.go`: Configuration management with validation and security improvements
- `loader.go`: File format abstraction layer supporting CSV and JSON formats
- `compose.yml`: Docker setup for MySQL development environment
- `testdata/`: Comprehensive test data files for various scenarios
- `*_test.go`: Extensive test suites with 87.2% coverage

## Testing

The project includes comprehensive test coverage (87.2%) with:

### Running Tests

```bash
# Run all tests
go test ./...

# Run tests with coverage report
go test -cover ./...

# Run specific test files
go test -v -run TestCSVLoader
go test -v -run TestJSONLoader
go test -v -run TestE2ESyncJSON
```

### Test Coverage Areas

- **Configuration Management**: YAML parsing, validation, fallback handling
- **File Processing**: CSV/JSON loading, format detection, error handling
- **Database Operations**: Sync modes, transactions, bulk operations
- **Error Scenarios**: File permissions, malformed data, connection failures
- **Edge Cases**: Empty files, invalid configurations, type conversions

### Development Environment

```bash
# Start MySQL database for development and testing
docker compose up -d

# Run tests against real database
go test ./...

# Stop development environment
docker compose down
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Security Considerations

Starting from the latest version, mydatasyncer requires explicit database configuration for enhanced security:

- **No Default DSN**: Database connection string must be explicitly provided in configuration
- **Configuration Validation**: Strict validation prevents runtime errors from invalid configurations
- **Transaction Safety**: All operations are wrapped in transactions with automatic rollback on errors

## Recent Improvements

### Version Updates
- **Enhanced Test Coverage**: Expanded from 77.1% to 87.2% with comprehensive error handling tests
- **JSON Format Support**: Full support for JSON file format with automatic type conversion
- **Security Hardening**: Removed default database credentials, requiring explicit configuration
- **Robust Error Handling**: Comprehensive error scenarios testing including file permissions, malformed data, and edge cases

## Future Enhancements

- Support for additional file formats (Excel, XML, YAML)
- Command-line parameters for overriding configuration values
- Enhanced logging and monitoring features with structured output
- Support for additional database systems (PostgreSQL, SQLite)
- Performance optimizations for very large datasets
