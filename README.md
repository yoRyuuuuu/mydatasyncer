The mydatasyncer is a utility for synchronizing data from various file formats to a relational database. It provides flexible synchronization options including full overwrite and differential update modes.

## Features

- **Two sync modes**:
  - **Overwrite mode**: Completely replaces all existing data in the target table
  - **Differential mode**: Updates only changed records, adds new records, and optionally deletes records not in the source file
- **Bulk operations**: Efficiently handles large datasets using bulk insert/update/delete operations
- **Transaction support**: All operations are wrapped in a database transaction to ensure data integrity
- **Simple configuration**: Easy to define target tables, columns, and primary keys
- **Multiple format support**: Supports CSV with plans to add JSON, YAML, and other data formats

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
  dsn: "user:password@tcp(127.0.0.1:3306)/testdb?parseTime=true"
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

Example configuration:

```yaml
# Database connection settings
db:
  dsn: "user:password@tcp(127.0.0.1:3306)/testdb?parseTime=true"

# Data synchronization settings
sync:
  filePath: "./testdata.csv"
  tableName: "products"
  columns:
    - id
    - name
    - price
  primaryKey: "id"
  syncMode: "diff" # "overwrite" or "diff"
  deleteNotInFile: true
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

- `main.go`: Entry point and CSV file loading logic
- `dbsync.go`: Database synchronization operations
- `config.go`: Configuration definitions and loading
- `init-sql/`: SQL files for database initialization
- `testdata.csv`: Sample data file for testing

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Future Enhancements

- Support for additional file formats (Excel, JSON, XML)
- Configuration through external files (YAML, TOML, JSON)
- Command-line parameters for overriding configuration
- Enhanced logging and monitoring features
- Support for additional database systems
