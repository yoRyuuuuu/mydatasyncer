The mydatasyncer is a utility for synchronizing data from various file formats to a relational database. It provides flexible synchronization options including full overwrite and differential update modes.

## Features

- **Two sync modes**:
  - **Overwrite mode**: Completely replaces all existing data in the target table
  - **Differential mode**: Updates only changed records, adds new records, and optionally deletes records not in the source file
- **Bulk operations**: Efficiently handles large datasets using bulk insert/update/delete operations
- **Transaction support**: All operations are wrapped in a database transaction to ensure data integrity
- **Simple configuration**: Easy to define target tables, columns, and primary keys
- **Multiple format support**: Supports CSV with plans to add JSON, YAML, and other data formats
- **Dry Run support**: Preview changes before actual execution with detailed operation information

## Dry Run Mode

The Dry Run feature allows you to preview all database operations that would be performed without actually executing them. This is useful for:

- Validating the changes before actual execution
- Reviewing the impact of synchronization operations
- Debugging configuration issues
- Auditing purposes

### Using Dry Run

Enable Dry Run mode in your configuration file:

```yaml
sync:
  dryRun: true
  # other sync settings...
```

When Dry Run is enabled, mydatasyncer will:
1. Analyze the source file and target database
2. Generate a detailed preview of all operations (INSERT/UPDATE/DELETE)
3. Display the preview including SQL statements and affected records
4. Exit without making any actual changes

Example output:

```
=== Sync Preview ===
Total changes: 15

INSERT Operation:
SQL: INSERT INTO products (id,name,price) VALUES (?,?,?)
Affected records: 5
  - Record 1: {id: "101", name: "New Product 1", price: "1500"}
  - Record 2: {id: "102", name: "New Product 2", price: "2000"}
  ... and 3 more records

UPDATE Operation:
SQL: UPDATE products SET name = ?, price = ? WHERE id = ?
Affected records: 8
  - Record 1: {id: "001", name: "Updated Name", price: "1200"}
  - Record 2: {id: "002", name: "Another Update", price: "800"}
  ... and 6 more records

DELETE Operation:
SQL: DELETE FROM products WHERE id IN (?)
Affected records: 2
  - Record 1: {id: "099"}
  - Record 2: {id: "100"}

=== End Preview ===
```

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
  syncMode: "diff"            # "overwrite" or "diff"
  deleteNotInFile: true      # Whether to delete records not in file (diff mode only)
  timestampColumns:          # Columns to automatically update with current timestamp
    - updated_at
  immutableColumns:          # Columns that should not be updated in diff mode
    - created_at
  dryRun: false             # Enable/disable dry run mode
```

Configuration Options:
- `filePath`: Path to the source data file (currently supports CSV)
- `tableName`: Target database table name
- `columns`: List of columns to sync (must match CSV column order)
- `primaryKey`: Primary key column (required for diff mode)
- `syncMode`: Synchronization mode ("overwrite" or "diff")
- `deleteNotInFile`: Whether to delete records that exist in DB but not in file
- `timestampColumns`: Columns to automatically set to current timestamp on insert/update
- `immutableColumns`: Columns that should not be modified during updates
- `dryRun`: Enable preview mode without making actual changes

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
