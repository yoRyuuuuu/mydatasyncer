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
