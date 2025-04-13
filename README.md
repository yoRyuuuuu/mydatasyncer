The mydatasyncer is a utility for synchronizing data from various file formats to a relational database. It provides flexible synchronization options including full overwrite and differential update modes.

## Features

- **Two sync modes**:
  - **Overwrite mode**: Completely replaces all existing data in the target table
  - **Differential mode**: Updates only changed records, adds new records, and optionally deletes records not in the source file
- **Bulk operations**: Efficiently handles large datasets using bulk insert/update/delete operations
- **Transaction support**: All operations are wrapped in a database transaction to ensure data integrity
- **Simple configuration**: Easy to define target tables, columns, and primary keys
- **Multiple format support**: Supports CSV with plans to add Excel, JSON, XML, and other data formats

## Installation

### Prerequisites

- Go 1.16 or later
- MySQL or compatible database (configurable for other database systems)

### Installing

```bash
go get github.com/yoRyuuuuu/mydatasyncer
```

Or clone the repository and build:

```bash
git clone https://github.com/yoRyuuuuu/mydatasyncer.git
cd mydatasyncer
go build
```

## Usage

### Basic Usage

Run the synchronization with:

```bash
./mydatasyncer
```

By default, it will use the built-in configuration. You can customize the behavior through environment variables or configuration files (planned feature).

### Configuration

Edit the configuration in `config.go` or provide a configuration file (future enhancement). Key configuration options:

- Database connection string
- Input file path
- Target table name
- Column mappings
- Synchronization mode (overwrite/diff)
- Whether to delete records not in source file

Example configuration:

```go
Config{
    DB: struct{ DSN string }{DSN: "user:password@tcp(127.0.0.1:3306)/testdb?parseTime=true"},
    Sync: struct {
        FilePath        string
        TableName       string
        Columns         []string
        PrimaryKey      string
        SyncMode        string
        DeleteNotInFile bool
    }{
        FilePath:        "./testdata.csv",
        TableName:       "products",
        Columns:         []string{"id", "name", "price"},
        PrimaryKey:      "id",
        SyncMode:        "diff",
        DeleteNotInFile: true,
    },
}
```

## Using with Docker

A Docker Compose setup is included for easy development and testing:

```bash
# Start the MySQL database
docker-compose up -d

# Build and run the application
go build
./mydatasyncer
```

The provided `docker-compose.yml` sets up a MySQL database and initializes it with sample tables.

## Project Structure

- `main.go`: Entry point and CSV file loading logic
- `dbsync.go`: Database synchronization operations
- `config.go`: Configuration definitions and loading
- `init-sql/`: SQL files for database initialization
- `testdata.csv`: Sample data file for testing

## Contributing

Contributions are welcome! Here are some ways you can contribute:

1. Report bugs or suggest features by opening issues
2. Submit pull requests with improvements
3. Improve documentation
4. Add support for additional file formats
5. Enhance database compatibility

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Future Enhancements

- Support for additional file formats (Excel, JSON, XML)
- Configuration through external files (YAML, TOML, JSON)
- Command-line parameters for overriding configuration
- Enhanced logging and monitoring features
- Support for additional database systems
