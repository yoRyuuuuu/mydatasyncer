The mydatasyncer is a utility for synchronizing data from various file formats to a relational database. It provides flexible synchronization options including full overwrite and differential update modes.

## Features

- **Two sync modes**:
  - **Overwrite mode**: Completely replaces all existing data in the target table
  - **Differential mode**: Updates only changed records, adds new records, and optionally deletes records not in the source file
- **Multi-table synchronization**: Supports synchronizing multiple tables with dependency-aware ordering
- **Parent-child relationships**: Automatic sync order determination based on foreign key dependencies
- **Bulk operations**: Efficiently handles large datasets using bulk insert/update/delete operations
- **Transaction support**: All operations are wrapped in database transactions to ensure data integrity
- **Simple configuration**: Easy to define target tables, columns, and primary keys
- **Multiple format support**: Supports CSV and JSON formats with automatic type detection

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

The configuration file uses YAML format. There are two configuration styles: single-table and multi-table.

#### Single-Table Configuration

```yaml
# Database connection settings
db:
  dsn: "user:password@tcp(127.0.0.1:3306)/testdb?parseTime=true"

# Synchronization settings
sync:
  # Input file path (CSV or JSON format)
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

#### Multi-Table Configuration

```yaml
# Database connection settings
db:
  dsn: "user:password@tcp(127.0.0.1:3306)/testdb?parseTime=true"

# Multi-table synchronization settings
tables:
  - tableName: "categories"
    filePath: "./categories.json"
    primaryKey: "category_id"
    syncMode: "diff"
    dependencies: []  # No dependencies (parent table)
    
  - tableName: "products"
    filePath: "./products.csv"
    primaryKey: "product_id"
    syncMode: "diff"
    dependencies: ["categories"]  # Depends on categories table
    columns:
      - product_id
      - name
      - category_id
      - price
    timestamps:
      updatedAt: "updated_at"
```

#### Transaction Boundaries

**Single-Table Synchronization:**
- Each table sync runs in its own dedicated transaction
- If the sync fails, only that table's changes are rolled back

**Multi-Table Synchronization:**
- **All tables are synchronized within a single global transaction**
- **All-or-nothing approach**: If ANY table sync fails, the ENTIRE multi-table operation is rolled back
- Ensures ACID properties and referential integrity across related tables with foreign key relationships
- Dependency-aware processing order:
  - Delete operations: Child tables → Parent tables (avoids foreign key violations)
  - Insert/Update operations: Parent tables → Child tables (satisfies foreign key constraints)

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
