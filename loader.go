package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// DataRecord represents one record of data loaded from file
// A map with column names as keys
type DataRecord map[string]any

// convertJSONValueToString converts JSON values to strings while preserving type information
// This function handles different JSON types appropriately:
// - Numbers: converted to strings without scientific notation when possible
// - Booleans: "true" or "false"
// - Strings: preserved as-is
// - null: converted to empty string
// convertValue converts JSON values to appropriate Go types
func convertValue(val any) any {
	if val == nil {
		return nil
	}

	// Try to convert string values that look like RFC3339 timestamps to time.Time
	if str, ok := val.(string); ok {
		if t, err := time.Parse(time.RFC3339, str); err == nil {
			return t
		}
		return str
	}

	// Return other types as-is
	return val
}

// Loader is the basic interface for data loaders
type Loader interface {
	// Load loads data according to specified column definitions
	// For CSVLoader, the columns argument is ignored and headers are read from the CSV file.
	Load(columns []string) ([]DataRecord, error)
}

// CSVLoader loads data from CSV files
type CSVLoader struct {
	Delimiter rune // CSV delimiter character
	// HasHeader bool   // Whether file has a header row - For CSV, we now always assume a header
	FilePath string // Path to file to be loaded
}

// NewCSVLoader creates a new CSV loader instance
func NewCSVLoader(filePath string) *CSVLoader {
	return &CSVLoader{
		Delimiter: ',', // Default is comma delimiter
		// HasHeader: true, // Default is no header - Now always true for CSV
		FilePath: filePath,
	}
}

// WithDelimiter returns a CSV loader with the specified delimiter
func (l *CSVLoader) WithDelimiter(delimiter rune) *CSVLoader {
	l.Delimiter = delimiter
	return l
}

// Load loads data from CSV file.
// If 'columns' is specified, only those columns will be included in the result.
// If 'columns' is empty, all columns from the CSV header will be included.
func (l *CSVLoader) Load(columns []string) ([]DataRecord, error) {
	file, err := os.Open(l.FilePath)
	if err != nil {
		return nil, fmt.Errorf("cannot open file '%s': %w", l.FilePath, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = l.Delimiter

	// Read header row
	headerNames, err := reader.Read()
	if err != nil {
		if err == csv.ErrFieldCount || errors.Is(err, io.EOF) { // Check for empty file or just header
			return nil, fmt.Errorf("CSV file '%s' must contain a header row and at least one data row: %w", l.FilePath, err)
		}
		return nil, fmt.Errorf("error reading header row from CSV file '%s': %w", l.FilePath, err)
	}
	if len(headerNames) == 0 {
		return nil, fmt.Errorf("CSV file '%s' header row is empty", l.FilePath)
	}

	csvRows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV data rows from '%s': %w", l.FilePath, err)
	}

	// Determine which columns to include in the result
	var targetColumns []string
	if len(columns) == 0 {
		// If no columns specified, use all columns from CSV header
		targetColumns = headerNames
	} else {
		// Filter to only include specified columns that exist in CSV header
		for _, col := range columns {
			for _, headerCol := range headerNames {
				if col == headerCol {
					targetColumns = append(targetColumns, col)
					break
				}
			}
		}
	}

	var records []DataRecord
	for i, row := range csvRows {
		// Line number reported to user should be i+2 because 1 for header, 1 for 0-indexed loop
		if len(row) != len(headerNames) {
			return nil, fmt.Errorf("CSV file '%s', line %d: column count (%d) does not match header column count (%d)", l.FilePath, i+2, len(row), len(headerNames))
		}
		record := make(DataRecord)
		// Create a map of all data from the row
		allData := make(map[string]any)
		for j, colName := range headerNames {
			allData[colName] = convertValue(row[j])
		}
		// Include only target columns in the result
		for _, colName := range targetColumns {
			record[colName] = allData[colName]
		}
		records = append(records, record)
	}
	return records, nil
}

// JSONLoader loads data from JSON files
type JSONLoader struct {
	FilePath string // Path to file to be loaded
}

// NewJSONLoader creates a new JSON loader instance
func NewJSONLoader(filePath string) *JSONLoader {
	return &JSONLoader{
		FilePath: filePath,
	}
}

// Load loads data from JSON file.
// It expects an array of objects. If 'columns' is empty, it auto-detects all keys from the first object.
// If 'columns' is specified, it filters to only those columns.
func (l *JSONLoader) Load(columns []string) ([]DataRecord, error) {
	fileData, err := os.ReadFile(l.FilePath)
	if err != nil {
		return nil, fmt.Errorf("cannot read JSON file '%s': %w", l.FilePath, err)
	}

	if len(fileData) == 0 {
		return nil, fmt.Errorf("JSON file '%s' is empty", l.FilePath)
	}

	var jsonData []map[string]any
	err = json.Unmarshal(fileData, &jsonData)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling JSON data from '%s': %w", l.FilePath, err)
	}

	if len(jsonData) == 0 {
		return nil, nil // Return empty slice for empty JSON array
	}

	// Auto-detect columns from the first JSON object if not specified
	var actualColumns []string
	if len(columns) == 0 {
		// Get all keys from the first object
		for key := range jsonData[0] {
			actualColumns = append(actualColumns, key)
		}
		// Sort for consistent ordering
		sort.Strings(actualColumns)
	} else {
		actualColumns = columns
	}

	var records []DataRecord
	for i, jsonObj := range jsonData {
		record := make(DataRecord)
		for _, colName := range actualColumns {
			val, ok := jsonObj[colName]
			if !ok {
				return nil, fmt.Errorf("JSON file '%s', record %d: missing required key '%s'", l.FilePath, i, colName)
			}
			record[colName] = convertValue(val)
		}
		records = append(records, record)
	}
	return records, nil
}

// GetLoader creates a loader instance for the specified file path
// Returns appropriate loader based on file extension
func GetLoader(filePath string) (Loader, error) {
	ext := strings.ToLower(filepath.Ext(filePath))
	switch ext {
	case ".csv":
		return NewCSVLoader(filePath), nil
	case ".json":
		return NewJSONLoader(filePath), nil
	default:
		return nil, fmt.Errorf("unsupported file type: '%s'. Only .csv and .json are supported", ext)
	}
}

// MultiTableData represents data loaded from multiple files, keyed by table name
type MultiTableData map[string][]DataRecord

// MultiTableLoader handles loading data from multiple files for multi-table synchronization
type MultiTableLoader struct {
	TableConfigs []TableSyncConfig
}

// NewMultiTableLoader creates a new multi-table loader instance
func NewMultiTableLoader(tableConfigs []TableSyncConfig) *MultiTableLoader {
	return &MultiTableLoader{
		TableConfigs: tableConfigs,
	}
}

// LoadAll loads data from all configured table files
// Returns a map where keys are table names and values are the loaded records
func (ml *MultiTableLoader) LoadAll() (MultiTableData, error) {
	if len(ml.TableConfigs) == 0 {
		return nil, fmt.Errorf("no table configurations provided")
	}

	result := make(MultiTableData)
	
	for _, tableConfig := range ml.TableConfigs {
		// Create appropriate loader for each file
		loader, err := GetLoader(tableConfig.FilePath)
		if err != nil {
			return nil, fmt.Errorf("error creating loader for table '%s' file '%s': %w", tableConfig.Name, tableConfig.FilePath, err)
		}
		
		// Load data from the file
		records, err := loader.Load(tableConfig.Columns)
		if err != nil {
			return nil, fmt.Errorf("error loading data for table '%s' from file '%s': %w", tableConfig.Name, tableConfig.FilePath, err)
		}
		
		// Store the loaded records mapped by table name
		result[tableConfig.Name] = records
	}
	
	return result, nil
}

// LoadForTable loads data for a specific table by name
func (ml *MultiTableLoader) LoadForTable(tableName string) ([]DataRecord, error) {
	for _, tableConfig := range ml.TableConfigs {
		if tableConfig.Name == tableName {
			loader, err := GetLoader(tableConfig.FilePath)
			if err != nil {
				return nil, fmt.Errorf("error creating loader for table '%s' file '%s': %w", tableConfig.Name, tableConfig.FilePath, err)
			}
			
			records, err := loader.Load(tableConfig.Columns)
			if err != nil {
				return nil, fmt.Errorf("error loading data for table '%s' from file '%s': %w", tableConfig.Name, tableConfig.FilePath, err)
			}
			
			return records, nil
		}
	}
	
	return nil, fmt.Errorf("table configuration not found for '%s'", tableName)
}

// ValidateFilePaths checks if all configured file paths exist and are readable
func (ml *MultiTableLoader) ValidateFilePaths() error {
	for _, tableConfig := range ml.TableConfigs {
		if _, err := os.Stat(tableConfig.FilePath); os.IsNotExist(err) {
			return fmt.Errorf("file does not exist for table '%s': %s", tableConfig.Name, tableConfig.FilePath)
		}
	}
	return nil
}
