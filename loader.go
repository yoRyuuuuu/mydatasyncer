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
	"strconv"
	"strings"
)

// DataRecord represents one record of data loaded from file
// A map with column names as keys
type DataRecord map[string]string

// convertJSONValueToString converts JSON values to strings while preserving type information
// This function handles different JSON types appropriately:
// - Numbers: converted to strings without scientific notation when possible
// - Booleans: "true" or "false"
// - Strings: preserved as-is
// - null: converted to empty string
func convertJSONValueToString(val interface{}) string {
	if val == nil {
		return ""
	}

	switch v := val.(type) {
	case string:
		return v
	case bool:
		return strconv.FormatBool(v)
	case float64:
		// Check if it's a whole number to avoid unnecessary decimal places
		if v == float64(int64(v)) {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case int64:
		return strconv.FormatInt(v, 10)
	case int:
		return strconv.Itoa(v)
	default:
		// Fallback for other types (arrays, objects, etc.)
		return fmt.Sprintf("%v", v)
	}
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
// The 'columns' argument is ignored for CSVLoader; column names are derived from the CSV header.
func (l *CSVLoader) Load(_ []string) ([]DataRecord, error) {
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

	var records []DataRecord
	for i, row := range csvRows {
		// Line number reported to user should be i+2 because 1 for header, 1 for 0-indexed loop
		if len(row) != len(headerNames) {
			return nil, fmt.Errorf("CSV file '%s', line %d: column count (%d) does not match header column count (%d)", l.FilePath, i+2, len(row), len(headerNames))
		}
		record := make(DataRecord)
		for j, colName := range headerNames {
			record[colName] = row[j]
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

	var jsonData []map[string]interface{}
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
			record[colName] = convertJSONValueToString(val)
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
