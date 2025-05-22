package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// DataRecord represents one record of data loaded from file
// A map with column names as keys
type DataRecord map[string]string

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
		if err == csv.ErrFieldCount || err.Error() == "EOF" { // Check for empty file or just header
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
// It expects an array of objects, and uses the 'columns' argument to extract specific fields.
func (l *JSONLoader) Load(columns []string) ([]DataRecord, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("JSON loader requires at least one column to be specified")
	}

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

	var records []DataRecord
	for i, jsonObj := range jsonData {
		record := make(DataRecord)
		for _, colName := range columns {
			val, ok := jsonObj[colName]
			if !ok {
				return nil, fmt.Errorf("JSON file '%s', record %d: missing required key '%s'", l.FilePath, i, colName)
			}
			record[colName] = fmt.Sprintf("%v", val) // Convert value to string
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
