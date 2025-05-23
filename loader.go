package main

import (
	"encoding/csv"
	"fmt"
	"os"
)

// DataRecord represents one record of data loaded from file
// A map with column names as keys
type DataRecord map[string]string

// Loader is the basic interface for data loaders
type Loader interface {
	// Load loads data according to specified column definitions
	Load(columns []string) ([]DataRecord, error)
}

// CSVLoader loads data from CSV files
type CSVLoader struct {
	Delimiter rune   // CSV delimiter character
	HasHeader bool   // Whether file has a header row
	FilePath  string // Path to file to be loaded
}

// NewCSVLoader creates a new CSV loader instance
func NewCSVLoader(filePath string) *CSVLoader {
	return &CSVLoader{
		Delimiter: ',',   // Default is comma delimiter
		HasHeader: false, // Default is no header
		FilePath:  filePath,
	}
}

// WithDelimiter returns a CSV loader with the specified delimiter
func (l *CSVLoader) WithDelimiter(delimiter rune) *CSVLoader {
	l.Delimiter = delimiter
	return l
}

// WithHeader returns a CSV loader with header setting
func (l *CSVLoader) WithHeader(hasHeader bool) *CSVLoader {
	l.HasHeader = hasHeader
	return l
}

// Load loads data from CSV file
func (l *CSVLoader) Load(columns []string) ([]DataRecord, error) {
	file, err := os.Open(l.FilePath)
	if err != nil {
		return nil, fmt.Errorf("cannot open file '%s': %w", l.FilePath, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = l.Delimiter

	// Skip header row if applicable
	if l.HasHeader {
		_, err = reader.Read()
		if err != nil {
			return nil, fmt.Errorf("header row reading error: %w", err)
		}
	}

	csvRows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("CSV data reading error: %w", err)
	}

	var records []DataRecord
	for i, row := range csvRows {
		if len(row) != len(columns) {
			return nil, fmt.Errorf("line %d: column count (%d) does not match configuration (%d)", i+1, len(row), len(columns))
		}
		record := make(DataRecord)
		for j, colName := range columns {
			record[colName] = row[j]
		}
		records = append(records, record)
	}
	return records, nil
}

// GetLoader creates a loader instance for the specified file path
// Returns appropriate loader based on file extension
func GetLoader(filePath string) Loader {
	// Currently only supports CSV
	// TODO: Add support for other formats (Excel, JSON, XML, etc.) in the future
	return NewCSVLoader(filePath)
}
