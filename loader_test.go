package main

import (
	// Added for io.EOF
	"os"
	"path/filepath"
	"reflect"
	"sort" // Added for SortStrings
	"strings"
	"testing"
)

// Helper function to create a temporary CSV file for testing
func createTempCSV(t *testing.T, name string, content string) string {
	t.Helper()
	dir := t.TempDir()
	filePath := filepath.Join(dir, name)
	err := os.WriteFile(filePath, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create temp CSV file %s: %v", name, err)
	}
	return filePath
}

func TestCSVLoader_Load_Success(t *testing.T) {
	tests := []struct {
		name           string
		csvContent     string
		delimiter      rune
		expected       []DataRecord
		expectedHeader []string // Used to verify loaded header via DataRecord keys
	}{
		{
			name: "comma delimited",
			csvContent: `id,name,value
1,productA,100
2,productB,200`,
			delimiter: ',',
			expected: []DataRecord{
				{"id": "1", "name": "productA", "value": "100"},
				{"id": "2", "name": "productB", "value": "200"},
			},
			expectedHeader: []string{"id", "name", "value"},
		},
		{
			name:       "tab delimited",
			csvContent: "id\tname\tvalue\n1\tproductA\t100\n2\tproductB\t200",
			delimiter:  '\t',
			expected: []DataRecord{
				{"id": "1", "name": "productA", "value": "100"},
				{"id": "2", "name": "productB", "value": "200"},
			},
			expectedHeader: []string{"id", "name", "value"},
		},
		{
			name:           "header only",
			csvContent:     `id,name,value`,
			delimiter:      ',',
			expected:       nil, // Expect nil slice for no data records
			expectedHeader: []string{"id", "name", "value"},
		},
		{
			name:           "empty data with header line ending",
			csvContent:     "id,name,value\n",
			delimiter:      ',',
			expected:       nil, // Expect nil slice for no data records
			expectedHeader: []string{"id", "name", "value"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filePath := createTempCSV(t, "test.csv", tt.csvContent)
			loader := NewCSVLoader(filePath)
			if tt.delimiter != 0 && tt.delimiter != ',' {
				loader.WithDelimiter(tt.delimiter)
			}

			// CSVLoader.Load ignores its argument.
			records, err := loader.Load(nil)
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if !reflect.DeepEqual(records, tt.expected) {
				t.Errorf("Load() got = %v, want %v", records, tt.expected)
			}

			// Verify header by checking the keys of the first record if records are not empty.
			if len(records) > 0 && len(tt.expectedHeader) > 0 {
				firstRecordKeys := make([]string, 0, len(records[0]))
				for k := range records[0] {
					firstRecordKeys = append(firstRecordKeys, k)
				}
				sort.Strings(firstRecordKeys) // Sort keys from actual record

				sortedExpectedHeader := append([]string{}, tt.expectedHeader...)
				sort.Strings(sortedExpectedHeader) // Sort expected header keys

				if !reflect.DeepEqual(firstRecordKeys, sortedExpectedHeader) {
					t.Errorf("Header mismatch based on record keys: got %v, want %v", firstRecordKeys, sortedExpectedHeader)
				}
			} else if len(tt.expected) == 0 && len(tt.expectedHeader) > 0 {
				// If no records but header was expected (e.g. "header only" case),
				// we can't verify header via record keys.
				// The fact that Load didn't error implies header was parsed.
				// A more direct way would be to expose loader.header (if it existed and was populated before returning).
				// For now, this relies on the loader correctly parsing the header to produce DataRecord keys.
			}
		})
	}
}

func TestCSVLoader_Load_Error(t *testing.T) {
	tests := []struct {
		name          string
		csvContent    string
		delimiter     rune
		expectedError string
	}{
		{
			name:          "file not found",
			csvContent:    "", // Content doesn't matter, path will be invalid
			expectedError: "no such file or directory",
		},
		{
			name:          "empty file (no header)",
			csvContent:    "",
			expectedError: "must contain a header row and at least one data row", // Matches current error in loader.go for empty file
		},
		{
			name: "mismatch column count (too many)",
			csvContent: `id,name
1,productA,100`, // Header has 2, data has 3
			expectedError: "wrong number of fields", // csv.ErrFieldCount from ReadAll
		},
		{
			name: "mismatch column count (too few)",
			csvContent: `id,name,value
1,productA`, // Header has 3, data has 2
			expectedError: "wrong number of fields", // csv.ErrFieldCount from ReadAll
		},
		// Removed "column in columnsToMap not in CSV header" as Load doesn't take columnsToMap
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var filePath string
			if tt.name == "file not found" {
				filePath = filepath.Join(t.TempDir(), "non_existent_file.csv")
				// For "file not found", os.Open inside Load will fail.
			} else {
				filePath = createTempCSV(t, "test_error.csv", tt.csvContent)
			}

			loader := NewCSVLoader(filePath)
			if tt.delimiter != 0 && tt.delimiter != ',' {
				loader.WithDelimiter(tt.delimiter)
			}

			// CSVLoader.Load ignores its argument.
			_, err := loader.Load(nil)
			if err == nil {
				t.Fatalf("Load() expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.expectedError) {
				t.Errorf("Load() error = %q, want error containing %q", err.Error(), tt.expectedError)
			}
		})
	}
}
