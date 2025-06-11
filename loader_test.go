package main

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
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

// Helper function to create a temporary JSON file for testing
func createTempJSON(t *testing.T, name string, content string) string {
	t.Helper()
	dir := t.TempDir()
	filePath := filepath.Join(dir, name)
	err := os.WriteFile(filePath, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create temp JSON file %s: %v", name, err)
	}
	return filePath
}

func TestJSONLoader_Load_Success(t *testing.T) {
	tests := []struct {
		name        string
		jsonContent string
		columns     []string
		expected    []DataRecord
	}{
		{
			name: "valid json array of objects",
			jsonContent: `[
{"id": "1", "name": "Product Alpha", "price": "100.50"},
{"id": "2", "name": "Product Beta", "price": "25.75"}
]`,
			columns: []string{"id", "name", "price"},
			expected: []DataRecord{
				{"id": "1", "name": "Product Alpha", "price": "100.50"},
				{"id": "2", "name": "Product Beta", "price": "25.75"},
			},
		},
		{
			name: "extract specific columns",
			jsonContent: `[
{"id": "1", "name": "Product A", "category": "Electronics", "stock": 10},
{"id": "2", "name": "Product B", "category": "Books", "stock": 5}
]`,
			columns: []string{"id", "name", "stock"},
			expected: []DataRecord{
				{"id": "1", "name": "Product A", "stock": float64(10)},
				{"id": "2", "name": "Product B", "stock": float64(5)},
			},
		},
		{
			name:        "empty json array",
			jsonContent: `[]`,
			columns:     []string{"id", "name"},
			expected:    nil,
		},
		{
			name: "numeric and boolean values",
			jsonContent: `[
{"id": 1, "name": "Test", "available": true, "rating": 4.5}
]`,
			columns: []string{"id", "name", "available", "rating"},
			expected: []DataRecord{
				{"id": float64(1), "name": "Test", "available": true, "rating": 4.5},
			},
		},
		{
			name: "auto-detect columns (empty columns slice)",
			jsonContent: `[
{"id": "1", "name": "Product Alpha", "price": "100.50"},
{"id": "2", "name": "Product Beta", "price": "25.75"}
]`,
			columns: []string{}, // Empty - should auto-detect
			expected: []DataRecord{
				{"id": "1", "name": "Product Alpha", "price": "100.50"},
				{"id": "2", "name": "Product Beta", "price": "25.75"},
			},
		},
		{
			name: "auto-detect columns with mixed types",
			jsonContent: `[
{"name": "Test", "id": 1, "active": true}
]`,
			columns: []string{}, // Empty - should auto-detect and sort keys
			expected: []DataRecord{
				{"active": true, "id": float64(1), "name": "Test"},
			},
		},
		{
			name: "precise type conversion handling",
			jsonContent: `[
{"int_val": 42, "float_val": 3.14159, "bool_true": true, "bool_false": false, "null_val": null, "string_val": "text", "zero_int": 0, "zero_float": 0.0}
]`,
			columns: []string{"int_val", "float_val", "bool_true", "bool_false", "null_val", "string_val", "zero_int", "zero_float"},
			expected: []DataRecord{
				{
					"int_val":     float64(42),
					"float_val":   3.14159,
					"bool_true":   true,        // boolean true
					"bool_false":  false,       // boolean false
					"null_val":    nil,
					"string_val":  "text",
					"zero_int":    float64(0),
					"zero_float":  0.0,
				},
			},
		},
		{
			name: "large numbers and scientific notation",
			jsonContent: `[
{"large_int": 9007199254740991, "small_float": 0.000001, "large_float": 123456789.987654321, "negative_val": -42.5}
]`,
			columns: []string{"large_int", "small_float", "large_float", "negative_val"},
			expected: []DataRecord{
				{
					"large_int":    9.007199254740991e+15,
					"small_float":  1e-06,
					"large_float":  1.2345678998765433e+08,
					"negative_val": -42.5,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filePath := createTempJSON(t, "test.json", tt.jsonContent)
			loader := NewJSONLoader(filePath)
			records, err := loader.Load(tt.columns)
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if !reflect.DeepEqual(records, tt.expected) {
				t.Errorf("Load() got = %v, want %v", records, tt.expected)
				if len(records) > 0 && len(tt.expected) > 0 {
					for k, v := range records[0] {
						expectedV := tt.expected[0][k]
						if reflect.TypeOf(v) != reflect.TypeOf(expectedV) {
							t.Errorf("Type mismatch for key %s: got %T, want %T", k, v, expectedV)
						}
					}
				}
			}
		})
	}
}

func TestJSONLoader_Load_Error(t *testing.T) {
	tests := []struct {
		name          string
		jsonContent   string
		columns       []string
		expectedError string
	}{
		{
			name:          "file not found",
			jsonContent:   "", // Content doesn't matter
			columns:       []string{"id"},
			expectedError: "no such file or directory",
		},
		{
			name:          "empty file",
			jsonContent:   "",
			columns:       []string{"id"},
			expectedError: "' is empty", // Changed to be less specific about the path
		},
		{
			name:          "invalid json format",
			jsonContent:   `[{"id": 1, "name": "Test"}`, // Missing closing bracket and brace
			columns:       []string{"id", "name"},
			expectedError: "error unmarshalling JSON data",
		},
		{
			name: "missing required key",
			jsonContent: `[
{"id": "1", "name": "Product Alpha"},
{"id": "2"}
]`,
			columns:       []string{"id", "name"},
			expectedError: "missing required key 'name'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var filePath string
			if tt.name == "file not found" {
				filePath = filepath.Join(t.TempDir(), "non_existent_file.json")
			} else if tt.name == "empty file" {
				// Create an actual empty file for this specific test case
				filePath = createTempJSON(t, "test_error.json", "")
				// For other error cases, createTempJSON will handle content.
				// The "empty file" check in JSONLoader.Load happens after os.ReadFile but before json.Unmarshal.
			} else {
				filePath = createTempJSON(t, "test_error.json", tt.jsonContent)
			}

			loader := NewJSONLoader(filePath)
			_, err := loader.Load(tt.columns)
			if err == nil {
				t.Fatalf("Load() expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.expectedError) {
				t.Errorf("Load() error = %q, want error containing %q", err.Error(), tt.expectedError)
			}
		})
	}
}

func TestGetLoader(t *testing.T) {
	tests := []struct {
		name         string
		filePath     string
		expectedType interface{}
		expectError  bool
	}{
		{
			name:         "csv file",
			filePath:     "testdata.csv",
			expectedType: &CSVLoader{},
			expectError:  false,
		},
		{
			name:         "json file",
			filePath:     "testdata.json",
			expectedType: &JSONLoader{},
			expectError:  false,
		},
		{
			name:         "uppercase CSV extension",
			filePath:     "testdata.CSV",
			expectedType: &CSVLoader{},
			expectError:  false,
		},
		{
			name:         "uppercase JSON extension",
			filePath:     "testdata.JSON",
			expectedType: &JSONLoader{},
			expectError:  false,
		},
		{
			name:        "unsupported extension",
			filePath:    "testdata.txt",
			expectError: true,
		},
		{
			name:        "no extension",
			filePath:    "testdata",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create dummy files if they don't cause "file not found" type errors for GetLoader
			if !tt.expectError || (tt.expectError && tt.name != "unsupported extension" && tt.name != "no extension") {
				// For GetLoader, the file content doesn't matter, only the path/extension.
				// However, to avoid os.Stat errors if GetLoader were to check existence (it doesn't currently),
				// we can create empty dummy files.
				tempDir := t.TempDir()
				fullPath := filepath.Join(tempDir, tt.filePath)
				if _, err := os.Create(fullPath); err != nil {
					// Allow failure if the path is intentionally malformed for a test
					if !os.IsNotExist(err) && !strings.Contains(tt.filePath, "non_existent") {
						t.Fatalf("Failed to create dummy file %s: %v", fullPath, err)
					}
				}
				tt.filePath = fullPath // Use the path in the temp directory
			} else {
				// For tests expecting errors due to extension, ensure path is plausible
				// but doesn't need to exist if GetLoader doesn't check.
				// If GetLoader *did* check existence, these would need adjustment.
				tempDir := t.TempDir()
				tt.filePath = filepath.Join(tempDir, tt.filePath)
			}

			loader, err := GetLoader(tt.filePath)

			if tt.expectError {
				if err == nil {
					t.Errorf("GetLoader() expected error for %s, got nil", tt.filePath)
				}
				// Optionally, check for specific error message if needed
				//
				//	if !strings.Contains(err.Error(), "unsupported file type") {
				//		t.Errorf("GetLoader() error = %q, want error containing 'unsupported file type'", err.Error())
				//	}
			} else {
				if err != nil {
					t.Errorf("GetLoader() for %s error = %v, want nil", tt.filePath, err)
				}
				if reflect.TypeOf(loader) != reflect.TypeOf(tt.expectedType) {
					t.Errorf("GetLoader() for %s got type %T, want type %T", tt.filePath, loader, tt.expectedType)
				}
			}
		})
	}
}

func TestConvertJSONValueToString(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		// Nil value
		{"nil value", nil, ""},
		
		// String values
		{"string value", "hello", "hello"},
		{"empty string", "", ""},
		{"string with spaces", "hello world", "hello world"},
		
		// Boolean values
		{"bool true", true, "true"},
		{"bool false", false, "false"},
		
		// Integer values
		{"int value", 42, "42"},
		{"int64 value", int64(123), "123"},
		{"zero int", 0, "0"},
		{"negative int", -42, "-42"},
		
		// Float values
		{"float64 whole number", 5.0, "5"},
		{"float64 decimal", 3.14159, "3.14159"},
		{"zero float", 0.0, "0"},
		{"negative float", -2.5, "-2.5"},
		{"small float", 0.000001, "0.000001"},
		
		// Large numbers
		{"large int", int64(9007199254740991), "9007199254740991"},
		{"large float", 123456789.987654321, "123456789.98765433"}, // Go float64 precision limit
		
		// Edge cases for floats
		{"float with trailing zeros", 1.2300, "1.23"},
		{"very small decimal", 1e-10, "0.0000000001"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertJSONValueToString(tt.input)
			if result != tt.expected {
				t.Errorf("convertJSONValueToString(%v) = %q, want %q", tt.input, result, tt.expected)
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
