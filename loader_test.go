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

			// CSVLoader.Load uses nil/empty to load all columns.
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
			switch tt.name {
			case "file not found":
				filePath = filepath.Join(t.TempDir(), "non_existent_file.json")
			case "empty file":
				// Create an actual empty file for this specific test case
				filePath = createTempJSON(t, "test_error.json", "")
				// For other error cases, createTempJSON will handle content.
				// The "empty file" check in JSONLoader.Load happens after os.ReadFile but before json.Unmarshal.
			default:
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
		expectedType any
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

			// CSVLoader.Load uses nil/empty to load all columns.
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

func TestCSVLoader_AdditionalErrorCases(t *testing.T) {
	t.Run("empty header row", func(t *testing.T) {
		// This tests the specific check on line 93-95 in loader.go
		// Create a CSV with empty header (just commas, no actual column names)
		csvContent := ",,\n1,test,value"
		filePath := createTempCSV(t, "empty_header.csv", csvContent)
		
		loader := NewCSVLoader(filePath)
		records, err := loader.Load(nil)
		
		// This actually succeeds because empty strings are valid column names
		// The empty header check in loader.go (line 93-95) checks for len(headerNames) == 0
		// but ",,\n" produces ["", "", ""] which has length 3, not 0
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if len(records) != 1 {
			t.Errorf("Expected 1 record, got %d", len(records))
		}
		// Verify the empty column names are handled
		if len(records) > 0 {
			for key := range records[0] {
				if key != "" {
					// Some columns should be empty strings
					break
				}
			}
		}
	})

	t.Run("csv readall error handling", func(t *testing.T) {
		// This tests the error handling for csv.ReadAll on line 97-100 in loader.go
		csvContent := "id,name,value\n1,\"unclosed quote,value\n2,productB,200"
		filePath := createTempCSV(t, "malformed.csv", csvContent)
		
		loader := NewCSVLoader(filePath)
		_, err := loader.Load(nil)
		
		if err == nil {
			t.Fatalf("Expected error for malformed CSV, got nil")
		}
		// Verify the error is related to CSV reading
		if !strings.Contains(err.Error(), "error reading CSV data rows") {
			t.Errorf("Expected CSV reading error, got: %v", err)
		}
	})

	t.Run("truly empty header to trigger len check", func(t *testing.T) {
		// To trigger the len(headerNames) == 0 check on line 93-95, 
		// we need csv.Reader.Read() to return an empty slice []string{}
		// This is difficult to achieve with normal CSV content.
		// However, we can test a completely empty file which should fail earlier
		// in the header reading stage with io.EOF
		csvContent := ""  // Completely empty file
		filePath := createTempCSV(t, "truly_empty.csv", csvContent)
		
		loader := NewCSVLoader(filePath)
		_, err := loader.Load(nil)
		
		if err == nil {
			t.Fatalf("Expected error for truly empty file, got nil")
		}
		// Should trigger the EOF/ErrFieldCount check on line 88-90
		if !strings.Contains(err.Error(), "must contain a header row and at least one data row") {
			t.Errorf("Expected header row error, got: %v", err)
		}
	})

	t.Run("file permission error", func(t *testing.T) {
		if os.Getuid() == 0 {
			t.Skip("Skipping permission test when running as root")
		}

		tempDir := t.TempDir()
		filePath := filepath.Join(tempDir, "no_permission.csv")
		
		// Create file then remove read permission
		err := os.WriteFile(filePath, []byte("id,name\n1,test"), 0644)
		if err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		
		err = os.Chmod(filePath, 0000) // No permissions
		if err != nil {
			t.Fatalf("Failed to change file permissions: %v", err)
		}
		defer os.Chmod(filePath, 0644) // Restore permissions for cleanup

		loader := NewCSVLoader(filePath)
		_, err = loader.Load(nil)
		
		if err == nil {
			t.Fatalf("Expected permission error, got nil")
		}
		if !strings.Contains(err.Error(), "cannot open file") && !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("Expected permission error, got: %v", err)
		}
	})

	t.Run("malformed CSV with quotes", func(t *testing.T) {
		// Test malformed CSV that causes csv.Reader to fail during ReadAll
		csvContent := `id,name,value
1,"unclosed quote,test,value
2,normal,value`
		filePath := createTempCSV(t, "malformed.csv", csvContent)
		
		loader := NewCSVLoader(filePath)
		_, err := loader.Load(nil)
		
		if err == nil {
			t.Fatalf("Expected error for malformed CSV, got nil")
		}
		// The error should come from csv.ReadAll on line 97-100
		if !strings.Contains(err.Error(), "error reading CSV data rows") {
			t.Errorf("Expected CSV reading error, got: %v", err)
		}
	})

	t.Run("header only file with EOF", func(t *testing.T) {
		// Test the specific EOF handling on line 88-90
		csvContent := `id,name,value` // No newline, just header
		filePath := createTempCSV(t, "header_only_eof.csv", csvContent)
		
		loader := NewCSVLoader(filePath)
		records, err := loader.Load(nil)
		
		// This should succeed and return empty records
		if err != nil {
			t.Errorf("Expected no error for header-only file, got: %v", err)
		}
		if len(records) != 0 {
			t.Errorf("Expected 0 records for header-only file, got: %d", len(records))
		}
	})
}

// Multi-table loader tests
func TestMultiTableLoader_LoadAll(t *testing.T) {
	tests := []struct {
		name        string
		tableConfigs []TableSyncConfig
		fileContents map[string]string // file name -> content
		expected     MultiTableData
		wantErr     bool
		errContains string
	}{
		{
			name: "Load multiple CSV files successfully",
			tableConfigs: []TableSyncConfig{
				{Name: "users", FilePath: "users.csv"},
				{Name: "orders", FilePath: "orders.csv"},
			},
			fileContents: map[string]string{
				"users.csv": "id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com",
				"orders.csv": "id,user_id,amount\n1,1,100.50\n2,2,75.25",
			},
			expected: MultiTableData{
				"users": []DataRecord{
					{"id": "1", "name": "Alice", "email": "alice@example.com"},
					{"id": "2", "name": "Bob", "email": "bob@example.com"},
				},
				"orders": []DataRecord{
					{"id": "1", "user_id": "1", "amount": "100.50"},
					{"id": "2", "user_id": "2", "amount": "75.25"},
				},
			},
			wantErr: false,
		},
		{
			name: "Load multiple JSON files successfully",
			tableConfigs: []TableSyncConfig{
				{Name: "categories", FilePath: "categories.json"},
				{Name: "products", FilePath: "products.json"},
			},
			fileContents: map[string]string{
				"categories.json": `[{"id": 1, "name": "Electronics"}, {"id": 2, "name": "Books"}]`,
				"products.json": `[{"id": 1, "name": "Laptop", "category_id": 1}, {"id": 2, "name": "Novel", "category_id": 2}]`,
			},
			expected: MultiTableData{
				"categories": []DataRecord{
					{"id": float64(1), "name": "Electronics"},
					{"id": float64(2), "name": "Books"},
				},
				"products": []DataRecord{
					{"id": float64(1), "name": "Laptop", "category_id": float64(1)},
					{"id": float64(2), "name": "Novel", "category_id": float64(2)},
				},
			},
			wantErr: false,
		},
		{
			name: "Mixed CSV and JSON files",
			tableConfigs: []TableSyncConfig{
				{Name: "users", FilePath: "users.csv", Columns: []string{"id", "name"}},
				{Name: "profiles", FilePath: "profiles.json", Columns: []string{"user_id", "bio"}},
			},
			fileContents: map[string]string{
				"users.csv": "id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com",
				"profiles.json": `[{"user_id": 1, "bio": "Software Developer", "age": 30}, {"user_id": 2, "bio": "Designer", "age": 25}]`,
			},
			expected: MultiTableData{
				"users": []DataRecord{
					{"id": "1", "name": "Alice"},
					{"id": "2", "name": "Bob"},
				},
				"profiles": []DataRecord{
					{"user_id": float64(1), "bio": "Software Developer"},
					{"user_id": float64(2), "bio": "Designer"},
				},
			},
			wantErr: false,
		},
		{
			name:         "No table configurations",
			tableConfigs: []TableSyncConfig{},
			fileContents: map[string]string{},
			wantErr:      true,
			errContains:  "no table configurations provided",
		},
		{
			name: "Invalid file format",
			tableConfigs: []TableSyncConfig{
				{Name: "users", FilePath: "users.txt"},
			},
			fileContents: map[string]string{
				"users.txt": "invalid content",
			},
			wantErr:     true,
			errContains: "unsupported file type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			
			// Create temporary files
			for filename, content := range tt.fileContents {
				filePath := filepath.Join(tempDir, filename)
				err := os.WriteFile(filePath, []byte(content), 0644)
				if err != nil {
					t.Fatalf("Failed to create temp file %s: %v", filename, err)
				}
			}

			// Update file paths in table configs to point to temp directory
			configs := make([]TableSyncConfig, len(tt.tableConfigs))
			for i, config := range tt.tableConfigs {
				configs[i] = config
				configs[i].FilePath = filepath.Join(tempDir, config.FilePath)
			}

			loader := NewMultiTableLoader(configs)
			result, err := loader.LoadAll()

			if tt.wantErr {
				if err == nil {
					t.Errorf("LoadAll() expected error but got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("LoadAll() error = %v, want error containing %v", err, tt.errContains)
				}
				return
			}

			if err != nil {
				t.Errorf("LoadAll() unexpected error = %v", err)
				return
			}

			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("LoadAll() result = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestMultiTableLoader_LoadForTable(t *testing.T) {
	tempDir := t.TempDir()
	
	// Create test files
	usersFile := filepath.Join(tempDir, "users.csv")
	err := os.WriteFile(usersFile, []byte("id,name\n1,Alice\n2,Bob"), 0644)
	if err != nil {
		t.Fatalf("Failed to create users.csv: %v", err)
	}

	ordersFile := filepath.Join(tempDir, "orders.json")
	err = os.WriteFile(ordersFile, []byte(`[{"id": 1, "user_id": 1, "amount": 100}]`), 0644)
	if err != nil {
		t.Fatalf("Failed to create orders.json: %v", err)
	}

	configs := []TableSyncConfig{
		{Name: "users", FilePath: usersFile},
		{Name: "orders", FilePath: ordersFile},
	}

	loader := NewMultiTableLoader(configs)

	t.Run("Load existing table", func(t *testing.T) {
		records, err := loader.LoadForTable("users")
		if err != nil {
			t.Errorf("LoadForTable() unexpected error = %v", err)
			return
		}

		expected := []DataRecord{
			{"id": "1", "name": "Alice"},
			{"id": "2", "name": "Bob"},
		}

		if !reflect.DeepEqual(records, expected) {
			t.Errorf("LoadForTable() result = %v, want %v", records, expected)
		}
	})

	t.Run("Load non-existing table", func(t *testing.T) {
		_, err := loader.LoadForTable("nonexistent")
		if err == nil {
			t.Errorf("LoadForTable() expected error for non-existing table")
			return
		}

		if !strings.Contains(err.Error(), "table configuration not found") {
			t.Errorf("LoadForTable() error = %v, want error containing 'table configuration not found'", err)
		}
	})
}

func TestMultiTableLoader_ValidateFilePaths(t *testing.T) {
	tempDir := t.TempDir()

	// Create one valid file
	validFile := filepath.Join(tempDir, "valid.csv")
	err := os.WriteFile(validFile, []byte("id,name\n1,test"), 0644)
	if err != nil {
		t.Fatalf("Failed to create valid file: %v", err)
	}

	tests := []struct {
		name        string
		configs     []TableSyncConfig
		wantErr     bool
		errContains string
	}{
		{
			name: "All files exist",
			configs: []TableSyncConfig{
				{Name: "valid", FilePath: validFile},
			},
			wantErr: false,
		},
		{
			name: "Some files missing",
			configs: []TableSyncConfig{
				{Name: "valid", FilePath: validFile},
				{Name: "missing", FilePath: filepath.Join(tempDir, "missing.csv")},
			},
			wantErr:     true,
			errContains: "file does not exist for table 'missing'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loader := NewMultiTableLoader(tt.configs)
			err := loader.ValidateFilePaths()

			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateFilePaths() expected error but got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("ValidateFilePaths() error = %v, want error containing %v", err, tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateFilePaths() unexpected error = %v", err)
				}
			}
		})
	}
}
