package main

import (
	"strings"
	"testing"
)

func TestPrimaryKeyValidator_ValidateAllRecords(t *testing.T) {
	validator := NewPrimaryKeyValidator()

	tests := []struct {
		name             string
		records          []DataRecord
		primaryKeyColumn string
		expectError      bool
		expectedValid    int
		expectedInvalid  int
		expectedReasons  []string
	}{
		{
			name: "All valid records",
			records: []DataRecord{
				{"id": "1", "name": "Alice"},
				{"id": "2", "name": "Bob"},
				{"id": "3", "name": "Charlie"},
			},
			primaryKeyColumn: "id",
			expectError:      false,
			expectedValid:    3,
			expectedInvalid:  0,
			expectedReasons:  []string{},
		},
		{
			name: "NULL primary key should fail",
			records: []DataRecord{
				{"id": "1", "name": "Alice"},
				{"id": nil, "name": "Bob"}, // NULL primary key
				{"id": "3", "name": "Charlie"},
			},
			primaryKeyColumn: "id",
			expectError:      true,
			expectedValid:    2, // Valid records are counted even with errors
			expectedInvalid:  1,
			expectedReasons:  []string{"primary_key_null_or_empty"},
		},
		{
			name: "Empty primary key should fail",
			records: []DataRecord{
				{"id": "1", "name": "Alice"},
				{"id": "", "name": "Bob"}, // Empty primary key
				{"id": "3", "name": "Charlie"},
			},
			primaryKeyColumn: "id",
			expectError:      true,
			expectedValid:    2,
			expectedInvalid:  1,
			expectedReasons:  []string{"primary_key_null_or_empty"},
		},
		{
			name: "Various null representations should fail",
			records: []DataRecord{
				{"id": "null", "name": "Alice"},  // String 'null'
				{"id": "NULL", "name": "Bob"},    // String 'NULL'
				{"id": "nil", "name": "Charlie"}, // String 'nil'
				{"id": "n/a", "name": "David"},   // String 'n/a'
			},
			primaryKeyColumn: "id",
			expectError:      true,
			expectedValid:    0,
			expectedInvalid:  4, // All null representations detected
			expectedReasons:  []string{"primary_key_null_or_empty"},
		},
		{
			name: "Duplicate primary keys should fail",
			records: []DataRecord{
				{"id": "1", "name": "Alice"},
				{"id": "2", "name": "Bob"},
				{"id": "1", "name": "Charlie"}, // Duplicate primary key
			},
			primaryKeyColumn: "id",
			expectError:      true,
			expectedValid:    2,
			expectedInvalid:  1,
			expectedReasons:  []string{"primary_key_duplicate"},
		},
		{
			name: "Missing primary key column should fail",
			records: []DataRecord{
				{"id": "1", "name": "Alice"},
				{"name": "Bob"}, // Missing 'id' column
				{"id": "3", "name": "Charlie"},
			},
			primaryKeyColumn: "id",
			expectError:      true,
			expectedValid:    2,
			expectedInvalid:  1,
			expectedReasons:  []string{"primary_key_column_missing"},
		},
		{
			name: "Primary key with whitespace should fail",
			records: []DataRecord{
				{"id": "1", "name": "Alice"},
				{"id": " 2 ", "name": "Bob"}, // Primary key with leading/trailing spaces
				{"id": "3", "name": "Charlie"},
			},
			primaryKeyColumn: "id",
			expectError:      true,
			expectedValid:    2,
			expectedInvalid:  1,
			expectedReasons:  []string{"primary_key_invalid_format"},
		},
		{
			name: "Primary key with line breaks should fail",
			records: []DataRecord{
				{"id": "1", "name": "Alice"},
				{"id": "2\n", "name": "Bob"}, // Primary key with line break
				{"id": "3", "name": "Charlie"},
			},
			primaryKeyColumn: "id",
			expectError:      true,
			expectedValid:    2,
			expectedInvalid:  1,
			expectedReasons:  []string{"primary_key_invalid_format"},
		},
		{
			name: "Custom MaxKeyLength should be respected",
			records: []DataRecord{
				{"id": "short", "name": "Alice"},
				{"id": "toolong", "name": "Bob"}, // 7 chars, exceeds limit of 5
			},
			primaryKeyColumn: "id",
			expectError:      true,
			expectedValid:    1,
			expectedInvalid:  1,
			expectedReasons:  []string{"primary_key_invalid_format"},
		},
		{
			name: "Empty primary key column name should fail",
			records: []DataRecord{
				{"id": "1", "name": "Alice"},
			},
			primaryKeyColumn: "",
			expectError:      true,
			expectedValid:    0,
			expectedInvalid:  0,
			expectedReasons:  []string{},
		},
		{
			name:             "Empty records should pass",
			records:          []DataRecord{},
			primaryKeyColumn: "id",
			expectError:      false,
			expectedValid:    0,
			expectedInvalid:  0,
			expectedReasons:  []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use custom validator for MaxKeyLength test
			testValidator := validator
			if tt.name == "Custom MaxKeyLength should be respected" {
				testValidator = NewPrimaryKeyValidatorWithConfig(5, true)
			}
			result, err := testValidator.ValidateAllRecords(tt.records, tt.primaryKeyColumn)

			// Check error expectation
			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}

			if result == nil {
				if !tt.expectError {
					t.Errorf("Result should not be nil when no error expected")
				}
				return
			}

			// Check total records
			if result.TotalRecords != len(tt.records) {
				t.Errorf("Expected TotalRecords=%d, got %d", len(tt.records), result.TotalRecords)
			}

			// Check valid records count
			if result.ValidRecords != tt.expectedValid {
				t.Errorf("Expected ValidRecords=%d, got %d", tt.expectedValid, result.ValidRecords)
			}

			// Check invalid records count
			if len(result.InvalidRecords) != tt.expectedInvalid {
				t.Errorf("Expected InvalidRecords=%d, got %d", tt.expectedInvalid, len(result.InvalidRecords))
			}

			// Check reasons if any invalid records expected
			if tt.expectedInvalid > 0 && len(tt.expectedReasons) > 0 {
				found := false
				for _, invalid := range result.InvalidRecords {
					for _, expectedReason := range tt.expectedReasons {
						if invalid.Reason == expectedReason {
							found = true
							break
						}
					}
					if found {
						break
					}
				}
				if !found {
					t.Errorf("Expected reasons %v not found in invalid records", tt.expectedReasons)
				}
			}

			// Check IsValid flag
			expectedIsValid := len(result.InvalidRecords) == 0
			if result.IsValid != expectedIsValid {
				t.Errorf("Expected IsValid=%v, got %v", expectedIsValid, result.IsValid)
			}
		})
	}
}

func TestPrimaryKeyValidator_isNullOrEmpty(t *testing.T) {
	validator := NewPrimaryKeyValidator()

	tests := []struct {
		name     string
		value    string
		expected bool
	}{
		{"Empty string", "", true},
		{"null lowercase", "null", true},
		{"NULL uppercase", "NULL", true},
		{"nil", "nil", true},
		{"NIL uppercase", "NIL", true},
		{"n/a", "n/a", true},
		{"N/A uppercase", "N/A", true},
		{"na", "na", true},
		{"none", "none", true},
		{"undefined", "undefined", true},
		{"whitespace only", "   ", false}, // Trimmed, but not explicitly null
		{"valid number", "123", false},
		{"valid string", "abc", false},
		{"zero", "0", false},
		{"space null space", " null ", true}, // Should be trimmed and detected
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := validator.isNullOrEmpty(tt.value)
			if result != tt.expected {
				t.Errorf("isNullOrEmpty(%q) = %v, want %v", tt.value, result, tt.expected)
			}
		})
	}
}

func TestPrimaryKeyValidator_validatePrimaryKeyFormat(t *testing.T) {
	validator := NewPrimaryKeyValidator()

	tests := []struct {
		name        string
		value       string
		expectError bool
	}{
		{"Valid alphanumeric", "abc123", false},
		{"Valid with underscore", "user_123", false},
		{"Valid with hyphen", "item-456", false},
		{"Line break should fail", "123\n", true},
		{"Carriage return should fail", "123\r", true},
		{"Tab character should fail", "123\t", true},
		{"Leading space should fail", " 123", true},
		{"Trailing space should fail", "123 ", true},
		{"Both spaces should fail", " 123 ", true},
		{"Very long key should fail", strings.Repeat("a", 256), true},
		{"Max length should pass", strings.Repeat("a", 255), false},
		{"Valid special characters", "test@example.com", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.validatePrimaryKeyFormat(tt.value)
			hasError := err != nil
			if hasError != tt.expectError {
				if tt.expectError {
					t.Errorf("Expected error for value %q but got none", tt.value)
				} else {
					t.Errorf("Expected no error for value %q but got: %v", tt.value, err)
				}
			}
		})
	}
}

func TestPrimaryKeyValidator_ReportValidationFailure(t *testing.T) {
	validator := NewPrimaryKeyValidator()

	// Create a validation result with various types of errors
	result := &PrimaryKeyValidationResult{
		IsValid:      false,
		TotalRecords: 5,
		ValidRecords: 2,
		InvalidRecords: []InvalidPrimaryKeyRecord{
			{
				RecordIndex:     1,
				RecordData:      DataRecord{"id": "", "name": "Bob"},
				Reason:          "primary_key_null_or_empty",
				PrimaryKeyValue: "",
			},
			{
				RecordIndex:     3,
				RecordData:      DataRecord{"id": "1", "name": "Charlie"},
				Reason:          "primary_key_duplicate",
				PrimaryKeyValue: "1",
			},
			{
				RecordIndex:     4,
				RecordData:      DataRecord{"name": "David"},
				Reason:          "primary_key_column_missing",
				PrimaryKeyValue: "",
			},
		},
		DuplicateKeys: map[string][]int{
			"1": {0, 3},
		},
		ErrorSummary: "Found 3 invalid primary key records out of 5 total records",
	}

	// This test mainly ensures the reporting function doesn't panic
	// In a real scenario, you might want to capture log output to verify the format
	validator.ReportValidationFailure(result)

	// Test with valid result (should not print anything)
	validResult := &PrimaryKeyValidationResult{
		IsValid:      true,
		TotalRecords: 3,
		ValidRecords: 3,
	}
	validator.ReportValidationFailure(validResult)
}

func TestPrimaryKeyValidator_Integration(t *testing.T) {
	// Test the complete flow with realistic data
	validator := NewPrimaryKeyValidator()

	// Simulate realistic CSV data with various issues
	records := []DataRecord{
		{"user_id": "1", "email": "alice@example.com", "status": "active"},
		{"user_id": "2", "email": "bob@example.com", "status": "active"},
		{"user_id": "", "email": "charlie@example.com", "status": "inactive"},  // Empty ID
		{"user_id": "null", "email": "david@example.com", "status": "pending"}, // Null string
		{"user_id": "2", "email": "eve@example.com", "status": "active"},       // Duplicate ID
		{"email": "frank@example.com", "status": "active"},                     // Missing ID column
	}

	result, err := validator.ValidateAllRecords(records, "user_id")

	// Should fail due to multiple issues
	if err == nil {
		t.Errorf("Expected error due to multiple primary key violations")
	}

	if result == nil {
		t.Errorf("Result should not be nil")
		return
	}

	// Verify comprehensive error detection
	if result.TotalRecords != 6 {
		t.Errorf("Expected TotalRecords=6, got %d", result.TotalRecords)
	}

	if result.IsValid {
		t.Errorf("Expected IsValid=false due to violations")
	}

	// Should have at least one invalid record (strict mode stops at first error)
	if len(result.InvalidRecords) == 0 {
		t.Errorf("Expected at least one invalid record")
	}

	// Verify error summary is generated
	if result.ErrorSummary == "" {
		t.Errorf("Expected non-empty error summary")
	}

	// Test the reporting (should not panic)
	validator.ReportValidationFailure(result)
}

func TestPrimaryKeyValidator_CustomConfiguration(t *testing.T) {
	tests := []struct {
		name         string
		maxKeyLength int
		strictMode   bool
		records      []DataRecord
		expectError  bool
	}{
		{
			name:         "Non-strict mode should not fail on violations",
			maxKeyLength: 255,
			strictMode:   false,
			records: []DataRecord{
				{"id": "1", "name": "Alice"},
				{"id": "", "name": "Bob"}, // Empty primary key
			},
			expectError: false, // Should not fail in non-strict mode
		},
		{
			name:         "Custom max length should be enforced",
			maxKeyLength: 3,
			strictMode:   true,
			records: []DataRecord{
				{"id": "123", "name": "Alice"}, // Exactly 3 chars
				{"id": "1234", "name": "Bob"},  // 4 chars, exceeds limit
			},
			expectError: true,
		},
		{
			name:         "Zero max length should default to 255",
			maxKeyLength: 0,
			strictMode:   true,
			records: []DataRecord{
				{"id": strings.Repeat("a", 256), "name": "Alice"}, // 256 chars, exceeds default 255
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := NewPrimaryKeyValidatorWithConfig(tt.maxKeyLength, tt.strictMode)

			// Verify configuration
			if tt.maxKeyLength > 0 && validator.MaxKeyLength != tt.maxKeyLength {
				t.Errorf("Expected MaxKeyLength=%d, got %d", tt.maxKeyLength, validator.MaxKeyLength)
			}
			if tt.maxKeyLength <= 0 && validator.MaxKeyLength != 255 {
				t.Errorf("Expected default MaxKeyLength=255, got %d", validator.MaxKeyLength)
			}
			if validator.StrictMode != tt.strictMode {
				t.Errorf("Expected StrictMode=%v, got %v", tt.strictMode, validator.StrictMode)
			}

			result, err := validator.ValidateAllRecords(tt.records, "id")

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}

			if result == nil {
				t.Errorf("Result should not be nil")
			}
		})
	}
}
