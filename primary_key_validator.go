package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

// PrimaryKeyValidator validates primary key integrity with configurable settings
// This validator ensures data consistency by rejecting any records with NULL, empty, or duplicate primary keys
type PrimaryKeyValidator struct {
	MaxKeyLength int  // Maximum allowed length for primary keys (default: 255)
	StrictMode   bool // Whether to enforce strict validation (default: true)
}

// PrimaryKeyValidationResult contains the results of primary key validation
type PrimaryKeyValidationResult struct {
	IsValid        bool                      // Overall validation result
	TotalRecords   int                       // Total number of records processed
	ValidRecords   int                       // Number of valid records
	InvalidRecords []InvalidPrimaryKeyRecord // Details of invalid records
	ErrorSummary   string                    // Human-readable error summary
	DuplicateKeys  map[string][]int          // Map of duplicate key values to record indices
}

// InvalidPrimaryKeyRecord represents a record with an invalid primary key
type InvalidPrimaryKeyRecord struct {
	RecordIndex     int        // Zero-based index in the record slice
	RecordData      DataRecord // The actual record data
	Reason          string     // Reason for invalidity
	PrimaryKeyValue string     // The invalid primary key value
}

// NewPrimaryKeyValidator creates a new validator with default settings
func NewPrimaryKeyValidator() *PrimaryKeyValidator {
	return &PrimaryKeyValidator{
		MaxKeyLength: 255,
		StrictMode:   true,
	}
}

// NewPrimaryKeyValidatorWithConfig creates a new validator with custom configuration
func NewPrimaryKeyValidatorWithConfig(maxKeyLength int, strictMode bool) *PrimaryKeyValidator {
	if maxKeyLength <= 0 {
		maxKeyLength = 255 // Default fallback
	}
	return &PrimaryKeyValidator{
		MaxKeyLength: maxKeyLength,
		StrictMode:   strictMode,
	}
}

// ValidateAllRecords performs comprehensive primary key validation with strict enforcement
// This function will ALWAYS return an error if any primary key violations are found
func (pkv *PrimaryKeyValidator) ValidateAllRecords(records []DataRecord, primaryKeyColumn string) (*PrimaryKeyValidationResult, error) {
	if primaryKeyColumn == "" {
		return nil, fmt.Errorf("CRITICAL: Primary key column name cannot be empty")
	}

	result := &PrimaryKeyValidationResult{
		TotalRecords:   len(records),
		IsValid:        true,
		InvalidRecords: make([]InvalidPrimaryKeyRecord, 0),
		DuplicateKeys:  make(map[string][]int),
	}

	if len(records) == 0 {
		log.Printf("Warning: No records to validate")
		return result, nil
	}

	seenKeys := make(map[string]int) // Map key -> first occurrence index

	log.Printf("Starting strict primary key validation for %d records...", len(records))

	for i, record := range records {
		// 1. Check if primary key column exists in record
		pkValue, exists := record[primaryKeyColumn]
		if !exists {
			pkv.addInvalidRecord(result, i, record, "primary_key_column_missing", "")
			continue
		}

		// 2. Convert to string for validation
		pkStr := convertValueToString(pkValue)

		// 3. STRICT NULL/empty check - this is CRITICAL for data integrity
		if pkv.isNullOrEmpty(pkStr) {
			pkv.addInvalidRecord(result, i, record, "primary_key_null_or_empty", pkStr)
			continue
		}

		// 4. Check for duplicates
		if firstIndex, isDuplicate := seenKeys[pkStr]; isDuplicate {
			// Record both occurrences as invalid
			pkv.addInvalidRecord(result, i, record, "primary_key_duplicate", pkStr)

			// Track duplicates for detailed reporting
			if _, exists := result.DuplicateKeys[pkStr]; !exists {
				result.DuplicateKeys[pkStr] = []int{firstIndex}
			}
			result.DuplicateKeys[pkStr] = append(result.DuplicateKeys[pkStr], i)
			continue
		}

		// 5. Additional validation for primary key format
		if err := pkv.validatePrimaryKeyFormat(pkStr); err != nil {
			pkv.addInvalidRecord(result, i, record, "primary_key_invalid_format", pkStr)
			continue
		}

		// Record valid primary key
		seenKeys[pkStr] = i
	}

	// Calculate valid records count
	result.ValidRecords = result.TotalRecords - len(result.InvalidRecords)

	// Generate comprehensive error summary
	if len(result.InvalidRecords) > 0 {
		result.IsValid = false
		result.ErrorSummary = pkv.generateErrorSummary(result)

		// Check strict mode enforcement
		if pkv.StrictMode {
			return result, fmt.Errorf("🚨 PRIMARY KEY VALIDATION FAILED: %s", result.ErrorSummary)
		}
		// In non-strict mode, log warnings but don't fail
		log.Printf("⚠️ PRIMARY KEY VALIDATION WARNINGS: %s", result.ErrorSummary)
	}

	log.Printf("✅ Primary key validation passed: %d valid records", result.ValidRecords)
	return result, nil
}

// addInvalidRecord adds a record to the invalid records list
func (pkv *PrimaryKeyValidator) addInvalidRecord(result *PrimaryKeyValidationResult, index int, record DataRecord, reason, pkValue string) {
	invalid := InvalidPrimaryKeyRecord{
		RecordIndex:     index,
		RecordData:      record,
		Reason:          reason,
		PrimaryKeyValue: pkValue,
	}
	result.InvalidRecords = append(result.InvalidRecords, invalid)
}

// isNullOrEmpty checks if a primary key value is null or empty
// This includes various representations of null/empty values
func (pkv *PrimaryKeyValidator) isNullOrEmpty(value string) bool {
	if value == "" {
		return true
	}

	// Normalize and check common null representations
	lower := strings.ToLower(strings.TrimSpace(value))
	nullValues := []string{"null", "nil", "\\n", "n/a", "na", "none", "undefined"}

	for _, nullVal := range nullValues {
		if lower == nullVal {
			return true
		}
	}

	return false
}

// validatePrimaryKeyFormat performs additional format validation for primary keys
func (pkv *PrimaryKeyValidator) validatePrimaryKeyFormat(pkValue string) error {
	// Check for suspicious characters that might indicate data corruption
	if strings.Contains(pkValue, "\n") || strings.Contains(pkValue, "\r") {
		return fmt.Errorf("primary key contains line breaks")
	}

	if strings.Contains(pkValue, "\t") {
		return fmt.Errorf("primary key contains tab characters")
	}

	// Check for extremely long primary keys (potential data corruption)
	if len(pkValue) > pkv.MaxKeyLength {
		return fmt.Errorf("primary key exceeds maximum length (%d characters)", pkv.MaxKeyLength)
	}

	// Check for leading/trailing whitespace (data quality issue)
	if strings.TrimSpace(pkValue) != pkValue {
		return fmt.Errorf("primary key has leading or trailing whitespace")
	}

	return nil
}

// generateErrorSummary creates a comprehensive error summary
func (pkv *PrimaryKeyValidator) generateErrorSummary(result *PrimaryKeyValidationResult) string {
	if len(result.InvalidRecords) == 0 {
		return "No errors found"
	}

	// Count issues by type
	issueCount := make(map[string]int)
	for _, invalid := range result.InvalidRecords {
		issueCount[invalid.Reason]++
	}

	var summary strings.Builder
	summary.WriteString(fmt.Sprintf("Found %d invalid primary key records out of %d total records. ",
		len(result.InvalidRecords), result.TotalRecords))

	summary.WriteString("Issues: ")
	issues := make([]string, 0, len(issueCount))
	for reason, count := range issueCount {
		issues = append(issues, fmt.Sprintf("%s (%d)", reason, count))
	}
	summary.WriteString(strings.Join(issues, ", "))

	return summary.String()
}

// ReportValidationFailure provides detailed reporting of primary key validation failures
func (pkv *PrimaryKeyValidator) ReportValidationFailure(result *PrimaryKeyValidationResult) {
	if result.IsValid {
		return
	}

	log.Printf("❌ PRIMARY KEY VALIDATION FAILED - SYNC ABORTED")
	log.Printf("═══════════════════════════════════════════════")
	log.Printf("Total records: %d", result.TotalRecords)
	log.Printf("Valid records: %d", result.ValidRecords)
	log.Printf("Invalid records: %d", len(result.InvalidRecords))
	log.Printf("")

	// Report issues by category
	issueCount := make(map[string]int)
	for _, invalid := range result.InvalidRecords {
		issueCount[invalid.Reason]++
	}

	log.Printf("Issues breakdown:")
	for reason, count := range issueCount {
		log.Printf("  ▶ %s: %d records", pkv.formatReasonDescription(reason), count)
	}
	log.Printf("")

	// Report duplicate keys if any
	if len(result.DuplicateKeys) > 0 {
		log.Printf("Duplicate primary keys found:")
		for key, indices := range result.DuplicateKeys {
			log.Printf("  ▶ Key '%s' appears in records: %v", key, pkv.formatIndicesForDisplay(indices))
		}
		log.Printf("")
	}

	// Show sample invalid records (first 10)
	maxSamples := 10
	if len(result.InvalidRecords) > 0 {
		log.Printf("Sample invalid records (first %d):", min(maxSamples, len(result.InvalidRecords)))
		for i, invalid := range result.InvalidRecords {
			if i >= maxSamples {
				break
			}
			log.Printf("  Record %d: %s - Primary Key: '%s'",
				invalid.RecordIndex+1,
				pkv.formatReasonDescription(invalid.Reason),
				invalid.PrimaryKeyValue)
		}

		if len(result.InvalidRecords) > maxSamples {
			log.Printf("  ... and %d more invalid records", len(result.InvalidRecords)-maxSamples)
		}
	}

	log.Printf("")
	log.Printf("🚨 DATA SYNC ABORTED FOR DATA SAFETY")
	log.Printf("Please fix the primary key issues and retry the synchronization.")
	log.Printf("═══════════════════════════════════════════════")
}

// formatReasonDescription converts internal reason codes to user-friendly descriptions
func (pkv *PrimaryKeyValidator) formatReasonDescription(reason string) string {
	descriptions := map[string]string{
		"primary_key_column_missing": "Primary key column not found in record",
		"primary_key_null_or_empty":  "Primary key is NULL or empty",
		"primary_key_duplicate":      "Duplicate primary key value",
		"primary_key_invalid_format": "Invalid primary key format",
	}

	if desc, exists := descriptions[reason]; exists {
		return desc
	}
	return reason
}

// formatIndicesForDisplay formats record indices for user-friendly display
func (pkv *PrimaryKeyValidator) formatIndicesForDisplay(indices []int) string {
	displayIndices := make([]string, 0, len(indices))
	for _, idx := range indices {
		displayIndices = append(displayIndices, strconv.Itoa(idx+1)) // Convert to 1-based indexing
	}
	return strings.Join(displayIndices, ", ")
}
