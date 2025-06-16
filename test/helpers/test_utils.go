package helpers

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

// TestHelper provides utility functions for tests
type TestHelper struct {
	t       *testing.T
	tempDir string
}

// NewTestHelper creates a new test helper
func NewTestHelper(t *testing.T) *TestHelper {
	t.Helper()
	
	tempDir, err := os.MkdirTemp("", "test_helper_")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})
	
	return &TestHelper{
		t:       t,
		tempDir: tempDir,
	}
}

// GetTempDir returns the temporary directory for this test
func (th *TestHelper) GetTempDir() string {
	return th.tempDir
}

// CreateTempFile creates a temporary file with the given content
func (th *TestHelper) CreateTempFile(filename, content string) string {
	th.t.Helper()
	
	filePath := filepath.Join(th.tempDir, filename)
	
	// Create directory if needed
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		th.t.Fatalf("Failed to create directory %s: %v", dir, err)
	}
	
	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		th.t.Fatalf("Failed to create temp file %s: %v", filePath, err)
	}
	
	return filePath
}

// AssertEqual checks if two values are equal
func (th *TestHelper) AssertEqual(expected, actual interface{}, msgAndArgs ...interface{}) {
	th.t.Helper()
	
	if !reflect.DeepEqual(expected, actual) {
		msg := "Values are not equal"
		if len(msgAndArgs) > 0 {
			msg = fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
		}
		th.t.Errorf("%s:\nExpected: %+v\nActual: %+v", msg, expected, actual)
	}
}

// AssertNotEqual checks if two values are not equal
func (th *TestHelper) AssertNotEqual(expected, actual interface{}, msgAndArgs ...interface{}) {
	th.t.Helper()
	
	if reflect.DeepEqual(expected, actual) {
		msg := "Values should not be equal"
		if len(msgAndArgs) > 0 {
			msg = fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
		}
		th.t.Errorf("%s:\nBoth values: %+v", msg, expected)
	}
}

// AssertNil checks if a value is nil
func (th *TestHelper) AssertNil(value interface{}, msgAndArgs ...interface{}) {
	th.t.Helper()
	
	if value != nil {
		msg := "Expected nil value"
		if len(msgAndArgs) > 0 {
			msg = fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
		}
		th.t.Errorf("%s:\nActual: %+v", msg, value)
	}
}

// AssertNotNil checks if a value is not nil
func (th *TestHelper) AssertNotNil(value interface{}, msgAndArgs ...interface{}) {
	th.t.Helper()
	
	if value == nil {
		msg := "Expected non-nil value"
		if len(msgAndArgs) > 0 {
			msg = fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
		}
		th.t.Errorf("%s", msg)
	}
}

// AssertNoError checks if error is nil
func (th *TestHelper) AssertNoError(err error, msgAndArgs ...interface{}) {
	th.t.Helper()
	
	if err != nil {
		msg := "Unexpected error"
		if len(msgAndArgs) > 0 {
			msg = fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
		}
		th.t.Errorf("%s: %v", msg, err)
	}
}

// AssertError checks if error is not nil
func (th *TestHelper) AssertError(err error, msgAndArgs ...interface{}) {
	th.t.Helper()
	
	if err == nil {
		msg := "Expected error but got nil"
		if len(msgAndArgs) > 0 {
			msg = fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
		}
		th.t.Errorf("%s", msg)
	}
}

// AssertContains checks if a string contains a substring
func (th *TestHelper) AssertContains(str, substr string, msgAndArgs ...interface{}) {
	th.t.Helper()
	
	if !strings.Contains(str, substr) {
		msg := "String does not contain expected substring"
		if len(msgAndArgs) > 0 {
			msg = fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
		}
		th.t.Errorf("%s:\nString: %s\nSubstring: %s", msg, str, substr)
	}
}

// AssertNotContains checks if a string does not contain a substring
func (th *TestHelper) AssertNotContains(str, substr string, msgAndArgs ...interface{}) {
	th.t.Helper()
	
	if strings.Contains(str, substr) {
		msg := "String should not contain substring"
		if len(msgAndArgs) > 0 {
			msg = fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
		}
		th.t.Errorf("%s:\nString: %s\nSubstring: %s", msg, str, substr)
	}
}

// AssertGreaterThan checks if a value is greater than another
func (th *TestHelper) AssertGreaterThan(actual, expected interface{}, msgAndArgs ...interface{}) {
	th.t.Helper()
	
	actualVal := reflect.ValueOf(actual)
	expectedVal := reflect.ValueOf(expected)
	
	if actualVal.Kind() != expectedVal.Kind() {
		th.t.Errorf("Cannot compare different types: %T vs %T", actual, expected)
		return
	}
	
	switch actualVal.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if actualVal.Int() <= expectedVal.Int() {
			msg := "Expected value to be greater"
			if len(msgAndArgs) > 0 {
				msg = fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
			}
			th.t.Errorf("%s:\nActual: %v\nExpected greater than: %v", msg, actual, expected)
		}
	case reflect.Float32, reflect.Float64:
		if actualVal.Float() <= expectedVal.Float() {
			msg := "Expected value to be greater"
			if len(msgAndArgs) > 0 {
				msg = fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
			}
			th.t.Errorf("%s:\nActual: %v\nExpected greater than: %v", msg, actual, expected)
		}
	default:
		th.t.Errorf("Cannot compare type %T", actual)
	}
}

// AssertFileExists checks if a file exists
func (th *TestHelper) AssertFileExists(filePath string, msgAndArgs ...interface{}) {
	th.t.Helper()
	
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		msg := "File does not exist"
		if len(msgAndArgs) > 0 {
			msg = fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
		}
		th.t.Errorf("%s: %s", msg, filePath)
	}
}

// AssertFileNotExists checks if a file does not exist
func (th *TestHelper) AssertFileNotExists(filePath string, msgAndArgs ...interface{}) {
	th.t.Helper()
	
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		msg := "File should not exist"
		if len(msgAndArgs) > 0 {
			msg = fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
		}
		th.t.Errorf("%s: %s", msg, filePath)
	}
}

// RunWithTimeout runs a function with a timeout
func (th *TestHelper) RunWithTimeout(timeout time.Duration, fn func()) {
	th.t.Helper()
	
	done := make(chan bool, 1)
	go func() {
		fn()
		done <- true
	}()
	
	select {
	case <-done:
		// Function completed successfully
	case <-time.After(timeout):
		th.t.Errorf("Function timed out after %v", timeout)
	}
}

// GetFunctionName returns the name of the calling function
func GetFunctionName() string {
	pc := make([]uintptr, 10)
	runtime.Callers(2, pc)
	f := runtime.FuncForPC(pc[0])
	return f.Name()
}

// GetTestName extracts the test name from the current function
func GetTestName() string {
	name := GetFunctionName()
	parts := strings.Split(name, ".")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return name
}

// CreateTestTable creates a test table with the given name and schema
func CreateTestTable(th *TestHelper, db interface{}, tableName, schema string) {
	th.t.Helper()
	// This would be implemented based on the actual database interface
	// For now, it's a placeholder
}

// Retry executes a function with retry logic
func (th *TestHelper) Retry(maxAttempts int, delay time.Duration, fn func() error) {
	th.t.Helper()
	
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		if err := fn(); err != nil {
			lastErr = err
			if i < maxAttempts-1 {
				time.Sleep(delay)
				continue
			}
		} else {
			return // Success
		}
	}
	
	th.t.Errorf("Function failed after %d attempts. Last error: %v", maxAttempts, lastErr)
}

// Eventually waits for a condition to become true within a timeout
func (th *TestHelper) Eventually(timeout time.Duration, interval time.Duration, condition func() bool, msgAndArgs ...interface{}) {
	th.t.Helper()
	
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(interval)
	}
	
	msg := "Condition was not met within timeout"
	if len(msgAndArgs) > 0 {
		msg = fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
	}
	th.t.Errorf("%s (timeout: %v)", msg, timeout)
}

// CaptureOutput captures stdout and stderr during function execution
func (th *TestHelper) CaptureOutput(fn func()) (stdout, stderr string) {
	th.t.Helper()
	// This is a simplified version - a real implementation would
	// redirect os.Stdout and os.Stderr
	fn()
	return "", ""
}

// GenerateRandomString generates a random string of the specified length
func GenerateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[len(charset)/2] // Use a fixed character for reproducibility
	}
	return string(b)
}

// CreateMockConfig creates a mock configuration for testing
func CreateMockConfig(overrides map[string]interface{}) map[string]interface{} {
	config := map[string]interface{}{
		"db": map[string]interface{}{
			"dsn": "test:test@tcp(localhost:3306)/testdb?parseTime=true",
		},
		"sync": map[string]interface{}{
			"filePath":   "test.csv",
			"tableName":  "test_table",
			"primaryKey": "id",
			"syncMode":   "diff",
		},
	}
	
	// Apply overrides
	for key, value := range overrides {
		config[key] = value
	}
	
	return config
}

// CompareSlices compares two slices and returns differences
func CompareSlices(expected, actual interface{}) (bool, string) {
	expectedVal := reflect.ValueOf(expected)
	actualVal := reflect.ValueOf(actual)
	
	if expectedVal.Kind() != reflect.Slice || actualVal.Kind() != reflect.Slice {
		return false, "Both values must be slices"
	}
	
	if expectedVal.Len() != actualVal.Len() {
		return false, fmt.Sprintf("Length mismatch: expected %d, got %d", 
			expectedVal.Len(), actualVal.Len())
	}
	
	for i := 0; i < expectedVal.Len(); i++ {
		if !reflect.DeepEqual(expectedVal.Index(i).Interface(), actualVal.Index(i).Interface()) {
			return false, fmt.Sprintf("Element at index %d differs: expected %v, got %v",
				i, expectedVal.Index(i).Interface(), actualVal.Index(i).Interface())
		}
	}
	
	return true, ""
}

// WaitForCondition waits for a condition to be true or timeout
func WaitForCondition(condition func() bool, timeout time.Duration, interval time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(interval)
	}
	return false
}

// MeasureExecutionTime measures the execution time of a function
func MeasureExecutionTime(fn func()) time.Duration {
	start := time.Now()
	fn()
	return time.Since(start)
}

// LogTestProgress logs test progress with timing information
func LogTestProgress(t *testing.T, message string, args ...interface{}) {
	t.Helper()
	timestamp := time.Now().Format("15:04:05.000")
	formattedMsg := fmt.Sprintf(message, args...)
	t.Logf("[%s] %s", timestamp, formattedMsg)
}

// SkipIfShort skips a test if running in short mode
func SkipIfShort(t *testing.T, reason string) {
	if testing.Short() {
		t.Skipf("Skipping test in short mode: %s", reason)
	}
}

// RequireEnv requires an environment variable to be set
func RequireEnv(t *testing.T, envVar string) string {
	t.Helper()
	value := os.Getenv(envVar)
	if value == "" {
		t.Skipf("Required environment variable %s not set", envVar)
	}
	return value
}

// CleanupOnFailure runs cleanup only if the test fails
func CleanupOnFailure(t *testing.T, cleanup func()) {
	t.Cleanup(func() {
		if t.Failed() {
			cleanup()
		}
	})
}