package infrastructure

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"
)

// ResourceCleanupManager manages cleanup of test resources
type ResourceCleanupManager struct {
	resources []CleanupResource
	mutex     sync.RWMutex
	timeout   time.Duration
}

// CleanupResource represents a resource that needs cleanup
type CleanupResource struct {
	ID          string
	Type        ResourceType
	CleanupFunc func() error
	Priority    CleanupPriority
	Timeout     time.Duration
	CreatedAt   time.Time
}

// ResourceType defines the type of resource
type ResourceType int

const (
	DatabaseResource ResourceType = iota
	FileResource
	NetworkResource
	MemoryResource
	ProcessResource
)

// CleanupPriority defines the cleanup priority order
type CleanupPriority int

const (
	HighPriority   CleanupPriority = iota // Critical resources (connections, locks)
	MediumPriority                        // Normal resources (temp files, caches)
	LowPriority                           // Optional resources (logs, artifacts)
)

// NewResourceCleanupManager creates a new resource cleanup manager
func NewResourceCleanupManager(timeout time.Duration) *ResourceCleanupManager {
	if timeout == 0 {
		timeout = 30 * time.Second // Default timeout
	}
	
	return &ResourceCleanupManager{
		resources: make([]CleanupResource, 0),
		timeout:   timeout,
	}
}

// RegisterResource registers a resource for cleanup
func (rcm *ResourceCleanupManager) RegisterResource(resource CleanupResource) {
	rcm.mutex.Lock()
	defer rcm.mutex.Unlock()
	
	if resource.Timeout == 0 {
		resource.Timeout = rcm.timeout
	}
	
	if resource.CreatedAt.IsZero() {
		resource.CreatedAt = time.Now()
	}
	
	rcm.resources = append(rcm.resources, resource)
}

// CleanupAll performs cleanup of all registered resources
func (rcm *ResourceCleanupManager) CleanupAll() error {
	rcm.mutex.Lock()
	defer rcm.mutex.Unlock()
	
	if len(rcm.resources) == 0 {
		return nil
	}
	
	// Sort resources by priority (high priority first)
	sortedResources := rcm.sortResourcesByPriority()
	
	var errors []string
	var wg sync.WaitGroup
	errorChan := make(chan error, len(sortedResources))
	
	// Execute cleanup for each priority level sequentially
	priorities := []CleanupPriority{HighPriority, MediumPriority, LowPriority}
	
	for _, priority := range priorities {
		// Clean up all resources of this priority level in parallel
		for _, resource := range sortedResources {
			if resource.Priority == priority {
				wg.Add(1)
				go func(res CleanupResource) {
					defer wg.Done()
					
					ctx, cancel := context.WithTimeout(context.Background(), res.Timeout)
					defer cancel()
					
					done := make(chan error, 1)
					go func() {
						done <- res.CleanupFunc()
					}()
					
					select {
					case err := <-done:
						if err != nil {
							errorChan <- fmt.Errorf("cleanup failed for resource %s (%v): %w", 
								res.ID, res.Type, err)
						}
					case <-ctx.Done():
						errorChan <- fmt.Errorf("cleanup timeout for resource %s (%v) after %v", 
							res.ID, res.Type, res.Timeout)
					}
				}(resource)
			}
		}
		
		// Wait for this priority level to complete before moving to next
		wg.Wait()
	}
	
	// Collect all errors
	close(errorChan)
	for err := range errorChan {
		errors = append(errors, err.Error())
	}
	
	// Clear resources after cleanup
	rcm.resources = rcm.resources[:0]
	
	if len(errors) > 0 {
		return fmt.Errorf("cleanup errors: %v", errors)
	}
	
	return nil
}

// CleanupByType performs cleanup of resources of a specific type
func (rcm *ResourceCleanupManager) CleanupByType(resourceType ResourceType) error {
	rcm.mutex.Lock()
	defer rcm.mutex.Unlock()
	
	var errors []string
	var remaining []CleanupResource
	
	for _, resource := range rcm.resources {
		if resource.Type == resourceType {
			ctx, cancel := context.WithTimeout(context.Background(), resource.Timeout)
			
			done := make(chan error, 1)
			go func() {
				done <- resource.CleanupFunc()
			}()
			
			select {
			case err := <-done:
				if err != nil {
					errors = append(errors, fmt.Sprintf("resource %s: %v", resource.ID, err))
				}
			case <-ctx.Done():
				errors = append(errors, fmt.Sprintf("resource %s: timeout after %v", 
					resource.ID, resource.Timeout))
			}
			
			cancel()
		} else {
			remaining = append(remaining, resource)
		}
	}
	
	rcm.resources = remaining
	
	if len(errors) > 0 {
		return fmt.Errorf("cleanup errors for type %v: %v", resourceType, errors)
	}
	
	return nil
}

// RemoveResource removes a specific resource from cleanup list
func (rcm *ResourceCleanupManager) RemoveResource(resourceID string) bool {
	rcm.mutex.Lock()
	defer rcm.mutex.Unlock()
	
	for i, resource := range rcm.resources {
		if resource.ID == resourceID {
			rcm.resources = append(rcm.resources[:i], rcm.resources[i+1:]...)
			return true
		}
	}
	
	return false
}

// GetResourceCount returns the number of registered resources
func (rcm *ResourceCleanupManager) GetResourceCount() int {
	rcm.mutex.RLock()
	defer rcm.mutex.RUnlock()
	return len(rcm.resources)
}

// GetResourcesByType returns resources of a specific type
func (rcm *ResourceCleanupManager) GetResourcesByType(resourceType ResourceType) []CleanupResource {
	rcm.mutex.RLock()
	defer rcm.mutex.RUnlock()
	
	var result []CleanupResource
	for _, resource := range rcm.resources {
		if resource.Type == resourceType {
			result = append(result, resource)
		}
	}
	
	return result
}

// sortResourcesByPriority sorts resources by cleanup priority
func (rcm *ResourceCleanupManager) sortResourcesByPriority() []CleanupResource {
	sorted := make([]CleanupResource, len(rcm.resources))
	copy(sorted, rcm.resources)
	
	// Simple bubble sort by priority (high priority first)
	for i := 0; i < len(sorted)-1; i++ {
		for j := 0; j < len(sorted)-i-1; j++ {
			if sorted[j].Priority > sorted[j+1].Priority {
				sorted[j], sorted[j+1] = sorted[j+1], sorted[j]
			}
		}
	}
	
	return sorted
}

// Cleanup helper functions for common resource types

// CleanupTempFile creates a cleanup function for temporary files
func CleanupTempFile(filePath string) CleanupResource {
	return CleanupResource{
		ID:   fmt.Sprintf("tempfile_%s", filePath),
		Type: FileResource,
		CleanupFunc: func() error {
			if _, err := os.Stat(filePath); os.IsNotExist(err) {
				return nil // File doesn't exist, nothing to clean
			}
			return os.RemoveAll(filePath)
		},
		Priority: MediumPriority,
		Timeout:  5 * time.Second,
	}
}

// CleanupTempDir creates a cleanup function for temporary directories
func CleanupTempDir(dirPath string) CleanupResource {
	return CleanupResource{
		ID:   fmt.Sprintf("tempdir_%s", dirPath),
		Type: FileResource,
		CleanupFunc: func() error {
			if _, err := os.Stat(dirPath); os.IsNotExist(err) {
				return nil // Directory doesn't exist, nothing to clean
			}
			return os.RemoveAll(dirPath)
		},
		Priority: MediumPriority,
		Timeout:  10 * time.Second,
	}
}

// CleanupDatabase creates a cleanup function for database resources
func CleanupDatabase(envManager *TestEnvManager, envID string) CleanupResource {
	return CleanupResource{
		ID:   fmt.Sprintf("database_%s", envID),
		Type: DatabaseResource,
		CleanupFunc: func() error {
			return envManager.CleanupEnvironment(envID)
		},
		Priority: HighPriority,
		Timeout:  30 * time.Second,
	}
}

// GlobalCleanupManager provides a global cleanup manager for tests
var globalCleanupManager *ResourceCleanupManager
var globalCleanupOnce sync.Once

// GetGlobalCleanupManager returns the global cleanup manager instance
func GetGlobalCleanupManager() *ResourceCleanupManager {
	globalCleanupOnce.Do(func() {
		globalCleanupManager = NewResourceCleanupManager(30 * time.Second)
	})
	return globalCleanupManager
}

// RegisterGlobalCleanup registers a resource for global cleanup
func RegisterGlobalCleanup(resource CleanupResource) {
	GetGlobalCleanupManager().RegisterResource(resource)
}

// CleanupAllGlobal performs cleanup of all global resources
func CleanupAllGlobal() error {
	return GetGlobalCleanupManager().CleanupAll()
}

// WithResourceCleanup wraps a test function with automatic resource cleanup
func WithResourceCleanup(testFunc func(*ResourceCleanupManager)) error {
	manager := NewResourceCleanupManager(30 * time.Second)
	
	defer func() {
		if err := manager.CleanupAll(); err != nil {
			fmt.Printf("Warning: cleanup failed: %v\n", err)
		}
	}()
	
	testFunc(manager)
	return nil
}

// SafeCleanup wraps cleanup functions with panic recovery
func SafeCleanup(cleanupFunc func() error) func() error {
	return func() error {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("Warning: cleanup panic recovered: %v\n", r)
			}
		}()
		
		return cleanupFunc()
	}
}

// CreateResourceCleanupForTest creates a cleanup manager specifically for a test
func CreateResourceCleanupForTest(testName string) *ResourceCleanupManager {
	manager := NewResourceCleanupManager(45 * time.Second) // Longer timeout for tests
	
	// Register common cleanup patterns for tests
	if tempDir := os.Getenv("TEST_TEMP_DIR"); tempDir != "" {
		testTempDir := fmt.Sprintf("%s/test_%s_%d", tempDir, testName, time.Now().UnixNano())
		manager.RegisterResource(CleanupTempDir(testTempDir))
	}
	
	return manager
}

// CleanupReport provides information about cleanup operations
type CleanupReport struct {
	TotalResources int
	CleanedCount   int
	FailedCount    int
	Duration       time.Duration
	Errors         []string
}

// GenerateCleanupReport creates a detailed cleanup report
func (rcm *ResourceCleanupManager) GenerateCleanupReport() CleanupReport {
	rcm.mutex.RLock()
	defer rcm.mutex.RUnlock()
	
	return CleanupReport{
		TotalResources: len(rcm.resources),
	}
}

// String returns a string representation of resource type
func (rt ResourceType) String() string {
	switch rt {
	case DatabaseResource:
		return "Database"
	case FileResource:
		return "File"
	case NetworkResource:
		return "Network"
	case MemoryResource:
		return "Memory"
	case ProcessResource:
		return "Process"
	default:
		return "Unknown"
	}
}

// String returns a string representation of cleanup priority
func (cp CleanupPriority) String() string {
	switch cp {
	case HighPriority:
		return "High"
	case MediumPriority:
		return "Medium"
	case LowPriority:
		return "Low"
	default:
		return "Unknown"
	}
}