package infrastructure

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"
)

// PerformanceMonitor tracks performance metrics during test execution
type PerformanceMonitor struct {
	startTime  time.Time
	endTime    time.Time
	metrics    *TestMetrics
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	monitoring bool
	mutex      sync.RWMutex
}

// MonitoringOptions configures the performance monitoring behavior
type MonitoringOptions struct {
	SampleInterval time.Duration // How often to sample metrics
	TrackMemory    bool          // Whether to track memory usage
	TrackCPU       bool          // Whether to track CPU usage
	TrackDB        bool          // Whether to track database operations
}

// DefaultMonitoringOptions returns sensible defaults for performance monitoring
func DefaultMonitoringOptions() MonitoringOptions {
	return MonitoringOptions{
		SampleInterval: 100 * time.Millisecond,
		TrackMemory:    true,
		TrackCPU:       true,
		TrackDB:        true,
	}
}

// NewPerformanceMonitor creates a new performance monitor
func NewPerformanceMonitor(opts MonitoringOptions) *PerformanceMonitor {
	ctx, cancel := context.WithCancel(context.Background())

	return &PerformanceMonitor{
		metrics: &TestMetrics{
			StartTime: time.Now(),
		},
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start begins performance monitoring
func (pm *PerformanceMonitor) Start(opts MonitoringOptions) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	if pm.monitoring {
		return // Already monitoring
	}

	pm.monitoring = true
	pm.startTime = time.Now()
	pm.metrics.StartTime = pm.startTime

	// Start monitoring goroutines
	if opts.TrackMemory {
		pm.wg.Add(1)
		go pm.monitorMemory(opts.SampleInterval)
	}

	if opts.TrackCPU {
		pm.wg.Add(1)
		go pm.monitorCPU(opts.SampleInterval)
	}

	if opts.TrackDB {
		pm.wg.Add(1)
		go pm.monitorDatabase(opts.SampleInterval)
	}
}

// Stop ends performance monitoring and finalizes metrics
func (pm *PerformanceMonitor) Stop() *TestMetrics {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	if !pm.monitoring {
		return pm.metrics
	}

	pm.monitoring = false
	pm.endTime = time.Now()
	pm.metrics.EndTime = pm.endTime
	pm.metrics.Duration = pm.endTime.Sub(pm.startTime)

	// Cancel monitoring goroutines
	pm.cancel()
	pm.wg.Wait()

	return pm.metrics
}

// GetCurrentMetrics returns the current metrics snapshot
func (pm *PerformanceMonitor) GetCurrentMetrics() *TestMetrics {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	// Create a copy of metrics
	metrics := *pm.metrics
	if pm.monitoring {
		metrics.Duration = time.Since(pm.startTime)
	}

	return &metrics
}

// IncrementQueryCount increments the database query counter
func (pm *PerformanceMonitor) IncrementQueryCount() {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.metrics.QueryCount++
}

// IncrementErrorCount increments the error counter
func (pm *PerformanceMonitor) IncrementErrorCount() {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.metrics.ErrorCount++
}

// SetDBConnections sets the current number of database connections
func (pm *PerformanceMonitor) SetDBConnections(count int) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.metrics.DBConnections = count
}

// monitorMemory periodically samples memory usage
func (pm *PerformanceMonitor) monitorMemory(interval time.Duration) {
	defer pm.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-pm.ctx.Done():
			return
		case <-ticker.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			// Convert bytes to MB
			currentMemMB := uint64(m.Alloc / 1024 / 1024)

			pm.mutex.Lock()
			if currentMemMB > pm.metrics.PeakMemoryMB {
				pm.metrics.PeakMemoryMB = currentMemMB
			}
			pm.mutex.Unlock()
		}
	}
}

// monitorCPU periodically samples CPU usage
func (pm *PerformanceMonitor) monitorCPU(interval time.Duration) {
	defer pm.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastSampleTime time.Time

	for {
		select {
		case <-pm.ctx.Done():
			return
		case <-ticker.C:
			// Simple CPU usage estimation based on runtime stats
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			currentTime := time.Now()
			if !lastSampleTime.IsZero() {
				// This is a simplified CPU usage calculation
				// In a real implementation, you might want to use more sophisticated methods
				elapsed := currentTime.Sub(lastSampleTime)
				if elapsed > 0 {
					// Estimate based on GC and memory allocation activity
					gcCPUFraction := m.GCCPUFraction

					pm.mutex.Lock()
					pm.metrics.CPUUsagePercent = gcCPUFraction * 100
					pm.mutex.Unlock()
				}
			}
			lastSampleTime = currentTime
		}
	}
}

// monitorDatabase periodically updates database connection statistics
func (pm *PerformanceMonitor) monitorDatabase(interval time.Duration) {
	defer pm.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-pm.ctx.Done():
			return
		case <-ticker.C:
			// Database monitoring would require access to the actual DB connection
			// This is a placeholder for more sophisticated monitoring
			// In a real implementation, you might track:
			// - Connection pool statistics
			// - Query execution times
			// - Active connections
			// - Deadlocks and timeouts
		}
	}
}

// WithPerformanceMonitoring wraps a test function with performance monitoring
func WithPerformanceMonitoring(testFunc func()) *TestMetrics {
	monitor := NewPerformanceMonitor(DefaultMonitoringOptions())
	monitor.Start(DefaultMonitoringOptions())

	defer func() {
		if r := recover(); r != nil {
			monitor.IncrementErrorCount()
			monitor.Stop()
			panic(r)
		}
	}()

	testFunc()
	return monitor.Stop()
}

// MonitoredTestFunc is a function type for tests that need performance monitoring
type MonitoredTestFunc func(monitor *PerformanceMonitor)

// RunWithMonitoring executes a test function with performance monitoring
func RunWithMonitoring(testFunc MonitoredTestFunc, opts MonitoringOptions) *TestMetrics {
	monitor := NewPerformanceMonitor(opts)
	monitor.Start(opts)

	defer func() {
		if r := recover(); r != nil {
			monitor.IncrementErrorCount()
			monitor.Stop()
			panic(r)
		}
	}()

	testFunc(monitor)
	return monitor.Stop()
}

// BenchmarkResult represents the result of a performance benchmark
type BenchmarkResult struct {
	Name         string
	Iterations   int
	TotalTime    time.Duration
	AverageTime  time.Duration
	PeakMemory   uint64
	TotalQueries int64
	Errors       int
}

// Benchmark runs a test function multiple times and collects performance statistics
func Benchmark(name string, iterations int, testFunc func()) *BenchmarkResult {
	result := &BenchmarkResult{
		Name:       name,
		Iterations: iterations,
	}

	start := time.Now()
	var totalPeakMemory uint64
	var totalQueries int64
	var totalErrors int

	for i := 0; i < iterations; i++ {
		metrics := WithPerformanceMonitoring(testFunc)

		totalPeakMemory += metrics.PeakMemoryMB
		totalQueries += metrics.QueryCount
		totalErrors += metrics.ErrorCount
	}

	result.TotalTime = time.Since(start)
	result.AverageTime = result.TotalTime / time.Duration(iterations)
	result.PeakMemory = totalPeakMemory / uint64(iterations)
	result.TotalQueries = totalQueries
	result.Errors = totalErrors

	return result
}

// ComparePerformance compares two benchmark results
func ComparePerformance(baseline, current *BenchmarkResult) PerformanceComparison {
	return PerformanceComparison{
		Baseline:          baseline,
		Current:           current,
		TimeImprovement:   float64(baseline.AverageTime-current.AverageTime) / float64(baseline.AverageTime) * 100,
		MemoryImprovement: float64(int64(baseline.PeakMemory)-int64(current.PeakMemory)) / float64(baseline.PeakMemory) * 100,
		QueryReduction:    float64(baseline.TotalQueries-current.TotalQueries) / float64(baseline.TotalQueries) * 100,
	}
}

// PerformanceComparison represents a comparison between two performance results
type PerformanceComparison struct {
	Baseline          *BenchmarkResult
	Current           *BenchmarkResult
	TimeImprovement   float64 // Percentage improvement (positive = better)
	MemoryImprovement float64 // Percentage improvement (positive = less memory)
	QueryReduction    float64 // Percentage reduction in queries (positive = fewer queries)
}

// IsImprovement returns true if the current result is better than baseline
func (pc *PerformanceComparison) IsImprovement() bool {
	return pc.TimeImprovement > 0 && pc.MemoryImprovement >= 0 && pc.Current.Errors <= pc.Baseline.Errors
}

// PerformanceThreshold defines acceptable performance criteria
type PerformanceThreshold struct {
	MaxExecutionTime time.Duration
	MaxMemoryMB      uint64
	MaxErrors        int
	MaxQueries       int64
}

// CheckThreshold validates performance metrics against defined thresholds
func (pm *PerformanceMonitor) CheckThreshold(threshold PerformanceThreshold) []string {
	metrics := pm.GetCurrentMetrics()
	var violations []string

	if metrics.Duration > threshold.MaxExecutionTime {
		violations = append(violations,
			fmt.Sprintf("Execution time %v exceeds threshold %v",
				metrics.Duration, threshold.MaxExecutionTime))
	}

	if metrics.PeakMemoryMB > threshold.MaxMemoryMB {
		violations = append(violations,
			fmt.Sprintf("Peak memory %d MB exceeds threshold %d MB",
				metrics.PeakMemoryMB, threshold.MaxMemoryMB))
	}

	if metrics.ErrorCount > threshold.MaxErrors {
		violations = append(violations,
			fmt.Sprintf("Error count %d exceeds threshold %d",
				metrics.ErrorCount, threshold.MaxErrors))
	}

	if metrics.QueryCount > threshold.MaxQueries {
		violations = append(violations,
			fmt.Sprintf("Query count %d exceeds threshold %d",
				metrics.QueryCount, threshold.MaxQueries))
	}

	return violations
}

// GenerateReport creates a detailed performance report
func (pm *PerformanceMonitor) GenerateReport() string {
	metrics := pm.GetCurrentMetrics()

	report := "Performance Report\n"
	report += "==================\n\n"
	report += fmt.Sprintf("Execution Time: %v\n", metrics.Duration)
	report += fmt.Sprintf("Peak Memory: %d MB\n", metrics.PeakMemoryMB)
	report += fmt.Sprintf("CPU Usage: %.2f%%\n", metrics.CPUUsagePercent)
	report += fmt.Sprintf("Database Connections: %d\n", metrics.DBConnections)
	report += fmt.Sprintf("Total Queries: %d\n", metrics.QueryCount)
	report += fmt.Sprintf("Errors: %d\n", metrics.ErrorCount)
	report += fmt.Sprintf("Start Time: %s\n", metrics.StartTime.Format(time.RFC3339))

	if !metrics.EndTime.IsZero() {
		report += fmt.Sprintf("End Time: %s\n", metrics.EndTime.Format(time.RFC3339))
	}

	return report
}
