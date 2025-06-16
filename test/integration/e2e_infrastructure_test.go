package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/yoRyuuuuu/mydatasyncer/test/fixtures"
	"github.com/yoRyuuuuu/mydatasyncer/test/helpers"
	"github.com/yoRyuuuuu/mydatasyncer/test/infrastructure"
)

// TestInfrastructureIntegration tests the complete test infrastructure
func TestInfrastructureIntegration(t *testing.T) {
	// Create test helper
	th := helpers.NewTestHelper(t)

	// Test environment isolation
	t.Run("EnvironmentIsolation", func(t *testing.T) {
		env1, helper1 := infrastructure.GetIsolatedTestDB(t, "test_env_1")
		env2, helper2 := infrastructure.GetIsolatedTestDB(t, "test_env_2")

		// Verify environments are isolated
		th.AssertNotEqual(env1.ID, env2.ID, "Environment IDs should be different")
		th.AssertNotEqual(env1.DB, env2.DB, "Database connections should be different")

		// Create tables in both environments
		th.AssertNoError(helper1.CreateStandardTestTable(), "Failed to create table in env1")
		th.AssertNoError(helper2.CreateStandardTestTable(), "Failed to create table in env2")

		// Verify tables exist independently
		exists1, err1 := helper1.TableExists("test_table")
		th.AssertNoError(err1, "Failed to check table existence in env1")
		th.AssertEqual(true, exists1, "Table should exist in env1")

		exists2, err2 := helper2.TableExists("test_table")
		th.AssertNoError(err2, "Failed to check table existence in env2")
		th.AssertEqual(true, exists2, "Table should exist in env2")
	})

	// Test data factory integration
	t.Run("DataFactoryIntegration", func(t *testing.T) {
		factoryHelper := fixtures.NewFactoryHelper(t)

		// Generate different dataset sizes
		config := fixtures.DataGenerationConfig{
			Size:       fixtures.Small,
			Pattern:    fixtures.Realistic,
			CustomSeed: 12345,
		}
		factoryHelperWithConfig := fixtures.NewFactoryHelperWithConfig(t, config)

		// Create simple test data
		categoriesFile, productsFile, err := factoryHelper.CreateSimpleTestData()
		th.AssertNoError(err, "Failed to create simple test data")
		th.AssertFileExists(categoriesFile, "Categories file should exist")
		th.AssertFileExists(productsFile, "Products file should exist")

		// Create large dataset
		largeDataset, err := factoryHelperWithConfig.CreateLargeDataset(fixtures.Medium)
		th.AssertNoError(err, "Failed to create large dataset")
		th.AssertGreaterThan(len(largeDataset.Products), 900, "Large dataset should have many products")

		// Create multi-table test data
		multiTableFiles, err := factoryHelper.CreateMultiTableTestData()
		th.AssertNoError(err, "Failed to create multi-table test data")
		th.AssertGreaterThan(len(multiTableFiles), 2, "Should have multiple table files")

		// Validate data integrity
		th.AssertNoError(factoryHelper.ValidateDataIntegrity(largeDataset), "Data integrity should be valid")
	})

	// Test performance monitoring
	t.Run("PerformanceMonitoring", func(t *testing.T) {
		monitor := infrastructure.NewPerformanceMonitor(infrastructure.DefaultMonitoringOptions())

		// Start monitoring
		monitor.Start(infrastructure.DefaultMonitoringOptions())

		// Simulate some work
		time.Sleep(100 * time.Millisecond)
		monitor.IncrementQueryCount()
		monitor.IncrementQueryCount()
		monitor.SetDBConnections(5)

		// Stop monitoring and get metrics
		metrics := monitor.Stop()

		th.AssertGreaterThan(metrics.Duration, time.Duration(0), "Duration should be positive")
		th.AssertEqual(int64(2), metrics.QueryCount, "Should have recorded 2 queries")
		th.AssertEqual(5, metrics.DBConnections, "Should have 5 DB connections")

		// Test benchmark functionality
		benchmarkResult := infrastructure.Benchmark("test_operation", 3, func() {
			time.Sleep(10 * time.Millisecond)
		})

		th.AssertEqual("test_operation", benchmarkResult.Name, "Benchmark name should match")
		th.AssertEqual(3, benchmarkResult.Iterations, "Should have 3 iterations")
		th.AssertGreaterThan(benchmarkResult.TotalTime, 25*time.Millisecond, "Total time should be reasonable")
	})

	// Test resource cleanup
	t.Run("ResourceCleanup", func(t *testing.T) {
		manager := infrastructure.NewResourceCleanupManager(5 * time.Second)

		// Register some test resources
		tempFile := th.CreateTempFile("cleanup_test.txt", "test content")
		th.AssertFileExists(tempFile, "Temp file should exist initially")

		cleanupResource := infrastructure.CleanupTempFile(tempFile)
		manager.RegisterResource(cleanupResource)

		// Verify resource is registered
		th.AssertEqual(1, manager.GetResourceCount(), "Should have 1 registered resource")

		// Perform cleanup
		th.AssertNoError(manager.CleanupAll(), "Cleanup should succeed")
		th.AssertFileNotExists(tempFile, "Temp file should be cleaned up")
		th.AssertEqual(0, manager.GetResourceCount(), "Should have 0 resources after cleanup")
	})

	// Test configuration helpers
	t.Run("ConfigurationHelpers", func(t *testing.T) {
		configHelper := helpers.NewConfigHelper(t)

		// Create basic configuration
		basicConfig := configHelper.CreateBasicConfig()
		th.AssertNotNil(basicConfig, "Basic config should not be nil")
		th.AssertNotNil(basicConfig.Sync, "Sync config should not be nil")

		// Validate configuration
		errors := configHelper.ValidateConfig(basicConfig)
		th.AssertEqual(0, len(errors), "Basic config should be valid")

		// Create invalid configuration
		invalidConfig := configHelper.CreateInvalidConfig("missing_primary_key")
		invalidErrors := configHelper.ValidateConfig(invalidConfig)
		th.AssertGreaterThan(len(invalidErrors), 0, "Invalid config should have errors")

		// Save and load configuration
		configFile := configHelper.SaveConfigToFile(basicConfig, "test_config.yml")
		th.AssertFileExists(configFile, "Config file should exist")
	})
}

// TestEndToEndWorkflow tests a complete end-to-end workflow using the new infrastructure
func TestEndToEndWorkflow(t *testing.T) {
	// Skip if running in short mode
	helpers.SkipIfShort(t, "End-to-end test requires full infrastructure")

	// Create test helper
	th := helpers.NewTestHelper(t)

	// Set up isolated test environment
	env, dbHelper := infrastructure.GetIsolatedTestDB(t, "e2e_workflow")

	// Set up performance monitoring
	monitor := infrastructure.NewPerformanceMonitor(infrastructure.DefaultMonitoringOptions())
	monitor.Start(infrastructure.DefaultMonitoringOptions())

	// Set up resource cleanup
	cleanup := infrastructure.CreateResourceCleanupForTest("e2e_workflow")
	defer func() {
		if err := cleanup.CleanupAll(); err != nil {
			t.Logf("Cleanup warning: %v", err)
		}
	}()

	// Create database schema
	th.AssertNoError(dbHelper.CreateMultiTableSchema(), "Failed to create multi-table schema")

	// Generate test data
	testFiles := make(map[string]string)
	testFiles["categories"] = th.CreateTempFile("categories.json", `[{"id":1,"name":"Electronics","description":"Electronic products"}]`)
	testFiles["products"] = th.CreateTempFile("products.csv", "id,name,category_id,price\n1,Laptop,1,999.99")

	// Generate sample dataset for validation demonstration
	factoryHelper := fixtures.NewFactoryHelper(t)
	dataset, err := factoryHelper.GetFactory().GenerateTestDataset()
	th.AssertNoError(err, "Failed to generate dataset for validation")
	th.AssertNoError(factoryHelper.ValidateDataIntegrity(dataset), "Test data should have referential integrity")

	// Create configuration
	configHelper := helpers.NewConfigHelper(t)
	config := configHelper.CreateMultiTableConfig()

	// Update file paths in configuration
	for tableName, filePath := range testFiles {
		// Update configuration with actual file paths
		for i := range config.MultiTable.Tables {
			if config.MultiTable.Tables[i].TableName == tableName {
				config.MultiTable.Tables[i].FilePath = filePath
			}
		}
	}

	configFile := configHelper.SaveConfigToFile(config, "e2e_workflow_config.yml")
	th.AssertFileExists(configFile, "Config file should exist")

	// Simulate running the sync operation
	// (In a real test, this would call the actual mydatasyncer code)
	helpers.LogTestProgress(t, "Simulating sync operation with config: %s", configFile)

	// Verify initial state
	initialCategoryCount, err := dbHelper.GetRowCount("categories")
	th.AssertNoError(err, "Failed to get initial category count")

	initialProductCount, err := dbHelper.GetRowCount("products")
	th.AssertNoError(err, "Failed to get initial product count")

	// Simulate data insertion
	categoryData := []map[string]interface{}{
		{"id": 1, "name": "Electronics", "description": "Electronic products"},
		{"id": 2, "name": "Books", "description": "Book products"},
	}
	th.AssertNoError(dbHelper.InsertTestData("categories", categoryData), "Failed to insert category data")

	productData := []map[string]interface{}{
		{"id": 1, "name": "Laptop", "category_id": 1, "price": 999.99},
		{"id": 2, "name": "Novel", "category_id": 2, "price": 19.99},
	}
	th.AssertNoError(dbHelper.InsertTestData("products", productData), "Failed to insert product data")

	// Validate data integrity after insertion
	dataValidator := helpers.NewDataValidator(t, env.DB)
	dataValidator.ValidateRowCount("categories", initialCategoryCount+2)
	dataValidator.ValidateRowCount("products", initialProductCount+2)
	dataValidator.ValidateDataIntegrity("products", "category_id", "categories", "id")

	// Test data validation
	dataValidator.ValidateRecordExists("categories", "id", "1")
	dataValidator.ValidateRecordExists("products", "id", "1")
	dataValidator.ValidateRecordValue("products", "id", "1", "name", "Laptop")

	// Verify performance metrics
	finalMetrics := monitor.Stop()
	th.AssertGreaterThan(finalMetrics.Duration, time.Duration(0), "Should have measured execution time")

	helpers.LogTestProgress(t, "E2E workflow completed successfully")
	helpers.LogTestProgress(t, "Execution time: %v", finalMetrics.Duration)
	helpers.LogTestProgress(t, "Peak memory: %d MB", finalMetrics.PeakMemoryMB)
}

// TestStressTestInfrastructure tests the infrastructure under stress
func TestStressTestInfrastructure(t *testing.T) {
	helpers.SkipIfShort(t, "Stress test requires extended execution time")

	th := helpers.NewTestHelper(t)

	// Test multiple parallel environments
	const numEnvironments = 5
	envChannels := make([]chan *infrastructure.TestEnvironment, numEnvironments)

	// Create environments in parallel
	for i := 0; i < numEnvironments; i++ {
		envChannels[i] = make(chan *infrastructure.TestEnvironment, 1)

		go func(index int) {
			env, _ := infrastructure.GetIsolatedTestDB(t, fmt.Sprintf("stress_test_%d", index))
			envChannels[index] <- env
		}(i)
	}

	// Collect all environments
	environments := make([]*infrastructure.TestEnvironment, numEnvironments)
	for i := 0; i < numEnvironments; i++ {
		environments[i] = <-envChannels[i]
		th.AssertNotNil(environments[i], "Environment should be created successfully")
	}

	// Verify all environments are unique
	for i := 0; i < numEnvironments; i++ {
		for j := i + 1; j < numEnvironments; j++ {
			th.AssertNotEqual(environments[i].ID, environments[j].ID,
				"Environment IDs should be unique")
		}
	}

	// Test large dataset generation
	largeConfig := fixtures.DataGenerationConfig{
		Size:       fixtures.Large,
		Pattern:    fixtures.Realistic,
		CustomSeed: 12345,
	}

	largeFactoryHelper := fixtures.NewFactoryHelperWithConfig(t, largeConfig)

	// Measure performance of large dataset generation
	start := time.Now()
	largeDataset, err := largeFactoryHelper.CreateLargeDataset(fixtures.Large)
	generationTime := time.Since(start)

	th.AssertNoError(err, "Large dataset generation should succeed")
	th.AssertGreaterThan(len(largeDataset.Products), 9000, "Large dataset should have many products")

	helpers.LogTestProgress(t, "Generated %d products in %v",
		len(largeDataset.Products), generationTime)

	// Test memory cleanup under stress
	cleanupManager := infrastructure.NewResourceCleanupManager(30 * time.Second)

	// Register many cleanup resources
	for i := 0; i < 100; i++ {
		tempFile := th.CreateTempFile(fmt.Sprintf("stress_test_%d.txt", i), "test")
		cleanupManager.RegisterResource(infrastructure.CleanupTempFile(tempFile))
	}

	th.AssertEqual(100, cleanupManager.GetResourceCount(), "Should have 100 cleanup resources")

	// Perform cleanup
	cleanupStart := time.Now()
	th.AssertNoError(cleanupManager.CleanupAll(), "Stress cleanup should succeed")
	cleanupTime := time.Since(cleanupStart)

	helpers.LogTestProgress(t, "Cleaned up 100 resources in %v", cleanupTime)

	th.AssertEqual(0, cleanupManager.GetResourceCount(), "All resources should be cleaned up")
}

// Package initialization for integration tests is handled automatically
