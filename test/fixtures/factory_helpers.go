package fixtures

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// FactoryHelper provides convenient methods for creating test data in tests
type FactoryHelper struct {
	factory   *DataFactory
	outputDir string
}

// NewFactoryHelper creates a new factory helper with default configuration
func NewFactoryHelper(t *testing.T) *FactoryHelper {
	t.Helper()
	
	// Create temporary directory for test data
	outputDir, err := os.MkdirTemp("", "test_data_")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	
	// Clean up directory when test finishes
	t.Cleanup(func() {
		os.RemoveAll(outputDir)
	})
	
	// Default configuration for tests
	config := DataGenerationConfig{
		Size:    Small,
		Pattern: Realistic,
		CustomSeed: 12345, // Fixed seed for reproducible tests
	}
	
	return &FactoryHelper{
		factory:   NewDataFactory(config),
		outputDir: outputDir,
	}
}

// NewFactoryHelperWithConfig creates a factory helper with custom configuration
func NewFactoryHelperWithConfig(t *testing.T, config DataGenerationConfig) *FactoryHelper {
	t.Helper()
	
	outputDir, err := os.MkdirTemp("", "test_data_")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	
	t.Cleanup(func() {
		os.RemoveAll(outputDir)
	})
	
	return &FactoryHelper{
		factory:   NewDataFactory(config),
		outputDir: outputDir,
	}
}

// CreateSimpleTestData creates basic test data for standard tests
func (fh *FactoryHelper) CreateSimpleTestData() (string, string, error) {
	// Generate small dataset
	dataset, err := fh.factory.GenerateTestDataset()
	if err != nil {
		return "", "", fmt.Errorf("failed to generate dataset: %w", err)
	}
	
	// Create CSV file for categories
	categoriesCSV := filepath.Join(fh.outputDir, "categories.csv")
	if err := fh.createCategoriesCSV(categoriesCSV, dataset.Categories[:5]); err != nil {
		return "", "", fmt.Errorf("failed to create categories CSV: %w", err)
	}
	
	// Create JSON file for products
	productsJSON := filepath.Join(fh.outputDir, "products.json")
	if err := fh.createProductsJSON(productsJSON, dataset.Products[:10]); err != nil {
		return "", "", fmt.Errorf("failed to create products JSON: %w", err)
	}
	
	return categoriesCSV, productsJSON, nil
}

// CreateLargeDataset creates a large dataset for performance testing
func (fh *FactoryHelper) CreateLargeDataset(size DatasetSize) (*TestDataset, error) {
	// Update factory configuration for large dataset
	fh.factory.config.Size = size
	
	dataset, err := fh.factory.GenerateTestDataset()
	if err != nil {
		return nil, fmt.Errorf("failed to generate large dataset: %w", err)
	}
	
	return dataset, nil
}

// CreateTestDataWithPattern creates test data with specific patterns
func (fh *FactoryHelper) CreateTestDataWithPattern(pattern DataPattern, recordCount int) (string, error) {
	// Update factory configuration
	fh.factory.config.Pattern = pattern
	fh.factory.config.Size = DatasetSize(recordCount)
	
	dataset, err := fh.factory.GenerateTestDataset()
	if err != nil {
		return "", fmt.Errorf("failed to generate dataset with pattern: %w", err)
	}
	
	// Save as CSV
	filename := filepath.Join(fh.outputDir, fmt.Sprintf("test_data_%d.csv", int(pattern)))
	if err := fh.createProductsCSV(filename, dataset.Products); err != nil {
		return "", fmt.Errorf("failed to create CSV: %w", err)
	}
	
	return filename, nil
}

// CreateMultiTableTestData creates related test data for multi-table tests
func (fh *FactoryHelper) CreateMultiTableTestData() (map[string]string, error) {
	dataset, err := fh.factory.GenerateTestDataset()
	if err != nil {
		return nil, fmt.Errorf("failed to generate dataset: %w", err)
	}
	
	files := make(map[string]string)
	
	// Create categories file
	categoriesFile := filepath.Join(fh.outputDir, "categories.json")
	if err := fh.createCategoriesJSON(categoriesFile, dataset.Categories); err != nil {
		return nil, fmt.Errorf("failed to create categories file: %w", err)
	}
	files["categories"] = categoriesFile
	
	// Create products file
	productsFile := filepath.Join(fh.outputDir, "products.csv")
	if err := fh.createProductsCSV(productsFile, dataset.Products); err != nil {
		return nil, fmt.Errorf("failed to create products file: %w", err)
	}
	files["products"] = productsFile
	
	// Create orders file
	ordersFile := filepath.Join(fh.outputDir, "orders.json")
	if err := fh.createOrdersJSON(ordersFile, dataset.Orders); err != nil {
		return nil, fmt.Errorf("failed to create orders file: %w", err)
	}
	files["orders"] = ordersFile
	
	// Create order items file
	orderItemsFile := filepath.Join(fh.outputDir, "order_items.csv")
	if err := fh.createOrderItemsCSV(orderItemsFile, dataset.OrderItems); err != nil {
		return nil, fmt.Errorf("failed to create order items file: %w", err)
	}
	files["order_items"] = orderItemsFile
	
	return files, nil
}

// CreateEdgeCaseData creates data with edge cases for robustness testing
func (fh *FactoryHelper) CreateEdgeCaseData() (string, error) {
	// Configure for edge cases
	fh.factory.config.Pattern = Pathological
	fh.factory.config.EdgeCaseValues = true
	fh.factory.config.UnicodeData = true
	fh.factory.config.NullValueRatio = 0.1 // 10% null values
	
	dataset, err := fh.factory.GenerateTestDataset()
	if err != nil {
		return "", fmt.Errorf("failed to generate edge case dataset: %w", err)
	}
	
	// Add specific edge cases
	edgeCaseProducts := []ProductRecord{
		{ID: 999999999, Name: "", CategoryID: 1, Price: 0.01},                                    // Empty name, min price
		{ID: 999999998, Name: string(make([]byte, 255)), CategoryID: 1, Price: 99999999.99},     // Max length name, max price
		{ID: 999999997, Name: "Test\x00NULL\xFF", CategoryID: 1, Price: -0.01},                  // Special characters, negative price
		{ID: 999999996, Name: "Unicode测试データテスト🚀", CategoryID: 1, Price: 123.456789},     // Unicode, high precision
		{ID: 999999995, Name: "SQL'; DROP TABLE products; --", CategoryID: 1, Price: 0},         // SQL injection attempt
	}
	
	dataset.Products = append(dataset.Products, edgeCaseProducts...)
	
	filename := filepath.Join(fh.outputDir, "edge_case_data.csv")
	if err := fh.createProductsCSV(filename, dataset.Products); err != nil {
		return "", fmt.Errorf("failed to create edge case CSV: %w", err)
	}
	
	return filename, nil
}

// File creation helper methods

func (fh *FactoryHelper) createCategoriesCSV(filename string, categories []CategoryRecord) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	
	writer := csv.NewWriter(file)
	defer writer.Flush()
	
	// Write header
	if err := writer.Write([]string{"id", "name", "description"}); err != nil {
		return err
	}
	
	// Write records
	for _, cat := range categories {
		record := []string{
			strconv.Itoa(cat.ID),
			cat.Name,
			cat.Description,
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	
	return nil
}

func (fh *FactoryHelper) createProductsCSV(filename string, products []ProductRecord) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	
	writer := csv.NewWriter(file)
	defer writer.Flush()
	
	// Write header
	if err := writer.Write([]string{"id", "name", "category_id", "price", "sku", "stock"}); err != nil {
		return err
	}
	
	// Write records
	for _, prod := range products {
		record := []string{
			strconv.Itoa(prod.ID),
			prod.Name,
			strconv.Itoa(prod.CategoryID),
			fmt.Sprintf("%.2f", prod.Price),
			prod.SKU,
			strconv.Itoa(prod.Stock),
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	
	return nil
}

func (fh *FactoryHelper) createOrderItemsCSV(filename string, orderItems []OrderItemRecord) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	
	writer := csv.NewWriter(file)
	defer writer.Flush()
	
	// Write header
	if err := writer.Write([]string{"id", "order_id", "product_id", "quantity", "unit_price"}); err != nil {
		return err
	}
	
	// Write records
	for _, item := range orderItems {
		record := []string{
			strconv.Itoa(item.ID),
			strconv.Itoa(item.OrderID),
			strconv.Itoa(item.ProductID),
			strconv.Itoa(item.Quantity),
			fmt.Sprintf("%.2f", item.UnitPrice),
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	
	return nil
}

func (fh *FactoryHelper) createCategoriesJSON(filename string, categories []CategoryRecord) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(categories)
}

func (fh *FactoryHelper) createProductsJSON(filename string, products []ProductRecord) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(products)
}

func (fh *FactoryHelper) createOrdersJSON(filename string, orders []OrderRecord) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(orders)
}

// GetOutputDir returns the temporary output directory for test files
func (fh *FactoryHelper) GetOutputDir() string {
	return fh.outputDir
}

// GetFactory returns the underlying data factory for advanced usage
func (fh *FactoryHelper) GetFactory() *DataFactory {
	return fh.factory
}

// CreateBenchmarkDataset creates datasets of various sizes for benchmarking
func (fh *FactoryHelper) CreateBenchmarkDataset(sizes []DatasetSize) (map[DatasetSize]string, error) {
	results := make(map[DatasetSize]string)
	
	for _, size := range sizes {
		// Update factory configuration
		fh.factory.config.Size = size
		
		dataset, err := fh.factory.GenerateTestDataset()
		if err != nil {
			return nil, fmt.Errorf("failed to generate dataset for size %v: %w", size, err)
		}
		
		filename := filepath.Join(fh.outputDir, fmt.Sprintf("benchmark_%v.csv", size))
		if err := fh.createProductsCSV(filename, dataset.Products); err != nil {
			return nil, fmt.Errorf("failed to create benchmark file: %w", err)
		}
		
		results[size] = filename
	}
	
	return results, nil
}

// ValidateDataIntegrity validates that generated data maintains referential integrity
func (fh *FactoryHelper) ValidateDataIntegrity(dataset *TestDataset) error {
	// Check that all products reference valid categories
	categoryIDs := make(map[int]bool)
	for _, cat := range dataset.Categories {
		categoryIDs[cat.ID] = true
	}
	
	for _, prod := range dataset.Products {
		if !categoryIDs[prod.CategoryID] {
			return fmt.Errorf("product %d references non-existent category %d", prod.ID, prod.CategoryID)
		}
	}
	
	// Check that all order items reference valid orders and products
	orderIDs := make(map[int]bool)
	for _, order := range dataset.Orders {
		orderIDs[order.ID] = true
	}
	
	productIDs := make(map[int]bool)
	for _, prod := range dataset.Products {
		productIDs[prod.ID] = true
	}
	
	for _, item := range dataset.OrderItems {
		if !orderIDs[item.OrderID] {
			return fmt.Errorf("order item %d references non-existent order %d", item.ID, item.OrderID)
		}
		if !productIDs[item.ProductID] {
			return fmt.Errorf("order item %d references non-existent product %d", item.ID, item.ProductID)
		}
	}
	
	return nil
}