package fixtures

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// DatasetSize defines the size categories for test datasets
type DatasetSize int

const (
	Tiny   DatasetSize = 10      // Debug/quick tests
	Small  DatasetSize = 100     // Unit tests
	Medium DatasetSize = 10000   // Integration tests
	Large  DatasetSize = 1000000 // Performance tests
)

// DataPattern defines the pattern of generated data
type DataPattern int

const (
	Sequential   DataPattern = iota // Sequential data (1, 2, 3...)
	Random                          // Random data
	Realistic                       // Realistic business data
	Pathological                    // Edge cases and boundary values
	Mixed                           // Combination of patterns
)

// DataGenerationConfig configures how test data is generated
type DataGenerationConfig struct {
	Size            DatasetSize
	Pattern         DataPattern
	DuplicateRatio  float64       // Ratio of duplicate records (0.0-1.0)
	NullValueRatio  float64       // Ratio of NULL values (0.0-1.0)
	UnicodeData     bool          // Include Unicode characters
	LongTextFields  bool          // Include long text fields
	EdgeCaseValues  bool          // Include edge case values
	TimestampSpread time.Duration // Spread of timestamps
	CustomSeed      int64         // Random seed for reproducibility
}

// TestDataset represents a complete set of test data
type TestDataset struct {
	Categories []CategoryRecord
	Products   []ProductRecord
	Orders     []OrderRecord
	OrderItems []OrderItemRecord
	Metadata   DatasetMetadata
}

// DatasetMetadata contains information about the generated dataset
type DatasetMetadata struct {
	GeneratedAt    time.Time
	TotalRecords   int
	GenerationTime time.Duration
	EstimatedSize  int64 // Estimated size in bytes
	Config         DataGenerationConfig
}

// Record types for different tables
type CategoryRecord struct {
	ID          int    `json:"id" csv:"id"`
	Name        string `json:"name" csv:"name"`
	Description string `json:"description" csv:"description"`
}

type ProductRecord struct {
	ID         int     `json:"id" csv:"id"`
	Name       string  `json:"name" csv:"name"`
	CategoryID int     `json:"category_id" csv:"category_id"`
	Price      float64 `json:"price" csv:"price"`
	SKU        string  `json:"sku,omitempty" csv:"sku"`
	Stock      int     `json:"stock,omitempty" csv:"stock"`
}

type OrderRecord struct {
	ID         int     `json:"id" csv:"id"`
	CustomerID int     `json:"customer_id" csv:"customer_id"`
	Total      float64 `json:"total" csv:"total"`
	Status     string  `json:"status" csv:"status"`
	OrderDate  string  `json:"order_date,omitempty" csv:"order_date"`
}

type OrderItemRecord struct {
	ID        int     `json:"id" csv:"id"`
	OrderID   int     `json:"order_id" csv:"order_id"`
	ProductID int     `json:"product_id" csv:"product_id"`
	Quantity  int     `json:"quantity" csv:"quantity"`
	UnitPrice float64 `json:"unit_price" csv:"unit_price"`
}

// DataFactory generates test data with various patterns and configurations
type DataFactory struct {
	rng        *rand.Rand
	config     DataGenerationConfig
	categories []string
	statuses   []string
	nameWords  []string
	adjectives []string
}

// NewDataFactory creates a new data factory with the given configuration
func NewDataFactory(config DataGenerationConfig) *DataFactory {
	seed := config.CustomSeed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	return &DataFactory{
		rng:    rand.New(rand.NewSource(seed)),
		config: config,
		categories: []string{
			"Electronics", "Books", "Clothing", "Home & Garden", "Sports",
			"Toys", "Automotive", "Health", "Beauty", "Food & Beverages",
		},
		statuses: []string{
			"pending", "processing", "shipped", "delivered", "cancelled", "returned",
		},
		nameWords: []string{
			"Super", "Ultra", "Premium", "Professional", "Deluxe", "Standard",
			"Basic", "Advanced", "Pro", "Lite", "Max", "Mini", "Mega", "Smart",
		},
		adjectives: []string{
			"Amazing", "Fantastic", "Great", "Excellent", "Perfect", "Quality",
			"Durable", "Reliable", "Efficient", "Modern", "Classic", "Vintage",
		},
	}
}

// GenerateTestDataset generates a complete test dataset
func (df *DataFactory) GenerateTestDataset() (*TestDataset, error) {
	start := time.Now()

	var dataset TestDataset

	// Calculate record counts based on size
	counts := df.calculateRecordCounts()

	// Generate data for each table
	dataset.Categories = df.generateCategories(counts.Categories)
	dataset.Products = df.generateProducts(counts.Products, dataset.Categories)
	dataset.Orders = df.generateOrders(counts.Orders)
	dataset.OrderItems = df.generateOrderItems(counts.OrderItems, dataset.Orders, dataset.Products)

	// Apply data patterns
	df.applyDataPatterns(&dataset)

	// Generate metadata
	dataset.Metadata = DatasetMetadata{
		GeneratedAt:    start,
		TotalRecords:   len(dataset.Categories) + len(dataset.Products) + len(dataset.Orders) + len(dataset.OrderItems),
		GenerationTime: time.Since(start),
		EstimatedSize:  df.estimateDatasetSize(&dataset),
		Config:         df.config,
	}

	return &dataset, nil
}

// recordCounts holds the number of records to generate for each table
type recordCounts struct {
	Categories int
	Products   int
	Orders     int
	OrderItems int
}

// calculateRecordCounts determines how many records to generate for each table
func (df *DataFactory) calculateRecordCounts() recordCounts {
	baseSize := int(df.config.Size)

	switch df.config.Size {
	case Tiny:
		return recordCounts{
			Categories: 3,
			Products:   baseSize,
			Orders:     baseSize / 2,
			OrderItems: baseSize,
		}
	case Small:
		return recordCounts{
			Categories: 10,
			Products:   baseSize,
			Orders:     baseSize / 2,
			OrderItems: baseSize * 2,
		}
	case Medium:
		return recordCounts{
			Categories: 50,
			Products:   baseSize / 10,
			Orders:     baseSize / 5,
			OrderItems: baseSize,
		}
	case Large:
		return recordCounts{
			Categories: 100,
			Products:   baseSize / 100,
			Orders:     baseSize / 50,
			OrderItems: baseSize,
		}
	default:
		return recordCounts{
			Categories: 10,
			Products:   100,
			Orders:     50,
			OrderItems: 100,
		}
	}
}

// generateCategories generates category records
func (df *DataFactory) generateCategories(count int) []CategoryRecord {
	categories := make([]CategoryRecord, count)

	for i := 0; i < count; i++ {
		name := df.categories[i%len(df.categories)]
		if count > len(df.categories) {
			// Add suffix for uniqueness when we have more categories than predefined names
			name = fmt.Sprintf("%s %d", name, (i/len(df.categories))+1)
		}

		categories[i] = CategoryRecord{
			ID:          i + 1,
			Name:        name,
			Description: df.generateDescription(name),
		}
	}

	return categories
}

// generateProducts generates product records
func (df *DataFactory) generateProducts(count int, categories []CategoryRecord) []ProductRecord {
	products := make([]ProductRecord, count)

	for i := 0; i < count; i++ {
		category := categories[df.rng.Intn(len(categories))]

		products[i] = ProductRecord{
			ID:         i + 1,
			Name:       df.generateProductName(),
			CategoryID: category.ID,
			Price:      df.generatePrice(),
			SKU:        df.generateSKU(),
			Stock:      df.generateStock(),
		}
	}

	return products
}

// generateOrders generates order records
func (df *DataFactory) generateOrders(count int) []OrderRecord {
	orders := make([]OrderRecord, count)

	for i := 0; i < count; i++ {
		orders[i] = OrderRecord{
			ID:         i + 1,
			CustomerID: df.rng.Intn(1000) + 1, // Customer IDs 1-1000
			Total:      df.generateOrderTotal(),
			Status:     df.statuses[df.rng.Intn(len(df.statuses))],
			OrderDate:  df.generateOrderDate(),
		}
	}

	return orders
}

// generateOrderItems generates order item records
func (df *DataFactory) generateOrderItems(count int, orders []OrderRecord, products []ProductRecord) []OrderItemRecord {
	if len(orders) == 0 || len(products) == 0 {
		return []OrderItemRecord{}
	}

	orderItems := make([]OrderItemRecord, count)

	for i := 0; i < count; i++ {
		order := orders[df.rng.Intn(len(orders))]
		product := products[df.rng.Intn(len(products))]
		quantity := df.rng.Intn(10) + 1

		orderItems[i] = OrderItemRecord{
			ID:        i + 1,
			OrderID:   order.ID,
			ProductID: product.ID,
			Quantity:  quantity,
			UnitPrice: product.Price,
		}
	}

	return orderItems
}

// generateProductName creates a realistic product name
func (df *DataFactory) generateProductName() string {
	adjective := df.adjectives[df.rng.Intn(len(df.adjectives))]
	nameWord := df.nameWords[df.rng.Intn(len(df.nameWords))]

	patterns := []string{
		"%s %s",
		"%s %s Pro",
		"%s %s Series",
		"%s %s Edition",
	}

	pattern := patterns[df.rng.Intn(len(patterns))]
	return fmt.Sprintf(pattern, adjective, nameWord)
}

// generateDescription creates a description for categories
func (df *DataFactory) generateDescription(categoryName string) string {
	templates := []string{
		"%s products and accessories",
		"High-quality %s items",
		"Best selection of %s products",
		"Premium %s collection",
	}

	template := templates[df.rng.Intn(len(templates))]
	return fmt.Sprintf(template, strings.ToLower(categoryName))
}

// generateSKU creates a stock keeping unit code
func (df *DataFactory) generateSKU() string {
	letters := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	digits := "0123456789"

	sku := ""
	// 3 letters
	for i := 0; i < 3; i++ {
		sku += string(letters[df.rng.Intn(len(letters))])
	}
	sku += "-"
	// 4 digits
	for i := 0; i < 4; i++ {
		sku += string(digits[df.rng.Intn(len(digits))])
	}

	return sku
}

// generatePrice creates a realistic price
func (df *DataFactory) generatePrice() float64 {
	switch df.config.Pattern {
	case Sequential:
		return float64(df.rng.Intn(1000)) + 0.99
	case Pathological:
		// Include edge cases
		edgeCases := []float64{0.01, 99999999.99, 0.00, -0.01}
		if df.rng.Float64() < 0.1 { // 10% chance of edge case
			return edgeCases[df.rng.Intn(len(edgeCases))]
		}
		fallthrough
	default:
		// Random price between $1 and $1000
		price := df.rng.Float64()*999 + 1
		return math.Round(price*100) / 100 // Round to 2 decimal places
	}
}

// generateStock creates a stock quantity
func (df *DataFactory) generateStock() int {
	if df.config.Pattern == Pathological && df.rng.Float64() < 0.1 {
		// Include edge cases: out of stock or very high stock
		return []int{0, 999999}[df.rng.Intn(2)]
	}
	return df.rng.Intn(1000)
}

// generateOrderTotal creates an order total
func (df *DataFactory) generateOrderTotal() float64 {
	total := df.rng.Float64()*2000 + 10 // $10 - $2010
	return math.Round(total*100) / 100
}

// generateOrderDate creates an order date
func (df *DataFactory) generateOrderDate() string {
	now := time.Now()
	spread := df.config.TimestampSpread
	if spread == 0 {
		spread = 365 * 24 * time.Hour // Default: 1 year spread
	}

	randomDuration := time.Duration(df.rng.Int63n(int64(spread)))
	orderTime := now.Add(-randomDuration)

	return orderTime.Format("2006-01-02 15:04:05")
}

// applyDataPatterns applies the configured data patterns to the dataset
func (df *DataFactory) applyDataPatterns(dataset *TestDataset) {
	if df.config.DuplicateRatio > 0 {
		df.introduceDuplicates(dataset)
	}

	if df.config.NullValueRatio > 0 {
		df.introduceNullValues(dataset)
	}

	if df.config.UnicodeData {
		df.introduceUnicodeData(dataset)
	}

	if df.config.EdgeCaseValues {
		df.introduceEdgeCases(dataset)
	}
}

// introduceDuplicates introduces duplicate records based on the configured ratio
func (df *DataFactory) introduceDuplicates(dataset *TestDataset) {
	// Introduce duplicates in products (most common case)
	productCount := len(dataset.Products)
	duplicateCount := int(float64(productCount) * df.config.DuplicateRatio)

	for i := 0; i < duplicateCount && i < productCount-1; i++ {
		// Duplicate a random product with a new ID
		sourceIdx := df.rng.Intn(productCount - duplicateCount)
		duplicate := dataset.Products[sourceIdx]
		duplicate.ID = productCount + i + 1
		dataset.Products = append(dataset.Products, duplicate)
	}
}

// introduceNullValues introduces NULL values in optional fields
func (df *DataFactory) introduceNullValues(dataset *TestDataset) {
	// Introduce NULL values in optional fields like SKU, Stock, OrderDate
	for i := range dataset.Products {
		if df.rng.Float64() < df.config.NullValueRatio {
			dataset.Products[i].SKU = ""
		}
		if df.rng.Float64() < df.config.NullValueRatio {
			dataset.Products[i].Stock = 0 // Represent NULL as 0 for int fields
		}
	}

	for i := range dataset.Orders {
		if df.rng.Float64() < df.config.NullValueRatio {
			dataset.Orders[i].OrderDate = ""
		}
	}
}

// introduceUnicodeData introduces Unicode characters in text fields
func (df *DataFactory) introduceUnicodeData(dataset *TestDataset) {
	unicodeNames := []string{
		"测试产品", "テストプロダクト", "Тестовый продукт", "🚀 Super Product",
		"Café Münü", "naïve résumé", "東京スカイツリー", "北京大学",
	}

	// Apply to some category names
	for i := range dataset.Categories {
		if df.rng.Float64() < 0.2 { // 20% chance
			dataset.Categories[i].Name = unicodeNames[df.rng.Intn(len(unicodeNames))]
		}
	}

	// Apply to some product names
	for i := range dataset.Products {
		if df.rng.Float64() < 0.1 { // 10% chance
			dataset.Products[i].Name = unicodeNames[df.rng.Intn(len(unicodeNames))]
		}
	}
}

// introduceEdgeCases introduces edge case values
func (df *DataFactory) introduceEdgeCases(dataset *TestDataset) {
	// Introduce edge cases in the last few records
	if len(dataset.Products) > 5 {
		edgeCaseProducts := []ProductRecord{
			{ID: len(dataset.Products) + 1, Name: "", CategoryID: 1, Price: 0.01},                              // Empty name, minimum price
			{ID: len(dataset.Products) + 2, Name: strings.Repeat("A", 255), CategoryID: 1, Price: 99999999.99}, // Max length name, max price
			{ID: len(dataset.Products) + 3, Name: "Test\x00NULL", CategoryID: 1, Price: -0.01},                 // NULL character, negative price
		}

		// Replace last few products with edge cases
		for i, edgeCase := range edgeCaseProducts {
			if i < len(dataset.Products) {
				dataset.Products[len(dataset.Products)-len(edgeCaseProducts)+i] = edgeCase
			}
		}
	}
}

// estimateDatasetSize estimates the memory size of the dataset
func (df *DataFactory) estimateDatasetSize(dataset *TestDataset) int64 {
	size := int64(0)

	// Rough estimation based on struct sizes and string lengths
	for _, cat := range dataset.Categories {
		size += int64(24 + len(cat.Name) + len(cat.Description)) // 24 bytes for struct overhead + strings
	}

	for _, prod := range dataset.Products {
		size += int64(48 + len(prod.Name) + len(prod.SKU)) // 48 bytes for struct overhead + strings
	}

	for _, order := range dataset.Orders {
		size += int64(32 + len(order.Status) + len(order.OrderDate)) // 32 bytes for struct overhead + strings
	}

	for range dataset.OrderItems {
		size += 40 // 40 bytes for struct overhead (mostly numeric fields)
	}

	return size
}

// SaveToFiles saves the dataset to CSV and JSON files
func (dataset *TestDataset) SaveToFiles(outputDir string) error {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Save as CSV files
	if err := dataset.saveCategoriesCSV(filepath.Join(outputDir, "categories.csv")); err != nil {
		return fmt.Errorf("failed to save categories CSV: %w", err)
	}

	if err := dataset.saveProductsCSV(filepath.Join(outputDir, "products.csv")); err != nil {
		return fmt.Errorf("failed to save products CSV: %w", err)
	}

	if err := dataset.saveOrdersCSV(filepath.Join(outputDir, "orders.csv")); err != nil {
		return fmt.Errorf("failed to save orders CSV: %w", err)
	}

	if err := dataset.saveOrderItemsCSV(filepath.Join(outputDir, "order_items.csv")); err != nil {
		return fmt.Errorf("failed to save order items CSV: %w", err)
	}

	// Save as JSON files
	if err := dataset.saveCategoriesJSON(filepath.Join(outputDir, "categories.json")); err != nil {
		return fmt.Errorf("failed to save categories JSON: %w", err)
	}

	if err := dataset.saveProductsJSON(filepath.Join(outputDir, "products.json")); err != nil {
		return fmt.Errorf("failed to save products JSON: %w", err)
	}

	// Save metadata
	if err := dataset.saveMetadata(filepath.Join(outputDir, "metadata.json")); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	return nil
}

// CSV save methods
func (dataset *TestDataset) saveCategoriesCSV(filename string) error {
	return dataset.saveRecordsCSV(filename, dataset.Categories, []string{"id", "name", "description"})
}

func (dataset *TestDataset) saveProductsCSV(filename string) error {
	return dataset.saveRecordsCSV(filename, dataset.Products, []string{"id", "name", "category_id", "price", "sku", "stock"})
}

func (dataset *TestDataset) saveOrdersCSV(filename string) error {
	return dataset.saveRecordsCSV(filename, dataset.Orders, []string{"id", "customer_id", "total", "status", "order_date"})
}

func (dataset *TestDataset) saveOrderItemsCSV(filename string) error {
	return dataset.saveRecordsCSV(filename, dataset.OrderItems, []string{"id", "order_id", "product_id", "quantity", "unit_price"})
}

// Generic CSV save method
func (dataset *TestDataset) saveRecordsCSV(filename string, records interface{}, headers []string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	if err := writer.Write(headers); err != nil {
		return err
	}

	// Write records - this would need reflection or type assertion for a real implementation
	// For now, we'll implement specific methods for each type

	return nil
}

// JSON save methods
func (dataset *TestDataset) saveCategoriesJSON(filename string) error {
	return dataset.saveJSON(filename, dataset.Categories)
}

func (dataset *TestDataset) saveProductsJSON(filename string) error {
	return dataset.saveJSON(filename, dataset.Products)
}

func (dataset *TestDataset) saveMetadata(filename string) error {
	return dataset.saveJSON(filename, dataset.Metadata)
}

// Generic JSON save method
func (dataset *TestDataset) saveJSON(filename string, data interface{}) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(data)
}
