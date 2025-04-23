-- Select database
USE testdb;

-- Create products table
CREATE TABLE products (
  id VARCHAR(50) NOT NULL,
  name VARCHAR(100) NOT NULL,
  price VARCHAR(20) NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;

-- Insert sample data
INSERT INTO
  products (id, name, price, created_at, updated_at)
VALUES
  ('P001', 'Product 1', '1000', NOW(), NOW()),
  ('P002', 'Product 2', '2000', NOW(), NOW()),
  ('P003', 'Product 3', '3000', NOW(), NOW());

-- Define additional tables here if needed
