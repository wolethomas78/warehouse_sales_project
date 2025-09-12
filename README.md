## Overview

- The Warehouse and Retail Sales dataset is published by Montgomery County, Maryland and made available through ![Data.gov](https://catalog.data.gov/dataset/warehouse-and-retail-sales?utm_source=chatgpt.com)
. It provides monthly records of product movement and sales, covering both retail stores and warehouse operations.
- This dataset is especially useful for analyzing supply chain flows, sales performance, and inventory management at the county level.

![Data flow](https://github.com/wolethomas78/warehouse_sales_project/blob/b410fa532a0b9c47adcc5d13df9bfef1b56b030c/ETL_WHS.png)


 ```
- queries/
- ├── bronze_cleaning.sql
- ├── silver_transforms.sql
- ├── gold_analytics.sql
- └── exploratory_analysis.sql
 ```
  - ### Dataset Structure
Each record captures the movement of a specific item within a given month. The dataset includes the following key fields:

### Column	Description
- ```supplier```	The supplier, vendor, or manufacturer of the product.
- ```itemcode```	Unique identifier assigned to the product.
- ```item_description```	Text description of the product (e.g., brand, packaging).
- ```item_type```	Category or type of item (e.g., department classification).
- ```retail_sales```	value of items sold at retail outlets.
- ```retail_transfer```	value of items transferred between retail stores or back to warehouse.
- ```warehouse_sales```	value of items distributed or sold from the warehouse.

 ### Coverage
- Time Span: Data available from 2017 onwards
- Frequency: Updated monthly
- Geography: Montgomery County, Maryland, USA

 ## Bronze Layer
- - The Bronze code represents the raw data ingestion layer, where the Warehouse and Retail Sales dataset was ingested to ![Bronze layer](https://github.com/wolethomas78/warehouse_sales_project/blob/fbd2c6c172512009c07ecac736967c0a0ffae01c/bronze_wh_code)
 The data is first loaded in its original form to preserve data fidelity before any cleaning, transformation, or enrichment is applied.

```-- ===========================================================================
-- Script: Bronze Layer Table and Load Procedure
-- Purpose: 
--   1. Create the bronze_wh_sales table (raw staging layer).
--   2. Load raw CSV data into bronze_wh_sales using a stored procedure.
--   3. Capture load time, row count, and handle errors.
--
-- ===========================================================================

-- Drop the table if it exists, to ensure a fresh definition
DROP TABLE bronze_wh_sales;

-- Create the bronze layer table to hold raw data from CSV
CREATE TABLE IF NOT EXISTS bronze_wh_sales (
    year INT,                          -- Year of the record
    month INT,                         -- Month of the record
    supplier VARCHAR(100),             -- Supplier name
    itemcode VARCHAR(100),             -- Unique item code
    item_description VARCHAR(100),     -- Description of the item
    item_type VARCHAR(100),            -- Type or category of the item
    retail_sales FLOAT,                -- Retail sales value
    retail_transfer FLOAT,             -- Retail transfer value
    warehouse_sales FLOAT              -- Warehouse sales value
);
```

 ## Silver Layer
The ![Silver code](https://github.com/wolethomas78/warehouse_sales_project/blob/4ab918409a69aa3252bf05dbed3af7c20cbc9f05/silver_wh_code) transforms raw Bronze data into a structured and cleaned format by applying validation, standardization, and enrichment rules, making the dataset ready for analytical and business use.
```
-- Drop the table if it already exists to avoid conflicts when recreating
DROP TABLE silver_wh_sales;

-- Create the table silver_wh_sales if it does not already exist
CREATE TABLE IF NOT EXISTS silver_wh_sales (
    year INT,                           -- The year of the sales record
    month INT,                          -- The month of the sales record
    supplier VARCHAR(100),              -- Supplier name (up to 100 characters)
    itemcode VARCHAR(100),              -- Unique code identifying the item
    item_description VARCHAR(100),      -- Description of the item
    item_type VARCHAR(100),             -- Category/type of the item
    retail_sales FLOAT,                 -- Sales revenue from retail channels
    retail_transfer FLOAT,              -- Transfers to retail (not direct sales)
    warehouse_sales FLOAT               -- Sales revenue from the warehouse
);


-- Procedure to load and transform data from silver_wh_sales into gold_wh_sales
-- Adds data cleansing (capitalization, handling nulls), and performance logging

CREATE OR REPLACE PROCEDURE gold_wh_sales()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;  -- Track when the load starts
    end_time TIMESTAMP;    -- Track when the load ends
    duration INTERVAL;     -- Calculate total execution duration
    row_count BIGINT;      -- Store number of rows loaded into gold_wh_sales
BEGIN
    BEGIN 
        -- Capture the starting timestamp
        start_time := clock_timestamp();
```

## Gold Layer

The ![Gold code](https://github.com/wolethomas78/warehouse_sales_project/blob/b32f6f4e0b10de95b9fa0c9a2f9f3c7f754ff356/gold_wh_clean_data) produces curated, business-ready datasets optimized for reporting, dashboards, and advanced analytics, ensuring high performance and usability for decision-making.
```
-- ===========================================================================
-- Script: Gold Layer Table Definition
-- Purpose:
--   1. Drop existing gold_wh_sales table if it exists.
--   2. Create the gold_wh_sales table, which holds
--      curated, cleaned, and business-ready data.
--   3. This is the final reporting/analytics layer.
--
-- ===========================================================================

-- Step 1: Drop existing table to avoid conflicts
DROP TABLE gold_wh_sales;

-- Step 2: Create gold layer table if it does not exist
CREATE TABLE IF NOT EXISTS gold_wh_sales (
    year INT,                          -- Year of the record
    month_name VARCHAR(100),           -- Month name (e.g., 'Jan', 'Feb')
    supplier VARCHAR(100),             -- Supplier name (standardized/cleaned)
    itemcode VARCHAR(100),             -- Unique item identifier
    item_description VARCHAR(100),     -- Item description (cleaned/standardized)
    item_type VARCHAR(100),            -- Item category/type
    retail_sales FLOAT,                -- Final retail sales value
    retail_transfer FLOAT,             -- Final retail transfer value
    warehouse_sales FLOAT              -- Final warehouse sales value
);
```

## Exploratory Analysis using POSTGRESQL :
```
	 -- Total sales: Sum of retail sales, transfers, and warehouse sales.
	SELECT TO_CHAR(SUM(retail_sales), '$9,999,999.00') AS total_retail_sales,
		  TO_CHAR(SUM(retail_transfer), '$9,999,999.00')  AS total_transfer,
		  TO_CHAR(SUM(warehouse_sales), '$9,999,999.00') AS total_wh_sales
	FROM gold_wh_sales;
 ```

### Explanation:
The SQL query is calculating total sales across three key categories from the gold_wh_sales table:
- Retail Sales – These are direct sales made to customers.
- Retail Transfers – These represent inventory or product transfers between retail locations (e.g., from one store to another). While not always revenue, they reflect stock movement that supports sales.
- Warehouse Sales – These are sales fulfilled directly from the warehouse (often bulk sales, wholesale orders, or online fulfillment).

The query:
- Adds up (SUM) each of these categories.
- Formats the results in a currency-friendly display (e.g., $1,234,567.00) for easy readability.
So, the final output will show:
- Total Retail Sales
- Total Transfer Value
- Total Warehouse Sales
This gives us a consolidated financial snapshot of how sales and product movements are distributed across channels.

### Why It Matters
- Retail Sales tell us how well our stores are performing with direct customers.
- Transfers help us understand how much effort is going into balancing inventory across stores, which can highlight supply chain efficiency or strain.
- Warehouse Sales reveal how much business is being done through wholesale, bulk orders, or warehouse-driven fulfillment channels (important for scaling and logistics).

```
	-- Average sales: Average retail sales per item type.
SELECT TO_CHAR(AVG(retail_sales), '$9,999,999.00') AS avg_retail_sales,
       item_type
FROM gold_wh_sales
GROUP BY  item_type
ORDER BY avg_retail_sales DESC;
```
### Explanation
- This query calculates the average retail sales per item type from the gold_wh_sales table.
- AVG(retail_sales) → Finds the mean retail sales amount for each type of product (Non-alcohol products, Beer, liquor, Ref, Wine).
- GROUP BY item_type → Breaks down the calculation by product category.
- ORDER BY avg_retail_sales DESC → Sorts the results so the item types with the highest average retail sales appear at the top.
- TO_CHAR(..., '$9,999,999.00') → Formats the numbers into a dollar format for readability.
- The result will show, for each item type, the average sales amount in descending order.

### Why It Matters
This tells us which categories are performing best on average. For example:
- Non-alcohol has the average higher sales than Beer, it shows stronger consumer demand and/or higher price points.
- Lower averages might indicate underperformance, overstocking, or weak customer demand.
This information helps identify:
- Top-performing categories worth investing more in.
- Low-performing categories that may need better marketing, pricing changes, or product assortment reviews.

```
	-- --------------------------- Time-based Analysis-------------------------------------------------------
--Yearly trends: Total sales per year to identify growth or decline.
SELECT year,
	  TO_CHAR(SUM(retail_sales), '$9,999,999.00') AS total_retail_sales,
	  TO_CHAR(SUM(retail_transfer), '$9,999,999.00')  AS total_transfer,
	  TO_CHAR(SUM(warehouse_sales), '$9,999,999.00') AS total_wh_sales
FROM gold_wh_sales
    GROUP BY year
```

###  Explanation
- This SQL query is designed to analyze yearly sales trends from the gold_wh_sales dataset.
- GROUP BY year → Aggregates results by each year in the dataset.
- SUM(retail_sales) → Adds up total sales made directly in retail stores.
- SUM(retail_transfer) → Adds up the value of product transfers between retail locations.
- SUM(warehouse_sales) → Adds up total warehouse-driven sales (bulk, wholesale, or online fulfillment).
- TO_CHAR(..., '$9,999,999.00') → Formats totals in a clear dollar format for reporting.
- ORDER BY year → Displays results chronologically, allowing us to see growth or decline year over year.
- The output will be a table of yearly totals across three sales channels.

###  Why It Matters
- This analysis answers:
- Are sales growing year over year, or are they declining?
- Which channel is driving growth (retail, transfers, or warehouse)?
- Are there shifts in how customers are buying? For example:
- A rise in warehouse sales may suggest more bulk/online demand.
- A decline in retail sales might point to reduced store performance.
- High transfers year over year may reveal supply chain inefficiencies.
- Understanding these yearly patterns is critical for strategic planning, forecasting, and investment decisions.

### Key Insights
- 2017 → 2018 sharp decline
- Retail sales dropped ~78%.
- Warehouse sales also declined by ~78%.
- Possible causes: economic downturn, supply disruptions, or reduced demand.

#### 2018 → 2019 strong rebound
 -Retail sales grew >500%, warehouse sales grew ~582%.
- This suggests a strong recovery — possibly due to expansion, restocking, or improved market conditions.
- 
#### 2019 → 2020 decline again
- Sales fell by ~62% in retail and ~61% in warehouse.
- This aligns with COVID-19 impacts (store closures, supply chain issues, reduced consumer spending).

### Warehouse sales consistently dominate
- Every year, warehouse sales were ~3x to 4x higher than retail sales.
- Indicates the bulk/online/wholesale channel is the primary revenue driver.

### Retail Transfers closely mirror Retail Sales
- Suggests inventory balancing between stores is proportional to direct customer sales.
 High transfer activity may signal supply chain inefficiencies or demand mismatches between locations.
	ORDER BY year;
```
