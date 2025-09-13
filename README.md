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

### Sales Channel Performance
```
	 -- Total sales: Sum of retail sales, transfers, and warehouse sales.
	SELECT TO_CHAR(SUM(retail_sales), '$9,999,999.00') AS total_retail_sales,
		  TO_CHAR(SUM(retail_transfer), '$9,999,999.00')  AS total_transfer,
		  TO_CHAR(SUM(warehouse_sales), '$9,999,999.00') AS total_wh_sales
	FROM gold_wh_sales;
 ```
#### Outputs
 ```
Retail Sales	Transfers Sales	Warehouse Sales
$2,160,899.37	$2,133,968.63	$7,781,756.28
 ```
#### Analysis
- Warehouse-driven revenue: At $7.78M, warehouse sales dominate overall performance.
- Balanced secondary streams: Retail and transfers each contribute ~$2.1M, indicating steady but smaller channels.
- Operational balance: Transfers support stock distribution effectively, keeping retail and warehouse aligned.
#### Business Impact
- Heavy reliance on wholesale/warehouse sales — opportunity to expand direct-to-consumer retail for more diversified growth.
- Balanced transfer volumes suggest an efficient supply chain, which can be leveraged for scaling.
	## Average Retail Sales by Item Type
 ```
	-- Average sales: Average retail sales per item type.
SELECT TO_CHAR(AVG(retail_sales), '$9,999,999.00') AS avg_retail_sales,
       item_type
FROM gold_wh_sales
GROUP BY  item_type
ORDER BY avg_retail_sales DESC;
 ```
  #### Outputs
 ```
Item Type	Avg Retail Sales
Non-Alcohol	$17.86
Beer	$13.54
Liquor	$12.37
Str_Supplies	$6.77
Ref	$5.23
Wine	$3.98
Kegs	$0.00
Dunnage	$0.00
Unknown	$0.00
 ```
### Key Takeaways
- Non-Alcohol leads with the highest average sales ($17.86 per item).
- Beer and Liquor follow as strong contributors, each above $12 average.
- Wine and Ref categories underperform (<$6 average).
- Kegs, Dunnage, Unknown show no measurable sales, possibly placeholders or underutilized categories.

 ### Yearly Sales Trends
  ```
	--Yearly trends: Total sales per year to identify growth or decline.
SELECT year,
	  TO_CHAR(SUM(retail_sales), '$9,999,999.00') AS total_retail_sales,
	  TO_CHAR(SUM(retail_transfer), '$9,999,999.00')  AS total_transfer,
	  TO_CHAR(SUM(warehouse_sales), '$9,999,999.00') AS total_wh_sales
FROM gold_wh_sales
    GROUP BY year
	ORDER BY year;
```
 #### Outputs
 ```
Year	Retail Sales  Transfers Sales	  Warehouse Sales
2017	$686,734.57	   $676,620.50 	        $2,333,849.13
2018	$153,595.90	   $153,652.92	        $519,526.19
2019	$960,191.20    $957,562.40	        $3,543,371.23
2020	$360,377.70    $346,132.81	        $1,385,009.73
 ```
### Key Takeaways
- 2017 → 2018 Decline: Sales dropped sharply across all channels in 2018.
- 2019 Recovery: Strong rebound, with warehouse sales exceeding $3.5M.
- 2020 Contraction: Noticeable dip again, likely due to external factor.

 ```
	-- Monthly trends: Total sales per month across years to find seasonal patterns.
SELECT month_name,
	  TO_CHAR(SUM(retail_sales), '$9,999,999.00') AS total_retail_sales,
	  TO_CHAR(SUM(retail_transfer), '$9,999,999.00')  AS total_transfer,
	  TO_CHAR(SUM(warehouse_sales), '$9,999,999.00') AS total_wh_sales
FROM gold_wh_sales
    GROUP BY month_name
	ORDER BY EXTRACT(MONTH FROM TO_DATE(month_name, 'Mon'));
 ```
 #### Outputs
```
	"Jan"	"$   226,211.07"	"$   226,689.12"	"$   819,013.63"
"Feb"	"$   157,917.67"	"$   151,672.76"	"$   513,596.20"
"Mar"	"$   193,852.33"	"$   201,920.50"	"$   610,713.70"
"Apr"	"$    80,342.58"	"$    83,633.90"	"$   298,840.87"
"May"	"$    94,953.10"	"$    88,056.80"	"$   383,791.58"
"Jun"	"$   188,217.65"	"$   180,463.73"	"$   725,977.91"
"Jul"	"$   277,927.73"	"$   273,299.16"	"$ 1,109,919.19"
"Aug"	"$   177,740.39"	"$   178,360.13"	"$   731,789.78"
"Sep"	"$   254,687.49"	"$   246,880.75"	"$   988,834.16"
"Oct"	"$   177,467.37"	"$   181,461.56"	"$   653,917.94"
"Nov"	"$   199,947.50"	"$   200,400.71"	"$   638,404.10"
"Dec"	"$   131,634.49"	"$   121,129.51"	"$   306,957.22"
```

 ### Observations from the data:
- Retail sales + transfers
- Peak in July (≈ $278K retail, $273K transfer).
- Secondary highs in Sep (≈ $255K retail, $247K transfer).
- Lowest in Apr & May (≈ $80–95K).
- Stronger performance mid-year vs. early spring.

#### Warehouse sales
- Very strong in July ($1.1M) and Sep ($989K).
- Secondary strength in Jun & Aug (≈ $726K & $732K).
- Weakest in Apr & Dec (≈ $299K & $307K).

#### Seasonality
- Clear mid-year peak (Jun–Sep) for all sales channels.
- Holiday season (Nov–Dec) doesn’t show the expected spike — possibly meaning the products aren’t holiday-driven.
- Spring (Apr–May) is consistently low.
- Overall Pattern: Business shows volatility year-to-year, with warehouse consistently being the largest revenue driver.

 ```
	-- item trends: Track monthly sales for a item.
SELECT month_name, item_type,
	  TO_CHAR(SUM(retail_sales), '$9,999,999.00') AS total_retail_sales,
	  TO_CHAR(SUM(retail_transfer), '$9,999,999.00')  AS total_transfer,
	  TO_CHAR(SUM(warehouse_sales), '$9,999,999.00') AS total_wh_sales
FROM gold_wh_sales
    GROUP BY month_name,item_type
	ORDER BY EXTRACT(MONTH FROM TO_DATE(month_name, 'Mon'));
```
#### Outputs
 ```
"Jan"	"Beer"	"$    51,965.12"	"$    60,527.52"	"$   672,824.38"
"Jan"	"Dunnage"	"$          .00"	"$          .00"	"$   -15,988.00"
"Jan"	"Kegs"	"$          .00"	"$          .00"	"$    14,838.00"
"Jan"	"Liquor"	"$    87,152.32"	"$    83,188.56"	"$    11,748.04"
"Jan"	"Non-Alcohol"	"$     3,093.88"	"$     2,795.95"	"$     3,255.63"
"Jan"	"Ref"	"$        70.43"	"$        50.00"	"$    -2,531.00"
"Jan"	"Str_Supplies"	"$       300.32"	"$       887.88"	"$          .00"
"Jan"	"Wine"	"$    83,629.00"	"$    79,239.21"	"$   134,866.58"
"Feb"	"Beer"	"$    37,654.84"	"$    34,024.10"	"$   417,068.56"
"Feb"	"Dunnage"	"$          .00"	"$          .00"	"$    -9,130.00"
"Feb"	"Kegs"	"$          .00"	"$          .00"	"$     9,156.00"
"Feb"	"Liquor"	"$    58,667.41"	"$    56,482.41"	"$     6,918.18"
"Feb"	"Non-Alcohol"	"$     1,785.27"	"$     1,700.12"	"$     1,805.50"
"Feb"	"Ref"	"$        67.87"	"$        25.00"	"$      -721.00"
"Feb"	"Str_Supplies"	"$       138.06"	"$       812.00"	"$          .00"
"Feb"	"Wine"	"$    59,604.22"	"$    58,629.13"	"$    88,498.96"
"Mar"	"Beer"	"$    51,115.74"	"$    53,229.85"	"$   503,925.45"
"Mar"	"Dunnage"	"$          .00"	"$          .00"	"$    -7,982.00"
"Mar"	"Kegs"	"$          .00"	"$          .00"	"$     8,210.00"
"Mar"	"Liquor"	"$    72,798.43"	"$    76,884.11"	"$     6,958.52"
"Mar"	"Non-Alcohol"	"$     3,192.57"	"$     2,741.00"	"$     2,035.34"
"Mar"	"Ref"	"$        42.03"	"$        13.00"	"$      -551.00"
"Mar"	"Str_Supplies"	"$       177.54"	"$       849.00"	"$          .00"
"Mar"	"Wine"	"$    66,526.02"	"$    68,203.54"	"$    98,117.39"
"Apr"	"Beer"	"$    20,535.97"	"$    21,679.38"	"$   246,457.69"
"Apr"	"Dunnage"	"$          .00"	"$          .00"	"$    -5,533.00"
"Apr"	"Kegs"	"$          .00"	"$          .00"	"$     5,217.00"
"Apr"	"Liquor"	"$    29,719.95"	"$    30,490.38"	"$     4,483.78"
"Apr"	"Non-Alcohol"	"$       993.85"	"$     1,039.12"	"$     1,011.42"
"Apr"	"Ref"	"$        22.62"	"$        16.00"	"$      -459.00"
"Apr"	"Str_Supplies"	"$        94.71"	"$       340.00"	"$          .00"
"Apr"	"Wine"	"$    28,975.48"	"$    30,069.02"	"$    47,662.98"
"May"	"Beer"	"$    27,905.13"	"$    24,966.79"	"$   321,526.33"
"May"	"Wine"	"$    31,036.31"	"$    29,005.58"	"$    55,323.98"
"May"	"Str_Supplies"	"$       116.21"	"$       408.00"	"$          .00"
"May"	"Ref"	"$        22.40"	"$        23.00"	"$       -59.00"
"May"	"Non-Alcohol"	"$     1,302.34"	"$     1,170.46"	"$     1,255.34"
"May"	"Liquor"	"$    34,570.71"	"$    32,482.97"	"$     5,420.93"
"May"	"Kegs"	"$          .00"	"$          .00"	"$     5,860.00"
"May"	"Dunnage"	"$          .00"	"$          .00"	"$    -5,536.00"
"Jun"	"Wine"	"$    61,210.15"	"$    56,353.32"	"$    94,249.25"
"Jun"	"Beer"	"$    55,730.30"	"$    55,872.84"	"$   619,732.48"
"Jun"	"Dunnage"	"$          .00"	"$          .00"	"$   -11,616.00"
"Jun"	"Kegs"	"$          .00"	"$          .00"	"$    11,594.00"
"Jun"	"Liquor"	"$    68,530.33"	"$    64,744.37"	"$     9,391.01"
"Jun"	"Non-Alcohol"	"$     2,465.89"	"$     2,534.46"	"$     2,503.17"
"Jun"	"Ref"	"$        55.23"	"$        59.00"	"$       124.00"
"Jun"	"Str_Supplies"	"$       225.75"	"$       899.74"	"$          .00"
"Jul"	"Beer"	"$    84,840.84"	"$    79,859.68"	"$   957,368.84"
"Jul"	"Dunnage"	"$          .00"	"$          .00"	"$   -13,684.00"
"Jul"	"Kegs"	"$          .00"	"$        -1.00"	"$    12,680.00"
"Jul"	"Liquor"	"$   101,831.18"	"$   102,255.82"	"$    10,929.60"
"Jul"	"Non-Alcohol"	"$     7,083.31"	"$     4,276.59"	"$     3,747.54"
"Jul"	"Ref"	"$       103.74"	"$        42.00"	"$      -731.00"
"Jul"	"Str_Supplies"	"$       271.55"	"$     1,521.96"	"$          .00"
"Jul"	"Wine"	"$    83,797.11"	"$    85,344.11"	"$   139,608.21"
"Aug"	"Wine"	"$    59,141.09"	"$    57,513.65"	"$    95,681.60"
"Aug"	"Str_Supplies"	"$       187.51"	"$       816.00"	"$          .00"
"Aug"	"Ref"	"$        41.49"	"$        26.00"	"$    -8,614.00"
"Aug"	"Non-Alcohol"	"$     2,394.85"	"$     2,417.16"	"$     2,472.46"
"Aug"	"Liquor"	"$    65,810.90"	"$    64,750.40"	"$     8,341.85"
"Aug"	"Kegs"	"$          .00"	"$          .00"	"$    11,430.00"
"Aug"	"Dunnage"	"$          .00"	"$          .00"	"$   -11,068.00"
"Aug"	"Beer"	"$    50,164.55"	"$    52,836.92"	"$   633,545.87"
"Sep"	"Beer"	"$    69,973.19"	"$    65,520.10"	"$   838,612.67"
"Sep"	"Wine"	"$    84,676.80"	"$    82,783.69"	"$   138,034.26"
"Sep"	"Str_Supplies"	"$       298.42"	"$     1,359.00"	"$          .00"
"Sep"	"Ref"	"$        65.16"	"$        40.00"	"$      -866.00"
"Sep"	"Non-Alcohol"	"$     4,660.11"	"$     2,938.27"	"$     2,914.05"
"Sep"	"Liquor"	"$    95,013.81"	"$    94,239.69"	"$    10,472.18"
"Sep"	"Kegs"	"$          .00"	"$          .00"	"$    12,966.00"
"Sep"	"Dunnage"	"$          .00"	"$          .00"	"$   -13,299.00"
"Oct"	"Beer"	"$    46,083.93"	"$    44,256.76"	"$   543,662.14"
"Oct"	"Dunnage"	"$          .00"	"$          .00"	"$   -12,254.00"
"Oct"	"Kegs"	"$          .00"	"$          .00"	"$    11,172.00"
"Oct"	"Liquor"	"$    65,616.97"	"$    69,686.63"	"$     8,787.10"
"Oct"	"Non-Alcohol"	"$     2,800.27"	"$     1,978.59"	"$     2,070.59"
"Oct"	"Ref"	"$        44.73"	"$        37.00"	"$    -1,763.00"
"Oct"	"Str_Supplies"	"$       227.69"	"$       775.00"	"$          .00"
"Oct"	"Unknown"	"$          .00"	"$          .00"	"$         1.00"
"Oct"	"Wine"	"$    62,693.78"	"$    64,727.58"	"$   102,242.11"
"Nov"	"Beer"	"$    49,428.76"	"$    48,961.53"	"$   522,007.80"
"Nov"	"Dunnage"	"$          .00"	"$          .00"	"$   -10,472.00"
"Nov"	"Kegs"	"$          .00"	"$          .00"	"$    10,474.00"
"Nov"	"Liquor"	"$    73,457.14"	"$    74,270.20"	"$     8,035.57"
"Nov"	"Non-Alcohol"	"$     2,995.53"	"$     1,946.24"	"$     2,132.80"
"Nov"	"Ref"	"$        86.72"	"$        28.92"	"$    -2,528.00"
"Nov"	"Str_Supplies"	"$       263.48"	"$     1,260.00"	"$          .00"
"Nov"	"Wine"	"$    73,715.87"	"$    73,933.82"	"$   108,753.93"
"Dec"	"Wine"	"$    51,492.76"	"$    48,815.39"	"$    53,945.66"
"Dec"	"Str_Supplies"	"$       439.64"	"$       918.00"	"$          .00"
"Dec"	"Ref"	"$        41.21"	"$        29.00"	"$    -1,800.00"
"Dec"	"Non-Alcohol"	"$     1,316.44"	"$     1,128.42"	"$       945.75"
"Dec"	"Liquor"	"$    49,522.28"	"$    45,260.17"	"$     3,419.51"
"Dec"	"Kegs"	"$          .00"	"$          .00"	"$     4,834.00"
"Dec"	"Dunnage"	"$          .00"	"$          .00"	"$    -4,892.00"
"Dec"	"Beer"	"$    28,822.16"	"$    24,978.53"	"$   250,504.30"
```
