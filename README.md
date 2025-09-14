Table of Content
- [Overview](#overview)
- [Data Structure](#data-structure)
- [Bronze Layer](#bronze-layer)
- [Silver Layer](#silver-layer)
- [gold Layer](#gold-layer)
- [Exploratory Analysis using POSTGRESQL](#exploratory-analysis-using-postgresql)
- [Recommendations](#recommendations)
- [Tools](#tools)
- [Contact Me](#contact-me)


## Overview

- The Warehouse and Retail Sales dataset is published by Montgomery County, Maryland and made available through ![Data.gov](https://catalog.data.gov/dataset/warehouse-and-retail-sales?utm_source=chatgpt.com)
. It provides monthly records of product movement and sales, covering both retail stores and warehouse operations.
- This dataset is especially useful for analyzing supply chain flows, sales performance, and inventory management at the county level.
- The dataset will be integrated into an ETL pipeline and structured using Medallion Architecture, organizing data into Bronze (raw), Silver (cleaned), and Gold (analytics-ready) layers to ensure reliable, high-quality reporting and analysis.

![Data flow](https://github.com/wolethomas78/warehouse_sales_project/blob/b410fa532a0b9c47adcc5d13df9bfef1b56b030c/ETL_WHS.png)


 ```
- queries/
- ├── bronze_cleaning.sql
- ├── silver_transforms.sql
- ├── gold_analytics.sql
- └── exploratory_analysis.sql
 ```
  - ### Data Structure
Each record captures the movement of a specific item within a given month. The dataset includes the following key fields:

#### Column	Description
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

 ### Bronze Layer
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

 ### Silver Layer
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

### Gold Layer

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

### Exploratory Analysis using POSTGRESQL
![Analyses codes](https://github.com/wolethomas78/warehouse_sales_project/blob/03aa8b2707fd159b6f43d5a2879d4fd4831b29f7/wh_sales_Analyses)
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
- 
	### Average Retail Sales by Item Type
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

 ### Total Sales by Month
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

 ### Monthly Sales
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
#### Beer
- Warehouse Sales dominate: $600K–950K mid-year, dropping to ~$250K in Dec/Apr.
- Retail sales rise in Jul ($85K) and Sep ($70K), dip in Apr/Dec (~$20K–30K).
- Pattern: Strong summer demand (Jun–Sep), weak in winter/early spring.

#### Wine
- Stable retail & transfer: ~$50K–85K per month.
- Warehouse sales peak in Jul–Sep ($130K–140K).
- Dip in Apr/Dec (~$47K–54K).
- Pattern: Steady year-round, with summer/fall peak similar to beer.

#### Liquor
- Retail sales grow mid-year: ~70K–100K in summer vs. ~30K–50K in spring/winter.
- Warehouse sales are small (~$3K–11K monthly).
- Pattern: Seasonal uplift in summer (Jun–Sep).

#### Non-Alcohol
- Very small values (~$1K–7K retail).
- Slight increase in Jul–Sep, but negligible compared to alcohol.
- Pattern: Minor contribution, consistent all year.

#### Kegs
- Only warehouse channel: ~5K–13K per month.
- No strong seasonal spike, but a slight lift mid-year.
- Pattern: Relatively flat.

#### Dunnage & Ref (Refunds/Adjustments)
- Always negative warehouse sales (e.g., –$16K Jan, –$13K Sep).
- Likely represent returns/adjustments, not actual product trends.

#### Str_Supplies (Store Supplies)
- Very small (~$100–$400 per month).
 No meaningful seasonality.

#### Insights:
- Summer peak (Jun–Sep) is consistent across Beer, Wine, and Liquor, suggesting seasonal demand (possibly linked to outdoor events, warm-weather drinking, etc.).
- End of year (Nov–Dec) does not show a holiday bump, unlike typical retail liquor trends — could mean your customer base is more summer-event driven (bars, festivals, etc.).
- Dunnage & Ref lines distort totals — should probably be excluded from trend visualizations unless you want to track returns separately.

 ### Top Suppliers
```
-- Top-selling items: Items with the top 5 highest total retail sales, For “top suppliers” in terms of revenue:

SELECT supplier,
	  TO_CHAR(SUM(retail_sales), '$9,999,999.00') AS total_retail_sales
FROM gold_wh_sales 
GROUP BY supplier
ORDER BY total_retail_sales DESC
LIMIT 5;
```
 #### Outputs
```
  Supplier               total_retail_sales
"E & J Gallo Winery"	"$   134,199.27"
"Diageo North America Inc"	"$   118,800.90"
"Constellation Brands"	"$   108,269.49"
"Anheuser Busch Inc"	"$   105,052.60"
"Jim Beam Brands Co"	"$    95,899.38"
```

#### - Current Top 5 Suppliers (Retail Sales)
- E & J Gallo Winery – $134K
- Diageo North America Inc – $119K
- Constellation Brands – $108K
- Anheuser Busch Inc – $105K
- Jim Beam Brands Co – $96K

#### Observations
- E & J Gallo Winery leads by a small margin, but the top 4 are very close (~$105K–134K range).
- These names map well to Wine, Spirits, and Beer. The top suppliers represent all major categories
- Suggests balanced revenue contribution across beer, wine, and liquor, rather than one category dominating.

  ### Recommendations
- Seasonal Focus: Prioritize inventory, marketing, and promotions for summer months (Jun–Sep) when sales peak. Use Apr–May and Dec for targeted campaigns to boost low sales.
- Item Strategy: Focus on beer for volume in summer, maintain steady wine and liquor supply year-round. Monitor returns/adjustments separately.
- Supplier Management: Strengthen relationships with top suppliers (Gallo, Diageo, Constellation, Anheuser Busch, Jim Beam) and track which product types drive revenue.

- ### Tools
- - Postgresql
  - Github
  - Draw I.O

### Contact Me
- Connect with me on LinkedIn: [LinkedIn](https://www.linkedin.com/in/oluwolefagbemi)
- Email: wolethomas78@gmail.com
- Open to collaborations, discussions, and data analytics opportunities
