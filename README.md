## Overview

- The Warehouse and Retail Sales dataset is published by Montgomery County, Maryland and made available through ![Data.gov](https://catalog.data.gov/dataset/warehouse-and-retail-sales?utm_source=chatgpt.com)
. It provides monthly records of product movement and sales, covering both retail stores and warehouse operations.
- This dataset is especially useful for analyzing supply chain flows, sales performance, and inventory management at the county level.

![Data flow](https://github.com/wolethomas78/warehouse_sales_project/blob/b410fa532a0b9c47adcc5d13df9bfef1b56b030c/ETL_WHS.png)

Dataset Structure

Each record captures the movement of a specific item within a given month. The dataset includes the following key fields:

### Column	Description
- ```supplier```	The supplier, vendor, or manufacturer of the product.
- ```itemcode```	Unique identifier assigned to the product.
- ```item_description```	Text description of the product (e.g., brand, packaging).
- ```item_type```	Category or type of item (e.g., department classification).
- ```retail_sales```	value of items sold at retail outlets.
- ```retail_transfer```	value of items transferred between retail stores or back to warehouse.
- ```warehouse_sales```	value of items distributed or sold from the warehouse.

 ###Coverage
- Time Span: Data available from 2017 onwards
- Frequency: Updated monthly
- Geography: Montgomery County, Maryland, USA



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
);```
