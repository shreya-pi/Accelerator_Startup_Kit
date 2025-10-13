Here's the Snowflake SQL code to generate the two views for each table in the schema.

### AUDIT_TABLE Views

```sql
-- AUDIT_TABLE views
CREATE OR REPLACE VIEW AUDIT_TABLE_duplicate_records AS
WITH duplicate_records AS (
  SELECT 
    Job_ID,
    Job_Type,
    Status,
    Start_Time,
    End_Time,
    Duration,
    Details,
    ROW_NUMBER() OVER (
      PARTITION BY 
        Job_Type,
        Status,
        Start_Time,
        End_Time,
        Duration,
        Details
      ORDER BY 
        Job_ID
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.AUDIT_TABLE
)
SELECT 
  Job_ID,
  Job_Type,
  Status,
  Start_Time,
  End_Time,
  Duration,
  Details
FROM 
  duplicate_records
WHERE 
  row_num > 1;

CREATE OR REPLACE VIEW AUDIT_TABLE_clean_view AS
WITH clean_records AS (
  SELECT 
    Job_ID,
    Job_Type,
    Status,
    Start_Time,
    End_Time,
    Duration,
    Details,
    ROW_NUMBER() OVER (
      PARTITION BY 
        Job_Type,
        Status,
        Start_Time,
        End_Time,
        Duration,
        Details
      ORDER BY 
        Job_ID
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.AUDIT_TABLE
)
SELECT 
  Job_ID,
  Job_Type,
  Status,
  Start_Time,
  End_Time,
  Duration,
  Details
FROM 
  clean_records
WHERE 
  row_num = 1;
```

### CUST Views

```sql
-- CUST views
CREATE OR REPLACE VIEW CUST_duplicate_records AS
WITH duplicate_records AS (
  SELECT 
    INDEX,
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    COMPANY,
    CITY,
    COUNTRY,
    PHONE_1,
    PHONE_2,
    EMAIL,
    SUBSCRIPTION,
    WEBSITE,
    LOAD_DATE,
    ROW_NUMBER() OVER (
      PARTITION BY 
        FIRST_NAME,
        LAST_NAME,
        COMPANY,
        CITY,
        COUNTRY,
        PHONE_1,
        PHONE_2,
        EMAIL,
        SUBSCRIPTION,
        WEBSITE
      ORDER BY 
        INDEX
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.CUST
)
SELECT 
  INDEX,
  CUSTOMER_ID,
  FIRST_NAME,
  LAST_NAME,
  COMPANY,
  CITY,
  COUNTRY,
  PHONE_1,
  PHONE_2,
  EMAIL,
  SUBSCRIPTION,
  WEBSITE,
  LOAD_DATE
FROM 
  duplicate_records
WHERE 
  row_num > 1;

CREATE OR REPLACE VIEW CUST_clean_view AS
WITH clean_records AS (
  SELECT 
    INDEX,
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    COMPANY,
    CITY,
    COUNTRY,
    PHONE_1,
    PHONE_2,
    EMAIL,
    SUBSCRIPTION,
    WEBSITE,
    LOAD_DATE,
    ROW_NUMBER() OVER (
      PARTITION BY 
        FIRST_NAME,
        LAST_NAME,
        COMPANY,
        CITY,
        COUNTRY,
        PHONE_1,
        PHONE_2,
        EMAIL,
        SUBSCRIPTION,
        WEBSITE
      ORDER BY 
        INDEX
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.CUST
)
SELECT 
  INDEX,
  CUSTOMER_ID,
  FIRST_NAME,
  LAST_NAME,
  COMPANY,
  CITY,
  COUNTRY,
  PHONE_1,
  PHONE_2,
  EMAIL,
  SUBSCRIPTION,
  WEBSITE,
  LOAD_DATE
FROM 
  clean_records
WHERE 
  row_num = 1;
```

### CUSTOMERS Views

```sql
-- CUSTOMERS views
CREATE OR REPLACE VIEW CUSTOMERS_duplicate_records AS
WITH duplicate_records AS (
  SELECT 
    CUSTOMER_ID,
    NAME,
    EMAIL,
    PHONE,
    ADDRESS,
    LOAD_DATE,
    ROW_NUMBER() OVER (
      PARTITION BY 
        NAME,
        EMAIL,
        PHONE,
        ADDRESS
      ORDER BY 
        CUSTOMER_ID
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.CUSTOMERS
)
SELECT 
  CUSTOMER_ID,
  NAME,
  EMAIL,
  PHONE,
  ADDRESS,
  LOAD_DATE
FROM 
  duplicate_records
WHERE 
  row_num > 1;

CREATE OR REPLACE VIEW CUSTOMERS_clean_view AS
WITH clean_records AS (
  SELECT 
    CUSTOMER_ID,
    NAME,
    EMAIL,
    PHONE,
    ADDRESS,
    LOAD_DATE,
    ROW_NUMBER() OVER (
      PARTITION BY 
        NAME,
        EMAIL,
        PHONE,
        ADDRESS
      ORDER BY 
        CUSTOMER_ID
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.CUSTOMERS
)
SELECT 
  CUSTOMER_ID,
  NAME,
  EMAIL,
  PHONE,
  ADDRESS,
  LOAD_DATE
FROM 
  clean_records
WHERE 
  row_num = 1;
```

### CUST_DATA Views

```sql
-- CUST_DATA views
CREATE OR REPLACE VIEW CUST_DATA_duplicate_records AS
WITH duplicate_records AS (
  SELECT 
    INDEX,
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    COMPANY,
    CITY,
    COUNTRY,
    PHONE_1,
    PHONE_2,
    EMAIL,
    SUBSCRIPTION,
    WEBSITE,
    LOAD_DATE,
    ROW_NUMBER() OVER (
      PARTITION BY 
        FIRST_NAME,
        LAST_NAME,
        COMPANY,
        CITY,
        COUNTRY,
        PHONE_1,
        PHONE_2,
        EMAIL,
        SUBSCRIPTION,
        WEBSITE
      ORDER BY 
        INDEX
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.CUST_DATA
)
SELECT 
  INDEX,
  CUSTOMER_ID,
  FIRST_NAME,
  LAST_NAME,
  COMPANY,
  CITY,
  COUNTRY,
  PHONE_1,
  PHONE_2,
  EMAIL,
  SUBSCRIPTION,
  WEBSITE,
  LOAD_DATE
FROM 
  duplicate_records
WHERE 
  row_num > 1;

CREATE OR REPLACE VIEW CUST_DATA_clean_view AS
WITH clean_records AS (
  SELECT 
    INDEX,
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    COMPANY,
    CITY,
    COUNTRY,
    PHONE_1,
    PHONE_2,
    EMAIL,
    SUBSCRIPTION,
    WEBSITE,
    LOAD_DATE,
    ROW_NUMBER() OVER (
      PARTITION BY 
        FIRST_NAME,
        LAST_NAME,
        COMPANY,
        CITY,
        COUNTRY,
        PHONE_1,
        PHONE_2,
        EMAIL,
        SUBSCRIPTION,
        WEBSITE
      ORDER BY 
        INDEX
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.CUST_DATA
)
SELECT 
  INDEX,
  CUSTOMER_ID,
  FIRST_NAME,
  LAST_NAME,
  COMPANY,
  CITY,
  COUNTRY,
  PHONE_1,
  PHONE_2,
  EMAIL,
  SUBSCRIPTION,
  WEBSITE,
  LOAD_DATE
FROM 
  clean_records
WHERE 
  row_num = 1;
```

### MINING_DATA Views

```sql
-- MINING_DATA views
CREATE OR REPLACE VIEW MINING_DATA_duplicate_records AS
WITH duplicate_records AS (
  SELECT 
    DATE,
    -- Add other columns here
    ROW_NUMBER() OVER (
      PARTITION BY 
        -- Add other columns here
      ORDER BY 
        DATE
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.MINING_DATA
)
SELECT 
  DATE,
  -- Add other columns here
FROM 
  duplicate_records
WHERE 
  row_num > 1;

CREATE OR REPLACE VIEW MINING_DATA_clean_view AS
WITH clean_records AS (
  SELECT 
    DATE,
    -- Add other columns here
    ROW_NUMBER() OVER (
      PARTITION BY 
        -- Add other columns here
      ORDER BY 
        DATE
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.MINING_DATA
)
SELECT 
  DATE,
  -- Add other columns here
FROM 
  clean_records
WHERE 
  row_num = 1;
```

### ORDERS Views

```sql
-- ORDERS views
CREATE OR REPLACE VIEW ORDERS_duplicate_records AS
WITH duplicate_records AS (
  SELECT 
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_DATE,
    TOTAL_AMOUNT,
    STATUS,
    LOAD_DATE,
    ROW_NUMBER() OVER (
      PARTITION BY 
        ORDER_DATE,
        TOTAL_AMOUNT,
        STATUS
      ORDER BY 
        ORDER_ID
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.ORDERS
)
SELECT 
  ORDER_ID,
  CUSTOMER_ID,
  ORDER_DATE,
  TOTAL_AMOUNT,
  STATUS,
  LOAD_DATE
FROM 
  duplicate_records
WHERE 
  row_num > 1;

CREATE OR REPLACE VIEW ORDERS_clean_view AS
WITH clean_records AS (
  SELECT 
    O.ORDER_ID,
    O.CUSTOMER_ID,
    O.ORDER_DATE,
    O.TOTAL_AMOUNT,
    O.STATUS,
    O.LOAD_DATE,
    ROW_NUMBER() OVER (
      PARTITION BY 
        O.ORDER_DATE,
        O.TOTAL_AMOUNT,
        O.STATUS
      ORDER BY 
        O.ORDER_ID
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.ORDERS O
  INNER JOIN 
    ICEBERG_SCHEMA.CUSTOMERS C ON O.CUSTOMER_ID = C.CUSTOMER_ID
)
SELECT 
  O.ORDER_ID,
  O.CUSTOMER_ID,
  O.ORDER_DATE,
  O.TOTAL_AMOUNT,
  O.STATUS,
  O.LOAD_DATE
FROM 
  clean_records
WHERE 
  row_num = 1;
```

### PRODUCTIVITY_DATA Views

```sql
-- PRODUCTIVITY_DATA views
CREATE OR REPLACE VIEW PRODUCTIVITY_DATA_duplicate_records AS
WITH duplicate_records AS (
  SELECT 
    LOAD_DATE,
    -- Add other columns here
    ROW_NUMBER() OVER (
      PARTITION BY 
        -- Add other columns here
      ORDER BY 
        LOAD_DATE
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.PRODUCTIVITY_DATA
)
SELECT 
  LOAD_DATE,
  -- Add other columns here
FROM 
  duplicate_records
WHERE 
  row_num > 1;

CREATE OR REPLACE VIEW PRODUCTIVITY_DATA_clean_view AS
WITH clean_records AS (
  SELECT 
    LOAD_DATE,
    -- Add other columns here
    ROW_NUMBER() OVER (
      PARTITION BY 
        -- Add other columns here
      ORDER BY 
        LOAD_DATE
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.PRODUCTIVITY_DATA
)
SELECT 
  LOAD_DATE,
  -- Add other columns here
FROM 
  clean_records
WHERE 
  row_num = 1;
```

### PRODUCTS Views

```sql
-- PRODUCTS views
CREATE OR REPLACE VIEW PRODUCTS_duplicate_records AS
WITH duplicate_records AS (
  SELECT 
    PRODUCT_ID,
    PRODUCT_NAME,
    CATEGORY,
    PRICE,
    STOCK_QUANTITY,
    LOAD_DATE,
    ROW_NUMBER() OVER (
      PARTITION BY 
        PRODUCT_NAME,
        CATEGORY,
        PRICE,
        STOCK_QUANTITY
      ORDER BY 
        PRODUCT_ID
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.PRODUCTS
)
SELECT 
  PRODUCT_ID,
  PRODUCT_NAME,
  CATEGORY,
  PRICE,
  STOCK_QUANTITY,
  LOAD_DATE
FROM 
  duplicate_records
WHERE 
  row_num > 1;

CREATE OR REPLACE VIEW PRODUCTS_clean_view AS
WITH clean_records AS (
  SELECT 
    PRODUCT_ID,
    PRODUCT_NAME,
    CATEGORY,
    PRICE,
    STOCK_QUANTITY,
    LOAD_DATE,
    ROW_NUMBER() OVER (
      PARTITION BY 
        PRODUCT_NAME,
        CATEGORY,
        PRICE,
        STOCK_QUANTITY
      ORDER BY 
        PRODUCT_ID
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.PRODUCTS
)
SELECT 
  PRODUCT_ID,
  PRODUCT_NAME,
  CATEGORY,
  PRICE,
  STOCK_QUANTITY,
  LOAD_DATE
FROM 
  clean_records
WHERE 
  row_num = 1;
```

### SALES_ICEBERG_TABLE Views

```sql
-- SALES_ICEBERG_TABLE views
CREATE OR REPLACE VIEW SALES_ICEBERG_TABLE_duplicate_records AS
WITH duplicate_records AS (
  SELECT 
    `ORDER ID`,
    -- Add other columns here
    ROW_NUMBER() OVER (
      PARTITION BY 
        -- Add other columns here
      ORDER BY 
        `ORDER ID`
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.SALES_ICEBERG_TABLE
)
SELECT 
  `ORDER ID`,
  -- Add other columns here
FROM 
  duplicate_records
WHERE 
  row_num > 1;

CREATE OR REPLACE VIEW SALES_ICEBERG_TABLE_clean_view AS
WITH clean_records AS (
  SELECT 
    `ORDER ID`,
    -- Add other columns here,
    ROW_NUMBER() OVER (
      PARTITION BY 
        -- Add other columns here
      ORDER BY 
        `ORDER ID`
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.SALES_ICEBERG_TABLE
)
SELECT 
  `ORDER ID`,
  -- Add other columns here
FROM 
  clean_records
WHERE 
  row_num = 1;
```

### SHIPMENTS Views

```sql
-- SHIPMENTS views
CREATE OR REPLACE VIEW SHIPMENTS_duplicate_records AS
WITH duplicate_records AS (
  SELECT 
    SHIPMENT_ID,
    ORDER_ID,
    SHIPMENT_DATE,
    CARRIER,
    TRACKING_NUMBER,
    LOAD_DATE,
    ROW_NUMBER() OVER (
      PARTITION BY 
        SHIPMENT_DATE,
        CARRIER,
        TRACKING_NUMBER
      ORDER BY 
        SHIPMENT_ID
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.SHIPMENTS
)
SELECT 
  SHIPMENT_ID,
  ORDER_ID,
  SHIPMENT_DATE,
  CARRIER,
  TRACKING_NUMBER,
  LOAD_DATE
FROM 
  duplicate_records
WHERE 
  row_num > 1;

CREATE OR REPLACE VIEW SHIPMENTS_clean_view AS
WITH clean_records AS (
  SELECT 
    S.SHIPMENT_ID,
    S.ORDER_ID,
    S.SHIPMENT_DATE,
    S.CARRIER,
    S.TRACKING_NUMBER,
    S.LOAD_DATE,
    ROW_NUMBER() OVER (
      PARTITION BY 
        S.SHIPMENT_DATE,
        S.CARRIER,
        S.TRACKING_NUMBER
      ORDER BY 
        S.SHIPMENT_ID
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.SHIPMENTS S
  INNER JOIN 
    ICEBERG_SCHEMA.ORDERS O ON S.ORDER_ID = O.ORDER_ID
)
SELECT 
  S.SHIPMENT_ID,
  S.ORDER_ID,
  S.SHIPMENT_DATE,
  S.CARRIER,
  S.TRACKING_NUMBER,
  S.LOAD_DATE
FROM 
  clean_records
WHERE 
  row_num = 1;
```

### SUPPLIERS Views

```sql
-- SUPPLIERS views
CREATE OR REPLACE VIEW SUPPLIERS_duplicate_records AS
WITH duplicate_records AS (
  SELECT 
    SUPPLIER_ID,
    SUPPLIER_NAME,
    CONTACT_NAME,
    PHONE,
    EMAIL,
    LOAD_DATE,
    ROW_NUMBER() OVER (
      PARTITION BY 
        SUPPLIER_NAME,
        CONTACT_NAME,
        PHONE,
        EMAIL
      ORDER BY 
        SUPPLIER_ID
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.SUPPLIERS
)
SELECT 
  SUPPLIER_ID,
  SUPPLIER_NAME,
  CONTACT_NAME,
  PHONE,
  EMAIL,
  LOAD_DATE
FROM 
  duplicate_records
WHERE 
  row_num > 1;

CREATE OR REPLACE VIEW SUPPLIERS_clean_view AS
WITH clean_records AS (
  SELECT 
    SUPPLIER_ID,
    SUPPLIER_NAME,
    CONTACT_NAME,
    PHONE,
    EMAIL,
    LOAD_DATE,
    ROW_NUMBER() OVER (
      PARTITION BY 
        SUPPLIER_NAME,
        CONTACT_NAME,
        PHONE,
        EMAIL
      ORDER BY 
        SUPPLIER_ID
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.SUPPLIERS
)
SELECT 
  SUPPLIER_ID,
  SUPPLIER_NAME,
  CONTACT_NAME,
  PHONE,
  EMAIL,
  LOAD_DATE
FROM 
  clean_records
WHERE 
  row_num = 1;
```

### TUBERCULOSIS_DATA Views

```sql
-- TUBERCULOSIS_DATA views
CREATE OR REPLACE VIEW TUBERCULOSIS_DATA_duplicate_records AS
WITH duplicate_records AS (
  SELECT 
    COUNTRY_OR_TERRITORY_NAME,
    YEAR,
    -- Add other columns here
    ROW_NUMBER() OVER (
      PARTITION BY 
        COUNTRY_OR_TERRITORY_NAME,
        YEAR,
        -- Add other columns here
      ORDER BY 
        COUNTRY_OR_TERRITORY_NAME,
        YEAR
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.TUBERCULOSIS_DATA
)
SELECT 
  COUNTRY_OR_TERRITORY_NAME,
  YEAR,
  -- Add other columns here
FROM 
  duplicate_records
WHERE 
  row_num > 1;

CREATE OR REPLACE VIEW TUBERCULOSIS_DATA_clean_view AS
WITH clean_records AS (
  SELECT 
    COUNTRY_OR_TERRITORY_NAME,
    YEAR,
    -- Add other columns here,
    ROW_NUMBER() OVER (
      PARTITION BY 
        COUNTRY_OR_TERRITORY_NAME,
        YEAR,
        -- Add other columns here
      ORDER BY 
        COUNTRY_OR_TERRITORY_NAME,
        YEAR
    ) AS row_num
  FROM 
    ICEBERG_SCHEMA.TUBERCULOSIS_DATA
)
SELECT 
  COUNTRY_OR_TERRITOSRY_NAME,
  YEAR,
  -- Add other columns here
FROM 
  clean_records
WHERE 
  row_num = 1;
```

Note: For tables with composite primary keys (like TUBERCULOSIS_DATA), you should include all columns in the `PARTITION BY` clause of the `ROW_NUMBER()` function. For tables with many columns, you should include all columns in the `PARTITION BY` clause of the `ROW_NUMBER()` function, except for the primary key columns. 

Additionally, for tables with foreign key relationships, you should join the related tables in the `clean_view` to ensure referential integrity. 

Please replace `-- Add other columns here` with the actual column names in your tables. 

Also, note that the `CUST` and `CUST_DATA` tables appear to be duplicates, so you may want to consider removing one of them to avoid data inconsistencies. 

Finally, the `MINING_DATA` and `PRODUCTIVITY_DATA` tables have primary keys that are dates, so you may want to consider adding additional columns to the `PARTITION BY` clause of the `ROW_NUMBER()` function to avoid false duplicates. 

The `SALES_ICEBERG_TABLE` table has a column named `ORDER ID` which is not a valid column name in Snowflake. You should rename it to a valid column name, such as `ORDER_ID`. 

The `TUBERCULOSIS_DATA` table has a composite primary key, so you should include all columns in the `PARTITION BY` clause of the `ROW_NUMBER()` function. 

Please test the views thoroughly to ensure they are working as expected.