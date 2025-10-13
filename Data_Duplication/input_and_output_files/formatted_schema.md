I'll analyze the schema and identify the primary and foreign key relationships. Here's the reconstructed model:

ICEBERG_SCHEMA.AUDIT_TABLE
- Job ID (text) - Primary_Key
- Job Type (text)
- Status (text)
- Start Time (UTC) (text)
- End Time (UTC) (text)
- Duration (s) (float)
- Details (text)

ICEBERG_SCHEMA.CUST
- INDEX (number)
- CUSTOMER_ID (text) - Primary_Key
- FIRST_NAME (text)
- LAST_NAME (text)
- COMPANY (text)
- CITY (text)
- COUNTRY (text)
- PHONE_1 (text)
- PHONE_2 (text)
- EMAIL (text)
- SUBSCRIPTION (text)
- WEBSITE (text)
- LOAD_DATE (text)

ICEBERG_SCHEMA.CUSTOMERS
- CUSTOMER_ID (number) - Primary_Key
- NAME (text)
- EMAIL (text)
- PHONE (text)
- ADDRESS (text)
- LOAD_DATE (text)

ICEBERG_SCHEMA.CUST_DATA
(Same structure as CUST table, appears to be a duplicate)
- CUSTOMER_ID (text) - Primary_Key

ICEBERG_SCHEMA.MINING_DATA
- DATE (text) - Primary_Key
(remaining columns omitted for brevity but included in actual table)

ICEBERG_SCHEMA.ORDERS
- ORDER_ID (number) - Primary_Key
- CUSTOMER_ID (number) - Foreign_Key references CUSTOMERS(CUSTOMER_ID)
- ORDER_DATE (text)
- TOTAL_AMOUNT (float)
- STATUS (text)
- LOAD_DATE (text)

ICEBERG_SCHEMA.PRODUCTIVITY_DATA
- LOAD_DATE (text) - Primary_Key
(remaining columns are metrics, no additional keys)

ICEBERG_SCHEMA.PRODUCTS
- PRODUCT_ID (number) - Primary_Key
- PRODUCT_NAME (text)
- CATEGORY (text)
- PRICE (float)
- STOCK_QUANTITY (number)
- LOAD_DATE (text)

ICEBERG_SCHEMA.SALES_ICEBERG_TABLE
- ORDER ID (number) - Primary_Key
(remaining columns are attributes, no additional keys)

ICEBERG_SCHEMA.SHIPMENTS
- SHIPMENT_ID (number) - Primary_Key
- ORDER_ID (number) - Foreign_Key references ORDERS(ORDER_ID)
- SHIPMENT_DATE (text)
- CARRIER (text)
- TRACKING_NUMBER (text)
- LOAD_DATE (text)

ICEBERG_SCHEMA.SUPPLIERS
- SUPPLIER_ID (number) - Primary_Key
- SUPPLIER_NAME (text)
- CONTACT_NAME (text)
- PHONE (text)
- EMAIL (text)
- LOAD_DATE (text)

ICEBERG_SCHEMA.TUBERCULOSIS_DATA
- Composite_Primary_Key (COUNTRY_OR_TERRITORY_NAME, YEAR)
- COUNTRY_OR_TERRITORY_NAME (text)
- YEAR (number)
(remaining columns omitted for brevity but included in actual table)

Notes:
1. The CUST and CUST_DATA tables appear to be duplicates with the same structure
2. Some tables use natural keys while others use surrogate keys
3. The relationships between ORDERS-CUSTOMERS and SHIPMENTS-ORDERS are clearly defined through foreign keys
4. TUBERCULOSIS_DATA uses a composite key of country and year to uniquely identify records
5. MINING_DATA uses DATE as its primary key assuming measurements are taken once per day
6. PRODUCTIVITY_DATA uses LOAD_DATE as its primary key assuming one record per day