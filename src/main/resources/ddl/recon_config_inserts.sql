-- Sample 1: Source-to-Target Reconciliation Job (Corrected)
INSERT INTO RECON_CONFIG (
    JOB_ID, JOB_NAME, RECON_MODE,
    SOURCE_TYPE, SOURCE_PATH, SOURCE_PRIMARY_KEYS,
    TARGET_TYPE, TARGET_HIVE_TABLE, TARGET_PRIMARY_KEYS,
    DO_COLUMN_COMPARISON, DO_COUNT_RECONCILIATION, DO_EXTRA_MISSING_CHECK, DO_SCHEMA_DRIFT_DETECTION,
    COLUMN_MAPPINGS,
    EMAIL_RECIPIENTS, EMAIL_SUBJECT,
    AUDIT_HIVE_TABLE,
    IS_ACTIVE
) VALUES (
    1, 'SALES_CSV_TO_HIVE', 'SOURCE_TO_TARGET',
    'CSV', '/path/to/source/sales_data.csv', 'ORDER_ID,PRODUCT_ID',
    'HIVE', 'sales_recon.sales_data_target', 'ORDER_ID,PRODUCT_ID', -- Corrected: Use same PK names
    'Y', 'Y', 'Y', 'Y',
    'ORDER_ID:ORDER_ID,PRODUCT_ID:PRODUCT_ID,SALE_AMOUNT:SALE_AMOUNT,SALE_DATE:SALE_DATE', -- Corrected: Use 1-to-1 mapping
    'user1@example.com,user2@example.com', 'Sales Reconciliation Report',
    'sales_recon.audit_log',
    'Y'
);

-- Sample 2: Source-to-SQL Business Rule Validation Job (Corrected)
INSERT INTO RECON_CONFIG (
    JOB_ID, JOB_NAME, RECON_MODE,
    SOURCE_TYPE, SOURCE_HIVE_TABLE, SOURCE_PRIMARY_KEYS,
    RECON_SQL,
    DO_BUSINESS_RULE_VALIDATION, DO_THRESHOLD_VALIDATION,
    THRESHOLD_SETTINGS,
    EMAIL_RECIPIENTS, EMAIL_SUBJECT,
    AUDIT_HIVE_TABLE,
    IS_ACTIVE
) VALUES (
    2, 'INVENTORY_VALIDATION', 'SOURCE_TO_SQL',
    'HIVE', 'inventory.products', 'PRODUCT_ID',
    'SELECT PRODUCT_ID, PRODUCT_NAME, STOCK_LEVEL, LAST_UPDATED FROM inventory.products WHERE STOCK_LEVEL >= 0', -- Corrected: Meaningful business rule validation
    'Y', 'Y',
    'STOCK_LEVEL:absolute:10',
    'inventory_manager@example.com', 'Inventory Validation Report',
    'inventory.audit_log',
    'Y'
);

-- Sample 3: Source-to-Target Threshold Violation Job
INSERT INTO RECON_CONFIG (
    JOB_ID, JOB_NAME, RECON_MODE,
    SOURCE_TYPE, SOURCE_PATH, SOURCE_PRIMARY_KEYS,
    TARGET_TYPE, TARGET_PATH, TARGET_PRIMARY_KEYS,
    DO_THRESHOLD_VALIDATION, THRESHOLD_SETTINGS,
    EMAIL_RECIPIENTS, EMAIL_SUBJECT,
    AUDIT_HIVE_TABLE,
    IS_ACTIVE
) VALUES (
    3, 'INVENTORY_THRESHOLD_CHECK', 'SOURCE_TO_TARGET',
    'CSV', 'src/main/resources/data/inventory_products.csv', 'PRODUCT_ID',
    'CSV', 'src/main/resources/data/inventory_products_previous_day.csv', 'PRODUCT_ID',
    'Y', 'STOCK_LEVEL:absolute:50', -- Set low threshold to trigger violation
    'ops_team@example.com', 'Inventory Threshold Violation Alert',
    'inventory.audit_log',
    'Y'
);

-- Sample 4: Source-to-SQL Threshold Violation Job
INSERT INTO RECON_CONFIG (
    JOB_ID, JOB_NAME, RECON_MODE,
    SOURCE_TYPE, SOURCE_HIVE_TABLE, SOURCE_PRIMARY_KEYS,
    RECON_SQL,
    DO_BUSINESS_RULE_VALIDATION, DO_THRESHOLD_VALIDATION,
    THRESHOLD_SETTINGS,
    EMAIL_RECIPIENTS, EMAIL_SUBJECT,
    AUDIT_HIVE_TABLE,
    IS_ACTIVE
) VALUES (
    4, 'INVENTORY_SQL_THRESHOLD_CHECK', 'SOURCE_TO_SQL',
    'HIVE', 'inventory.products', 'PRODUCT_ID',
    'SELECT PRODUCT_ID, PRODUCT_NAME, STOCK_LEVEL, LAST_UPDATED FROM inventory.products WHERE STOCK_LEVEL > 0',
    'Y', 'Y',
    'STOCK_LEVEL:percentage:5', -- Set low percentage threshold
    'inventory_manager@example.com', 'Inventory SQL Threshold Report',
    'inventory.audit_log',
    'Y'
);
