-- Sample 1: Source-to-Target Reconciliation Job
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
    'HIVE', 'sales_recon.sales_data_target', 'SALE_ID,PROD_ID',
    'Y', 'Y', 'Y', 'Y',
    'ORDER_ID:SALE_ID,PRODUCT_ID:PROD_ID,SALE_AMOUNT:REVENUE,SALE_DATE:TXN_DATE',
    'user1@example.com,user2@example.com', 'Sales Reconciliation Report',
    'sales_recon.audit_log',
    'Y'
);

-- Sample 2: Source-to-SQL Business Rule Validation Job
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
    'SELECT PRODUCT_ID, STOCK_LEVEL, (LAST_MONTH_STOCK - CURRENT_STOCK) AS DERIVED_SALES FROM inventory.products_monthly_snapshot',
    'Y', 'Y',
    'STOCK_LEVEL:absolute:10',
    'inventory_manager@example.com', 'Inventory Validation Report',
    'inventory.audit_log',
    'Y'
);
