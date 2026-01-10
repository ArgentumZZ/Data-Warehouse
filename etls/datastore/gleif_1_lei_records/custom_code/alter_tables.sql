/*
The purpose of this file is to track all schema changes made to tables in this project.

RULES:
  1. Do NOT delete or modify existing entries (audit history must remain intact)
  2. Always append new changes at the bottom
  3. Always include a link to the Jira task
  4. Always include the date of the change
  5. Always include the exact SQL script executed
-----------------------------------------------------------


-- =======================================================
-- 1. ADDING OR RENAMING COLUMNS
-- Purpose: Add new fields or rename existing ones
-- =======================================================

-- Add a new column
ALTER TABLE schema_name.table_name
ADD COLUMN new_column_name data_type;

-- Example:
ALTER TABLE staging.stg_orders
ADD COLUMN is_active BOOLEAN;


-- Rename an existing column
ALTER TABLE schema_name.table_name
RENAME COLUMN old_column_name TO new_column_name;

-- Example:
ALTER TABLE staging.stg_orders
RENAME COLUMN orderdate TO order_date;


-- =======================================================
-- 2. ALTERING DATA TYPES OR DROPPING COLUMNS
-- Purpose: Change column types or remove obsolete fields
-- =======================================================

-- Change column data type
ALTER TABLE schema_name.table_name
ALTER COLUMN column_name TYPE new_data_type;

-- Example:
ALTER TABLE staging.stg_orders
ALTER COLUMN amount TYPE NUMERIC(18,2);


-- Drop a column
ALTER TABLE schema_name.table_name
DROP COLUMN column_name;

-- Example:
ALTER TABLE staging.stg_orders
DROP COLUMN legacy_flag;


-- =======================================================
-- 3. ADDING OR DROPPING CONSTRAINTS
-- Purpose: Manage PK, FK, UNIQUE, CHECK constraints
-- =======================================================

-- Drop a constraint
ALTER TABLE schema_name.table_name
DROP CONSTRAINT constraint_name;

-- Example:
ALTER TABLE staging.stg_orders
DROP CONSTRAINT stg_orders_amount_check;


-- Add UNIQUE constraint
ALTER TABLE schema_name.table_name
ADD CONSTRAINT constraint_name UNIQUE (column_1, column_2);

-- Example:
ALTER TABLE staging.stg_orders
ADD CONSTRAINT stg_orders_unique UNIQUE (order_id, customer_id);


-- Add PRIMARY KEY
ALTER TABLE schema_name.table_name
ADD CONSTRAINT table_name_pkey PRIMARY KEY (id);

-- Example:
ALTER TABLE dwh.dim_customer
ADD CONSTRAINT dim_customer_pkey PRIMARY KEY (customer_key);


-- Drop PRIMARY KEY
ALTER TABLE schema_name.table_name
DROP CONSTRAINT table_name_pkey;

-- Example:
ALTER TABLE dwh.dim_customer
DROP CONSTRAINT dim_customer_pkey;


-- Add FOREIGN KEY
ALTER TABLE schema_name.table_name
ADD CONSTRAINT fk_name FOREIGN KEY (column_name)
REFERENCES other_schema.other_table (other_column);

-- Example:
ALTER TABLE dwh.fact_orders
ADD CONSTRAINT fact_orders_customer_fk FOREIGN KEY (customer_key)
REFERENCES dwh.dim_customer (customer_key);


-- Drop FOREIGN KEY
ALTER TABLE schema_name.table_name
DROP CONSTRAINT fk_name;

-- Example:
ALTER TABLE dwh.fact_orders
DROP CONSTRAINT fact_orders_customer_fk;


-- Add CHECK constraint
ALTER TABLE schema_name.table_name
ADD CONSTRAINT chk_name CHECK (amount >= 0);

-- Example:
ALTER TABLE staging.stg_orders
ADD CONSTRAINT stg_orders_amount_check CHECK (amount >= 0);


-- Drop CHECK constraint
ALTER TABLE schema_name.table_name
DROP CONSTRAINT chk_name;

-- Example:
ALTER TABLE staging.stg_orders
DROP CONSTRAINT stg_orders_amount_check;


-- =======================================================
-- 4. NULL / NOT NULL CONSTRAINTS
-- Purpose: Enforce or relax nullability rules
-- =======================================================

ALTER TABLE schema_name.table_name
ALTER COLUMN column_name SET NOT NULL;

-- Example:
ALTER TABLE dwh.dim_customer
ALTER COLUMN customer_key SET NOT NULL;


ALTER TABLE schema_name.table_name
ALTER COLUMN column_name DROP NOT NULL;

-- Example:
ALTER TABLE dwh.dim_customer
ALTER COLUMN middle_name DROP NOT NULL;


-- =======================================================
-- 5. DEFAULT VALUES
-- Purpose: Set or remove default values for columns
-- =======================================================

ALTER TABLE schema_name.table_name
ALTER COLUMN column_name SET DEFAULT default_value;

-- Example:
ALTER TABLE staging.stg_orders
ALTER COLUMN is_active SET DEFAULT TRUE;


ALTER TABLE schema_name.table_name
ALTER COLUMN column_name DROP DEFAULT;

-- Example:
ALTER TABLE staging.stg_orders
ALTER COLUMN is_active DROP DEFAULT;


-- =======================================================
-- 6. RENAMING TABLES
-- Purpose: Rename tables during refactoring or standardization
-- =======================================================

ALTER TABLE schema_name.old_table_name
RENAME TO new_table_name;

-- Example:
ALTER TABLE staging.stg_orders_old
RENAME TO stg_orders;


-- =======================================================
-- 7. ALTERING TABLE SCHEMA
-- Purpose: Move tables between schemas
-- =======================================================

ALTER TABLE schema_name.table_name
SET SCHEMA new_schema;

-- Example:
ALTER TABLE staging.stg_orders
SET SCHEMA refined;


-- =======================================================
-- 8. INDEX MANAGEMENT
-- Purpose: Improve performance or remove obsolete indexes
-- =======================================================

-- Create index
CREATE INDEX idx_table_column ON schema_name.table_name (column_name);

-- Example:
CREATE INDEX idx_stg_orders_customer_id ON staging.stg_orders (customer_id);


-- Drop index
DROP INDEX IF EXISTS schema_name.idx_table_column;

-- Example:
DROP INDEX IF EXISTS staging.idx_stg_orders_customer_id;


-- =======================================================
-- 9. IDENTITY / SERIAL COLUMNS
-- Purpose: Add auto-incrementing surrogate keys
-- =======================================================

ALTER TABLE schema_name.table_name
ADD COLUMN id BIGINT GENERATED ALWAYS AS IDENTITY;

-- Example:
ALTER TABLE dwh.dim_product
ADD COLUMN product_key BIGINT GENERATED ALWAYS AS IDENTITY;


-- =======================================================
-- 10. COMMENTS ON TABLES AND COLUMNS
-- Purpose: Document schema directly in PostgreSQL
-- =======================================================

COMMENT ON TABLE schema_name.table_name IS 'Description of the table';

-- Example:
COMMENT ON TABLE dwh.fact_orders IS 'Fact table storing order transactions';


COMMENT ON COLUMN schema_name.table_name.column_name IS 'Description of the column';

-- Example:
COMMENT ON COLUMN dwh.fact_orders.amount IS 'Order amount in EUR';


-- =======================================================
-- 11. TABLE INHERITANCE (PostgreSQL-specific)
-- Purpose: Advanced partitioning or audit table patterns
-- =======================================================

ALTER TABLE child_table INHERIT parent_table;

-- Example:
ALTER TABLE audit.orders_2024 INHERIT audit.orders_base;


ALTER TABLE child_table NO INHERIT parent_table;

-- Example:
ALTER TABLE audit.orders_2024 NO INHERIT audit.orders_base;


-----------------------------------------------------------
-- TEMPLATE FOR NEW CHANGES
-----------------------------------------------------------

-- https://jira.fxcorp/browse/DWH-707
-- 2020-03-25
ALTER TABLE staging.stg_orders
ADD COLUMN is_expiration BOOLEAN;

-----------------------------------------------------------
*/
