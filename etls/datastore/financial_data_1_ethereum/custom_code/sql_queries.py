# 1. Initialize an empty sql_queries dictionary
sql_queries = {}

# 2. Create a source_cols_create variable with columns and data types
source_columns_create = '''
            id                      TEXT, 
            raw_number_value        BIGINT,
            tax_hash                TEXT NOT NULL,
            ethereum_amount         NUMERIC,
            transaction_index       INT, 
            gas_limit               NUMERIC,
            sender_address          TEXT,
            contract_address        TEXT, 
            input_data              TEXT,
            metadata                JSONB,
            event_date              DATE,
            is_valid                BOOLEAN,
            raw_id_str              TEXT,             
            raw_value_float         TEXT,             
            dirty_text              TEXT,            
            unique_key_test         TEXT,            
            required_field          TEXT,
            confirmed_at            TIMESTAMPTZ,
            source_created_at       TIMESTAMPTZ,
            source_updated_at       TIMESTAMPTZ,    
    '''

# 3. Create a source_cols_unique variable with unique columns
source_columns_unique = '''tax_hash'''

# 4. Define a create_table query
sql_queries['create_table'] = '''
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
        etl_runs_key                    BIGINT,
        {table}_key                     BIGSERIAL PRIMARY KEY,
        ''' + source_columns_create + '''
        created_at                      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        modified_at                     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(''' + source_columns_unique + ''')
        );

        CREATE INDEX ON {schema}.{table}(''' + source_columns_unique + ''');
    '''

# 5. Create a get_data query
sql_queries['get_data'] = '''
                          SELECT id, 
                                BLCK_NBR_raw_VAL, 
                                tx_hash, 
                                eth_amt_001, 
                                source_created_at, 
                                source_updated_at,
                                transaction_index,
                                gas_limit,
                                sender_address,
                                CONTRACT_ADDR_X,
                                input_data,
                                metadata,
                                event_date,
                                confirmed_at,
                                f_is_vld_bool,
                                raw_id_str,
                                raw_value_float,
                                dirty_text,
                                unique_key_test,
                                required_field
                          FROM financial_data.ethereum_data
                          WHERE (source_created_at BETWEEN '{sdt}' AND '{edt}'
                            OR source_updated_at BETWEEN '{sdt}' AND '{edt}');
                          '''

# 6. Create a set_comments query
sql_queries['set_comments'] = '''
        COMMENT ON TABLE {schema}.{table} IS 'Table sourced from {schema}.';
        COMMENT ON COLUMN {schema}.{table}.{table}_key IS 'Serial key generated for each record in the table.';
        COMMENT ON COLUMN {schema}.{table}.etl_runs_key IS 'Serial key of the ETL run.';
        '''

# 6.1. Use this to read the table comment
"""
SELECT obj_description('financial_data.ethereum'::regclass, 'pg_class') AS table_comment;
"""

# 6.2. Use this to read column comments
"""SELECT
    a.attname AS column_name,
    d.description AS column_comment
FROM pg_attribute AS a
LEFT JOIN pg_description AS d
    ON d.objoid = a.attrelid AND d.objsubid = a.attnum
WHERE a.attrelid = 'financial_data.ethereum'::regclass AND a.attnum > 0;"""

# 7. Previous data max date query
sql_queries['prev_max_date_query'] = """SELECT data_max_date
                                         FROM audit.etl_runs
                                         WHERE etl_runs_key=(SELECT max(etl_runs_key) 
                                                             FROM audit.etl_runs 
                                                             WHERE target_table='{schema}.{table}'
                                                             AND status='Complete')
                                                       """


# 8. MERGE INTO syntax for on_clause, update_clause, insert_columns and insert_values

# 8.1. Construct ON_CLAUSE using target (t) and source (s) UNIQUE columns
sql_queries['on_clause'] = " AND ".join([f"t.{col.strip()} = s.{col.strip()}" for col in source_columns_unique.split(',')])

# 8.2. Construct the UPDATE_CLAUSE

# Extract all column names from source_cols_create
source_cols_general_list = [
    line.split()[0]                                            # Take the first word as column name
    for line in source_columns_create.strip().split('\n')      # Remove whitespace and split into lines
    if line.strip() and not line.strip().startswith('--')      # Skip empty lines & comments
]

# Identify non-unique columns
normal_cols_list = [col for col in source_cols_general_list
    if col not in [u.strip() for u in source_columns_unique.split(',')]
]

# Assemble the update_clause
update_parts = ["etl_runs_key = s.etl_runs_key"] + \
               [f"{col} = s.{col}" for col in normal_cols_list] + \
               ["modified_at = CURRENT_TIMESTAMP"]

sql_queries['update_clause'] = ", ".join(update_parts)

# 8.3. Construct the INSERT_COLUMNS and INSERT_VALUES for MERGE INTO

# Extract normal columns from CREATE TABLE, skip comments
cols = [line.split()[0] for line in source_columns_create.strip().split('\n') if line.strip() and not line.strip().startswith('--')]

# Add etl_runs_key, the source columns, created_at and modified_at in the column list
sql_queries['insert_columns'] = 'etl_runs_key, ' + ', '.join(cols) + ', created_at, modified_at'

# Add etl_runs_key value, the source values, and two current_timestamps (for created_at and modified_at)
sql_queries['insert_values'] = 's.etl_runs_key, ' + ', '.join([f"s.{c}" for c in cols]) + ', current_timestamp, current_timestamp'