# 1. Initialize an empty sql_queries dict
sql_queries = {}

# 2. Create a source_cols_create variable with columns and data types
source_columns_create = '''
            id                      TEXT NOT NULL, 
            block_number            BIGINT, 
            tax_hash                TEXT, 
            value_ethereum          NUMERIC, 
            source_created_at       TIMESTAMPTZ,
            source_updated_at       TIMESTAMPTZ,    
    '''

# 3. Create a source_cols_unique variable with unique columns
source_columns_unique = '''id'''

# 4. Create table query
sql_queries['create_table'] = '''
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
        {table}_key                     BIGSERIAL PRIMARY KEY,
        etl_runs_key                    BIGINT,
        ''' + source_columns_create + '''
        created_at                      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        modified_at                     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(''' + source_columns_unique + ''')
        );

        CREATE INDEX ON {schema}.{table}(''' + source_columns_unique + ''');
    '''

# 5. GET data query
sql_queries['get_data'] = '''
                          SELECT id, 
                                 block_number, 
                                 tx_hash, 
                                 value_eth, 
                                 source_created_at,
                                 source_updated_at
                          FROM financial_data.ethereum
                          WHERE (source_created_at  BETWEEN '{sdt}' AND '{edt}'
                            OR source_updated_at BETWEEN '{sdt}' AND '{edt}')
                          '''

# 6. Create comments query
sql_queries['set_comments'] = '''
        COMMENT ON TABLE {schema}.{table} IS 'Table sourced from {schema}.';
        COMMENT ON COLUMN {schema}.{table}.{table}_key IS 'Serial key generated for each record in the table.';
        COMMENT ON COLUMN {schema}.{table}.etl_runs_key IS 'Serial key of the ETL run.';
        '''

# Use this to read table comment
"""
SELECT obj_description('financial_data.ethereum'::regclass, 'pg_class') AS table_comment;
"""


# Use this to read column comments
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


# 8. MERGE INTO syntax
# 8.1. Construct MERGE ON clause using target (t) and source (s) unique columns
# Later need to add "etl_runs_key = excluded.etl_runs_key, "
sql_queries['on_clause'] = " AND ".join([f"t.{col.strip()} = s.{col.strip()}" for col in source_columns_unique.split(',')])

# 8.2. UPDATE CLAUSE for MERGE INTO query
# ------------------------------------------------------------
# Extract all column names from source_cols_create
# ------------------------------------------------------------
source_cols_general_list = [
    line.split()[0]                                         # Take the first word as column name
    for line in source_columns_create.strip().split('\n')      # Remove whitespace and split into lines
    if line.strip() and not line.strip().startswith('--')   # Skip empty lines & comments
]

# ------------------------------------------------------------
# Identify "normal" columns (non-unique)
# ------------------------------------------------------------
normal_cols_list = [col for col in source_cols_general_list
    if col not in [u.strip() for u in source_columns_unique.split(',')]
]

# ------------------------------------------------------------
# Construct the UPDATE clause for MERGE
# ------------------------------------------------------------
update_parts = ["etl_runs_key = s.etl_runs_key"] + \
               [f"{col} = s.{col}" for col in normal_cols_list] + \
               ["modified_at = CURRENT_TIMESTAMP(0)"]

sql_queries['update_clause'] = ", ".join(update_parts)

# 8.3. INSERT for MERGE INTO
# Extract normal columns from CREATE TABLE, skip comments
cols = [line.split()[0] for line in source_columns_create.strip().split('\n') if line.strip() and not line.strip().startswith('--')]

# Include etl_runs_key in the column list and the source values
sql_queries['insert_columns'] = 'etl_runs_key, ' + ', '.join(cols) + ', created_at, modified_at'

sql_queries['insert_values'] = 's.etl_runs_key, ' + ', '.join([f"s.{c}" for c in cols]) + ', current_timestamp, current_timestamp'