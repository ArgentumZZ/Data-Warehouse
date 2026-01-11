# 1. Initialize an empty sql_queries dict
sql_queries = {}

# 2. Create a source_cols_create variable with columns and data types
source_columns_create = '''
            id              TEXT,
            type            TEXT,
            attributes	    TEXT,
            relationships	TEXT,
            links           TEXT,    
    '''

# 3. Create a source_cols_unique variable with unique columns
source_columns_unique = '''id, type, attributes, relationships,links'''

# 4. Create table query
sql_queries['create_table'] = '''
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
        {table}_key                     bigserial,
        -- etl_runs_key                    bigint,
        ''' + source_columns_create + '''
        created_at                      timestamptz default current_timestamp,
        modified_at                     timestamptz default current_timestamp,
        unique(''' + source_columns_unique + ''')
        );

        CREATE INDEX ON {schema}.{table}(''' + source_columns_unique + ''');
    '''

# 5. GET data query
sql_queries['get_data'] = '''
                          SELECT id, 
                                 fxcm_customer_id, 
                                 account_id, 
                                 account_number, 
                                 notification, 
                                 notification_read, 
                                 timestamp, 
                                 notification_type
                          FROM notifications
                          WHERE (timestamp >= '{sdt}' AND timestamp <= '{edt}')
                          '''

# 6. Create comments query
sql_queries['create_comments'] = '''
        COMMENT ON TABLE {schema}.{table} IS 'Table sourced from ...';
        COMMENT ON COLUMN {schema}.{table}.{table}_key IS 'Serial key generated for each record in the table.';
        COMMENT ON COLUMN {schema}.{table}.etl_runs_key IS 'Serial key of the ETL run.';
        '''

# 7. MERGE INTO syntax
# 7.1. Construct MERGE ON clause using target (t) and source (s) unique columns
# Later need to add "etl_runs_key = excluded.etl_runs_key, "
sql_queries['on_clause'] = " AND ".join([f"t.{col.strip()} = s.{col.strip()}" for col in source_columns_unique.split(',')])

# 7.2. UPDATE CLAUSE for MERGE INTO query
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
# Later need to add "target.etl_runs_key = source.etl_runs_key, "
sql_queries['update_clause'] = (
    ", ".join([f"{col} = s.{col}" for col in normal_cols_list])  # Update normal columns
    + " modified_at = CURRENT_TIMESTAMP"                        # Always update timestamp
)


# 7.3. INSERT for MERGE INTO
# Extract normal columns from CREATE TABLE, skip comments
cols = [line.split()[0] for line in source_columns_create.strip().split('\n') if line.strip() and not line.strip().startswith('--')]

# Add timestamps
cols += ['created_at', 'modified_at']

# INSERT columns (target)
sql_queries['insert_columns'] = ', '.join(cols)

# INSERT values (source + current timestamps)
sql_queries['insert_values'] = ', '.join([f"s.{c}" for c in cols[:-2]] + ['current_timestamp', 'current_timestamp'])