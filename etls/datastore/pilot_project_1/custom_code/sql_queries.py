# 1. Initialize an empty sql_queries dict
# 2. Create a source_cols_create variable with columns and data types
# 3. Create a source_cols_unique variable with unique columns
# 3. sql_queries['get_data'] - SQL query to extract data
# 4. sql_queries['create_comments'] - Add comments for each column
# 5. sql_queries['create_table'] - SQL query to create the time

sql_queries = {}

source_cols_create = '''       
            lei_id          TEXT,
            data_json       TEXT,   
    '''

source_cols_unique = '''lei_id'''

sql_queries['create_table'] = '''
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
        {table}_key                     bigserial,
        -- etl_runs_key                    bigint,
        ''' + source_cols_create + '''
        created_at                      timestamptz default current_timestamp,
        modified_at                     timestamptz default current_timestamp,
        unique(''' + source_cols_unique + ''')
        );

        CREATE INDEX ON {schema}.{table}(''' + source_cols_unique + ''');
    '''

sql_queries['get_data'] = '''
                          SELECT id, \
                                 fxcm_customer_id, \
                                 account_id, \
                                 account_number, \
                                 notification, \
                                 notification_read, timestamp, notification_type

                          FROM notifications
                          WHERE (timestamp >= '{sdt}' \
                            AND timestamp <= '{edt}')

                              {limit} \

                          '''

sql_queries['create_comments'] = '''
        COMMENT ON TABLE {schema}.{table} IS 'Table sourced from EMI';
        COMMENT ON COLUMN {schema}.{table}.{table}_key IS 'Serial key generated for each record in the table.';
        COMMENT ON COLUMN {schema}.{table}.etl_runs_key IS 'Serial key of the ETL run.';
        '''

##############################################################
## This code from here is template and must not be changed! ##
##############################################################
# 1. Construct MERGE ON clause using target (t) and source (s) unique columns
sql_queries['on_clause'] = " AND ".join([f"t.{col.strip()} = s.{col.strip()}" for col in source_cols_unique.split(',')])

# 2. UPDATE CLAUSE for MERGE INTO query
# ------------------------------
# Extract all column names from source_cols_create
# ------------------------------
source_cols_general_list = [
    line.split()[0]                                         # Take the first word as column name
    for line in source_cols_create.strip().split('\n')      # Split into lines
    if line.strip() and not line.strip().startswith('--')   # Skip empty lines & comments
]

# ------------------------------
# Identify "normal" columns (non-unique)
# ------------------------------
normal_cols_list = [col for col in source_cols_general_list
    if col not in [u.strip() for u in source_cols_unique.split(',')]
]

# ------------------------------
# Construct the UPDATE clause for MERGE
# ------------------------------
sql_queries['update_clause'] = (
    ", ".join([f"t.{col} = s.{col}" for col in normal_cols_list])  # Update normal columns
    + ", t.modified_at = CURRENT_TIMESTAMP"                        # Always update timestamp
)


# III. New Snowflake syntax
# Create the SQL that updates the columns on conflict
normal_cols_list = [col for col in source_cols_general_list if col.strip() not in source_cols_unique.split(',')]
sql_queries['snowflake_on_conflict_update'] = (
                                    "target.etl_runs_key = source.etl_runs_key, " +
                                    ",".join([f"target.{col} = source.{col}" for col in normal_cols_list]) +
                                    ", target.modified_at = current_timestamp"
                                    )

# --- Prepare INSERT clause for MERGE ---
# Extract normal columns from CREATE TABLE, skip comments
cols = [line.split()[0] for line in source_cols_create.strip().split('\n') if line.strip() and not line.strip().startswith('--')]

# Add timestamps
cols += ['created_at', 'modified_at']

# INSERT columns (target)
sql_queries['insert_columns'] = ', '.join(cols)

# INSERT values (source + current timestamps)
sql_queries['insert_values'] = ', '.join([f"s.{c}" for c in cols[:-2]] + ['current_timestamp', 'current_timestamp'])



"""# From table definition:

source_cols_trigger_exclude = ''''''

# From table definition:

# Split the string into lines and extract column from each line
source_cols_general_list = [line.split()[0] for line in source_cols_create.strip().split('\n') if line.strip()]
# remove comments
source_cols_general_list = [line for line in source_cols_general_list if not line.startswith('--')]

# Remove unique columns from columns
trigger_cols_general_list = (
[col for col in source_cols_general_list if col.strip() not in source_cols_unique.split(',')])
trigger_cols_general_list = (
[col for col in source_cols_general_list if col.strip() not in source_cols_trigger_exclude.split(',')])



sql_queries['on_conflict_action'] = 'ON CONFLICT (' + source_cols_unique + ') DO UPDATE SET '

# Create the SQL that updates the columns on conflict
sql_queries['on_conflict_update'] = "etl_runs_key = excluded.etl_runs_key, " \
                                    + (",".join(
    ["{col} = excluded.{col}".format(col=x) for x in source_cols_general_list])) \
                                    + ", modified_at = current_timestamp"


# Create the formatted list of columns used to trigger a log
sql_queries['column_list_trigger'] = (",".join(["{{type}}.{col}".format(col=x) for x in trigger_cols_general_list]))
"""