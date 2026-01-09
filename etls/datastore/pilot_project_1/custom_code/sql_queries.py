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

source_cols_unique = '''id'''

sql_queries['create_table'] = '''
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
        {table}_key                     bigserial,
        etl_runs_key                    bigint,
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

# From table definition:

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

# From query code:
#
# # Find the index of the "select" word
# select_index = sql_queries['get_data'].lower().find("select".lower())
#
# # Remove "select" and everything after it
# if select_index >= 0:
#     query_filtered = sql_queries['get_data'][select_index+6:]
#
# # Find the index of the "from" word
# from_index = query_filtered.lower().find("from".lower())
#
# # Remove "from" and everything after it
# if from_index >= 0:
#     query_filtered = query_filtered[:from_index]
#
# # Strip leading and trailing whitespace from the filtered query
# query_filtered = query_filtered.strip()
#
# # Split the filtered query by commas to create a list of columns
# query_columns_list = query_filtered.split(',')
#
# # Replace each element in the list with its value from the dot position until the end e.g. remove alias
# for i in range(len(query_columns_list)):
#     query_columns_list[i] = query_columns_list[i].split('.')[-1]
#
# # Remove leading and trailing whitespace from each value and create a list of values
# sql_queries['query_columns_list']  = [value.strip() for value in query_columns_list]




sql_queries['on_conflict_action'] = 'ON CONFLICT (' + source_cols_unique + ') DO UPDATE SET '

# Create the SQL that updates the columns on conflict
sql_queries['on_conflict_update'] = "etl_runs_key = excluded.etl_runs_key, " \
                                    + (",".join(
    ["{col} = excluded.{col}".format(col=x) for x in source_cols_general_list])) \
                                    + ", modified_at = current_timestamp"

sql_queries['source_cols_unique'] = source_cols_unique
# Create the formatted list of columns used to trigger a log
sql_queries['column_list_trigger'] = (",".join(["{{type}}.{col}".format(col=x) for x in trigger_cols_general_list]))
