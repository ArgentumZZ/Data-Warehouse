# import libraries
import psycopg2, io, csv
import pandas as pd
from io import StringIO
from psycopg2 import DatabaseError
from psycopg2.extensions import connection as PGConnection
import configparser, os
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import utilities.logging_manager as lg

class PostgresConnector:
    """
    PostgreSQL Connector for managing connections, executing queries,
    and performing common ETL tasks.

    Features:
    ---------
    1. Config Loader: Read DB credentials from .cfg/.ini files.
    2. Connection Factory: Return psycopg2 connection objects.
    3. Query Execution: Run SQL with optional results and commit.
    4. DDL Helpers: Create schemas and tables if not exists.
    5. DML / ETL Helpers: Upload CSVs via temporary tables and merge into targets.
    """

    def __init__(self,
                 credential_name: str) -> None:
        self.credential_name = credential_name


    # ---------------------------------------------------------
    # CONFIG LOADER
    # ---------------------------------------------------------
    def load_db_config(self,
                       credential_name: str) -> Dict[str, str]:
        """
        Load a specific credential name from .cfg file.

        Args:
            cfg_path: Path to the configuration file (e.g. "config/local/db.cfg").
            credential_name: The credential_name name inside the config file that contains the PostgreSQL credentials
            (e.g. "postgresql_prod").

        Returns: A dictionary containing the key/value pairs from the credential_name

        Raises:
            FileNotFoundError:
                If the config file does not exist.
            ValueError:
                If the credential_name does not exist or is empty.

        Example config file:

            [postgres_prod]
            host=localhost
            port=5432
            database=mydb
            user=myuser
            password=mypass
        """

        # 1. Ensure the config file exists before reading
        # os.environ['CONFIG_DIR'] defined in setenv.bat
        cfg_path = os.path.join(os.environ['CONFIG_DIR'])
        if not os.path.isfile(cfg_path):
            raise FileNotFoundError(f"Config file not found: {cfg_path}.")

        parser = configparser.ConfigParser()

        # 2. Read the file; unreadable files result in an empty read list
        if not parser.read(cfg_path):
            raise ValueError(f"Config file '{cfg_path}' could not be read.")

        # 3. Ensure the requested credential_name exists
        if credential_name not in parser:
            raise ValueError(f"Credential: {credential_name} not found in config file!")

        # 4. Convert the credential_name into a Python dictionary
        cfg = dict(parser[credential_name])
        return cfg

    # ---------------------------------------------------------
    # CONNECTION FACTORY
    # ---------------------------------------------------------
    def get_connection(self, cfg: Dict[str, str]) -> PGConnection:
        """
        Create and return a new PostgreSQL connection.

        Args:
            cfg:
                Dictionary containing connection parameters
                Expected keys: host, port, database, user, password
                Optional keys: sslmode, connection_timeout.

        Returns:
            A psycopg2 connection object.

        Raises:
            KeyError:
                If required connection keys are missing.
            psycopg2.Error:
                If the connection attempt fails.
        """

        # 1. Validate required keys exist
        required = ['host', 'port', 'database', 'user', 'password']
        for key in required:
            if key not in cfg:
                raise KeyError(f"Missing required DB config key: '{key}'.")

        # 2. Optional parameters (safe defaults)
        sslmode = cfg.get('sslmode')             # e.g. 'require', 'disable'
        timeout = cfg.get('connection_timeout')  # seconds

        # 3. Create the connection
        return psycopg2.connect(
            host        = cfg['host'],
            port        = cfg['port'],
            database    = cfg['database'],
            user        = cfg['user'],
            password    = cfg['password']
        )

    # ---------------------------------------------------------
    # EXECUTE QUERY
    # ---------------------------------------------------------
    def run_query(self,
                  query: str,
                  params: Optional[Tuple[Any, ...]] = None,
                  get_result: bool = True,
                  commit: bool = False) -> pd.DataFrame:
        """
        Execute a SQL statement with optional result fetching and optional commit.

        Args:
            query: SQL query to execute.
            params: Parameters for the SQL query
            get_result: If True, fetch and return rows (empty if none).
            commit: If True, commit the transaction after execution.

        Returns: A list of rows if get_result=True and the query returns data, otherwise an empty list.

        COMMIT RULES (PostgreSQL):

        CREATE SCHEMA
        ✅ COMMIT
        Creates database structure
        Rolled back otherwise

        CREATE TABLE / ALTER TABLE / DROP TABLE
        ✅ COMMIT
        Modifies schema
        Rolled back otherwise

        INSERT
        ✅ COMMIT
        Adds new rows
        Rolled back otherwise

        UPDATE
        ✅ COMMIT
        Modifies existing rows
        Rolled back otherwise

        DELETE
        ✅ COMMIT
        Removes rows
        Rolled back otherwise

        SELECT
        ❌ DO NOT COMMIT
        Read-only
        Commit has no effect
        """

        # 1. Open a new PostgreSQL database connection.
        try:
            with self.get_connection(self.load_db_config(self.credential_name)) as conn:

                # 1.1. Create a cursor for executing SQL
                with conn.cursor() as cur:
                    # 1.2. Execute the SQL statement
                    cur.execute(query, params)

                    # 1.3. Commit only when explicitly requested
                    if commit:
                        conn.commit()

                    # 1.4. Warn if a write query was executed without commit
                    if not commit and cur.description is None:
                        lg.warning("Write/DDL query executed without commit")

                    # 1.5. Fetch rows only when requested and when query returns data
                    # Detect if results exist before fetching
                    if get_result and cur.description:
                        rows = cur.fetchall()
                        col_names = [desc[0] for desc in cur.description]

                        lg.info(f"Returned {len(rows)} rows.")
                        lg.info(f"The column names: {col_names}")

                        return pd.DataFrame(rows, columns=col_names)

                    return pd.DataFrame()

        except DatabaseError:
            # Catch and log database-related errors (SQL syntax, constraint violations, etc.)
            lg.info("Database error while executing query")
            raise

        except Exception:
            # Catch and log any unexpected non-database errors, then re-raise
            lg.info("Unexpected error while executing query")
            raise

    # ---------------------------------------------------------
    # DDL HELPERS
    # ---------------------------------------------------------
    def create_schema(self, schema: str) -> None:
        """
        Create a PostgreSQL schema if it does not already exist.

        Args:
            schema: Name of the schema to create (e.g. 'shift', 'figment')
        """

        # 1. Idempotent: does nothing if schema already exists
        query = f"CREATE SCHEMA IF NOT EXISTS {schema};"

        # 2. Execute and commit
        lg.info("Creating schema if not exists: %s", schema)
        self.run_query(query=query, commit=True)

    def create_table(self, query: str) -> None:
        """
        Create a PostgreSQL table if it does not already exist (in the format <schema>.<table>).

        Args:
            query: CREATE TABLE IF NOT EXISTS {schema}.{table} (...)
            schema: Name of the schema (e.g. 'shift', 'figment')
            table: Name of the table
        """

        # 1. Idempotent: does nothing if the <schema>.<table> already exists
        # The input query has format query = CREATE TABLE IF NOT EXISTS {schema}.{table} ()
        # query = query.format(schema=schema, table=table)

        # 2. Execute and commit
        # lg.info(f"Creating table if not exists: {schema}.{table}")
        self.run_query(query=query, commit=True)

    def init_schema_and_table(self, query: str, schema: str, table: str) -> None:
        # 1. Create schema
        self.create_schema(schema=schema)

        # 2. Create table (parametrize the query from sql_queries.py)
        self.create_table(query=query.format(schema=schema, table=table))

    # ---------------------------------------------------------
    # UPLOAD TO DB (using a TEMPORARY TABLE + MERGE INTO LOGIC)
    # ---------------------------------------------------------
    def upload_to_pg(self,
                    file_path: str,
                    schema: str,
                    table: str,
                    on_clause: str = '',
                    update_clause: str = '',
                    insert_columns: str = '',
                    insert_values: str = '') -> None:
        """
        Upload a CSV to PostgreSQL by:

        1. Creating a temporary table (temp_<table>).
        2. Loading the CSV into the temporary table.
        3. MERGE INTO target table for updates/inserts.
        4. DROP the temporary table.

        Args:
            file_path: Path to the CSV file
            schema: Target schema
            table: Target table name
            on_clause: SQL ON condition for MERGE (attributes from the unique constraint)
            update_clause: SQL SET clause for updates (non-unique attributes)
            insert_columns: Columns for INSERT (list all attributes)
            insert_values: Values for INSERT (list all attributes coming from the source)
        """

        # 1. Open a database connection (the temporary table will live in this session)
        with self.get_connection(self.load_db_config(self.credential_name)) as conn:
            with conn.cursor() as cur:

                # 2. Create a temporary table
                temp_table = f"temp_{table}"
                lg.info(f"Temporary table: {temp_table}")

                create_temp_query = f"""
                CREATE TEMP TABLE {temp_table} 
                (LIKE {schema}.{table} INCLUDING CONSTRAINTS INCLUDING INDEXES INCLUDING DEFAULTS); 
                """

                lg.info(f"Executing create temporary query: {create_temp_query}")
                cur.execute(create_temp_query)

                # 3. Read the CSV (Pandas automatically detects the columns from the header row)
                df = pd.read_csv(file_path, sep=';')

                # 4. Get the columns that actually exist in the CSV
                df_columns = df.columns.tolist()
                lg.info(f"The dataframe columns: {df_columns}")

                # 5. Use an in-memory buffer to stream the data to Postgres
                # This is much faster than running individual INSERT statements
                buffer = io.StringIO()
                df.to_csv(buffer,
                          index=False,
                          header=False,
                          sep=';',
                          quoting=csv.QUOTE_NONE,
                          escapechar='\\')
                buffer.seek(0)  # Go back to the start of the virtual file

                # 6. Execute the copy
                lg.info(f"Loading {len(df)} rows into {temp_table}...")
                try:
                    cur.copy_from(buffer, temp_table, sep=';', columns=df_columns)
                    lg.info("Load successful.")
                except Exception as e:
                    lg.error(f"Failed to load CSV via Pandas buffer: {e}")
                    raise

                # 7. MERGE INTO target table
                merge_query = f"""
                    MERGE INTO {schema}.{table} AS t
                    USING {temp_table} AS s
                        ON {on_clause}
                    WHEN MATCHED THEN 
                        UPDATE SET {update_clause}
                    WHEN NOT MATCHED THEN
                        INSERT ({insert_columns})
                        VALUES ({insert_values});
                    """
                lg.info("Merging temp table into target table.")
                cur.execute(merge_query)

                # 8. Explicitly drop the temporary table
                lg.info(f"Dropping temporary table {temp_table}")
                drop_query = f"DROP TABLE {temp_table}"
                cur.execute(drop_query)

            # 9. Commit once at the end
            conn.commit()