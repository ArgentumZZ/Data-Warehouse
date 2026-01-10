# import libraries
import psycopg2
from psycopg2 import DatabaseError
from psycopg2.extensions import connection as PGConnection
import configparser, os
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import utilities.logging_manager as lg

class PostgresConnector:
    """
    Imports & Constants

    Config Loader (reads .cfg or .ini)

    Connection Factory (returns psycopg2/asyncpg connection)

    DDL Helpers (create schema, create table)

    DML Helpers (load temp table, merge into target)

    High‑level ETL Operations (orchestrated steps)

    """

    def __init__(self,
                 cfg_path: str,
                 section: str):
        self.cfg_path = cfg_path
        self.section = section
        # Load DB config and store it for later use
        self.db_config = self.load_db_config(cfg_path, section)

    # ---------------------------------------------------------
    # CONFIG LOADER
    # ---------------------------------------------------------
    def load_db_config(self,
                       cfg_path: str,
                       section: str) -> Dict[str, str]:
        """
        Load a specific credential name from .cfg file.

        Args:
            cfg_path: Path to the configuration file (e.g. "config/local/db.cfg").
            section: The section name inside the config file that contains the PostgreSQL credentials
            (e.g. "postgresql_prod").

        Returns: A dictionary containing the key/value pairs from the section

        Raises:
            FileNotFoundError:
                If the config file does not exist.
            ValueError:
                If the section does not exist or is empty.

        Example config file:

            [postgres_prod]
            host=localhost
            port=5432
            database=mydb
            user=myuser
            password=mypass
        """

        # 1. Ensure the config file exists before reading
        if not os.path.isfile(cfg_path):
            raise FileNotFoundError(f"Config fine not found: {cfg_path}.")

        parser = configparser.ConfigParser()

        # 2. Read the file; unreadable files result in an empty read list
        if not parser.read(cfg_path):
            raise ValueError(f"Config file '{cfg_path}' could not be read.")

        # 3. Ensure the requested section exists
        if section not in parser:
            raise ValueError(f"Credential: {section} not found in config file!")

        # 4. Convert the section into a Python dictionary
        cfg = dict(parser[section])
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
                  commit: bool = False) -> List[Any]:
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
            with self.get_connection(self.db_config) as conn:

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
                        lg.info(f"Returned {len(rows)} rows.")
                        return rows

                    return []

        except DatabaseError:
            # Catch and log database-related errors (SQL syntax, constraint violations, etc.)
            lg.exception("Database error while executing query")
            raise

        except Exception:
            # Catch and log any unexpected non-database errors, then re-raise
            lg.exception("Unexpected error while executing query")
            raise

    # ---------------------------------------------------------
    # DDL HELPERS
    # ---------------------------------------------------------
    def create_schema(self, schema: str) -> None:
        """
        Create a PostgreSQL schema if it does not already exist.

        Args:
            schema:
                Name of the schema to create (e.g. 'shift', 'figment')
        """

        # 1. Idempotent: does nothing if schema already exists
        sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"

        # 2. Execute and commit
        self.run_query(query=sql, commit=True, description=f"Create schema '{schema}'")

    def create_table(self):
        """
        IDEMPOTENT: CREATE TABLE IF NOT EXISTS
        :return:
        """
        pass

    # ---------------------------------------------------------
    # TEMPORARY TABLE + MERGE LOGIC
    # ---------------------------------------------------------
    def create_temporary_table(self):
        """
        CREATE TEMPORARY TABLE
        :return:
        """
        pass

    def load_temporary_table(self):
        """
        Load raw data into a temp table
        :return:
        """
        pass

    def merge_into_target(self):
        """
        Merge into target atomically
        :return:
        """
        merge_sql = f"""
            MERGE INTO {target_table} AS t
            USING {temp_table} AS s
            ON {on_clause}
            WHEN MATCHED THEN
                UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_vals});
            """

        pass

    # ---------------------------------------------------------
    # HIGH-LEVEL OPERATION
    # ---------------------------------------------------------
    def load_data(cfg_path: str, schema: str, table: str, rows: list[tuple]):
        cfg = self.load_db_config(cfg_path)
        conn = get_connection(cfg)

        try:
            create_schema(conn, schema)
            create_table(conn, schema, table)

            temp_table = "tmp_load"
            create_temp_table(conn, temp_table)
            load_temp_table(conn, temp_table, rows)

            merge_into_target(conn, temp_table, f"{schema}.{table}")

        finally:
            conn.close()


