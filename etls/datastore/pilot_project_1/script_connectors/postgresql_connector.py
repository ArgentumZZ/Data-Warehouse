# import libraries
import psycopg2
from psycopg2.extensions import connection as PGConnection
import configparser, os
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

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
            cfg_path:
                Path to the configuration file (e.g. "config/local/db.cfg").
            section:
                The section name inside the config file that contains the
                PostgreSQL credentials (e.g. "postgresql_prod").

        Returns:
            A dictionary containing the key/value pairs from the section

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
                  get_result: bool = True,
                  commit: bool = False) -> List[Any]:
        """ Execute a SQL statement with optional result fetching and optional commit.

        Args:
            sql:
                SQL query to execute.
            get_result:
                If True, fetch and return rows.
            commit:
                If True, commit the transaction after execution.

        Returns: A list of rows if get_result=True and the query returns data, otherwise an empty list. """

        # 1. Open a new database connection.

        with self.get_connection(self.db_config) as conn:
            try:
                # Create a cursor for executing SQL
                with conn.cursor() as cur:
                    # Execute the SQL statement
                    cur.execute(query)

                    # Commit only when explicitly requested
                    if commit:
                        conn.commit()

                    # Fetch rows only when requested and when querty returns data
                    if get_result:
                        return cur.fetchall()

                    return []

            except Exception:
                # Roll back the transaction on any failure
                conn.rollback()
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


