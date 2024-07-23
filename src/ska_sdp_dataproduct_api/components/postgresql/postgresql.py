"""Module adds a PostgreSQL interface for persistent storage of metadata files"""

import hashlib
import json
import logging

import psycopg

from ska_sdp_dataproduct_api.configuration.settings import (
    POSTGRESQL_HOST,
    POSTGRESQL_PASSWORD,
    POSTGRESQL_PORT,
    POSTGRESQL_TABLE_NAME,
    POSTGRESQL_USER,
)

logger = logging.getLogger(__name__)


class PostgresConnector:  # pylint: disable=too-many-instance-attributes
    """
    A class to connect to a PostgreSQL instance and test its availability.
    A class to connect to a PostgreSQL instance and t
    """

    def __init__(self):
        self.host: str = POSTGRESQL_HOST
        self.port: int = POSTGRESQL_PORT
        self.user: str = POSTGRESQL_USER
        self.password: str = POSTGRESQL_PASSWORD
        self.conn = None
        self.postgresql_running: bool = False
        self.postgresql_version: str = ""
        self.connection_established_at = ""
        self.connection_error = ""

        self.connect()
        self.create_metadata_table()

    def status(self) -> dict:
        """
        Retrieves the current status of the PostgreSQL connection.

        This method returns a dictionary containing the following information:

        * `host`: The hostname or IP address of the PostgreSQL instance.
        * `port`: The port number on which PostgreSQL is listening.
        * `database`: The name of the database being connected to.
        * `user`: The username used for authentication with PostgreSQL.
        * `running` (optional): A boolean indicating whether PostgreSQL is currently running
        (if available).
        * `postgresql_version` (optional): The version of PostgreSQL being used (if applicable).
        * `connection_established_at` (optional): A timestamp representing when a connection was
        last established with PostgreSQL. This value might not be available depending on the
        database library used.
        * `connection_error` (optional): An error object containing details about any connection
        errors that occurred (if applicable).

        Returns:
            A dictionary containing the current status information.
        """

        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "running": self.postgresql_running,
            "postgresql_version": self.postgresql_version,  # Optional
            "connection_error": self.connection_error,  # Optional
        }

    def connect(self) -> None:
        """
        Attempts to connect to the PostgreSQL instance.

        Returns:
          None.
        """
        try:
            self.conn = psycopg.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
            )
            self.postgresql_running = True

            # Attempt to retrieve PostgreSQL version if connected
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT version()")
                self.postgresql_version = cursor.fetchone()[0]

        except (Exception, psycopg.Error):  # pylint: disable=broad-exception-caught
            self.conn = None
            self.postgresql_running = False
            logger.error("PostgreSQL connection to %s:%s failed", str(self.host), str(self.port))

    def disconnect(self) -> None:
        """
        Closes the connection to the PostgreSQL instance if it's open.
        """
        if self.conn:
            self.conn.close()
            self.conn = None

    def close(self):
        """
        Closes the connection to the PostgreSQL instance if open.
        """
        if self.conn:
            self.conn.close()
            self.conn = None

    def create_metadata_table(self) -> None:
        """Creates the metadata table named as defined in the env variable POSTGRESQL_TABLE_NAME
        if it doesn't exist.

        Raises:
            psycopg.Error: If there's an error executing the SQL query.
        """

        try:
            logger.info("Creating PostgreSQL metadata table: %s", POSTGRESQL_TABLE_NAME)

            cursor = self.conn.cursor()
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {POSTGRESQL_TABLE_NAME} (
                    id SERIAL PRIMARY KEY,
                    data JSONB NOT NULL,
                    execution_block VARCHAR(255) DEFAULT NULL,
                    json_hash CHAR(64) UNIQUE
                );
            """
            cursor.execute(create_table_query)
            cursor.close()
            logger.info("PostgreSQL metadata table %s created.", POSTGRESQL_TABLE_NAME)

        except psycopg.Error as error:
            logger.error("Error creating metadata table: %s", error)
            raise

    def save_metadata_to_postgresql(self, metadata_file_json: dict) -> None:
        """
        Saves a Python metadata object to a PostgreSQL database, prioritizing hash-based
        uniqueness, then falling back to execution_block for updates.

        Args:
            conn: A psycopg2 connection object.
            metadata_file_json (dict): The metadata object to be saved.

        Raises:
            psycopg2.Error: If there's an error executing the SQL query.
        """

        try:
            cursor = self.conn.cursor()

            # Calculate a hash of the JSON data for uniqueness
            json_hash = hashlib.sha256(json.dumps(metadata_file_json).encode("utf-8")).hexdigest()
            metadata_file_dict = json.loads(metadata_file_json)
            execution_block = metadata_file_dict["execution_block"]

            # Check if the metadata already exists based on the hash
            check_query = (
                f"SELECT EXISTS(SELECT 1 FROM {POSTGRESQL_TABLE_NAME} WHERE json_hash = %s)"
            )
            cursor.execute(check_query, (json_hash,))
            hash_exists = cursor.fetchone()[0]

            if hash_exists:
                # Metadata already exists based on hash
                print(f"Metadata with hash {json_hash} already exists.")
                return

            # Check if the metadata exists based on execution_block
            check_query = f"SELECT id FROM {POSTGRESQL_TABLE_NAME} WHERE execution_block = %s"
            cursor.execute(check_query, (execution_block,))
            result = cursor.fetchone()

            if result:
                # Update the existing record
                update_query = (
                    f"UPDATE {POSTGRESQL_TABLE_NAME} SET data = %s, json_hash = %s WHERE id = %s"
                )
                cursor.execute(update_query, (metadata_file_json, json_hash, result[0]))
                print(f"Updated metadata with execution_block {execution_block}")
            else:
                # Insert a new record
                insert_query = f"INSERT INTO {POSTGRESQL_TABLE_NAME} \
(data, json_hash, execution_block) VALUES (%s, %s, %s)"
                cursor.execute(insert_query, (metadata_file_json, json_hash, execution_block))
                print(f"Inserted new metadata with execution_block {execution_block}")

            self.conn.commit()
            cursor.close()

        except psycopg.Error as error:
            print(f"Error saving metadata to PostgreSQL: {error}")
            raise
        except Exception as exception:  # pylint: disable=broad-exception-caught
            print(f"Error saving metadata to PostgreSQL: {exception}")

    def count_jsonb_objects(self):
        """Counts the number of JSON objects within a JSONB column.

        Returns:
            The total count of JSON objects.
        """

        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {POSTGRESQL_TABLE_NAME}")
        result = cursor.fetchone()[0]
        cursor.close()
        print(f"Number of items in the DB: '{result}'")
        return result

    def delete_postgres_table(self) -> bool:
        """Deletes a table from a PostgreSQL database.

        Args:
            None

        Returns:
            True if the table was deleted successfully, False otherwise.
        """

        try:
            logger.info("PostgreSQL deleting database table %s", POSTGRESQL_TABLE_NAME)
            cur = self.conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS {POSTGRESQL_TABLE_NAME}")
            self.conn.commit()
            cur.close()
            logger.info("PostgreSQL database table %s deleted.", POSTGRESQL_TABLE_NAME)
            return True

        except psycopg.OperationalError as error:
            logger.error(
                "An error occurred while connecting to the PostgreSQL database: %s", error
            )
            return False
        except psycopg.ProgrammingError as error:
            logger.error("An error occurred while executing the SQL statement: %s", error)
            return False


persistent_metadata_store = PostgresConnector()
