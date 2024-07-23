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

    def calculate_metadata_hash(self, metadata_file_json: dict) -> str:
        """Calculates a SHA256 hash of the given metadata JSON."""
        return hashlib.sha256(json.dumps(metadata_file_json).encode("utf-8")).hexdigest()

    def check_metadata_exists_by_hash(self, json_hash: str) -> bool:
        """Checks if metadata exists based on the given hash."""
        cursor = self.conn.cursor()
        check_query = f"SELECT EXISTS(SELECT 1 FROM {POSTGRESQL_TABLE_NAME} WHERE json_hash = %s)"
        cursor.execute(check_query, (json_hash,))
        exists = cursor.fetchone()[0]
        cursor.close()
        return exists

    def check_metadata_exists_by_execution_block(self, execution_block: str) -> int:
        """Checks if metadata exists based on the given execution block."""
        cursor = self.conn.cursor()
        check_query = f"SELECT id FROM {POSTGRESQL_TABLE_NAME} WHERE execution_block = %s"
        cursor.execute(check_query, (execution_block,))
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else None

    def update_metadata(self, metadata_file_json: dict, id_field: int) -> None:
        """Updates existing metadata with the given data and hash."""
        json_hash = self.calculate_metadata_hash(metadata_file_json)
        cursor = self.conn.cursor()
        update_query = (
            f"UPDATE {POSTGRESQL_TABLE_NAME} SET data = %s, json_hash = %s WHERE id = %s"
        )
        cursor.execute(update_query, (metadata_file_json, json_hash, id_field))
        self.conn.commit()
        cursor.close()

    def insert_metadata(self, metadata_file_json: dict, execution_block: str) -> None:
        """Inserts new metadata into the database."""
        json_hash = self.calculate_metadata_hash(metadata_file_json)
        cursor = self.conn.cursor()
        insert_query = f"INSERT INTO {POSTGRESQL_TABLE_NAME} (data, json_hash, execution_block) \
VALUES (%s, %s, %s)"
        cursor.execute(insert_query, (metadata_file_json, json_hash, execution_block))
        self.conn.commit()
        cursor.close()

    def save_metadata_to_postgresql(self, metadata_file_json: dict) -> None:
        """Saves metadata to PostgreSQL."""
        try:
            json_hash = self.calculate_metadata_hash(metadata_file_json)
            metadata_file_dict = json.loads(metadata_file_json)
            execution_block = metadata_file_dict["execution_block"]

            if self.check_metadata_exists_by_hash(json_hash):
                logger.info("Metadata with hash %s already exists.", json_hash)
                return

            metadata_id = self.check_metadata_exists_by_execution_block(execution_block)
            if metadata_id:
                self.update_metadata(metadata_file_json, metadata_id)
                logger.info("Updated metadata with execution_block %s", execution_block)
            else:
                self.insert_metadata(metadata_file_json, execution_block)
                logger.info("Inserted new metadata with execution_block %s", execution_block)

        except psycopg.Error as error:
            logger.error("Error saving metadata to PostgreSQL: %s", error)
            raise
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error("Error saving metadata to PostgreSQL: %s", exception)

    def count_jsonb_objects(self):
        """Counts the number of JSON objects within a JSONB column.

        Returns:
            The total count of JSON objects.
        """

        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {POSTGRESQL_TABLE_NAME}")
        result = cursor.fetchone()[0]
        cursor.close()
        logger.info("Number of items in the DB: %s", result)
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
            cursor = self.conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {POSTGRESQL_TABLE_NAME}")
            self.conn.commit()
            cursor.close()
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
