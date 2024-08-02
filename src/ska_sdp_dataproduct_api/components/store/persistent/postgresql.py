"""Module adds a PostgreSQL interface for persistent storage of metadata files"""

import hashlib
import json
import logging
from typing import Any, List

import psycopg
from psycopg.errors import OperationalError
from ska_sdp_dataproduct_metadata import MetaData

from ska_sdp_dataproduct_api.utilities.helperfunctions import (
    FilePaths,
    get_date_from_name,
    get_relative_path,
)

logger = logging.getLogger(__name__)

# pylint: disable=too-many-instance-attributes
# pylint: disable=too-many-arguments


class PostgresConnector:
    """
    A class to connect to a PostgreSQL instance and test its availability.
    """

    def __init__(self, host: str, port: int, user: str, password: str, table_name: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.table_name = table_name
        self.conn = None
        self.postgresql_running: bool = False
        self.postgresql_version: str = ""
        self.number_of_dataproducts: int = 0
        self._connect()
        if self.postgresql_running:
            self.postgresql_version = self._get_postgresql_version()
            self.create_metadata_table()

    def status(self) -> dict:
        """
        Retrieves the current status of the PostgreSQL connection.

        Returns:
            A dictionary containing the current status information.
        """
        return {
            "store_type": "Persistent PosgreSQL metadata store",
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "running": self.postgresql_running,
            "table_name": self.table_name,
            "number_of_dataproducts": self.number_of_dataproducts,
            "postgresql_version": self.postgresql_version,
        }

    def _connect(self) -> None:
        """
        Attempts to connect to the PostgreSQL instance.

        Returns:
          None.
        """
        try:
            self.conn = psycopg.connect(
                host=self.host, port=self.port, user=self.user, password=self.password
            )
            self.postgresql_running = True
            logger.info("Connected to PostgreSQL successfully")
        except OperationalError as error:
            self.postgresql_running = False
            logger.error(
                "An error occurred while connecting to the PostgreSQL database: %s", error
            )

    def _get_postgresql_version(self):
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT version()")
            return cursor.fetchone()[0]

    def create_metadata_table(self) -> None:
        """Creates the metadata table named as defined in the env variable self.table_name
        if it doesn't exist.

        Raises:
            psycopg.Error: If there's an error executing the SQL query.
        """

        try:
            logger.info("Creating PostgreSQL metadata table: %s", self.table_name)

            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    id SERIAL PRIMARY KEY,
                    data JSONB NOT NULL,
                    execution_block VARCHAR(255) DEFAULT NULL UNIQUE,
                    json_hash CHAR(64) UNIQUE
                );
            """
            cursor = self.conn.cursor()
            cursor.execute(create_table_query)
            cursor.close()
            logger.info("PostgreSQL metadata table %s created.", self.table_name)

        except psycopg.Error as error:
            logger.error("Error creating metadata table: %s", error)
            raise

    def calculate_metadata_hash(self, metadata_file_json: dict) -> str:
        """Calculates a SHA256 hash of the given metadata JSON."""
        return hashlib.sha256(json.dumps(metadata_file_json).encode("utf-8")).hexdigest()

    def check_metadata_exists_by_hash(self, json_hash: str) -> bool:
        """Checks if metadata exists based on the given hash."""
        cursor = self.conn.cursor()
        check_query = f"SELECT EXISTS(SELECT 1 FROM {self.table_name} WHERE json_hash = %s)"
        cursor.execute(check_query, (json_hash,))
        exists = cursor.fetchone()[0]
        cursor.close()
        return exists

    def check_metadata_exists_by_execution_block(self, execution_block: str) -> int:
        """Checks if metadata exists based on the given execution block."""
        cursor = self.conn.cursor()
        check_query = f"SELECT id FROM {self.table_name} WHERE execution_block = %s"
        cursor.execute(check_query, (execution_block,))
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else None

    def update_metadata(self, metadata_file_json: str, id_field: int) -> None:
        """Updates existing metadata with the given data and hash."""
        json_hash = self.calculate_metadata_hash(metadata_file_json)
        cursor = self.conn.cursor()
        update_query = f"UPDATE {self.table_name} SET data = %s, json_hash = %s WHERE id = %s"
        cursor.execute(update_query, (metadata_file_json, json_hash, id_field))
        self.conn.commit()
        cursor.close()

    def insert_metadata(self, metadata_file_json: str, execution_block: str) -> None:
        """Inserts new metadata into the database."""
        json_hash = self.calculate_metadata_hash(metadata_file_json)
        cursor = self.conn.cursor()
        insert_query = f"INSERT INTO {self.table_name} (data, json_hash, execution_block) \
VALUES (%s, %s, %s)"
        cursor.execute(insert_query, (metadata_file_json, json_hash, execution_block))
        self.conn.commit()
        cursor.close()

    def save_metadata_to_postgresql(self, metadata_file_dict: dict) -> None:
        """Saves metadata to PostgreSQL."""
        try:
            if not self.postgresql_running:
                logger.error("Error saving metadata to PostgreSQL, instance not available")
                return

            json_hash = self.calculate_metadata_hash(metadata_file_dict)
            execution_block = metadata_file_dict["execution_block"]

            if self.check_metadata_exists_by_hash(json_hash):
                logger.info("Metadata with hash %s already exists.", json_hash)
                return

            metadata_id = self.check_metadata_exists_by_execution_block(execution_block)
            if metadata_id:
                self.update_metadata(json.dumps(metadata_file_dict), metadata_id)
                logger.info("Updated metadata with execution_block %s", execution_block)
                self.count_jsonb_objects()
            else:
                self.insert_metadata(json.dumps(metadata_file_dict), execution_block)
                logger.info("Inserted new metadata with execution_block %s", execution_block)
                self.count_jsonb_objects()

        except psycopg.Error as error:
            logger.error("Error saving metadata to PostgreSQL: %s", error)
            raise
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error("Error saving metadata to PostgreSQL: %s", exception)
            raise

    def count_jsonb_objects(self):
        """Counts the number of JSON objects within a JSONB column.

        Returns:
            The total count of JSON objects.
        """

        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
        result = cursor.fetchone()[0]
        cursor.close()
        self.number_of_dataproducts = int(result)
        print("result:")
        print(result)
        return result

    def delete_postgres_table(self) -> bool:
        """Deletes a table from a PostgreSQL database.

        Args:
            None

        Returns:
            True if the table was deleted successfully, False otherwise.
        """

        try:
            logger.info("PostgreSQL deleting database table %s", self.table_name)
            cursor = self.conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
            self.conn.commit()
            cursor.close()
            logger.info("PostgreSQL database table %s deleted.", self.table_name)
            return True

        except psycopg.OperationalError as error:
            logger.error(
                "An error occurred while connecting to the PostgreSQL database: %s", error
            )
            return False
        except psycopg.ProgrammingError as error:
            logger.error("An error occurred while executing the SQL statement: %s", error)
            return False

    def fetch_data(self, table_name: str) -> list[dict]:
        """Fetches JSONB data from Postgresql table.

        Args:
            table_name (str): Name of the Postgresql table.

        Returns:
            list[dict]: List of JSON objects.
        """
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT id, data FROM {table_name}")
        data = cursor.fetchall()
        cursor.close()
        return [{"id": row[0], "data": row[1]} for row in data]

    def load_data_products_from_persistent_metadata_store(self) -> list:
        """ """
        data_products: list[dict] = self.fetch_data(self.table_name)

        return data_products

    def load_metadata(self, file_object: FilePaths) -> dict[str, Any]:
        """This function loads the content of a yaml file and returns it as a dict."""
        # Test that the metadata file exists
        cursor = self.conn.cursor()
        check_query = f"SELECT data FROM {self.table_name} WHERE execution_block = %s"
        cursor.execute(check_query, (file_object.execution_block,))
        result = cursor.fetchone()
        cursor.close()
        metadata_dict = result[0]

        # Validate the metadata against the schema
        validation_errors = MetaData.validator.iter_errors(metadata_dict)

        # Loop over the errors
        for validation_error in validation_errors:
            logger.error(
                "Not loading dataproduct due to schema validation error when ingesting: %s : %s",
                str(file_object.fullPathName),
                str(validation_error.message),
            )

            if (
                str(validation_error.validator) == "required"
                or str(validation_error.message) == "None is not of type 'object'"
            ):
                logger.error(
                    "Not loading dataproduct due to schema validation error when ingesting: %s : %s",
                    str(file_object.fullPathName),
                    str(validation_error.message),
                )
                return {}

        try:
            metadata_date = get_date_from_name(metadata_dict["execution_block"])
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error(
                "Not loading dataproduct due to failure to extract the date from execution block: %s : %s",
                str(file_object.fullPathName),
                exception,
            )
            return {}

        metadata_dict.update(
            {
                "date_created": metadata_date,
                "dataproduct_file": str(file_object.relativePathName.parent),
                "metadata_file": str(file_object.relativePathName),
            }
        )

        return metadata_dict

    def get_metadata(self, execution_block: str) -> dict[str, Any]:
        """Retrieves metadata for the given execution block.

        Args:
            execution_block: The execution block identifier.

        Returns:
            A dictionary containing the metadata for the execution block, or None if not found.
        """
        try:
            return self.get_data_by_execution_block(execution_block)
        except KeyError:
            logger.warning(f"Metadata not found for execution block: {execution_block}")
            return {}

    def get_data_by_execution_block(self, execution_block: str) -> dict[str, Any]:
        """Retrieves data from the PostgreSQL table based on the execution_block.

        Args:
            execution_block: The execution block string.

        Returns:
            The data (JSONB) associated with the execution block, or None if not found.
        """
        cursor = self.conn.cursor()
        check_query = f"SELECT data FROM {self.table_name} WHERE execution_block = %s"
        cursor.execute(check_query, (execution_block,))
        result = cursor.fetchone()
        cursor.close()
        metadata_dict = result[0]
        if metadata_dict:
            return metadata_dict
        else:
            return {}
