"""Module adds a PostgreSQL interface for persistent storage of metadata files"""

import hashlib
import json
import logging
import pathlib
from typing import Any

import psycopg
from psycopg.errors import OperationalError

from ska_sdp_dataproduct_api.components.metadata.metadata import DataProductMetadata
from ska_sdp_dataproduct_api.components.store.metadata_store_base_class import MetadataStore
from ska_sdp_dataproduct_api.configuration.settings import (
    METADATA_FILE_NAME,
    PERSISTENT_STORAGE_PATH,
)
from ska_sdp_dataproduct_api.utilities.helperfunctions import verify_persistent_storage_file_path

logger = logging.getLogger(__name__)

# pylint: disable=too-many-instance-attributes
# pylint: disable=too-many-arguments


class PostgresConnector(MetadataStore):
    """
    A class to connect to a PostgreSQL instance and test its availability.
    """

    def __init__(self, host: str, port: int, user: str, password: str, table_name: str):
        super().__init__()
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
            self.count_jsonb_objects()

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

    def reindex_persistent_volume(self) -> None:
        """ """
        list_of_data_product_paths = self.list_all_data_product_files(PERSISTENT_STORAGE_PATH)
        for product_path in list_of_data_product_paths:
            self.ingest_file(product_path)
        self.count_jsonb_objects()

    def list_all_data_product_files(self, full_path_name: pathlib.Path) -> list:
        """
        Lists all data product files within the specified directory path.

        This method recursively traverses the directory structure starting at `full_path_name`
        and identifies files that are considered data products based on pre-defined criteria
        of the folder containing a metadata file.

        Args:
            full_path_name (pathlib.Path): The path to the directory containing data products.

        Returns:
            List[pathlib.Path]: A list of `pathlib.Path` objects representing the identified
                                data product files within the directory and its subdirectories.
                                If no data product files are found, an empty list is returned.

        Raises:
            ValueError: If `full_path_name` does not represent a valid directory or is a symbolic
            link.
        """

        if not verify_persistent_storage_file_path(full_path_name):
            return
        logger.info("Identifying data product files within directory: %s", full_path_name)

        list_of_data_product_paths = []
        for file_path in PERSISTENT_STORAGE_PATH.rglob(METADATA_FILE_NAME):
            if file_path not in list_of_data_product_paths:
                list_of_data_product_paths.append(file_path)

        return list_of_data_product_paths

    def ingest_file(self, data_product_metadata_file_path: pathlib.Path) -> None:
        """
        Ingests a data product file by loading its metadata, structuring the information,
        and inserting it into the metadata store.

        Args:
            data_product_metadata_file_path (pathlib.Path): The path to the data file.
        """
        data_product_metadata_instance: DataProductMetadata = DataProductMetadata()
        data_product_metadata_instance.load_metadata_from_yaml_file(
            data_product_metadata_file_path
        )

        try:
            self.save_metadata_to_postgresql(
                metadata_file_dict=data_product_metadata_instance.metadata_dict
            )

        except Exception as error:
            logger.error(
                "Failed to ingest_file dataproduct %s into PostgreSQL. Error: %s",
                data_product_metadata_file_path,
                error,
            )
        self.update_data_store_date_modified()

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

    def get_data_product_file_path(self, execution_block: str) -> pathlib.Path:
        """Retrieves the file path to the data product for the given execution block.

        Args:
            execution_block: The execution block to retrieve metadata for.

        Returns:
            The file path as a pathlib.Path object, or {} if not found.
        """

        try:
            data_product_metadata = self.get_data_by_execution_block(execution_block)
            return pathlib.Path(data_product_metadata["dataproduct_file"])
        except KeyError:
            logger.warning(f"File path not found for execution block: {execution_block}")
            return {}
