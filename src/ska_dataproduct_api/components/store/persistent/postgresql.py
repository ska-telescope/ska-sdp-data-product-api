"""Module adds a PostgreSQL interface for persistent storage of metadata files"""

import hashlib
import json
import logging
import pathlib
import time
from typing import Any

import psycopg
from psycopg.errors import OperationalError

from ska_dataproduct_api.components.metadata.metadata import DataProductMetadata
from ska_dataproduct_api.components.store.metadata_store_base_class import MetadataStore
from ska_dataproduct_api.configuration.settings import PERSISTENT_STORAGE_PATH

logger = logging.getLogger(__name__)

# pylint: disable=too-many-instance-attributes
# pylint: disable=too-many-arguments


class PostgresConnector(MetadataStore):
    """
    A class to connect to a PostgreSQL instance and test its availability.
    """

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        dbname: str,
        schema: str,
        table_name: str,
    ):
        super().__init__()
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname
        self.schema = schema
        self.table_name = table_name
        self.conn = None
        self.max_retries = 3  # The maximum number of retries
        self.retry_delay = 5  # The delay between retries in seconds
        self.postgresql_running: bool = False
        self.postgresql_version: str = ""
        self.number_of_dataproducts: int = 0
        self.list_of_data_product_paths: list[pathlib.Path] = []

        self._connect(self.build_connection_string())
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
            "dbname": self.dbname,
            "schema": self.schema,
            "table_name": self.table_name,
            "number_of_dataproducts": self.number_of_dataproducts,
            "postgresql_version": self.postgresql_version,
            "last_metadata_update_time": self.date_modified,
        }

    def build_connection_string(self) -> str:
        """
        Builds the connection string for PostgreSQL based on provided credentials.

        Returns:
            str: The connection string.
        """
        return (
            f"dbname='{self.dbname}' "
            f"user='{self.user}' "
            f"password='{self.password}' "
            f"host='{self.host}' "
            f"port='{self.port}' "
            f"options='-c search_path=\"{self.schema}\"'"
        )

    def _connect(self, connection_string: str) -> None:
        """
        Attempts to connect to the PostgreSQL instance.

        Returns:
          None.
        """
        try:
            self.conn = psycopg.connect(connection_string)
            self.postgresql_running = True
            logger.info("Connected to PostgreSQL successfully")

        except OperationalError as error:
            self.postgresql_running = False
            logger.error(
                "An error occurred while connecting to the PostgreSQL database: %s", error
            )

    def _postgresql_query(self, query: str, params: tuple = ()) -> psycopg.cursor:
        """
        Executes a PostgreSQL query and returns the cursor object.

        Args:
            query (str): The SQL query to execute.
            params (tuple, optional): A tuple of parameters to be passed to the query. Defaults to
            an empty tuple.


        Returns:
            psycopg.extensions.cursor: The cursor object representing the query result.

        Raises:
            psycopg.OperationalError: If the query fails after multiple attempts.
            psycopg.Error: If there's a general PostgreSQL error during execution.
        """

        for attempt in range(self.max_retries):
            try:
                cursor = self.conn.cursor()
                cursor.execute(query, params)
                return cursor

            except psycopg.OperationalError as error:
                if attempt < self.max_retries - 1:
                    logger.warning(
                        "PostgreSQL connection error (attempt %s/%s): %s",
                        str(attempt + 1),
                        self.max_retries,
                        error,
                    )
                    self._connect(self.build_connection_string())  # Attempt reconnect here
                    time.sleep(self.retry_delay)
                else:
                    logger.error(
                        "Failed to connect to PostgreSQL after multiple attempts: %s", error
                    )
                    self.postgresql_running = False
                    raise psycopg.OperationalError(error)

            except psycopg.Error as error:
                logger.error("Error executing PostgreSQL query: %s", error)
                raise psycopg.Error(error)

        raise psycopg.OperationalError("Failed to execute query after multiple attempts")

    def _get_postgresql_version(self):
        with self._postgresql_query(query="SELECT version()") as cursor:
            return cursor.fetchone()[0]

    def create_metadata_table(self) -> None:
        """Creates the metadata table named as defined in the env variable self.table_name
        if it doesn't exist.

        Raises:
            psycopg.Error: If there's an error executing the SQL query.
        """

        try:
            logger.info(
                "Creating PostgreSQL metadata table: %s, in schema: %s",
                self.table_name,
                self.schema,
            )

            # Use parameterization to avoid SQL injection
            query_string = f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{self.table_name} (
                id SERIAL PRIMARY KEY,
                data JSONB NOT NULL,
                execution_block VARCHAR(255) DEFAULT NULL UNIQUE,
                json_hash CHAR(64) UNIQUE
            );
            """
            with self._postgresql_query(query=query_string) as _:
                self.conn.commit()
                logger.info(
                    "PostgreSQL metadata table %s created in schema: %s.",
                    self.table_name,
                    self.schema,
                )

        except psycopg.Error as error:
            logger.error("Error creating metadata table: %s", error)
            raise

    def reindex_persistent_volume(self) -> None:
        """
        Reindexes the persistent volume by ingesting all data product files.

        This method iterates over all data product files in the persistent storage path,
        ingests each file, and finally counts the JSONB objects.

        Raises:
            Exception: If an error occurs during the reindexing process.
        """
        logger.info("Re-indexing persistent volume store...")
        self.indexing = True
        self.list_of_data_product_paths.clear()
        self.list_of_data_product_paths: list[str] = self.list_all_data_product_files(
            PERSISTENT_STORAGE_PATH
        )
        for product_path in self.list_of_data_product_paths:
            self.ingest_file(product_path)
        self.count_jsonb_objects()
        self.indexing = False
        logger.info("Metadata store re-indexed")

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

        except Exception as error:  # pylint: disable=broad-exception-caught
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
        query_string = f"SELECT EXISTS(SELECT 1 FROM {self.schema}.{self.table_name} WHERE \
json_hash = %s)"

        with self._postgresql_query(query=query_string, params=(json_hash,)) as cursor:
            return cursor.fetchone()[0]

    def check_metadata_exists_by_execution_block(self, execution_block: str) -> int:
        """Checks if metadata exists based on the given execution block."""
        query_string = f"SELECT id FROM {self.schema}.{self.table_name} WHERE execution_block = %s"
        with self._postgresql_query(query=query_string, params=(execution_block,)) as cursor:
            result = cursor.fetchone()
            return result[0] if result else None

    def update_metadata(self, metadata_file_json: str, id_field: int) -> None:
        """Updates existing metadata with the given data and hash."""
        json_hash = self.calculate_metadata_hash(metadata_file_json)
        query_string = f"UPDATE {self.schema}.{self.table_name} SET data = %s, json_hash = %s \
WHERE id = %s"
        with self._postgresql_query(
            query=query_string, params=(metadata_file_json, json_hash, id_field)
        ) as _:
            self.conn.commit()

    def insert_metadata(self, metadata_file_json: str, execution_block: str) -> None:
        """Inserts new metadata into the database."""
        json_hash = self.calculate_metadata_hash(metadata_file_json)
        table: str = self.schema + "." + self.table_name
        query_string = f"INSERT INTO {table} (data, json_hash, execution_block) VALUES \
(%s, %s, %s)"
        with self._postgresql_query(
            query=query_string, params=(metadata_file_json, json_hash, execution_block)
        ) as _:
            self.conn.commit()

    def ingest_metadata(self, metadata_file_dict: dict) -> None:
        """Saves or update metadata to PostgreSQL."""
        self.save_metadata_to_postgresql(metadata_file_dict)

    def save_metadata_to_postgresql(self, metadata_file_dict: dict) -> None:
        """Saves metadata to PostgreSQL."""
        try:
            if not self.postgresql_running:
                logger.error("Error saving metadata to PostgreSQL, instance not available")
                return

            data_product_metadata_instance: DataProductMetadata = DataProductMetadata()
            data_product_metadata_instance.load_metadata_from_class(metadata_file_dict)

            json_hash = self.calculate_metadata_hash(data_product_metadata_instance.metadata_dict)
            execution_block = data_product_metadata_instance.metadata_dict["execution_block"]

            if self.check_metadata_exists_by_hash(json_hash):
                logger.info("Metadata with hash %s already exists.", json_hash)
                return

            metadata_id = self.check_metadata_exists_by_execution_block(execution_block)
            if metadata_id:
                self.update_metadata(
                    json.dumps(data_product_metadata_instance.metadata_dict), metadata_id
                )
                logger.info("Updated metadata with execution_block %s", execution_block)
                self.count_jsonb_objects()
            else:
                self.insert_metadata(
                    json.dumps(data_product_metadata_instance.metadata_dict), execution_block
                )
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
        query_string = f"SELECT COUNT(*) FROM {self.schema}.{self.table_name}"
        with self._postgresql_query(query=query_string) as cursor:
            result = cursor.fetchone()[0]
            self.number_of_dataproducts = int(result)
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
            query_string = f"DROP TABLE IF EXISTS {self.schema}.{self.table_name}"
            with self._postgresql_query(query=query_string) as _:
                self.conn.commit()
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
            list[dict]: list of JSON objects.
        """
        query_string = f"SELECT id, data FROM {self.schema}.{table_name}"
        with self._postgresql_query(query=query_string) as cursor:
            result = cursor.fetchall()
            return [{"id": row[0], "data": row[1]} for row in result]

    def load_data_products_from_persistent_metadata_store(self) -> list[dict[str, any]]:
        """Loads data products metadata from the persistent metadata store.

        Returns:
            list[Dict[str, any]]: list of data products.
        """
        return self.fetch_data(self.table_name)

    def get_metadata(self, execution_block: str) -> dict[str, Any]:
        """Retrieves metadata for the given execution block.

        Args:
            execution_block: The execution block identifier.

        Returns:
            A dictionary containing the metadata for the execution block, or None if not found.
        """
        try:
            data_product_metadata = self.get_data_by_execution_block(execution_block)
            if data_product_metadata:
                return data_product_metadata
            return {}

        except KeyError:
            logger.warning("Metadata not found for execution block ID: %s", execution_block)
            return {}

    def get_data_by_execution_block(self, execution_block: str) -> dict[str, Any] | None:
        """Retrieves data from the PostgreSQL table based on the execution_block.

        Args:
            execution_block: The execution block string.

        Returns:
            The data (JSONB) associated with the execution block, or None if not found.
        """
        try:
            query_string = (
                f"SELECT data FROM {self.schema}.{self.table_name} WHERE execution_block = %s"
            )
            with self._postgresql_query(query=query_string, params=(execution_block,)) as cursor:
                result = cursor.fetchone()
                if result[0]:
                    return result[0]
                return {}

        except (psycopg.OperationalError, psycopg.DatabaseError) as error:
            logger.error("Database error: %s", error)
            return {}
        except (IndexError, TypeError) as error:
            logger.warning(
                "Metadata not found for execution block ID: %s, error: %s", execution_block, error
            )
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
            if data_product_metadata:
                return pathlib.Path(data_product_metadata["dataproduct_file"])
            return {}
        except KeyError:
            logger.warning("File path not found for execution block: %s", execution_block)
            return {}
