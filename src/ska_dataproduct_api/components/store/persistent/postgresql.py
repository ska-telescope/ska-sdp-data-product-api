"""Module adds a PostgreSQL interface for persistent storage of metadata files"""

import json
import logging
import pathlib
import uuid
from typing import Any, List

import psycopg
from psycopg.rows import class_row

from ska_dataproduct_api.components.annotations.annotation import DataProductAnnotation
from ska_dataproduct_api.components.metadata.metadata import DataProductMetadata
from ska_dataproduct_api.components.store.metadata_store_base_class import MetadataStore
from ska_dataproduct_api.configuration.settings import PERSISTENT_STORAGE_PATH
from ska_dataproduct_api.utilities.helperfunctions import (
    DataProductIdentifier,
    validate_data_product_identifier,
)

logger = logging.getLogger(__name__)

# pylint: disable=too-many-instance-attributes
# pylint: disable=too-many-arguments
# pylint: disable=too-many-positional-arguments
# pylint: disable=too-many-public-methods
# pylint: disable=duplicate-code
# pylint: disable=not-context-manager


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
        annotations_table_name: str,
    ):
        super().__init__()
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname
        self.schema = schema
        self.table_name = table_name
        self.annotations_table_name = annotations_table_name
        self.conn = None
        self.max_retries = 3  # The maximum number of retries
        self.retry_delay = 5  # The delay between retries in seconds
        self.postgresql_running: bool = False
        self.number_of_dataproducts: int = 0
        self.list_of_data_product_paths: list[pathlib.Path] = []
        self.connection_string: str = self.build_connection_string()
        self.connect()

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
            "configured": self.postgresql_configured,
            "running": self.postgresql_running,
            "indexing": self.indexing,
            "dbname": self.dbname,
            "schema": self.schema,
            "table_name": self.table_name,
            "annotations_table_name": self.annotations_table_name,
            "number_of_dataproducts": self.number_of_dataproducts,
            "postgresql_version": self.postgresql_version,
            "last_metadata_update_time": self.date_modified,
        }

    def connect(self) -> None:
        """Connects to the PostgreSQL database and performs initialization tasks.

        This method establishes a connection to the PostgreSQL database, retrieves the
        PostgreSQL version, creates the metadata table and annotations table if they
        do not exist, and counts the number of data products stored as JSONB objects.
        """
        self.postgresql_version: str = self.get_postgresql_version()
        if self.postgresql_running:
            self.create_metadata_table()
            self.create_annotations_table()
            self.number_of_dataproducts = self.count_jsonb_objects()

    def build_connection_string(self) -> str:
        """
        Builds the connection string for PostgreSQL based on provided credentials.

        Returns:
            str: The connection string.
        """
        if not self.dbname or not self.user or not self.password or not self.host:
            self.postgresql_configured: bool = False
            raise ConnectionError(
                "Postgres connection string is not configured. Please check your configuration."
            )
        self.postgresql_configured: bool = True
        return (
            f"dbname='{self.dbname}' "
            f"user='{self.user}' "
            f"password='{self.password}' "
            f"host='{self.host}' "
            f"port='{self.port}' "
            f"options='-c search_path=\"{self.schema}\"'"
        )

    def get_postgresql_version(self) -> str:
        """
        Retrieves the PostgreSQL version from the database.

        Args:
            None

        Returns:
            The PostgreSQL version string.
        """
        query_string = "SELECT version()"
        with psycopg.connect(self.connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute(query=query_string)
                self.postgresql_running = True
                return cur.fetchone()[0]

    def create_metadata_table(self) -> None:
        """Creates the metadata table named as defined in the env variable self.table_name
        if it doesn't exist.

        Raises:
            psycopg.Error: If there's an error executing the SQL query.
        """

        logger.info(
            "Creating PostgreSQL metadata table: %s, in schema: %s",
            self.table_name,
            self.schema,
        )

        query_string = f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{self.table_name} (
                id SERIAL PRIMARY KEY,
                data JSONB NOT NULL,
                execution_block VARCHAR(255) DEFAULT NULL,
                uuid CHAR(64) UNIQUE,
                json_hash CHAR(64) UNIQUE
            );
            """

        with psycopg.connect(self.connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute(query=query_string)
                conn.commit()
                logger.info(
                    "PostgreSQL metadata table %s created in schema: %s.",
                    self.table_name,
                    self.schema,
                )

    def create_annotations_table(self) -> None:
        """Creates the annotations table named as defined in the env variable
        self.annotations_table_name if it doesn't exist.

        Raises:
            psycopg.Error: If there's an error executing the SQL query.
        """

        logger.info(
            "Creating PostgreSQL annotations table: %s, in schema: %s",
            self.annotations_table_name,
            self.schema,
        )

        query_string = f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{self.annotations_table_name} (
                id SERIAL PRIMARY KEY,
                uuid VARCHAR(64),
                annotation_text TEXT,
                user_principal_name VARCHAR(255),
                timestamp_created TIMESTAMP,
                timestamp_modified TIMESTAMP
            );
            """

        with psycopg.connect(self.connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute(query=query_string)
                conn.commit()
                logger.info(
                    "PostgreSQL annotations table %s created in schema: %s.",
                    self.annotations_table_name,
                    self.schema,
                )

    def reindex_persistent_volume(self) -> None:
        """
        Reindexes the persistent volume by ingesting all data product files.

        This method iterates over all data product files in the persistent storage path,
        ingests each file, and finally counts the JSONB objects.

        Raises:
            Exception: If an error occurs during the reindexing process.
        """
        logger.info("Re-indexing persistent volume store...")
        if not self.postgresql_running and self.postgresql_configured:
            self.connect()

        self.indexing = True
        self.list_of_data_product_paths.clear()
        self.list_of_data_product_paths: list[str] = self.list_all_data_product_files(
            PERSISTENT_STORAGE_PATH
        )
        for product_path in self.list_of_data_product_paths:
            try:
                _ = self.ingest_file(product_path)

            except psycopg.OperationalError as error:
                logger.error(
                    "An error occurred while connecting to the PostgreSQL database: %s",
                    error,
                )
                self.postgresql_running = False
                self.indexing = False
                raise
            except Exception as error:  # pylint: disable=broad-exception-caught
                logger.error(
                    "Failed to ingest data product at file location: %s, due to error: %s",
                    str(product_path),
                    error,
                )

        self.number_of_dataproducts = self.count_jsonb_objects()
        self.indexing = False
        logger.info("Metadata store re-indexed")

    def ingest_file(self, data_product_metadata_file_path: pathlib.Path) -> uuid.UUID:
        """
        Ingests a data product file by loading its metadata, structuring the information,
        and inserting it into the metadata store.

        Args:
            data_product_metadata_file_path (pathlib.Path): The path to the data file.
        """
        try:
            data_product_metadata_instance: DataProductMetadata = DataProductMetadata()
            data_product_metadata_instance.load_metadata_from_yaml_file(
                data_product_metadata_file_path
            )
        except Exception as error:
            logger.error(
                "Failed to ingest dataproduct %s in list of products paths. Error: %s",
                data_product_metadata_file_path,
                error,
            )
            raise error

        self.save_metadata_to_postgresql(data_product_metadata_instance)
        self.update_data_store_date_modified()
        return data_product_metadata_instance.data_product_uuid

    def check_metadata_exists_by_hash(self, json_hash: str) -> bool:
        """Checks if metadata exists based on the given hash."""
        query_string = f"SELECT EXISTS(SELECT 1 FROM {self.schema}.{self.table_name} WHERE \
json_hash = %s)"
        with psycopg.connect(self.connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute(query=query_string, params=(json_hash,))
                return cur.fetchone()[0]

    def check_metadata_exists_by_uuid(self, data_product_uuid: str) -> bool:
        """Checks if metadata exists based on the given execution block."""
        query_string = f"SELECT id FROM {self.schema}.{self.table_name} WHERE uuid = %s"
        with psycopg.connect(self.connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute(query=query_string, params=(data_product_uuid,))
                result = cur.fetchone()
                return result[0] if result else None

    def update_metadata(
        self, data_product_metadata_instance: DataProductMetadata, id_field: int
    ) -> None:
        """Updates existing metadata with the given data and hash."""
        query_string = f"UPDATE {self.schema}.{self.table_name} SET data = %s, json_hash = %s, \
uuid = %s WHERE id = %s"
        with psycopg.connect(self.connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query=query_string,
                    params=(
                        json.dumps(data_product_metadata_instance.metadata_dict),
                        data_product_metadata_instance.metadata_dict_hash,
                        str(data_product_metadata_instance.data_product_uuid),
                        id_field,
                    ),
                )
                conn.commit()

    def insert_metadata(self, data_product_metadata_instance: DataProductMetadata) -> None:
        """Inserts new metadata into the database."""
        table: str = self.schema + "." + self.table_name
        query_string = f"INSERT INTO {table} (data, json_hash, execution_block, uuid) VALUES \
(%s, %s, %s, %s)"
        with psycopg.connect(self.connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query=query_string,
                    params=(
                        json.dumps(data_product_metadata_instance.metadata_dict),
                        data_product_metadata_instance.metadata_dict_hash,
                        data_product_metadata_instance.execution_block,
                        str(data_product_metadata_instance.data_product_uuid),
                    ),
                )
                conn.commit()

    def ingest_metadata(self, metadata_file_dict: dict) -> uuid.UUID:
        """Saves or update metadata to PostgreSQL."""
        try:
            data_product_metadata_instance: DataProductMetadata = DataProductMetadata()
            data_product_metadata_instance.load_metadata_from_class(metadata_file_dict)
        except Exception as error:
            logger.error(
                "Failed to ingest dataproduct metadata: %s. Error: %s",
                metadata_file_dict,
                error,
            )
            raise error

        self.save_metadata_to_postgresql(data_product_metadata_instance)
        return data_product_metadata_instance.data_product_uuid

    def save_metadata_to_postgresql(
        self, data_product_metadata_instance: DataProductMetadata
    ) -> None:
        """Saves metadata to PostgreSQL."""
        if self.check_metadata_exists_by_hash(data_product_metadata_instance.metadata_dict_hash):
            logger.info(
                "Metadata with hash %s already exists.",
                data_product_metadata_instance.metadata_dict_hash,
            )
            return

        # Update if uuid exist
        metadata_table_id = self.check_metadata_exists_by_uuid(
            str(data_product_metadata_instance.data_product_uuid)
        )

        if metadata_table_id:
            self.update_metadata(data_product_metadata_instance, metadata_table_id)
            logger.info(
                "Updated metadata with execution_block %s",
                data_product_metadata_instance.execution_block,
            )
            self.number_of_dataproducts = self.count_jsonb_objects()
            return

        # Add if neither uuid or execution_block exist
        self.insert_metadata(data_product_metadata_instance)
        logger.info(
            "Inserted new metadata with execution_block %s",
            data_product_metadata_instance.execution_block,
        )
        self.number_of_dataproducts = self.count_jsonb_objects()

    def count_jsonb_objects(self) -> int:
        """Counts the number of JSON objects within a JSONB column.

        Returns:
            The total count of JSON objects.
        """
        try:
            query_string = f"SELECT COUNT(*) FROM {self.schema}.{self.table_name}"
            with psycopg.connect(self.connection_string) as conn:
                with conn.cursor() as cur:
                    cur.execute(query=query_string)
                    return int(cur.fetchone()[0])
        except (psycopg.OperationalError, psycopg.DatabaseError) as error:
            self.postgresql_running = False
            logger.error("Database error: %s", error)
            return self.number_of_dataproducts  # Count failed , returning previous count

    def load_data_products_from_persistent_metadata_store(self) -> list[dict[str, any]]:
        """Fetches JSONB data from Postgresql table.

        Args:
            None

        Returns:
            list[Dict[str, any]]: list of data products.
        """
        try:
            query_string = f"SELECT id, data FROM {self.schema}.{self.table_name}"
            with psycopg.connect(self.connection_string) as conn:
                with conn.cursor() as cur:
                    cur.execute(query=query_string)
                    result = cur.fetchall()
                    return [{"id": row[0], "data": row[1]} for row in result]
        except (psycopg.OperationalError, psycopg.DatabaseError) as error:
            self.postgresql_running = False
            logger.error("Database error: %s", error)
            return []

    def get_metadata(self, data_product_uuid: str) -> dict[str, Any]:
        """Retrieves metadata for the given uuid.

        Args:
            data_product_uuid: The data product uuid identifier.

        Returns:
            A dictionary containing the metadata for the uuid, or None if not found.
        """
        try:
            data_product_metadata = self.get_data_by_uuid(data_product_uuid)
            if data_product_metadata:
                return data_product_metadata
            return {}

        except KeyError:
            logger.warning("Metadata not found for uuid: %s", data_product_uuid)
            return {"Error": f"Metadata not found for uuid {data_product_uuid}"}
        except (psycopg.OperationalError, psycopg.DatabaseError) as error:
            logger.error("Database error: %s", error)
            return {"Error": "Failed to retrieve data, database not available"}

    def get_data_by_execution_block(self, execution_block: str) -> dict[str, Any] | None:
        """Retrieves data from the PostgreSQL table based on the execution_block.

        Args:
            execution_block: The execution block string.

        Returns:
            The data (JSONB) associated with the execution block, or None if not found.
        """
        query_string = (
            f"SELECT data FROM {self.schema}.{self.table_name} WHERE execution_block = %s"
        )

        try:
            with psycopg.connect(self.connection_string) as conn:
                with conn.cursor() as cur:
                    try:
                        cur.execute(query=query_string, params=(execution_block,))
                        result = cur.fetchone()
                        if result[0]:
                            return result[0]
                        return {}
                    except (IndexError, TypeError) as error:
                        logger.warning(
                            "Metadata not found for execution block ID: %s, error: %s",
                            execution_block,
                            error,
                        )
                        return {}
        except (psycopg.OperationalError, psycopg.DatabaseError) as error:
            self.postgresql_running = False
            raise error

    def get_data_by_uuid(self, data_product_uuid: str) -> dict[str, Any] | None:
        """Retrieves data from the PostgreSQL table based on the uuid.

        Args:
            data_product_uuid: The uuid string.

        Returns:
            The data (JSONB) associated with the uuid, or None if not found.
        """
        query_string = f"SELECT data FROM {self.schema}.{self.table_name} WHERE uuid = %s"
        try:
            with psycopg.connect(self.connection_string) as conn:
                with conn.cursor() as cur:
                    try:
                        cur.execute(query=query_string, params=(data_product_uuid,))
                        result = cur.fetchone()
                        if result[0]:
                            return result[0]
                        return {}
                    except (IndexError, TypeError) as error:
                        logger.warning(
                            "Metadata not found for uuid: %s, error: %s", data_product_uuid, error
                        )
                        return {}
        except (psycopg.OperationalError, psycopg.DatabaseError) as error:
            self.postgresql_running = False
            raise error

    def get_data_product_file_paths(
        self, data_product_identifier: DataProductIdentifier
    ) -> list[pathlib.Path]:
        """Retrieves the file path to the data product for the given execution block.

        Args:
            execution_block: The execution block to retrieve metadata for.

        Returns:
            The file path as a pathlib.Path object, or {} if not found.
        """

        try:
            validate_data_product_identifier(data_product_identifier)
        except Exception as error:  # pylint: disable=broad-exception-caught
            logger.error(
                "File path not found for data product, error: %s",
                error,
            )
            return []

        try:
            if data_product_identifier.execution_block:
                data_product_metadata = self.get_data_by_execution_block(
                    data_product_identifier.execution_block
                )
                if data_product_metadata:
                    return [pathlib.Path(data_product_metadata["dataproduct_file"])]
            if data_product_identifier.uuid:
                data_product_metadata = self.get_data_by_uuid(data_product_identifier.uuid)
                if data_product_metadata:
                    return [pathlib.Path(data_product_metadata["dataproduct_file"])]

            return []
        except KeyError:
            logger.warning(
                "File path not found for execution block: %s",
                data_product_identifier.uuid or data_product_identifier.execution_block,
            )
            return []
        except (psycopg.OperationalError, psycopg.DatabaseError) as error:
            logger.error("Database error: %s", error)
            return []

    def save_annotation(self, data_product_annotation: DataProductAnnotation) -> None:
        """Inserts new annotation into the database."""
        table: str = self.schema + "." + self.annotations_table_name

        if data_product_annotation.annotation_id is None:
            query_string = f"INSERT INTO {table} \
                (uuid, annotation_text, \
                  user_principal_name, timestamp_created, timestamp_modified)\
                VALUES (%s, %s, %s, %s, %s)"
            with psycopg.connect(self.connection_string) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        query=query_string,
                        params=(
                            data_product_annotation.data_product_uuid,
                            data_product_annotation.annotation_text,
                            data_product_annotation.user_principal_name,
                            data_product_annotation.timestamp_created,
                            data_product_annotation.timestamp_modified,
                        ),
                    )
                    conn.commit()
        else:
            query_string = f"UPDATE {table} \
                    SET annotation_text = %s, user_principal_name = %s, timestamp_modified = %s\
                    WHERE id = %s"
            with psycopg.connect(self.connection_string) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        query=query_string,
                        params=(
                            data_product_annotation.annotation_text,
                            data_product_annotation.user_principal_name,
                            data_product_annotation.timestamp_modified,
                            data_product_annotation.annotation_id,
                        ),
                    )
                    conn.commit()

    def retrieve_annotations_by_uuid(self, data_product_uuid: str) -> List[DataProductAnnotation]:
        """Returns all annotations associated with a data product uuid."""
        table: str = self.schema + "." + self.annotations_table_name
        query_string = f"SELECT id as annotation_id, \
                            uuid as data_product_uuid, \
                            annotation_text, \
                            user_principal_name, \
                            timestamp_created, \
                            timestamp_modified \
                        from {table} WHERE uuid = %s"
        try:
            with psycopg.connect(self.connection_string) as conn:
                with conn.cursor(row_factory=class_row(DataProductAnnotation)) as cur:
                    try:
                        cur.execute(query=query_string, params=[data_product_uuid])
                        return cur.fetchall()
                    except (IndexError, TypeError) as error:
                        logger.error(
                            "Annotations not found for uuid: %s, error: %s",
                            data_product_uuid,
                            error,
                        )
                        raise error
        except (psycopg.OperationalError, psycopg.DatabaseError) as error:
            self.postgresql_running = False
            raise error
