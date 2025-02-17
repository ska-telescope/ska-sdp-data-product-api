"""Module adds a PostgreSQL interface for persistent storage of metadata files"""

import json
import logging
import pathlib
import uuid
from datetime import datetime, timezone
from typing import Any, List

import psycopg
from psycopg.rows import class_row

from ska_dataproduct_api.components.annotations.annotation import DataProductAnnotation
from ska_dataproduct_api.components.metadata.metadata import DataProductMetadata
from ska_dataproduct_api.components.muidatagrid.mui_datagrid import mui_data_grid_config_instance
from ska_dataproduct_api.components.pv_interface.pv_interface import PVIndex
from ska_dataproduct_api.configuration.settings import POSTGRESQL_QUERY_SIZE_LIMIT
from ska_dataproduct_api.utilities.helperfunctions import (
    DataProductIdentifier,
    find_metadata,
    validate_data_product_identifier,
)

logger = logging.getLogger(__name__)

# pylint: disable=too-many-instance-attributes
# pylint: disable=too-many-arguments
# pylint: disable=too-many-positional-arguments
# pylint: disable=too-many-public-methods
# pylint: disable=duplicate-code
# pylint: disable=not-context-manager
# pylint: disable=too-many-branches


class PostgresConnector:
    """
    A class to connect to a PostgreSQL database.
    """

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        dbname: str,
        schema: str,
    ):
        super().__init__()
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname
        self.schema = schema
        self.conn = None
        self.max_retries = 3  # The maximum number of retries
        self.retry_delay = 5  # The delay between retries in seconds
        self.connection_string: str = self.build_connection_string()
        self.postgresql_running: bool = False
        self.get_postgresql_version()

    def status(self) -> dict:
        """
        Retrieves the current status of the PostgreSQL connection.

        Returns:
            A dictionary containing the current status information.
        """
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "configured": self.postgresql_configured,
            "running": self.postgresql_running,
            "dbname": self.dbname,
            "schema": self.schema,
        }

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

        Returns:
            str: The PostgreSQL version.
        """
        try:
            query_string = "SELECT version()"
            with psycopg.connect(self.connection_string) as conn:
                with conn.cursor() as cur:
                    cur.execute(query=query_string)
                    self.postgresql_running = True
                    return cur.fetchone()[0]
        except (psycopg.OperationalError, psycopg.DatabaseError) as error:
            self.postgresql_running = False
            logger.error("Database error: %s", error)
            logger.error(
                "dbname='%s', user='%s', password='redacted', \
host='{%s}', port='%s', options='-c search_path='%s'",
                self.dbname,
                self.user,
                self.host,
                self.port,
                self.schema,
            )
            raise error


class PGMetadataStore:
    """
    A class contains the methods related to the Metadata Store.
    """

    def __init__(
        self,
        db: PostgresConnector,
        science_metadata_table_name: str,
        annotations_table_name: str,
    ):
        self.db: PostgresConnector = db
        self.science_metadata_table_name = science_metadata_table_name
        self.annotations_table_name = annotations_table_name
        self.metadata_list = []
        self.date_modified = datetime.now(tz=timezone.utc)

        if self.db.postgresql_running:
            self.create_metadata_table()
            self.create_annotations_table()

    @property
    def number_of_date_products_in_table(self) -> int:
        """Counts the number of JSON objects within the science metadata table.

        Returns:
            The total count of JSON objects.
        """
        try:
            query_string = (
                f"SELECT COUNT(*) FROM {self.db.schema}.{self.science_metadata_table_name}"
            )
            with psycopg.connect(self.db.connection_string) as conn:
                with conn.cursor() as cur:
                    cur.execute(query=query_string)
                    return int(cur.fetchone()[0])
        except (psycopg.OperationalError, psycopg.DatabaseError) as error:
            self.db.postgresql_running = False
            logger.error("Database error: %s", error)
            return None

    def status(self) -> dict:
        """
        Retrieves the current status of the Metadata Store.

        Returns:
            A dictionary containing the current status information.
        """
        return {
            "store_type": "Persistent PosgreSQL metadata store",
            "db_status": self.db.status(),
            "running": self.db.postgresql_running,
            "last_metadata_update_time": self.date_modified,
            "science_metadata_table_name": self.science_metadata_table_name,
            "annotations_table_name": self.annotations_table_name,
            "number_of_dataproducts": self.number_of_date_products_in_table,
        }

    def create_metadata_table(self) -> None:
        """Creates the metadata table named as defined in the env variable
        self.science_metadata_table_name if it doesn't exist.
        """

        logger.info(
            "Creating PostgreSQL metadata table: %s, in schema: %s",
            self.science_metadata_table_name,
            self.db.schema,
        )

        query_string = f"""
            CREATE TABLE IF NOT EXISTS {self.db.schema}.{self.science_metadata_table_name} (
                id SERIAL PRIMARY KEY,
                data JSONB NOT NULL,
                execution_block VARCHAR(255) DEFAULT NULL,
                uuid CHAR(64) UNIQUE,
                json_hash CHAR(64) UNIQUE
            );
            """

        with psycopg.connect(self.db.connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute(query=query_string)
                conn.commit()
                logger.info(
                    "PostgreSQL metadata table %s created in schema: %s.",
                    self.science_metadata_table_name,
                    self.db.schema,
                )

    def create_annotations_table(self) -> None:
        """Creates the annotations table named as defined in the env variable
        self.annotations_table_name if it doesn't exist.
        """

        logger.info(
            "Creating PostgreSQL annotations table: %s, in schema: %s",
            self.annotations_table_name,
            self.db.schema,
        )

        query_string = f"""
            CREATE TABLE IF NOT EXISTS {self.db.schema}.{self.annotations_table_name} (
                id SERIAL PRIMARY KEY,
                uuid CHAR(64),
                annotation_text TEXT,
                user_principal_name VARCHAR(255),
                timestamp_created TIMESTAMP,
                timestamp_modified TIMESTAMP
            );
            """

        with psycopg.connect(self.db.connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute(query=query_string)
                conn.commit()
                logger.info(
                    "PostgreSQL annotations table %s created in schema: %s.",
                    self.annotations_table_name,
                    self.db.schema,
                )

    def reload_all_data_products_in_index(self, pv_index: PVIndex) -> None:
        """
        Reloads all data product files from the pv_index.

        This method iterates over all data product files in the pv_index,
        ingests each file.
        """
        logger.info("Reloading all data products from PV index into metadata store...")

        for _, pv_data_product in pv_index.dict_of_data_products_on_pv.items():
            try:
                _ = self.ingest_file(pv_data_product.path)

            except psycopg.OperationalError as error:
                logger.error(
                    "An error occurred while connecting to the PostgreSQL database: %s",
                    error,
                )
                self.db.postgresql_running = False
                raise
            except Exception as error:  # pylint: disable=broad-exception-caught
                logger.error(
                    "Failed to ingest data product at file location: %s, due to error: %s",
                    str(pv_data_product.path),
                    error,
                )

        logger.info("Reloading into metadata store completed.")

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
        self.date_modified = datetime.now(tz=timezone.utc)
        return data_product_metadata_instance.data_product_uuid

    def check_metadata_exists_by_hash(self, json_hash: str) -> bool:
        """Checks if metadata exists based on the given hash."""
        query_string = f"SELECT EXISTS(SELECT 1 FROM {self.db.schema}.\
{self.science_metadata_table_name} WHERE json_hash = %s)"
        with psycopg.connect(self.db.connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute(query=query_string, params=(json_hash,))
                return cur.fetchone()[0]

    def get_metadata_id_by_uuid(self, data_product_uuid: str) -> str | None:
        """Checks if metadata exists based on the given execution block and return the PRIMARY KEY
        if it exists."""
        query_string = (
            f"SELECT id FROM {self.db.schema}.{self.science_metadata_table_name} WHERE uuid = %s"
        )
        with psycopg.connect(self.db.connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute(query=query_string, params=(data_product_uuid,))
                result = cur.fetchone()
                return result[0] if result else None

    def update_metadata(
        self, data_product_metadata_instance: DataProductMetadata, id_field: int
    ) -> None:
        """Updates existing metadata with the given data and hash."""
        query_string = f"UPDATE {self.db.schema}.{self.science_metadata_table_name} \
SET data = %s, json_hash = %s, uuid = %s WHERE id = %s"
        with psycopg.connect(self.db.connection_string) as conn:
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
        table: str = self.db.schema + "." + self.science_metadata_table_name
        query_string = f"INSERT INTO {table} (data, json_hash, execution_block, uuid) VALUES \
(%s, %s, %s, %s)"
        with psycopg.connect(self.db.connection_string) as conn:
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
        metadata_table_id = self.get_metadata_id_by_uuid(
            str(data_product_metadata_instance.data_product_uuid)
        )

        if metadata_table_id:
            self.update_metadata(data_product_metadata_instance, metadata_table_id)
            logger.info(
                "Updated metadata with execution_block %s",
                data_product_metadata_instance.execution_block,
            )
            return

        # Add if neither uuid or execution_block exist
        self.insert_metadata(data_product_metadata_instance)
        logger.info(
            "Inserted new metadata with execution_block %s",
            data_product_metadata_instance.execution_block,
        )

    def load_data_products_from_persistent_metadata_store(self) -> list[dict[str, any]]:
        """Fetches JSONB data from Postgresql table.

        Args:
            None

        Returns:
            list[Dict[str, any]]: list of data products.
        """
        try:
            query_string = (
                f"SELECT id, data FROM {self.db.schema}.{self.science_metadata_table_name}"
            )
            with psycopg.connect(self.db.connection_string) as conn:
                with conn.cursor() as cur:
                    cur.execute(query=query_string)
                    result = cur.fetchall()
                    return [{"id": row[0], "data": row[1]} for row in result]
        except (psycopg.OperationalError, psycopg.DatabaseError) as error:
            self.db.postgresql_running = False
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
        query_string = f"SELECT data FROM {self.db.schema}.{self.science_metadata_table_name} \
WHERE execution_block = %s"

        try:
            with psycopg.connect(self.db.connection_string) as conn:
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
            self.db.postgresql_running = False
            raise error

    def get_data_by_uuid(self, data_product_uuid: str) -> dict[str, Any] | None:
        """Retrieves data from the PostgreSQL table based on the uuid.

        Args:
            data_product_uuid: The uuid string.

        Returns:
            The data (JSONB) associated with the uuid, or None if not found.
        """
        query_string = (
            f"SELECT data FROM {self.db.schema}.{self.science_metadata_table_name} WHERE uuid = %s"
        )
        try:
            with psycopg.connect(self.db.connection_string) as conn:
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
            self.db.postgresql_running = False
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
        table: str = self.db.schema + "." + self.annotations_table_name

        if data_product_annotation.annotation_id is None:
            query_string = f"INSERT INTO {table} \
                (uuid, annotation_text, \
                  user_principal_name, timestamp_created, timestamp_modified)\
                VALUES (%s, %s, %s, %s, %s)"
            with psycopg.connect(self.db.connection_string) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        query=query_string,
                        params=(
                            data_product_annotation.data_product_uuid,
                            data_product_annotation.annotation_text,
                            data_product_annotation.user_principal_name,
                            datetime.now(tz=timezone.utc),
                            datetime.now(tz=timezone.utc),
                        ),
                    )
                    conn.commit()
        else:
            query_string = f"UPDATE {table} \
                    SET annotation_text = %s, timestamp_modified = %s\
                    WHERE id = %s"
            with psycopg.connect(self.db.connection_string) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        query=query_string,
                        params=(
                            data_product_annotation.annotation_text,
                            datetime.now(tz=timezone.utc),
                            data_product_annotation.annotation_id,
                        ),
                    )
                    conn.commit()

    def retrieve_annotations_by_uuid(self, data_product_uuid: str) -> List[DataProductAnnotation]:
        """Returns all annotations associated with a data product uuid."""
        table: str = self.db.schema + "." + self.annotations_table_name
        query_string = f"SELECT id as annotation_id, \
                            uuid as data_product_uuid, \
                            annotation_text, \
                            user_principal_name, \
                            timestamp_created, \
                            timestamp_modified \
                        from {table} WHERE uuid = %s"
        try:
            with psycopg.connect(self.db.connection_string) as conn:
                with conn.cursor(row_factory=class_row(DataProductAnnotation)) as cur:
                    try:
                        cur.execute(query=query_string, params=(data_product_uuid,))
                        return cur.fetchall()
                    except (IndexError, TypeError) as error:
                        logger.error(
                            "Annotations not found for uuid: %s, error: %s",
                            data_product_uuid,
                            error,
                        )
                        raise error
        except (psycopg.OperationalError, psycopg.DatabaseError) as error:
            self.db.postgresql_running = False
            raise error


class PGSearchStore:
    """
    A class contains the methods related to searching through the PostgreSQL Metadata Store.
    """

    def __init__(
        self,
        db: PostgresConnector,
        science_metadata_table_name: str,
        annotations_table_name: str,
    ):
        self.db: PostgresConnector = db
        self.science_metadata_table_name = science_metadata_table_name
        self.annotations_table_name = annotations_table_name
        self.metadata_list = []

    def status(self) -> dict:
        """
        Returns a dictionary containing the current status of the PGSearchStore.

        Includes information about:
            - metadata_store_in_use (str): The type of metadata store being used (e.g.,
            "PGSearchStore").
        """

        response = {
            "metadata_store_in_use": "PGSearchStore",
        }

        return response

    def access_filter(
        self, data: list[dict[str, Any]], users_user_groups: list[str]
    ) -> list[dict[str, Any]]:
        """Filters the mui_data_grid_filter_model based on access groups.

        Args:
            data: A list of dictionaries representing filter model data.
            users_user_groups: A list of user group names.

        Returns:
            A filtered list of dictionaries where either no access_group is assigned or the
            assigned access_group is in the users_user_groups list.
        """
        filtered_model = []
        for item in data:
            access_group = item.get("context.access_group", None)
            if access_group is None or access_group in users_user_groups:
                filtered_model.append(item)
        return filtered_model

    def filter_data(
        self,
        mui_data_grid_filter_model,
        search_panel_options,
        users_user_group_list: list[str],
    ):
        """Filters data based on provided criteria.

        Args:
            mui_data_grid_filter_model: Filter model from the MUI data grid.
            search_panel_options: Search panel options including date range and key value pairs.
            users_user_group_list: List of user groups.

        Returns:
            Filtered data.
        """
        mui_data_rows: list[dict] = []

        try:
            mui_data_grid_filter_model["items"].extend(search_panel_options.get("items", []))
        except KeyError:
            mui_data_grid_filter_model["items"] = search_panel_options.get("items", [])

        self.metadata_list.clear()
        sql_search_query, params = self.create_postgresql_query(
            filter_model=mui_data_grid_filter_model, table_name=self.science_metadata_table_name
        )
        self.search_metadata(sql_search_query=sql_search_query, params=params)

        mui_data_grid_config_instance.flattened_list_of_dataproducts_metadata.clear()
        for dataproduct in self.metadata_list:
            mui_data_grid_config_instance.update_flattened_list_of_keys(dataproduct)
            mui_data_grid_config_instance.update_flattened_list_of_dataproducts_metadata(
                mui_data_grid_config_instance.flatten_dict(dataproduct)
            )
        for row in mui_data_grid_config_instance.flattened_list_of_dataproducts_metadata:
            mui_data_rows.append(row)

        access_filtered_data = self.access_filter(
            data=mui_data_rows.copy(), users_user_groups=users_user_group_list
        )

        return access_filtered_data

    def create_annotations_postgresql_query(self, search_value) -> tuple[str, list]:
        """
        Creates the query string  for data products based on a partial value in the
        annotation_text.

        Args:
            search_value: The partial value to search for in the annotation_text.

        Returns:
            A tuple containing the query string and a list of parameters with wildcards for
            partial matching.
        """

        query = f"SELECT md.data \
FROM {self.db.schema}.{self.science_metadata_table_name} AS md \
JOIN {self.db.schema}.{self.annotations_table_name} AS ann ON md.uuid = ann.uuid \
WHERE ann.annotation_text ILIKE %s"

        search_value_with_wildcards = f"%{search_value}%"
        params = [search_value_with_wildcards]

        return query, params

    def create_postgresql_query(self, filter_model: dict, table_name: str) -> tuple[str, list]:
        """
        Creates a PostgreSQL query string from a MUI Data Grid filter model.

        Args:
            filter_model: The MUI Data Grid filter model.
            table_name: The name of the table to query.

        Returns:
            A PostgreSQL query string.
        """

        query = f"SELECT data FROM {self.db.schema}.{table_name}"
        where_clauses = []
        params = []

        for item in filter_model.get("items", []):

            # Use .get() with a default value to handle missing keys
            field = item.get("field", None)
            operator = item.get("operator", None)
            value = item.get("value", None)

            if (
                not field
                or not operator
                or not value
                or field not in mui_data_grid_config_instance.flattened_set_of_keys
            ):
                continue

            if field == "annotation":
                return self.create_annotations_postgresql_query(search_value=value)

            if operator == "greaterThan":
                where_clauses.append(f"data->>'{field}' > %s")
                params.append(value)
            elif operator == "lessThan":
                where_clauses.append(f"data->>'{field}' < %s")
                params.append(value)
            elif operator == "equals":
                where_clauses.append(f"data->>'{field}' = %s")
                params.append(value)
            elif operator == "contains":
                where_clauses.append(f"data->>'{field}' ILIKE %s")
                params.append(f"%{value}%")
            elif operator == "startsWith":
                where_clauses.append(f"data->>'{field}' ILIKE %s")
                params.append(f"{value}%")
            elif operator == "endsWith":
                where_clauses.append(f"data->>'{field}' ILIKE %s")
                params.append(f"%{value}")
            elif operator == "isEmpty":
                where_clauses.append(f"data->>'{field}' IS NULL OR data->>'{field}' = ''")
            elif operator == "isNotEmpty":
                where_clauses.append(f"data->>'{field}' IS NOT NULL AND data->>'{field}' != ''")
            elif operator == "isAnyOf":
                where_clauses.append(f"data->>'{field}' = ANY(%s)")
                params.append(value)

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        query += " ORDER BY (data->>'date_created')::timestamp DESC LIMIT " + str(
            POSTGRESQL_QUERY_SIZE_LIMIT
        )

        return query, params

    def search_metadata(self, sql_search_query, params):
        """Metadata search method"""
        try:
            with psycopg.connect(self.db.connection_string) as conn:
                with conn.cursor() as cur:
                    try:
                        cur.execute(query=sql_search_query, params=params)
                        result = cur.fetchall()
                        for value in result:
                            self.add_dataproduct(metadata_file=value[0])
                        return {}
                    except (IndexError, TypeError) as error:
                        logger.warning("Metadata search error %s", error)
                        return {}
        except (psycopg.OperationalError, psycopg.DatabaseError) as error:
            self.db.postgresql_running = False
            raise error

    def add_dataproduct(self, metadata_file: dict):
        """
        Populates the MUI Data Grid class the given metadata.

        Args:
            metadata_file: A dictionary containing the metadata for a data product.

        Raises:
            ValueError: If the provided metadata_file is not a dictionary.
        """
        required_keys = {
            "execution_block",
            "date_created",
            "dataproduct_file",
            "metadata_file",
            "data_product_uuid",
        }
        data_product_details = {}

        # Handle top-level required keys
        for key in required_keys:
            if key in metadata_file:
                metadata_file[key] = metadata_file[key]

        # Add additional keys based on query (assuming find_metadata is defined)
        for query_key in mui_data_grid_config_instance.flattened_set_of_keys:
            query_metadata = find_metadata(metadata_file, query_key)
            if query_metadata:
                data_product_details[query_metadata["key"]] = query_metadata["value"]

        self.update_dataproduct_list(metadata_file)

    def update_dataproduct_list(self, data_product_details):
        """
        Updates the internal list of data products with the provided metadata.

        This method adds the provided `data_product_details` dictionary to the internal
        `metadata_list` attribute. If the list is empty, it assigns an "id" of 1 to the
        first data product. Otherwise, it assigns an "id" based on the current length
        of the list + 1.

        Args:
            data_product_details: A dictionary containing the metadata for a data product.

        Returns:
            None
        """
        data_product_details["id"] = len(self.metadata_list) + 1
        self.metadata_list.append(data_product_details)
