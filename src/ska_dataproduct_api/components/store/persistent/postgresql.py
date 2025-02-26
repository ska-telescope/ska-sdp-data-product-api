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
from ska_dataproduct_api.utilities.helperfunctions import DataProductIdentifier, find_metadata

logger = logging.getLogger(__name__)

# pylint: disable=too-many-instance-attributes
# pylint: disable=too-many-arguments
# pylint: disable=too-many-positional-arguments
# pylint: disable=too-many-public-methods
# pylint: disable=duplicate-code
# pylint: disable=not-context-manager
# pylint: disable=too-many-branches
# pylint: disable=too-many-lines


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
        dlm_schema: str,
        dlm_data_item_table_name: str,
    ):
        self.db: PostgresConnector = db
        self.science_metadata_table_name = science_metadata_table_name
        self.sql_file_path = "src/ska_dataproduct_api/sql/dpd_metadata_table.sql"
        self.annotations_table_name = annotations_table_name
        self.date_modified = datetime.now(tz=timezone.utc)
        self.science_metadata_table_name = science_metadata_table_name
        self.annotations_table_name = annotations_table_name
        self.dlm_schema = dlm_schema
        self.dlm_data_item_table_name = dlm_data_item_table_name

        if self.db.postgresql_running:
            self.create_table(
                table_name=self.science_metadata_table_name, sql_definition_file=self.sql_file_path
            )
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

    def create_table(self, table_name, sql_definition_file) -> None:
        """Creates the metadata table if it doesn't exist by executing
        the SQL definition from a .sql file.

        Args:
            table_name (str): The table name
            sql_definition_file (str): The SQL script that will be executed to create the table.

        Returns:
            None
        """
        try:
            with open(sql_definition_file, "r", encoding="utf-8") as file:
                sql_query = file.read()
        except FileNotFoundError:
            logger.error("SQL file not found at: %s", sql_definition_file)
            raise

        sql_query = sql_query.replace("{schema_name}", self.db.schema).replace(
            "{table_name}", table_name
        )

        logger.info(
            "Creating PostgreSQL metadata table: %s, in schema: %s using SQL file: %s",
            table_name,
            self.db.schema,
            sql_definition_file,
        )

        try:
            with psycopg.connect(self.db.connection_string) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql_query)
                    conn.commit()
                    logger.info(
                        "PostgreSQL metadata table %s created in schema: %s.",
                        table_name,
                        self.db.schema,
                    )
        except psycopg.Error as error:
            logger.error("Error creating table: %s", error)
            conn.rollback()
            raise

    def create_annotations_table(self) -> None:
        """Creates the annotations table named as defined in the env variable
        self.annotations_table_name if it doesn't exist.

        Returns:
            None
        """

        logger.info(
            "Creating PostgreSQL annotations table: %s, in schema: %s",
            self.annotations_table_name,
            self.db.schema,
        )

        query_string = f"""
            CREATE TABLE IF NOT EXISTS {self.db.schema}.{self.annotations_table_name} (
                id SERIAL PRIMARY KEY,
                uid CHAR(64),
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

        Args:
            pv_index (PVIndex): Index containing all the data products PV file details.

        Returns:
            None
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

        Returns:
            data_product_uid (uuid.UUID) = The UID of the ingested data product.
        """
        try:
            data_product_metadata_instance: DataProductMetadata = DataProductMetadata(
                data_store="dpd"
            )
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
        return data_product_metadata_instance.data_product_uid

    def check_metadata_exists_by_hash(self, json_hash: str) -> bool:
        """Checks if metadata exists based on the given hash.

        json_hash (str): A SHA256 hash of the metadata JSON.

        Returns:
            Bool: True if the metadata exist with the given hash.
        """
        query_string = f"SELECT EXISTS(SELECT 1 FROM {self.db.schema}.\
{self.science_metadata_table_name} WHERE json_hash = %s)"
        with psycopg.connect(self.db.connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute(query=query_string, params=(json_hash,))
                return cur.fetchone()[0]

    def get_metadata_id_by_uid(self, data_product_uid: str) -> int | None:
        """Checks if metadata exists based on the given execution block and return the PRIMARY KEY
        if it exists.

        Args:
            data_product_uid (str): UUID of associated data product.

        Returns:
            str | None: PRIMARY KEY of the data product in the table.
        """
        query_string = (
            f"SELECT id FROM {self.db.schema}.{self.science_metadata_table_name} WHERE uid = %s"
        )
        with psycopg.connect(self.db.connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute(query=query_string, params=(data_product_uid,))
                result = cur.fetchone()
                return int(result[0]) if result else None

    def update_metadata(
        self, data_product_metadata_instance: DataProductMetadata, id_field: int
    ) -> None:
        """
        Updates an existing metadata record in the database with the provided data product
        metadata.

        This method updates the 'data' and 'json_hash' columns of a specific metadata record
        in the database, identified by the 'id_field'. It uses the metadata from the provided
        `data_product_metadata_instance` to perform the update.

        Args:
            data_product_metadata_instance (DataProductMetadata): An instance of the
                DataProductMetadata class containing the updated metadata.
            id_field (int): The integer ID of the metadata record to be updated.

        Returns:
            None

        Raises:
            psycopg.Error: If there is an error executing the database query.
        """
        query_string = f"UPDATE {self.db.schema}.{self.science_metadata_table_name} \
SET data = %s, json_hash = %s, uid = %s WHERE id = %s"
        with psycopg.connect(self.db.connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query=query_string,
                    params=(
                        json.dumps(data_product_metadata_instance.appended_metadata_dict()),
                        data_product_metadata_instance.metadata_dict_hash,
                        str(data_product_metadata_instance.data_product_uid),
                        id_field,
                    ),
                )
                conn.commit()

        query_string = f"UPDATE {self.db.schema}.{self.science_metadata_table_name} \
SET data = %s, json_hash = %s, uid = %s WHERE id = %s"
        with psycopg.connect(self.db.connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query=query_string,
                    params=(
                        json.dumps(data_product_metadata_instance.appended_metadata_dict()),
                        data_product_metadata_instance.metadata_dict_hash,
                        str(data_product_metadata_instance.data_product_uid),
                        id_field,
                    ),
                )
                conn.commit()

    def insert_metadata(self, data_product_metadata_instance: DataProductMetadata) -> None:
        """
        Inserts a new metadata record into the database.

        This method adds a new row to the database table specified by `self.db.schema` and
        `self.science_metadata_table_name`. The data for the new row is extracted from the
        provided `data_product_metadata_instance`.

        Args:
            data_product_metadata_instance (DataProductMetadata): An instance of the
                DataProductMetadata class containing the metadata to be inserted.

        Returns:
            None

        Raises:
            psycopg.Error: If there is an error executing the database query.
        """
        table: str = self.db.schema + "." + self.science_metadata_table_name
        query_string = f"INSERT INTO {table} (data, json_hash, execution_block, uid) VALUES \
(%s, %s, %s, %s)"
        with psycopg.connect(self.db.connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query=query_string,
                    params=(
                        json.dumps(data_product_metadata_instance.appended_metadata_dict()),
                        data_product_metadata_instance.metadata_dict_hash,
                        data_product_metadata_instance.execution_block,
                        str(data_product_metadata_instance.data_product_uid),
                    ),
                )
                conn.commit()

    def ingest_metadata(self, metadata_file_dict: dict, data_store: str = "dpd") -> uuid.UUID:
        """Saves or update metadata to PostgreSQL."""
        try:
            data_product_metadata_instance: DataProductMetadata = DataProductMetadata(
                data_store=data_store
            )
            data_product_metadata_instance.load_metadata_from_class(metadata_file_dict)
        except Exception as error:
            logger.error(
                "Failed to ingest dataproduct metadata: %s. Error: %s",
                metadata_file_dict,
                error,
            )
            raise error

        self.save_metadata_to_postgresql(data_product_metadata_instance)
        return data_product_metadata_instance.data_product_uid

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

        # Update if uid exist
        metadata_table_id = self.get_metadata_id_by_uid(
            str(data_product_metadata_instance.data_product_uid)
        )

        if metadata_table_id:
            self.update_metadata(data_product_metadata_instance, metadata_table_id)
            logger.info(
                "Updated metadata with execution_block %s",
                data_product_metadata_instance.execution_block,
            )
            return

        # Add if neither uid or execution_block exist
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

    def get_metadata(self, data_product_itentifier: DataProductIdentifier) -> dict[str, Any]:
        """Retrieves metadata for the given uid.

        Args:
            data_product_uid: The data product uid identifier.

        Returns:
            A dictionary containing the metadata for the uid, or None if not found.
        """
        try:
            data_product_metadata = self.get_data_by_uid(data_product_itentifier)
            if data_product_metadata:
                return data_product_metadata
            return {}

        except KeyError:
            logger.warning("Metadata not found for uid: %s", data_product_itentifier.uid)
            return {"Error": f"Metadata not found for uid {data_product_itentifier.uid}"}
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

    def get_data_by_uid(
        self, data_product_itentifier: DataProductIdentifier
    ) -> dict[str, Any] | None:
        """Retrieves data from the PostgreSQL table based on the uid.

        Args:
            data_product_uid: The uid string.

        Returns:
            The data (JSONB) associated with the uid, or None if not found.
        """

        #  {metadata_column} FROM {schema}.{table_name}

        if data_product_itentifier.data_store == "dpd":
            query_string = f"SELECT data FROM {self.db.schema}.{self.science_metadata_table_name} \
WHERE uid = %s"
        elif data_product_itentifier.data_store == "dlm":
            query_string = f"SELECT metadata FROM {self.dlm_schema}.\
{self.dlm_data_item_table_name} WHERE uid = %s"

        else:
            logger.error("Datasource not known %s", data_product_itentifier.data_store)
            return {}

        try:
            with psycopg.connect(self.db.connection_string) as conn:
                with conn.cursor() as cur:
                    try:
                        cur.execute(query=query_string, params=(data_product_itentifier.uid,))
                        result = cur.fetchone()
                        if result[0]:
                            return result[0]
                        return {}
                    except (IndexError, TypeError) as error:
                        logger.warning(
                            "Metadata not found for uid: %s, error: %s",
                            data_product_itentifier.uid,
                            error,
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
            if data_product_identifier.execution_block:
                data_product_metadata = self.get_data_by_execution_block(
                    data_product_identifier.execution_block
                )
                if data_product_metadata:
                    return [pathlib.Path(data_product_metadata["dataproduct_file"])]
            if data_product_identifier.uid:
                data_product_metadata = self.get_metadata(data_product_identifier.uid)
                if data_product_metadata:
                    return [pathlib.Path(data_product_metadata["dataproduct_file"])]

            return []
        except KeyError as error:
            logger.warning(
                "File path not found for execution block: %s",
                data_product_identifier.uid or data_product_identifier.execution_block,
            )
            raise FileNotFoundError(
                f"No data product file path found with data_product_identifier: \
{data_product_identifier}"
            ) from error
        except (psycopg.OperationalError, psycopg.DatabaseError) as error:
            logger.error("Database error: %s", error)
            return []

    def save_annotation(self, data_product_annotation: DataProductAnnotation) -> None:
        """Inserts new annotation into the database."""
        table: str = self.db.schema + "." + self.annotations_table_name

        if data_product_annotation.annotation_id is None:
            query_string = f"INSERT INTO {table} \
                (uid, annotation_text, \
                  user_principal_name, timestamp_created, timestamp_modified)\
                VALUES (%s, %s, %s, %s, %s)"
            with psycopg.connect(self.db.connection_string) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        query=query_string,
                        params=(
                            data_product_annotation.data_product_uid,
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

    def retrieve_annotations_by_uid(self, data_product_uid: str) -> List[DataProductAnnotation]:
        """Returns all annotations associated with a data product uid."""
        table: str = self.db.schema + "." + self.annotations_table_name
        query_string = f"SELECT id as annotation_id, \
                            uid as data_product_uid, \
                            annotation_text, \
                            user_principal_name, \
                            timestamp_created, \
                            timestamp_modified \
                        from {table} WHERE uid = %s"
        try:
            with psycopg.connect(self.db.connection_string) as conn:
                with conn.cursor(row_factory=class_row(DataProductAnnotation)) as cur:
                    try:
                        cur.execute(query=query_string, params=(data_product_uid,))
                        return cur.fetchall()
                    except (IndexError, TypeError) as error:
                        logger.error(
                            "Annotations not found for uid: %s, error: %s",
                            data_product_uid,
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
        metadata_strore: PGMetadataStore,
    ):
        self.db: PostgresConnector = db
        self.metadata_strore: PGMetadataStore = metadata_strore

        self.filtered_list_of_data_product_metadata_files = []

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
    ) -> list[dict]:
        """Filters data based on provided criteria.

        Args:
            mui_data_grid_filter_model: Filter model from the MUI data grid.
            search_panel_options: Search panel options including date range and key value pairs.
            users_user_group_list: List of user groups.

        Returns:
            A list of filtered metadata.
        """
        mui_data_rows: list[dict] = []

        try:
            mui_data_grid_filter_model["items"].extend(search_panel_options.get("items", []))
        except KeyError:
            mui_data_grid_filter_model["items"] = search_panel_options.get("items", [])

        self.filtered_list_of_data_product_metadata_files.clear()
        # Filter products in the DPD Metadata table
        sql_search_query, params = self.create_postgresql_query(
            filter_model=mui_data_grid_filter_model,
            schema=self.db.schema,
            table_name=self.metadata_strore.science_metadata_table_name,
            metadata_column="data",
        )
        self.search_metadata(sql_search_query=sql_search_query, params=params, data_store="dpd")

        # Filter products in the DLM data_item table
        sql_search_query, params = self.create_postgresql_query(
            filter_model=mui_data_grid_filter_model,
            schema=self.metadata_strore.dlm_schema,
            table_name=self.metadata_strore.dlm_data_item_table_name,
            metadata_column="metadata",
        )
        self.search_metadata(sql_search_query=sql_search_query, params=params, data_store="dlm")

        # Add all the keys to the MUI config, and add the products to the MUI list of products:
        mui_data_grid_config_instance.flattened_list_of_dataproducts_metadata.clear()
        for metadata_file in self.filtered_list_of_data_product_metadata_files:
            mui_data_grid_config_instance.update_flattened_list_of_keys(metadata_file)
            mui_data_grid_config_instance.update_flattened_list_of_dataproducts_metadata(
                mui_data_grid_config_instance.flatten_dict(metadata_file)
            )
        for row in mui_data_grid_config_instance.flattened_list_of_dataproducts_metadata:
            mui_data_rows.append(row)

        # Filter the MUI list of products based on the users user groups. Note, this can and should
        # be refactored when the access group architecture and implementation is done in the DLM:
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
FROM {self.db.schema}.{self.metadata_strore.science_metadata_table_name} AS md \
JOIN {self.db.schema}.{self.metadata_strore.annotations_table_name} AS ann ON md.uid = ann.uid \
WHERE ann.annotation_text ILIKE %s"

        search_value_with_wildcards = f"%{search_value}%"
        params = [search_value_with_wildcards]

        return query, params

    def create_postgresql_query(
        self, filter_model: dict, schema: str, table_name: str, metadata_column: str
    ) -> tuple[str, list]:
        """
        Creates a PostgreSQL query string from a MUI Data Grid filter model.

        Args:
            filter_model (dict): The MUI Data Grid filter model.
            schema (str): PostgreSQL schema name.
            table_name (str): The name of the table to query.
            metadata_column (str): The column containing the metadata.

        Returns:
            A PostgreSQL query string and its parameter lists as a tuple.
        """

        query = f"SELECT uid, {metadata_column} FROM {schema}.{table_name}"
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
                where_clauses.append(f"{metadata_column}->>'{field}' > %s")
                params.append(value)
            elif operator == "lessThan":
                where_clauses.append(f"{metadata_column}->>'{field}' < %s")
                params.append(value)
            elif operator == "equals":
                where_clauses.append(f"{metadata_column}->>'{field}' = %s")
                params.append(value)
            elif operator == "contains":
                where_clauses.append(f"{metadata_column}->>'{field}' ILIKE %s")
                params.append(f"%{value}%")
            elif operator == "startsWith":
                where_clauses.append(f"{metadata_column}->>'{field}' ILIKE %s")
                params.append(f"{value}%")
            elif operator == "endsWith":
                where_clauses.append(f"{metadata_column}->>'{field}' ILIKE %s")
                params.append(f"%{value}")
            elif operator == "isEmpty":
                where_clauses.append(
                    f"{metadata_column}->>'{field}' IS NULL OR {metadata_column}->>'{field}' = ''"
                )
            elif operator == "isNotEmpty":
                where_clauses.append(
                    f"{metadata_column}->>'{field}' IS NOT NULL AND {metadata_column}->>'{field}' \
!= ''"
                )
            elif operator == "isAnyOf":
                where_clauses.append(f"{metadata_column}->>'{field}' = ANY(%s)")
                params.append(value)

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        query += (
            " ORDER BY ("
            + metadata_column
            + "->>'date_created')::timestamp DESC LIMIT "
            + str(POSTGRESQL_QUERY_SIZE_LIMIT)
        )

        return query, params

    def search_metadata(self, sql_search_query: str, params: list, data_store: str) -> None:
        """Metadata search method

        Args:
            sql_search_query (str): A PostgreSQL query string.
            params (list): PostgreSQL query parameters.
            data_store (str): Name of the data store where the data product is located
            (defaults to "dpd").

        """
        try:
            with psycopg.connect(self.db.connection_string) as conn:
                with conn.cursor() as cur:
                    try:
                        cur.execute(query=sql_search_query, params=params)
                        result = cur.fetchall()
                        for value in result:
                            self.add_dataproduct(
                                uid=value[0], metadata_file=value[1], data_store=data_store
                            )

                        return
                    except (IndexError, TypeError) as error:
                        logger.warning(
                            "Metadata search error: %s with query: %s on data store: %s",
                            error,
                            sql_search_query,
                            data_store,
                        )
                        return

        except (psycopg.OperationalError, psycopg.DatabaseError) as error:
            self.db.postgresql_running = False
            raise error

    def add_dataproduct(self, uid: uuid, metadata_file: dict, data_store: str = "dpd"):
        """
        Adds the metadata to the filtered data product list.

        Args:
            uid (uuid.UUID): The unique identifier of the data product.
            metadata_file (dict): A dictionary containing the metadata for a data product.
            data_store (str): Name of the data store where the data product is located
            (defaults to "dpd").

        Raises:
            ValueError: If the provided metadata_file is not a dictionary.
        """
        required_keys = {
            "execution_block",
            "date_created",
            "dataproduct_file",
            "metadata_file",
            "data_product_uid",
        }
        appended_metadata_file = {}

        # Handle top-level required keys
        for key in required_keys:
            if key in metadata_file:
                metadata_file[key] = metadata_file[key]

        # Add additional keys based on query (assuming find_metadata is defined)
        for query_key in mui_data_grid_config_instance.flattened_set_of_keys:
            query_metadata = find_metadata(metadata_file, query_key)
            if query_metadata:
                appended_metadata_file[query_metadata["key"]] = query_metadata["value"]
        data_product_metadata_instance: DataProductMetadata = DataProductMetadata(
            data_store=data_store
        )
        data_product_metadata_instance.load_metadata_from_class(
            metadata=metadata_file, dlm_uid=uid
        )

        if data_store == "dlm":
            self.append_filtered_dataproduct_list(
                metadata_file=data_product_metadata_instance.appended_metadata_dict()
            )
        else:
            self.append_filtered_dataproduct_list(
                metadata_file=data_product_metadata_instance.metadata_dict
            )

    def get_date_from_name(self, execution_block: str) -> str:
        """
        Extracts a date string from an execution block (type-generatorID-datetime-localSeq from
        https://confluence.skatelescope.org/display/SWSI/SKA+Unique+Identifiers) and converts it
        to the format 'YYYY-MM-DD'.

        Args:
            execution_block (str): A string containing metadata information.

        Returns:
            str: The formatted date string in 'YYYY-MM-DD' format.

        Raises:
            ValueError: If the date cannot be parsed from the execution block.

        Example:
            >>> get_date_from_name("type-generatorID-20230411-localSeq")
            '2023-04-11'
        """
        try:
            metadata_date_str = execution_block.split("-")[2]
            date_obj = datetime.strptime(metadata_date_str, "%Y%m%d")
            return date_obj.strftime("%Y-%m-%d")
        except ValueError as error:
            logger.error(
                "The execution_block: %s is missing or not in the following format: "
                "type-generatorID-datetime-localSeq. Error: %s",
                execution_block,
                error,
            )
            raise

    def append_filtered_dataproduct_list(self, metadata_file) -> None:
        """
        Updates the internal list of data products with the provided metadata.

        This method adds the provided `appended_metadata_file` dictionary to the internal
        `filtered_list_of_data_product_metadata_files` attribute. If the list is empty, it assigns
        an "id" of 1 to the first data product. Otherwise, it assigns an "id" based on the current.
        length of the list + 1.

        Args:
            metadata_file: A dictionary containing the metadata for a data product.

        Returns:
            None
        """
        metadata_file["id"] = len(self.filtered_list_of_data_product_metadata_files) + 1
        self.filtered_list_of_data_product_metadata_files.append(metadata_file)
