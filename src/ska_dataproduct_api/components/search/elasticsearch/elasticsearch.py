"""Module to insert data into Elasticsearch instance."""
import base64
import datetime
import json
import logging
from pathlib import Path
from typing import Union

import elasticsearch
from elasticsearch import Elasticsearch

from ska_dataproduct_api.components.muidatagrid.mui_datagrid import muiDataGridInstance
from ska_dataproduct_api.components.search.search_store_base_class import MetadataSearchStore
from ska_dataproduct_api.components.store.in_memory.in_memory import (
    InMemoryVolumeIndexMetadataStore,
)
from ska_dataproduct_api.components.store.persistent.postgresql import PostgresConnector
from ska_dataproduct_api.configuration.settings import (
    CONFIGURATION_FILES_PATH,
    ELASTIC_HTTP_CA_BASE64_CERT,
    ELASTIC_HTTP_CA_FILE_NAME,
)
from ska_dataproduct_api.utilities.helperfunctions import find_metadata

logger = logging.getLogger(__name__)

# pylint: disable=too-many-instance-attributes
# pylint: disable=too-many-arguments


class ElasticsearchMetadataStore(MetadataSearchStore):
    """Class to insert data into Elasticsearch instance."""

    def __init__(
        self,
        host,
        port,
        user,
        password,
        indices,
        schema,
        metadata_store: Union[PostgresConnector, InMemoryVolumeIndexMetadataStore],
    ):
        super().__init__(metadata_store)
        self.indices = indices
        self.schema = schema

        self.host: str = host
        self.port: int = port
        self.url: str = self.host + ":" + str(self.port)
        self.user: str = user
        self.password: str = password
        self.ca_cert: str = None

        self.es_client: Elasticsearch = None
        self.elasticsearch_running: bool = False
        self.elasticsearch_version: str = ""
        self.connection_established_at: datetime = ""
        self.cluster_info: dict = {}

        self.metadata_list = []
        self.query_body = {
            "size": 100,
            "query": {
                "bool": {
                    "must": [],
                    "should": [],
                }
            },
        }
        self.number_of_dataproducts: int = 0

    def status(self) -> dict:
        """
        Returns a dictionary containing the current status of the Elasticsearch connection.

        Includes information about:
            - metadata_store_in_use (str): The type of metadata store being used (e.g.,
            "ElasticsearchMetadataStore").
            - url (str): The hostname or IP address and port of the Elasticsearch server.
            - user (str, optional): The username used to connect to Elasticsearch (if applicable).
            - running (bool): Whether the Elasticsearch server is currently running.
            - connection_established_at (datetime, optional): The timestamp when the connection to
             Elasticsearch was established (if applicable).
            - cluster_info (dict, optional): A dictionary containing additional cluster
            information retrieved from Elasticsearch (if desired).
        """

        response = {
            "metadata_store_in_use": "ElasticsearchMetadataStore",
            "url": self.url,
            "user": self.user,
            "running": self.elasticsearch_running,
            "connection_established_at": self.connection_established_at,
            "number_of_dataproducts": self.number_of_dataproducts,
            "indices": self.indices,
            "cluster_info": self.cluster_info,
        }

        return response

    def save_ca_cert_to_file(
        self, ca_cert_content: str, config_file_path: Path, ca_cert: str
    ) -> bool:
        """Saves CA certificate content to file."""
        try:
            ca_cert_path = config_file_path / ca_cert
            with open(ca_cert_path, "wb") as file:
                file.write(ca_cert_content)
            return True
        except (IOError, ValueError) as error:
            logging.error("Error saving CA certificate from environment: %s", error)
            return False

    def decode_base64(self, encoded_string):
        """Decodes a Base64 encoded string.

        Args:
            encoded_string: The Base64 encoded string.

        Returns:
            The decoded string.
        """
        try:
            return base64.b64decode(encoded_string)
        except (TypeError, ValueError, base64.binascii.Error) as exception:
            logger.exception(
                "Error decoding Base64 string: %s (encoded string: %s)", exception, encoded_string
            )
            raise exception

    def load_ca_cert(self, config_file_path: Path, ca_cert: str) -> None:
        """
        Attempts to load the CA certificate from the configured path.

        This method attempts to load the CA certificate from the file specified
        by the `ca_cert` parameter relative to the path provided in
        `config_file_path`.

        If no path is configured, the `ca_cert` parameter is empty, or the file
        cannot be accessed for any reason, this method sets the `ca_cert` attribute
        to `None` and logs an informative message describing the issue.

        Args:
            config_file_path (Path): The path to the configuration file (potentially
                                    containing the relative path to the CA cert).
            ca_cert (str): The name or relative path to the CA certificate file
                        within the configuration directory.

        Raises:
            FileNotFoundError: If the specified CA certificate file cannot be found.
            PermissionError: If there are permission issues accessing the CA
                            certificate file.
        """

        try:
            # Construct the path to the CA certificate file
            if not ca_cert:
                logging.info("No CA certificate file")
                self.ca_cert = None
                return

            ca_cert_path: Path = config_file_path / ca_cert

            if self.save_ca_cert_to_file(
                ca_cert_content=self.decode_base64(ELASTIC_HTTP_CA_BASE64_CERT),
                config_file_path=CONFIGURATION_FILES_PATH,
                ca_cert=ELASTIC_HTTP_CA_FILE_NAME,
            ):
                self.ca_cert: Path = ca_cert_path
                logging.info("CA certificate saved to file: %s", ca_cert_path)
            else:
                logging.info("CA certificate file not found: %s", ca_cert_path)
                self.ca_cert = None

        except (FileNotFoundError, PermissionError) as error:
            # Handle potential file access errors gracefully
            logging.error(
                "Error while trying to load CA certificate: %s",
                error,
            )
            self.ca_cert = None
            raise error

        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.exception(
                "Exception while trying to load CA certificate: %s",
                exception,
            )
            self.ca_cert = None
            raise exception

    def connect(self):
        """Connecting to Elasticsearch host and create default schema"""
        logger.info("Connecting to Elasticsearch hosted at %s...", self.url)

        try:
            self.load_ca_cert(
                config_file_path=CONFIGURATION_FILES_PATH, ca_cert=ELASTIC_HTTP_CA_FILE_NAME
            )
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.exception(
                "Exception when trying to load CA certificate, using in memory search: %s",
                exception,
            )
            return False

        try:
            self.es_client = Elasticsearch(
                hosts=self.url,
                http_auth=(self.user, self.password),
                verify_certs=True,
                ca_certs=self.ca_cert,
            )
            if self.es_client.ping():
                self.connection_established_at = datetime.datetime.now()
                self.elasticsearch_running = True
                self.cluster_info = self.es_client.info()
                logger.info("Connected to Elasticsearch; creating default schema...")
                self.create_schema_if_not_existing(index=self.indices, schema=self.schema)
                self.load_metadata_from_store()

                return True
            return False
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.exception(
                "Exception when trying to connect to Elasticsearch, using in memory search: %s",
                exception,
            )
            return False

    def check_and_reconnect(self) -> bool:
        """
        Checks if the connection to Elasticsearch is still alive and attempts to reconnect
        if necessary.

        Returns True if the connection is successfully established or re-established,
        False otherwise.
        """

        if not self.es_client:
            if not self.connect():
                return False

        try:
            if self.es_client.ping():
                return True
        except (ConnectionError, TimeoutError) as error:
            logger.error("Connection to Elasticsearch lost: %s", error)

        if self.connect():
            logger.info("Successfully reconnected to Elasticsearch.")
            return True

        logger.error("Failed to reconnect to Elasticsearch.")
        return False

    def create_schema_if_not_existing(self, index: str, schema: Path) -> None:
        """
        Creates an Elasticsearch index with the specified schema if it doesn't exist.

        Args:
            index (str): The name of the Elasticsearch index.
            schema (Path): The path to the JSON schema file.
        """
        try:
            self.es_client.indices.get(index=index)
            logger.info("Index %s already exists.", index)
        except elasticsearch.NotFoundError:
            try:
                with open(schema, "r", encoding="utf-8") as metadata_schema:
                    metadata_schema_json = json.load(metadata_schema)
                self.es_client.indices.create(  # pylint: disable=unexpected-keyword-arg
                    index=index, ignore=400, body=metadata_schema_json
                )
                logger.info("Index %s created successfully.", index)
            except (FileNotFoundError, json.JSONDecodeError) as error:
                logger.error("Error loading or parsing schema file: %s", error)
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.exception("Unexpected error creating index: %s", exception)

    def insert_metadata_in_search_store(self, metadata_dict: dict) -> None:
        """Inserts metadata from a JSON file into the Elasticsearch index.

        Args:
            metadata_file_json (dict): A dictionary containing the metadata to be inserted.
                The expected structure of the dictionary depends on your specific Elasticsearch
                schema, but it should generally represent the document you want to store
                in the index.

        """
        try:
            if self.index_metadata_to_elasticsearch(self.indices, metadata_dict):
                self.number_of_dataproducts = self.count_documents(self.indices)
            else:
                logger.warning("Inserting into search store failed for: %s", metadata_dict)
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error("Error inserting metadata into search store: %s", exception)

    def count_documents(self, indices):
        """Counts the number of documents in the specified Elasticsearch indices.

        Args:
            indices: The Elasticsearch indices to count documents in.

        Returns:
            The number of documents in each index.
        """
        return self.es_client.count(index=indices)["count"]

    def index_metadata_to_elasticsearch(self, index: str, metadata_dict: dict) -> bool:
        """Indexes metadata into Elasticsearch.

        Args:
            index: The Elasticsearch index name.
            metadata_dict: The metadata to be indexed as a dictionary.

        Raises:
            ValueError: If the metadata is invalid or missing 'execution_block'.
            elasticsearch.exceptions.ElasticsearchException: For Elasticsearch-specific errors.
        """

        try:
            data_product_uuid = metadata_dict.get("uuid")
            if not data_product_uuid:
                raise ValueError("Missing 'data_product_uuid' in metadata")

            response = self.es_client.index(index=index, id=data_product_uuid, body=metadata_dict)
            if response["result"] == "created" or response["result"] == "updated":
                return True
            logger.warning("Error inserting metadata into Elasticsearch: %s", str(response))
            return False
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error("Error inserting metadata into search store: %s", exception)
            return False

    def search_metadata(self):
        """Metadata Search method"""

        self.check_and_reconnect()

        resp = self.es_client.search(  # pylint: disable=E1123
            index=self.indices, body=self.query_body
        )
        all_hits = resp["hits"]["hits"]
        self.metadata_list = []
        for _num, doc in enumerate(all_hits):
            for key, value in doc.items():
                if key == "_source":
                    self.add_dataproduct(metadata_file=value)

    def add_dataproduct(self, metadata_file: dict):
        """
        Populates a list of data products with their associated metadata.

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
        for query_key in muiDataGridInstance.flattened_set_of_keys:
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
        # Adds the first dictionary to the list
        if len(self.metadata_list) == 0:
            data_product_details["id"] = 1
            self.metadata_list.append(data_product_details)
            return

        data_product_details["id"] = len(self.metadata_list) + 1
        self.metadata_list.append(data_product_details)
        return

    def filter_data(
        self,
        mui_data_grid_filter_model,
        search_panel_options,
        users_user_group_list: list[str],
    ):
        """Filters data based on provided criteria.

        Args:
            mui_data_grid_filter_model: Filter model from the MUI data grid.
            search_panel_options: Search panel options.
            users_user_group_list: List of user groups.

        Returns:
            Filtered data.
        """
        self.query_body = {
            "size": 100,
            "query": {
                "bool": {
                    "must": [],
                    "should": [],
                }
            },
        }
        self.add_search_panel_options_to_es_query(search_panel_options)
        self.add_mui_data_grid_filter_model_to_es_query(mui_data_grid_filter_model)
        self.add_access_group_to_query_body(users_user_group_list)
        self.search_metadata()
        muiDataGridInstance.rows.clear()
        muiDataGridInstance.flattened_list_of_dataproducts_metadata.clear()
        for dataproduct in self.metadata_list:
            muiDataGridInstance.update_flattened_list_of_keys(dataproduct)
            muiDataGridInstance.update_flattened_list_of_dataproducts_metadata(
                muiDataGridInstance.flatten_dict(dataproduct)
            )
        muiDataGridInstance.load_metadata_from_list(
            muiDataGridInstance.flattened_list_of_dataproducts_metadata
        )
        return muiDataGridInstance.rows

    def add_access_group_to_query_body(self, users_user_groups: list[str]) -> None:
        """
        Modifies the internal query body to include a filter based on the provided user groups.

        This method adds a nested boolean query with a "should" clause for each user group.
        Each "should" clause includes a "match" query that searches for documents
        where the "context.access_group" field matches the user group name.

        Args:
            users_user_groups (List[str]): A list of user group names.

        Raises:
            None

        Returns:
            dict: The updated query body.
        """
        access_group_query_body = {
            "bool": {
                "should": [
                    {"bool": {"must_not": [{"exists": {"field": "context.access_group"}}]}},
                ]
            }
        }

        # Add terms query for each user group
        for group in users_user_groups:
            access_group_query_body["bool"]["should"].append(
                {"match": {"context.access_group": group}}
            )

        self.query_body["query"]["bool"]["must"].append(access_group_query_body)

    def add_search_panel_options_to_es_query(self, search_panel_options) -> None:
        """
        Builds an Elasticsearch query body based on the provided data structure.

        Args:
            data: A dictionary representing the data for the query body.

        Returns:
            A dictionary representing the Elasticsearch query body.
        """

        if "items" not in search_panel_options:
            return

        gte_date = "1970-01-01"
        lte_date = "2050-12-31"

        # Add date_created search_panel_options
        for item in search_panel_options["items"]:
            if item["field"] == "date_created":
                if not item["value"] == "":
                    if item["operator"] == "greaterThan":
                        gte_date = item["value"]
                    elif item["operator"] == "lessThan":
                        lte_date = item["value"]

            elif item["field"] == "formFields":
                for key_pair in item["keyPairs"]:
                    # Check if both key and value exist before adding to query
                    if (
                        "keyPair" in key_pair
                        and "valuePair" in key_pair
                        and key_pair["keyPair"]
                        and key_pair["valuePair"]
                    ):
                        self.query_body["query"]["bool"]["should"].append(
                            {"match": {key_pair["keyPair"]: key_pair["valuePair"]}}
                        )
        date_ranges = {
            "range": {
                "date_created": {
                    "gte": gte_date,
                    "lte": lte_date,
                    "format": "yyyy-MM-dd",
                }
            }
        }

        self.query_body["query"]["bool"]["must"].append(date_ranges)

    def add_mui_data_grid_filter_model_to_es_query(self, mui_data_grid_filter_model) -> None:
        """
        Builds an Elasticsearch query body based on the provided data structure.

        Args:
            data: A dictionary representing the data for the query body.

        Returns:
            A dictionary representing the Elasticsearch query body.
        """
        if "items" not in mui_data_grid_filter_model:
            return

        for item in mui_data_grid_filter_model["items"]:
            if item["field"] == "date_created":
                pass
            # Check if both key and value exist before adding to query
            elif "field" in item and "value" in item:
                self.query_body["query"]["bool"]["should"].append(
                    {"match": {item["field"]: item["value"]}}
                )
