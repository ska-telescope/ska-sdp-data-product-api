"""Module to insert data into Elasticsearch instance."""
import datetime
import json
import logging
from pathlib import Path

import elasticsearch
from elasticsearch import Elasticsearch

from ska_sdp_dataproduct_api.components.metadatastore.datastore import SearchStoreSuperClass
from ska_sdp_dataproduct_api.components.muidatagrid.mui_datagrid import muiDataGridInstance
from ska_sdp_dataproduct_api.configuration.settings import (
    CONFIGURATION_FILES_PATH,
    ELASTICSEARCH_HOST,
    ELASTICSEARCH_HTTP_CA,
    ELASTICSEARCH_METADATA_SCHEMA_FILE,
    ELASTICSEARCH_PASSWORD,
    ELASTICSEARCH_PORT,
    ELASTICSEARCH_USER,
)

logger = logging.getLogger(__name__)


class ElasticsearchMetadataStore(
    SearchStoreSuperClass
):  # pylint: disable=too-many-instance-attributes
    """Class to insert data into Elasticsearch instance."""

    def __init__(self):
        super().__init__()
        self.metadata_index = "sdp_meta_data"

        self.host: str = ELASTICSEARCH_HOST
        self.port: int = ELASTICSEARCH_PORT
        self.url: str = self.host + ":" + str(self.port)
        self.user: str = ELASTICSEARCH_USER
        self.password: str = ELASTICSEARCH_PASSWORD
        self.ca_cert: str = None

        self.es_client: Elasticsearch = None
        self.elasticsearch_running: bool = False
        self.elasticsearch_version: str = ""
        self.connection_established_at: datetime = ""
        self.cluster_info: dict = {}

        self.query_body = {"query": {"bool": {"should": [], "filter": []}}}

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
            "cluster_info": self.cluster_info,
        }

        return response

    def load_ca_cert(self) -> None:
        """Loads the CA certificate from the configured path.

        If no path is configured or the file cannot be accessed, sets the
        `ca_cert` attribute to None and logs an informative message.
        """

        try:
            # Construct the path to the CA certificate file
            if not ELASTICSEARCH_HTTP_CA:
                logging.info("No CA certificate file")
                self.ca_cert = None
                return

            ca_cert_path: Path = CONFIGURATION_FILES_PATH / ELASTICSEARCH_HTTP_CA

            # Check if the file exists and is a regular file
            if ca_cert_path.is_file():
                self.ca_cert: Path = ca_cert_path

            else:
                logging.info("CA certificate file not found: %s", ca_cert_path)
                self.ca_cert = None

        except (FileNotFoundError, PermissionError) as error:
            # Handle potential file access errors gracefully
            logging.error("Error loading CA certificate: %s", error)
            self.ca_cert = None

    def connect(self):
        """Connecting to Elasticsearch host and create default schema"""
        logger.info("Connecting to Elasticsearch...")

        self.load_ca_cert()

        self.es_client = Elasticsearch(
            hosts=self.url,
            http_auth=(self.user, self.password),
            verify_certs=False,
            ca_certs=self.ca_cert,
        )

        if self.es_client.ping():
            self.connection_established_at = datetime.datetime.now()
            self.elasticsearch_running = True
            self.cluster_info = self.es_client.info()
            logger.info("Connected to Elasticsearch; creating default schema...")
            self.create_schema_if_not_existing(index=self.metadata_index)
            self.reindex()

            return True
        return False

    def check_and_reconnect(self) -> bool:
        """
        Checks if the connection to Elasticsearch is still alive and attempts to reconnect
        if necessary.

        Returns True if the connection is successfully established or re-established,
        False otherwise.
        """

        if not self.es_client:
            self.connect()

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

    def create_schema_if_not_existing(self, index: str):
        """Method to create a Schema from schema and index if it does not yet
        exist."""
        try:
            _ = self.es_client.indices.get(index=index)
        except elasticsearch.NotFoundError:
            with open(
                ELASTICSEARCH_METADATA_SCHEMA_FILE, "r", encoding="utf-8"
            ) as metadata_schema:
                metadata_schema_json = json.load(metadata_schema)
            self.es_client.indices.create(  # pylint: disable=E1123
                index=index, ignore=400, body=metadata_schema_json
            )

    def clear_metadata_indecise(self) -> None:
        """Deletes specific indices from the Elasticsearch instance and clear the metadata_list.

        Args:
            None
        """

        self.es_client.options(ignore_status=[400, 404]).indices.delete(index=self.metadata_index)
        self.metadata_list = []
        self.number_of_dataproducts = 0

    def insert_metadata_in_search_store(self, metadata_file_json: dict) -> dict:
        """Inserts metadata from a JSON file into the Elasticsearch index.

        Args:
            metadata_file_json (dict): A dictionary containing the metadata to be inserted.
                The expected structure of the dictionary depends on your specific Elasticsearch
                schema, but it should generally represent the document you want to store
                in the index.

        Returns:
            dict: The response dictionary from the Elasticsearch client's `index` method,
                containing information about the successful or failed indexing operation.
                The structure of this dictionary will vary depending on your Elasticsearch
                version and configuration. Consult the Elasticsearch documentation for
                details.

        """
        try:
            response = self.es_client.index(index=self.metadata_index, document=metadata_file_json)
            if response["result"] == "created":
                self.number_of_dataproducts = self.number_of_dataproducts + 1
            return True
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error("Error inserting metadata: %s", exception)
            return False

    def sort_metadata_list(self) -> None:
        """This method sorts the metadata_list according to the set key"""

    def search_metadata(self):
        """Metadata Search method"""

        self.check_and_reconnect()

        resp = self.es_client.search(  # pylint: disable=E1123
            index=self.metadata_index, body=self.query_body
        )
        all_hits = resp["hits"]["hits"]
        self.metadata_list = []
        for _num, doc in enumerate(all_hits):
            for key, value in doc.items():
                if key == "_source":
                    self.update_flattened_list_of_keys(value)
                    self.add_dataproduct(metadata_file=value)

    def filter_data(self, mui_data_grid_filter_model, search_panel_options):
        """This is implemented in subclasses."""
        self.query_body = {"query": {"bool": {"should": [], "filter": []}}}
        self.add_search_panel_options_to_es_query(search_panel_options)
        self.add_mui_data_grid_filter_model_to_es_query(mui_data_grid_filter_model)
        self.search_metadata()
        muiDataGridInstance.load_inmemory_store_data(self)
        result = muiDataGridInstance.rows.copy()
        return result

    def add_search_panel_options_to_es_query(self, search_panel_options):
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
                    if key_pair["keyPair"] and key_pair["valuePair"]:
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

        self.query_body["query"]["bool"]["filter"].append(date_ranges)

    def add_mui_data_grid_filter_model_to_es_query(self, mui_data_grid_filter_model):
        """
        Builds an Elasticsearch query body based on the provided data structure.

        Args:
            data: A dictionary representing the data for the query body.

        Returns:
            A dictionary representing the Elasticsearch query body.
        """
        if "items" not in mui_data_grid_filter_model:
            return

        # Add date_created search_panel_options
        for item in mui_data_grid_filter_model["items"]:
            if item["field"] == "date_created":
                pass
            elif "field" in item and "value" in item:
                # Check if both key and value exist before adding to query
                self.query_body["query"]["bool"]["should"].append(
                    {"match": {item["field"]: item["value"]}}
                )
