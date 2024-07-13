"""Module to insert data into Elasticsearch instance."""
import datetime
import json
import logging
from pathlib import Path

import elasticsearch
from elasticsearch import Elasticsearch

from ska_sdp_dataproduct_api.components.metadatastore.datastore import Store
from ska_sdp_dataproduct_api.configuration.settings import (
    DATE_FORMAT,
    SDP_DATAPRODUCT_API_ELASTIC_HTTP_CA,
    SDP_DATAPRODUCT_API_ELASTIC_METADATA_SCHEMA_FILE,
    SDP_DATAPRODUCT_API_ELASTIC_PASSWORD,
    SDP_DATAPRODUCT_API_ELASTIC_PORT,
    SDP_DATAPRODUCT_API_ELASTIC_URL,
    SDP_DATAPRODUCT_API_ELASTIC_USER,
)
from ska_sdp_dataproduct_api.utilities.helperfunctions import parse_valid_date

logger = logging.getLogger(__name__)


class ElasticsearchMetadataStore(Store):  # pylint: disable=too-many-instance-attributes
    """Class to insert data into Elasticsearch instance."""

    def __init__(self):
        super().__init__()
        self.metadata_index = "sdp_meta_data"

        self.url: str = SDP_DATAPRODUCT_API_ELASTIC_URL
        self.port: int = SDP_DATAPRODUCT_API_ELASTIC_PORT
        self.host: str = self.url + ":" + self.port
        self.user: str = SDP_DATAPRODUCT_API_ELASTIC_USER
        self.password: str = SDP_DATAPRODUCT_API_ELASTIC_PASSWORD
        self.ca_cert: str = None

        self.es_client: Elasticsearch = None
        self.elasticsearch_running: bool = False
        self.elasticsearch_version: str = ""
        self.connection_established_at: datetime = ""
        self.cluster_info: dict = {}

    def status(self) -> dict:
        """
        Returns a dictionary containing the current status of the Elasticsearch connection.

        Includes information about:
            - metadata_store_in_use (str): The type of metadata store being used (e.g.,
            "ElasticsearchMetadataStore").
            - host (str): The hostname or IP address of the Elasticsearch server.
            - user (str, optional): The username used to connect to Elasticsearch (if applicable).
            - running (bool): Whether the Elasticsearch server is currently running.
            - connection_established_at (datetime, optional): The timestamp when the connection to
             Elasticsearch was established (if applicable).
            - cluster_info (dict, optional): A dictionary containing additional cluster
            information retrieved from Elasticsearch (if desired).
        """

        response = {
            "metadata_store_in_use": "ElasticsearchMetadataStore",
            "host": self.host,
            "user": self.user,
            "running": self.elasticsearch_running,
        }

        # Optionally include connection_established_at if available
        if self.connection_established_at:
            response["connection_established_at"] = self.connection_established_at

        # Optionally include cluster_info if desired and available
        if self.cluster_info:
            response["cluster_info"] = self.cluster_info

        return response

    def load_ca_cert(self) -> None:
        """Loads the CA certificate from the configured path.

        If no path is configured or the file cannot be accessed, sets the
        `ca_cert` attribute to None and logs an informative message.
        """

        try:
            # Construct the path to the CA certificate file
            if not SDP_DATAPRODUCT_API_ELASTIC_HTTP_CA:
                logging.info("No CA certificate file")
                self.ca_cert = None
                return

            ca_cert_path: Path = (
                Path(__file__).parent.parent.parent.parent.parent
                / SDP_DATAPRODUCT_API_ELASTIC_HTTP_CA
            )

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
        logger.info("Connect to Elasticsearch...")

        self.load_ca_cert()

        self.es_client = Elasticsearch(
            hosts=self.host,
            http_auth=(self.user, self.password),
            verify_certs=False,
            ca_certs=self.ca_cert,
        )

        if self.es_client.ping():
            self.connection_established_at = datetime.datetime.now()
            self.elasticsearch_running = True
            self.cluster_info = self.es_client.info()
            logger.info("Connected to Elasticsearch creating default schema...")
            self.create_schema_if_not_existing(index=self.metadata_index)
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
            # No client created, likely not connected before
            return False

        try:
            # Try a simple ping to check connection health
            if self.es_client.ping():
                return True  # Connection already healthy

        except (ConnectionError, TimeoutError) as error:
            logger.error("Connection to Elasticsearch lost: %s", error)

        # Reconnect attempt
        logger.info("Attempting to reconnect to Elasticsearch...")
        self.es_client = None  # Reset client to trigger re-initialization

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
                SDP_DATAPRODUCT_API_ELASTIC_METADATA_SCHEMA_FILE, "r", encoding="utf-8"
            ) as metadata_schema:
                metadata_schema_json = json.load(metadata_schema)
            self.es_client.indices.create(  # pylint: disable=E1123
                index=index, ignore=400, body=metadata_schema_json
            )

    def clear_metadata_indecise(self):
        """Clear out all indices from elasticsearch instance"""
        self.es_client.options(ignore_status=[400, 404]).indices.delete(index=self.metadata_index)
        self.metadata_list = []

    def insert_metadata(self, metadata_file_json):
        """Method to insert metadata into Elasticsearch."""
        # Add new metadata to es
        result = self.es_client.index(index=self.metadata_index, document=metadata_file_json)
        return result

    def search_metadata(
        self,
        start_date: str = "1970-01-01",
        end_date: str = "2100-01-01",
        metadata_key_value_pairs=None,
    ):
        """Metadata Search method"""

        must = []
        meta_data_keys = []
        if metadata_key_value_pairs is not None and len(metadata_key_value_pairs) > 0:
            for key_value in metadata_key_value_pairs:
                if key_value["metadata_key"] != "*" and key_value["metadata_value"] != "*":
                    match_criteria = {
                        "match": {key_value["metadata_key"]: key_value["metadata_value"]}
                    }
                else:
                    match_criteria = {"match_all": {}}

                if match_criteria not in must:
                    must.append(match_criteria)
                    meta_data_keys.append(key_value["metadata_key"])
        else:
            match_criteria = {"match_all": {}}
            must.append(match_criteria)

        parse_valid_date(start_date, DATE_FORMAT)
        parse_valid_date(end_date, DATE_FORMAT)

        parse_valid_date(start_date, DATE_FORMAT)
        parse_valid_date(end_date, DATE_FORMAT)

        query_body = {
            "query": {
                "bool": {
                    "must": must,
                    "filter": [
                        {
                            "range": {
                                "date_created": {
                                    "gte": start_date[0:10],
                                    "lte": end_date[0:10],
                                    "format": "yyyy-MM-dd",
                                }
                            }
                        }
                    ],
                }
            }
        }
        if not self.es_client.ping():
            self.check_and_reconnect()

        resp = self.es_client.search(  # pylint: disable=E1123
            index=self.metadata_index, body=query_body
        )
        all_hits = resp["hits"]["hits"]
        self.metadata_list = []
        for _num, doc in enumerate(all_hits):
            for key, value in doc.items():
                if key == "_source":
                    self.add_dataproduct(
                        metadata_file=value,
                        query_key_list=meta_data_keys,
                    )
        return json.dumps(self.metadata_list)

    def apply_filters(self, data, filters):
        """This is implemented in Elasticsearch."""
        raise NotImplementedError
