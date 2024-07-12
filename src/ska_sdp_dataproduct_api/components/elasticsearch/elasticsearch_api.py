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
    METADATA_ES_SCHEMA_FILE,
    SDP_DATAPRODUCT_API_ELASTIC_HTTP_CA,
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
        self.es_client = None
        self.elasticsearch_running: bool = False
        self.elasticsearch_version: str = ""
        self.connection_established_at = ""
        self.connection_error = ""
        self.cluster_info: dict = {}

        self.load_ca_cert()

        # if self.host:
        #     self.connect()
        # self.get_elasticsearch_version()
        # logger.info(self.status())

    def status(self) -> dict:
        """
        Retrieves the current status of the Elasticsearch connection and metadata store.

        This method returns a dictionary containing the following information:

        * `metadata_store_in_use`: The type of metadata store being used (e.g.,
        "ElasticsearchMetadataStore").
        * `host`: The hostname or IP address of the Elasticsearch instance.
        * `port`: The port number on which Elasticsearch is listening.
        * `user`: The username used for authentication with Elasticsearch (if applicable).
        * `running`: A boolean indicating whether Elasticsearch is currently running.
        * `elasticsearch_version` : The version of Elasticsearch being used.
        * `connection_established_at` : A timestamp representing when a connection was
        last established with Elasticsearch.

        Returns:
            A dictionary containing the current status information.
        """

        return {
            "metadata_store_in_use": "ElasticsearchMetadataStore",
            "host": self.host,
            "user": self.user,
            "running": self.elasticsearch_running,
            "connection_established_at": self.connection_established_at,
            "cluster_info": self.cluster_info,
        }

    def get_elasticsearch_version(self) -> None:
        """
        Retrieves the version of the Elasticsearch instance the provided Elasticsearch client
        is connected to.

        Args:
            None

        Returns:
            None.
        """
        self.cluster_info = self.es_client.info()

    @property
    def es_search_enabled(self):
        """Generic interface to verify there is a Elasticsearch backend"""
        return True

    def load_ca_cert(self) -> None:
        """Attempts to load the CA certificate from a file.

        If the file exists and can be read, sets the `self.ca_cert` attribute to the
        certificate content as a string. Otherwise, sets `self.ca_cert` to None.

        Logs informational messages about the attempt to load the certificate.

        Args:
            self (object): The instance of the class containing the `ca_cert` attribute.

        Returns:
            None
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

        self.es_client = Elasticsearch(
            hosts=self.host,
            http_auth=(self.user, self.password),
            verify_certs=False,
            ca_certs=self.ca_cert,
        )
        if self.es_client.ping():
            self.connection_established_at = datetime.datetime.now()
            self.elasticsearch_running = True
            self.get_elasticsearch_version()
            logger.info("Connected to Elasticsearch creating default schema...")
            self.create_schema_if_not_existing(index=self.metadata_index)
            return True
        return False

    def create_schema_if_not_existing(self, index: str):
        """Method to create a Schema from schema and index if it does not yet
        exist."""
        try:
            _ = self.es_client.indices.get(index=index)
        except elasticsearch.NotFoundError:
            with open(METADATA_ES_SCHEMA_FILE, "r", encoding="utf-8") as metadata_schema:
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
            return json.dumps({"Error": "Elasticsearch unavailable"})

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
