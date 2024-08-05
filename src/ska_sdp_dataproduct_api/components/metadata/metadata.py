import logging
import pathlib

import yaml

from ska_sdp_dataproduct_api.utilities.helperfunctions import (
    DataProductMetaData,
    get_date_from_name,
)

logger = logging.getLogger(__name__)


class DataProductMetadata:
    """
    Encapsulates metadata for a data product.

    Attributes:
        data_product_file_path (pathlib.Path): Path to the metadata file.
        metadata_dict (dict): Loaded metadata as a dictionary.
    """

    def __init__(self):
        self.data_product_file_path: pathlib.Path = None
        self.data_product_metadata_file_path: pathlib.Path = None
        self.metadata_dict: dict = None
        self.date_created: str = None

    def load_metadata_from_yaml_file(self, file_path: pathlib.Path) -> dict:
        """
        Loads metadata from a YAML file.

        Args:
            data_product_file_path (pathlib.Path): Path to the metadata file.

        Returns:
            dict: Loaded metadata as a dictionary.

        Raises:
            FileNotFoundError: If the specified file does not exist.
            yaml.YAMLError: If there's an error parsing the YAML file.
        """
        self.data_product_metadata_file_path = file_path
        self.data_product_file_path = self.data_product_metadata_file_path.parent

        try:
            with open(self.data_product_metadata_file_path, "r", encoding="utf-8") as file:
                self.metadata_dict = yaml.safe_load(file)
            return self.metadata_dict
        except FileNotFoundError as error:
            raise FileNotFoundError(
                f"Metadata file not found: {self.data_product_metadata_file_path}"
            ) from error
        except yaml.YAMLError as error:
            raise yaml.YAMLError(
                f"Error parsing YAML file: {self.data_product_metadata_file_path}"
            ) from error

    def load_metadata_from_class(self, metadata: DataProductMetaData) -> dict:
        """
        Loads metadata from a DataProductMetaData class.

        Args:
            data_product_file_path (pathlib.Path): Path to the metadata file.

        Returns:
            dict: Loaded metadata as a dictionary.

        Raises:
            FileNotFoundError: If the specified file does not exist.
            yaml.YAMLError: If there's an error parsing the YAML file.
        """
        self.metadata_dict = yaml.safe_load(metadata)
        return self.metadata_dict

    def get_date_from_metadata(self) -> None:
        """ """
        try:
            self.date_created = get_date_from_name(self.metadata_dict["execution_block"])
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error(
                "Not loading dataproduct due to failure to extract the date from execution block: \
                    %s : %s",
                str(self.metadata_dict["execution_block"]),
                exception,
            )

    def append_metadata_file_details(self) -> None:
        """ """

        self.metadata_dict.update(
            {
                "date_created": self.date_created,
                "dataproduct_file": str(self.data_product_file_path),
                "metadata_file": str(self.data_product_metadata_file_path),
            }
        )


def load_and_append_metadata(data_product_metadata_file_path: pathlib.Path):
    """ """
    data_product_metadata_instance: DataProductMetadata = load_metadata(
        data_product_metadata_file_path
    )
    append_metadata(data_product_metadata_instance)
    return data_product_metadata_instance


def load_metadata(data_product_metadata_file_path: pathlib.Path):
    """ """
    data_product_metadata_instance: DataProductMetadata = DataProductMetadata()
    try:
        data_product_metadata_instance.load_metadata_from_yaml_file(
            file_path=data_product_metadata_file_path
        )
        return data_product_metadata_instance
    except Exception as error:
        logger.error(
            "Failed to load and append metadata for %s. Error: %s",
            data_product_metadata_file_path,
            error,
        )


def append_metadata(data_product_metadata_instance: DataProductMetadata):
    """ """
    try:
        data_product_metadata_instance.get_date_from_metadata()
        data_product_metadata_instance.append_metadata_file_details()
        return data_product_metadata_instance
    except Exception as error:
        logger.error(
            "Failed to append metadata, error: %s",
            error,
        )
