import logging
import pathlib
from typing import Any

import yaml
from ska_sdp_dataproduct_metadata import MetaData

from ska_sdp_dataproduct_api.utilities.helperfunctions import get_date_from_name

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
        except FileNotFoundError as e:
            raise FileNotFoundError(
                f"Metadata file not found: {self.data_product_metadata_file_path}"
            ) from e
        except yaml.YAMLError as e:
            raise yaml.YAMLError(
                f"Error parsing YAML file: {self.data_product_metadata_file_path}"
            ) from e

    def get_date_from_metadata(self) -> None:
        """ """
        try:
            self.date_created = get_date_from_name(self.metadata_dict["execution_block"])
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error(
                "Not loading dataproduct due to failure to extract the date from execution block: %s : %s",
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


def load_metadata_file(data_product_file_path: pathlib.Path) -> dict[str, Any]:
    """
    Load metadata from a YAML file.

    Args:
        file_object: An object representing the file to read metadata from.

    Returns:
        A dictionary containing the loaded metadata, or an empty dictionary if an error occurs
        during loading.
    """
    try:
        with open(data_product_file_path, "r", encoding="utf-8") as metadata_yaml_file:
            metadata_yaml_object = yaml.safe_load(metadata_yaml_file)
            return metadata_yaml_object
    except FileNotFoundError as file_not_found_error:
        logger.warning("Metadata file not found: %s", str(data_product_file_path))
        raise file_not_found_error
    except yaml.YAMLError as yaml_error:
        logger.warning("Error while parsing YAML: %s", yaml_error)
        raise yaml_error
    except Exception as exception:
        logger.warning("Unexpected error occurred: %s", exception)
        raise exception
