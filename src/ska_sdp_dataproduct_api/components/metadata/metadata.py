from ska_sdp_dataproduct_metadata import MetaData
import pathlib
from typing import Any
import yaml
import logging


logger = logging.getLogger(__name__)


def load_metadata_file(file_path: pathlib.Path) -> dict[str, Any]:
    """
    Load metadata from a YAML file.

    Args:
        file_object: An object representing the file to read metadata from.

    Returns:
        A dictionary containing the loaded metadata, or an empty dictionary if an error occurs
        during loading.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as metadata_yaml_file:
            metadata_yaml_object = yaml.safe_load(metadata_yaml_file)
            return metadata_yaml_object
    except FileNotFoundError as file_not_found_error:
        logger.warning("Metadata file not found: %s", str(file_path))
        raise file_not_found_error
    except yaml.YAMLError as yaml_error:
        logger.warning("Error while parsing YAML: %s", yaml_error)
        raise yaml_error
    except Exception as exception:
        logger.warning("Unexpected error occurred: %s", exception)
        raise exception