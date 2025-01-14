"""This module contains the methods used for loading data products directly from an accessible
Persistent Volume"""
import logging
import pathlib
from dataclasses import dataclass
from datetime import datetime, timezone

from ska_dataproduct_api.configuration.settings import (
    METADATA_FILE_NAME,
    PERSISTENT_STORAGE_PATH,
    PVCNAME,
)
from ska_dataproduct_api.utilities.helperfunctions import verify_persistent_storage_file_path

# pylint: disable=too-few-public-methods


logger = logging.getLogger(__name__)


@dataclass
class PVDataProduct:
    """
    This class contains the information related to the data product saved on a PV
    """

    def __init__(self):
        self.path: pathlib.Path = None
        self.size_on_disk: int = None
        self.timestamp_modified: datetime = None

    def get_folder_size(self, folder_path: pathlib.Path) -> int:
        """
        Calculates the size on disk of the given folder in bytes.

        Args:
            folder_path: The path to the folder containing the data product.

        Returns:
            The size of the folder in bytes.
        """

        total_size = 0
        for data_product in pathlib.Path(folder_path).rglob("*"):
            if data_product.is_file():
                try:
                    total_size += data_product.stat().st_size
                except OSError:
                    logger.error(
                        "Error accessing %s, could not calculate product size", data_product
                    )

        logger.debug("Size on disk %s for %s", total_size, folder_path)
        return total_size

    def get_latest_modification_time(self, folder_path: pathlib.Path) -> datetime:
        """
        Finds the latest modification time among all files and subfolders within the given
        folder.

        Args:
            folder_path: The path to the folder containing the data product to check.

        Returns:
            datetime.datetime object representing the latest modification time,
            or None if no files or folders are found.
        """

        latest_time = None

        for data_product in folder_path.iterdir():
            try:
                modified_time = datetime.fromtimestamp(data_product.stat().st_mtime)
                if not latest_time or modified_time > latest_time:
                    latest_time = modified_time
            except OSError:
                logger.error(
                    "Error accessing %s, could not calculate product modified_time", data_product
                )

        logger.debug("Date modified on disk %s for %s", str(latest_time), folder_path)
        return latest_time

    def load_product_details(self) -> None:
        """
        Updates the derived metadata of a data product.

        This method calculates and sets the size on disk and
        latest modification timestamp for the given data product.

        Raises:
            FileNotFoundError: If the directory containing the data product does not exist.
        """
        try:
            self.size_on_disk = self.get_folder_size(self.path.parent)
            self.timestamp_modified = self.get_latest_modification_time(self.path.parent)
        except FileNotFoundError as error:
            logger.error("Load of product details failed due to error: %s", error)


class PVIndex:
    """
    This index contains a dictionary of all the PVDataProduct found on a PV.

    Attributes:
        dict_of_data_products_on_pv:
            A dictionary mapping data product names (str) to PVDataProduct objects.
        number_of_date_products_on_pv:
            The total number of data products in the index.
        time_of_last_index_run:
            The timestamp of the last time the PV was indexed.
        reindex_running:
            A boolean flag indicating whether a re-indexing operation is currently in progress.
        index_time_modified:
            The timestamp of the last modification to the index.
    """

    def __init__(self):
        self.dict_of_data_products_on_pv: dict[str, PVDataProduct] = {}
        self.number_of_date_products_on_pv: int = None
        self.time_of_last_index_run: datetime = None
        self.reindex_running: bool = False
        self.index_time_modified: datetime = None


class PVInterface:
    """
    Loads data products from a persistent volume and stores metadata.
    """

    def __init__(self):
        self.pv_available: bool = False
        self.pv_name: str = PVCNAME
        self.data_product_root_directory: pathlib.Path = PERSISTENT_STORAGE_PATH
        self.pv_index: PVIndex = PVIndex()

    def status(self) -> dict:
        """
        Retrieves the current status of the persistent volume.

        Returns:
            A dictionary containing the current status information.
        """
        return {
            "data_source": "Persistent volume",
            "pv_name": self.pv_name,
            "data_product_root_directory": self.data_product_root_directory,
            "pv_available": self.pv_available,
            "number_of_date_products_on_pv": self.pv_index.number_of_date_products_on_pv,
            "time_of_last_index_run": self.pv_index.time_of_last_index_run,
            "reindex_running": self.pv_index.reindex_running,
            "index_time_modified": self.pv_index.index_time_modified,
        }

    def index_all_data_product_files_on_pv(self) -> None:
        """This method indexes all data product files found on the persistent volume (PV).

        The method first verifies that the `data_product_root_directory` points to a valid
        directory. If not, it raises a `ValueError` with a descriptive message.

        Then, it sets the `pv_available` attribute to `True` and the `reindex_running`
        attribute of the `pv_index` object to `True` to indicate that a reindexing operation
        is in progress. It logs a message to inform the user about the start of the indexing
        process.

        The method iterates through all files in the `data_product_root_directory`.
        For each metadata file found, that folder is considered a data product file, then:

            * If the file path is not already present in the `dict_of_data_products_on_pv`
                dictionary of the `pv_index` object, a new `PVDataProduct` object is created and
                added to the index.
            * If the file path is already present in the dictionary, its details will be reloaded.

        Finally, the `time_of_last_index_run` attribute of the `pv_index` object is set to the
        current UTC time, and the `reindex_running` attribute is set back to `False` to indicate
        that the re-indexing operation is complete.
        """

        if not verify_persistent_storage_file_path(self.data_product_root_directory):
            self.pv_available = False
            raise ValueError(
                f"Invalid data_product_root_directory: {self.data_product_root_directory}"
            )  # TODO - Change the function above to raise a error and give better desc.

        self.pv_available = True
        self.pv_index.reindex_running = True
        logger.info(
            "Indexing data product files on the %s PV in the data product root directory: %s",
            self.pv_name,
            self.data_product_root_directory,
        )

        for data_product_file_path in self.data_product_root_directory.rglob(METADATA_FILE_NAME):
            if str(data_product_file_path) not in self.pv_index.dict_of_data_products_on_pv:
                pv_data_product = PVDataProduct()
                pv_data_product.path = data_product_file_path
                self.pv_index.dict_of_data_products_on_pv[
                    str(data_product_file_path)
                ] = pv_data_product
                self.pv_index.number_of_date_products_on_pv = len(
                    self.pv_index.dict_of_data_products_on_pv
                )
            else:
                pv_data_product: PVDataProduct = self.pv_index.dict_of_data_products_on_pv[
                    str(data_product_file_path)
                ]
                logger.debug(
                    "This item was already loaded, details updated: %s",
                    str(data_product_file_path),
                )
            pv_data_product.load_product_details()
            self.pv_index.index_time_modified = datetime.now(tz=timezone.utc)

        self.pv_index.time_of_last_index_run = datetime.now(tz=timezone.utc)
        self.pv_index.reindex_running = False
