"""
Module for handling data product annotations.

This module provides a class for encapsulating and managing annotations associated with data products.
It offers functionalities to validate annotations linked to a specific data product.

Classes:
    DataProductAnnotations: Encapsulates annotations for a data product.

Functions:
    None
"""

import datetime
import json
import logging

logger = logging.getLogger(__name__)

# pylint: disable=too-many-instance-attributes


class DataProductAnnotation:
    """
    Encapsulates annotations for a data product.

    Attributes:
        annotation_id (str): Unique id of annotation.
        data_product_uuid (str): UUID of associated data product.
        annotation_text (str): Text content of annotation.
        user_principal_name (str): User who created annotation.
        timestamp_created (str): Date and time when annotation was created.
        timestamp_modified (str): Date and time when annotation was modified.
    """

    def __init__(self):
        self.annotation_id: str  = None
        self.data_product_uuid: str = None
        self.annotation_text: str = None
        self.user_principal_name: str = None
        self.timestamp_created: str = None
        self.timestamp_modified: str = None


    def load_annotation_from_json(self, annotation: str) -> None:
        """
        Loads annotation from a json string.

        Args:
            annotation: The json string of the annotation data.

        Returns:
            None.
        """
        
        annotation = json.loads(annotation)

        self.annotation_id = annotation["annotation_id"]
        self.data_product_uuid = annotation["data_product_uuid"]
        self.annotation_text = annotation["annotation_text"]
        self.user_principal_name = annotation["user_principal_name"]
        self.timestamp_created = annotation["timestamp_created"]
        self.timestamp_modified = annotation["timestamp_modified"]