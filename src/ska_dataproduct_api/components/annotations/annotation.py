"""
Module for handling data product annotations.

This module provides a class for encapsulating and managing annotations
associated with data products. It offers functionalities to validate
annotations linked to a specific data product.

Classes:
    DataProductAnnotations: Encapsulates annotations for a data product.

Functions:
    None
"""

import logging
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
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

    annotation_id: int | None = None
    data_product_uuid: str = None
    annotation_text: str = None
    user_principal_name: str = None
    timestamp_created: datetime = None
    timestamp_modified: datetime = None