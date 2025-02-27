"""Class that mocks the methods of the postgresql.py"""

from datetime import datetime

from ska_dataproduct_api.components.annotations.annotation import DataProductAnnotation


class MockPostgresSQL:
    """Class that mocks the methods of the postgresql.py"""

    def __init__(self) -> None:
        self.annotations_table = []
        self.count = 1

    def save_annotation(self, data_product_annotation: DataProductAnnotation):
        """Save annotation to mock table."""
        if data_product_annotation.annotation_id is None:
            data_product_annotation.annotation_id = self.count
            self.annotations_table.append(data_product_annotation)
            self.count += 1
        else:
            for annotation in self.annotations_table:
                if annotation.annotation_id == data_product_annotation.annotation_id:
                    annotation.timestamp_modified = data_product_annotation.timestamp_modified
                    annotation.user_principal_name = data_product_annotation.user_principal_name
                    annotation.annotation_text = data_product_annotation.annotation_text
                    break

    def retrieve_annotations_by_uid(self, uid: str) -> list:
        """Mocks method to retrieve annotations by uid."""
        annotations = []

        for annotation in self.annotations_table:
            if annotation.data_product_uid == uid:
                annotations.append(annotation)

        return annotations

    def initialize_database(self) -> None:
        """Initialize the mock database with entries."""
        timestamp = datetime.now()

        annotation = DataProductAnnotation(
            data_product_uid="1f8250d0-0e2f-2269-1d9a-ad465ae15d5c",
            annotation_text="test annotation",
            user_principal_name="test.user@skao.int",
            timestamp_created=timestamp,
            timestamp_modified=timestamp,
        )
        annotation_2 = DataProductAnnotation(
            data_product_uid="1f8250d0-0e2f-2269-1d9a-ad465ae15d5c",
            annotation_text="test annotation",
            user_principal_name="test.user@skao.int",
            timestamp_created=timestamp,
            timestamp_modified=timestamp,
        )

        self.save_annotation(annotation)
        self.save_annotation(annotation_2)
