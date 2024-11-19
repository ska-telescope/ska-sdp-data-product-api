"""Class that mocks the methods of the postgresql.py"""

from datetime import datetime

from ska_dataproduct_api.components.annotations.annotation import DataProductAnnotation


class MockPostgresSQL:
    """Class that mocks the methods of the postgresql.py"""

    def __init__(self) -> None:
        self.annotations_table = []
        self.count = 1

    def insert_annotation(self, data_product_annotation: DataProductAnnotation):
        """Save annotation to mock table."""
        data_product_annotation.annotation_id = self.count
        self.annotations_table.append(data_product_annotation)
        self.count += 1

    def retrieve_annotation_by_id(self, annotation_id: int) -> DataProductAnnotation | None:
        """Mocks method to retrieve annotation by id."""
        for annotation in self.annotations_table:
            if annotation.annotation_id == annotation_id:
                return annotation
        return None

    def retrieve_annotations_by_uuid(self, uuid: str) -> list:
        """Mocks method to retrieve annotations by uuid."""
        annotations = []

        for annotation in self.annotations_table:
            if annotation.data_product_uuid == uuid:
                annotations.append(annotation)

        return annotations

    def initialize_database(self) -> None:
        """Initialize the mock database with entries."""
        timestamp = datetime.now()

        annotation = DataProductAnnotation(
            annotation_id=1,
            data_product_uuid="1f8250d0-0e2f-2269-1d9a-ad465ae15d5c",
            annotation_text="test annotation",
            user_principal_name="test.user@skao.int",
            timestamp_created=timestamp,
            timestamp_modified=timestamp,
        )
        annotation_2 = DataProductAnnotation(
            annotation_id=2,
            data_product_uuid="1f8250d0-0e2f-2269-1d9a-ad465ae15d5c",
            annotation_text="test annotation",
            user_principal_name="test.user@skao.int",
            timestamp_created=timestamp,
            timestamp_modified=timestamp,
        )

        self.insert_annotation(annotation)
        self.insert_annotation(annotation_2)
