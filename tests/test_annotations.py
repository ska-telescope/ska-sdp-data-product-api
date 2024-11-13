"""Test for the annotations.py methods."""

from ska_dataproduct_api.components.annotations.annotation import DataProductAnnotation

EXAMPLE_ANNOTATION = '{ \
    "data_product_uuid": "test_uuid", \
    "annotation_text": "test annotation", \
    "user_principal_name": "test user", \
    "timestamp_created": "2024-11-13:14:32:00", \
    "timestamp_modified": "2024-11-13:14:32:00" \
}'


def test_load_annotation_from_json():
    """Test to see if json string loads a class correctly."""
    data_product_annotation = DataProductAnnotation()

    data_product_annotation.load_annotation_from_json(EXAMPLE_ANNOTATION)

    assert data_product_annotation.annotation_id is None
    assert data_product_annotation.data_product_uuid == "test_uuid"
    assert data_product_annotation.annotation_text == "test annotation"
    assert data_product_annotation.user_principal_name == "test user"
    assert data_product_annotation.timestamp_created == "2024-11-13:14:32:00"
    assert data_product_annotation.timestamp_modified == "2024-11-13:14:32:00"
