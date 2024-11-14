"""Test for the annotations.py methods."""

from ska_dataproduct_api.components.annotations.annotation import DataProductAnnotation

EXAMPLE_ANNOTATION = '{ \
    "data_product_uuid": "1f8250d0-0e2f-2269-1d9a-ad465ae15d5c", \
    "annotation_text": "Example annotation text message", \
    "user_principal_name": "test.user@skao.int", \
    "timestamp_created": "2024-11-13:14:32:00", \
    "timestamp_modified": "2024-11-13:14:32:00" \
}'


def test_load_annotation_from_json():
    """Test to see if json string loads a class correctly."""
    data_product_annotation = DataProductAnnotation()

    data_product_annotation.load_annotation_from_json(EXAMPLE_ANNOTATION)

    assert data_product_annotation.annotation_id is None
    assert data_product_annotation.data_product_uuid == "1f8250d0-0e2f-2269-1d9a-ad465ae15d5c"
    assert data_product_annotation.annotation_text == "Example annotation text message"
    assert data_product_annotation.user_principal_name == "test.user@skao.int"
    assert data_product_annotation.timestamp_created == "2024-11-13:14:32:00"
    assert data_product_annotation.timestamp_modified == "2024-11-13:14:32:00"
