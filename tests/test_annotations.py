"""Test for the annotations.py methods."""

from ska_dataproduct_api.components.annotations.annotation import DataProductAnnotation

example_annotation = '{ \
    "data_product_uuid": "test_uuid", \
    "annotation_text": "test annotation", \
    "user_principal_name": "test user", \
    "timestamp_created": "2024-11-13:14:32:00", \
    "timestamp_modified": "2024-11-13:14:32:00" \
}'


def test_load_annotation_from_json():
    dataProductAnnotation = DataProductAnnotation()

    dataProductAnnotation.load_annotation_from_json(example_annotation)

    assert dataProductAnnotation.annotation_id is None
    assert dataProductAnnotation.data_product_uuid == "test_uuid"
    assert dataProductAnnotation.annotation_text == "test annotation"
    assert dataProductAnnotation.user_principal_name == "test user"
    assert dataProductAnnotation.timestamp_created == "2024-11-13:14:32:00"
    assert dataProductAnnotation.timestamp_modified == "2024-11-13:14:32:00"
