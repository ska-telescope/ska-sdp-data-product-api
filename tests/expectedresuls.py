#!/usr/bin/env python
# pylint: disable=c0301
"""This file only contains expected results for some functions."""

filelist = {
    "id": "root",
    "name": "test_files",
    "relativefilename": ".",
    "type": "directory",
    "children": [
        {
            "id": 1,
            "name": "product",
            "relativefilename": "product",
            "type": "directory",
            "children": [
                {
                    "id": 2,
                    "name": "eb_id_2",
                    "relativefilename": "product/eb_id_2",
                    "type": "directory",
                    "children": [
                        {
                            "id": 3,
                            "name": "ska-sub-system",  # noqa
                            "relativefilename": "product/eb_id_2/ska-sub-system",  # noqa
                            "type": "directory",
                            "children": [
                                {
                                    "id": 4,
                                    "name": "scan_id_2",
                                    "relativefilename": "product/eb_id_2/ska-sub-system/scan_id_2",  # noqa
                                    "type": "directory",
                                    "children": [
                                        {
                                            "id": 5,
                                            "name": "pb_id_2",
                                            "relativefilename": "product/eb_id_2/ska-sub-system/scan_id_2/pb_id_2",  # noqa
                                            "type": "directory",
                                            "children": [
                                                {
                                                    "id": 6,
                                                    "name": "ska-data-product.yaml",  # noqa
                                                    "relativefilename": "product/eb_id_2/ska-sub-system/scan_id_2/pb_id_2/ska-data-product.yaml",  # noqa
                                                    "type": "file",
                                                },
                                                {
                                                    "id": 7,
                                                    "name": "TestDataFile4.txt",  # noqa
                                                    "relativefilename": "product/eb_id_2/ska-sub-system/scan_id_2/pb_id_2/TestDataFile4.txt",  # noqa
                                                    "type": "file",
                                                },
                                                {
                                                    "id": 8,
                                                    "name": "TestDataFile6.txt",  # noqa
                                                    "relativefilename": "product/eb_id_2/ska-sub-system/scan_id_2/pb_id_2/TestDataFile6.txt",  # noqa
                                                    "type": "file",
                                                },
                                                {
                                                    "id": 9,
                                                    "name": "TestDataFile5.txt",  # noqa
                                                    "relativefilename": "product/eb_id_2/ska-sub-system/scan_id_2/pb_id_2/TestDataFile5.txt",  # noqa
                                                    "type": "file",
                                                },
                                            ],
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                },
                {
                    "id": 10,
                    "name": "eb_id_1",
                    "relativefilename": "product/eb_id_1",
                    "type": "directory",
                    "children": [
                        {
                            "id": 11,
                            "name": "ska-sub-system",
                            "relativefilename": "product/eb_id_1/ska-sub-system",  # noqa
                            "type": "directory",
                            "children": [
                                {
                                    "id": 12,
                                    "name": "scan_id_1",
                                    "relativefilename": "product/eb_id_1/ska-sub-system/scan_id_1",  # noqa
                                    "type": "directory",
                                    "children": [
                                        {
                                            "id": 13,
                                            "name": "pb_id_1",
                                            "relativefilename": "product/eb_id_1/ska-sub-system/scan_id_1/pb_id_1",  # noqa
                                            "type": "directory",
                                            "children": [
                                                {
                                                    "id": 14,
                                                    "name": "TestDataFile2.txt",  # noqa
                                                    "relativefilename": "product/eb_id_1/ska-sub-system/scan_id_1/pb_id_1/TestDataFile2.txt",  # noqa
                                                    "type": "file",
                                                },
                                                {
                                                    "id": 15,
                                                    "name": "TestDataFile3.txt",  # noqa
                                                    "relativefilename": "product/eb_id_1/ska-sub-system/scan_id_1/pb_id_1/TestDataFile3.txt",  # noqa
                                                    "type": "file",
                                                },
                                                {
                                                    "id": 16,
                                                    "name": "ska-data-product.yaml",  # noqa
                                                    "relativefilename": "product/eb_id_1/ska-sub-system/scan_id_1/pb_id_1/ska-data-product.yaml",  # noqa
                                                    "type": "file",
                                                },
                                                {
                                                    "id": 17,
                                                    "name": "TestDataFile1.txt",  # noqa
                                                    "relativefilename": "product/eb_id_1/ska-sub-system/scan_id_1/pb_id_1/TestDataFile1.txt",  # noqa
                                                    "type": "file",
                                                },
                                            ],
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                },
            ],
        }
    ],
}

dataproductlist = {
    "id": "root",
    "name": "Data Products",
    "relativefilename": "",
    "type": "directory",
    "children": [
        {
            "id": 1,
            "name": "pb_id_2",
            "relativefilename": "product/eb_id_2/ska-sub-system/scan_id_2/pb_id_2",  # noqa
            "type": "directory",
            "children": [
                {
                    "id": 2,
                    "name": "ska-data-product.yaml",
                    "relativefilename": "product/eb_id_2/ska-sub-system/scan_id_2/pb_id_2/ska-data-product.yaml",  # noqa
                    "type": "file",
                },
                {
                    "id": 3,
                    "name": "TestDataFile4.txt",
                    "relativefilename": "product/eb_id_2/ska-sub-system/scan_id_2/pb_id_2/TestDataFile4.txt",  # noqa
                    "type": "file",
                },
                {
                    "id": 4,
                    "name": "TestDataFile6.txt",
                    "relativefilename": "product/eb_id_2/ska-sub-system/scan_id_2/pb_id_2/TestDataFile6.txt",  # noqa
                    "type": "file",
                },
                {
                    "id": 5,
                    "name": "TestDataFile5.txt",
                    "relativefilename": "product/eb_id_2/ska-sub-system/scan_id_2/pb_id_2/TestDataFile5.txt",  # noqa
                    "type": "file",
                },
            ],
        },
        {
            "id": 6,
            "name": "pb_id_1",
            "relativefilename": "product/eb_id_1/ska-sub-system/scan_id_1/pb_id_1",  # noqa
            "type": "directory",
            "children": [
                {
                    "id": 7,
                    "name": "TestDataFile2.txt",
                    "relativefilename": "product/eb_id_1/ska-sub-system/scan_id_1/pb_id_1/TestDataFile2.txt",  # noqa
                    "type": "file",
                },
                {
                    "id": 8,
                    "name": "TestDataFile3.txt",
                    "relativefilename": "product/eb_id_1/ska-sub-system/scan_id_1/pb_id_1/TestDataFile3.txt",  # noqa
                    "type": "file",
                },
                {
                    "id": 9,
                    "name": "ska-data-product.yaml",
                    "relativefilename": "product/eb_id_1/ska-sub-system/scan_id_1/pb_id_1/ska-data-product.yaml",  # noqa
                    "type": "file",
                },
                {
                    "id": 10,
                    "name": "TestDataFile1.txt",
                    "relativefilename": "product/eb_id_1/ska-sub-system/scan_id_1/pb_id_1/TestDataFile1.txt",  # noqa
                    "type": "file",
                },
            ],
        },
    ],
}
