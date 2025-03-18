Basic Usage
===========

The Data Product API is used to provide a list of SKA Data Products (files) that are hosted at a configurable storage location <PERSISTENT_STORAGE_PATH>.
The following sections list some basic usage commands of the API.

.. note:: This API is typically deployed behind a secure layer that encrypts communication (TLS/SSL) and likely requires user authentication through a separate system. When accessing the API through a browser, both the encryption and the authentication will be handled by the browser, but direct access with scripts or notebooks to the API from outside the cluster is currently not supported. To make use of this API directly, the user need to access it from within the cluster where it is hosted.

.. note:: If a data product have been assigned a context.access_group, then that data product will not be available/listed when accessing the api directly with scripts or notebooks. This is due the required access token of an authenticated user that is not available in this mode of operation.

Status endpoint
---------------

Verify the API's status by sending a GET request to the /status endpoint. The response will indicate the API's operational state.

*Request*

.. code-block:: bash

    GET /status

*Example Response*

.. code-block:: bash


    {
        "api_running": true,
        "api_version": "0.13.0",
        "startup_time": "2025-01-27T07:28:50.809548+00:00",
        "indexing": false,
        "indexing_timestamp": "2025-01-27T07:28:50.809541+00:00",
        "self.pv_interface_status": {
            "data_source": "Persistent volume",
            "pv_name": "None (using local test data)",
            "data_product_root_directory": "tests/test_files/product",
            "pv_available": true,
            "number_of_date_products_on_pv": 18,
            "time_of_last_index_run": "2025-01-27T07:28:50.869054+00:00",
            "reindex_running": false,
            "index_time_modified": "2025-01-27T07:28:50.866493+00:00"
        },
        "metadata_store_status": {
            "store_type": "Persistent PosgreSQL metadata store",
            "db_status": {
                "host": "localhost",
                "port": 5432,
                "user": "postgres",
                "configured": true,
                "running": true,
                "dbname": "postgres",
                "schema": "sdp_sdp_dataproduct_dashboard_dev"
            },
            "running": true,
            "last_metadata_update_time": "2025-01-27T09:28:51.134177",
            "science_metadata_table_name": "data_products_metadata_v2",
            "annotations_table_name": "data_products_annotations_v1",
            "number_of_dataproducts": 16
        },
        "search_store_status": {
            "metadata_store_in_use": "PGSearchStore"
        }
    }



Search endpoint
---------------

Use the search endpoint to query the data products. You can specify a time range and key-value pairs to filter the results. The response prioritizes products within the timeframe that best match your criteria.

*Request*

.. code-block:: bash

    POST /dataproductsearch

*Body*

.. code-block:: bash

    {
        "start_date": "2000-12-12",
        "end_date": "2032-12-12",
        "key_value_pairs": ["execution_block:eb-m005-20231031-12345"]
    }

*Response*

.. code-block:: bash

    [
        {
            "execution_block": "eb-m005-20231031-12345",
            "date_created": "2023-10-31",
            "dataproduct_file": "eb-m005-20231031-12345",
            "metadata_file": "eb-m005-20231031-12345/ska-data-product.yaml",
            "config.cmdline": "-dump /product/eb-m004-20191031-12345/ska-sdp/pb-m004-20191031-12345/vis.ms",
            ...
            "obscore.instrument_name": "SKA-LOW",
            "id": 6
        }
    ]

Re-index data products endpoint
-------------------------------

The data product metadata store can be re-indexed but making a get request to the /reindexdataproducts endpoint. This allows the user to update the metadata store if data products or metadata have been added or changed on the data volume since the previous indexing.

*Request*

.. code-block:: bash

    GET /reindexdataproducts

*Response*

.. code-block:: bash

    "Metadata is set to be re-indexed"

Download data product endpoint
------------------------------

Sending a post request to the download endpoint will return a stream response of the specified data product as a tar archive.

The body of the post request must contain the execution block id or the UUID of the data product you want to download. 


*Request*

.. code-block:: bash

    POST /download

*Body*

.. code-block:: bash

    {
        "execution_block": "eb-test-20200325-00001"
    }

or 

.. code-block:: bash

    {
        "uid": "a0a2a10f-e382-31ba-0949-9a79204dfcad"
    }

*Response*

A stream response of the specified data product as a tar archive

.. note:: A data product with an execution block id can contain 'sub' data products, that is defined by another metadata file. If the user request to download the product with the execution_block, all the product of that execution block id will be downloaded.

Retrieve metadata of a data product endpoint
--------------------------------------------

Sending a post request to the /dataproductmetadata endpoint will return a Response with the metadata of the data product in a JSON format.

The body of the post request must contain the UUID of the data product. 

For example, the post request body:

*Request*

.. code-block:: bash

    POST /dataproductmetadata

*Body*

.. code-block:: bash

    {
        "uid": "6a11ddaa-6b45-6759-47e7-a5abd5105b0e"
    }

*Response*

.. code-block:: bash

    {
        "interface": "http://schema.skao.int/ska-data-product-meta/0.1",
        "execution_block": "eb-m005-20231031-12345",
        "context": {
            "observer": "AIV_person_1",
            "intent": "Experimental run as part of XYZ-123",
            "notes": "Running that signal from XX/YY/ZZ through again, things seem a bit flaky"
        },
        "config": {
            "processing_block": "pb-m004-20191031-12345",
            ...
        },
        "files": [
            {
                "crc": "2a890fbe",
                ...
            }
        ],
        "obscore": {
            "access_estsize": 1,
            "dataproduct_type": "MS",
            "calib_level": 0,
            ...
        },
        "date_created": "2023-10-31",
        "dataproduct_file": "tests/test_files/product/eb-m005-20231031-12345",
        "metadata_file": "tests/test_files/product/eb-m005-20231031-12345/ska-data-product.yaml",
        "uid": "6a11ddaa-6b45-6759-47e7-a5abd5105b0e"
    }

Ingest new data product
-----------------------

Sending a POST request to the /ingestnewdataproduct endpoint will load and parse a file at the supplied filename, and add the data product to the metadata store.

*Request*

.. code-block:: bash

    POST /ingestnewdataproduct

*Body*

.. code-block:: bash

    {
        "execution_block": "eb-test-20200325-00001",
        "relativePathName": "product/eb-test-20200325-00001"
    }

*Response*

.. code-block:: bash

    [
        {
            "status": "success",
            "message": "New data product received and search store index updated",
            "uid": "f0b91aa5-d54b-e11a-410e-3e4edca5346f"
        },
        201
    ]

Ingest new metadata endpoint
----------------------------

.. note:: In this release, ingested metadata is not persistently stored. This means any data you add will be cleared when the API restarts. This functionality will be changed in future releases.

Sending a POST request to the /ingestnewmetadata endpoint will parse the supplied JSON data as data product metadata, and add the data product to the metadata store.

For example, the POST request body:

*Request*

.. code-block:: bash

    POST /ingestnewmetadata

*Body*

.. code-block:: bash

    {
        "interface": "http://schema.skao.int/ska-data-product-meta/0.1",
        "execution_block": "eb-test-20240806-99999",
        "context": {
            "observer": "REST ingest",
            "intent": "",
            "notes": ""
        },
        "config": {
            "processing_block": "",
            "processing_script": "",
            "image": "",
            "version": "",
            "commit": "",
            "cmdline": ""
        },
        "files": [],
        "obscore": {
            "access_estsize": 0,
            "access_format": "application/unknown",
            "access_url": "0",
            "calib_level": 0,
            "dataproduct_type": "MS",
            "facility_name": "SKA",
            "instrument_name": "SKA-LOW",
            "o_ucd": "stat.fourier",
            "obs_collection": "Unknown",
            "obs_id": "eb-test-20240806-99999",
            "obs_publisher_did": "",
            "pol_states": "XX/XY/YX/YY",
            "pol_xel": 0,
            "s_dec": 0,
            "s_ra": 0.0,
            "t_exptime": 5.0,
            "t_max": 57196.962848574476,
            "t_min": 57196.96279070411,
            "t_resolution": 0.9,
            "target_name": ""
        }
    }

*Response*

.. code-block:: bash

    [
        {
            "status": "success",
            "message": "New data product metadata received and search store index updated",
            "uid": "1f8250d0-0e2f-2269-1d9a-ad465ae15d5c"
        },
        201
    ]

Annotation POST endpoint
------------------------

.. note:: Annotation functionality is only available if the API is running with a PostgreSQL persistent metadata store.

Annotations are used to add notes to specific data products and are stored in the metadata store in a separate table.

Sending a POST request to the /annotation endpoint will parse the supplied JSON data as data product annotation, and add the annotation to the Postgres database.
This method can be used to create a data annotation or update and existing data annotation. The method used depends on the existence of the annotation_id.

For example, the POST request body for a create request:

*Request*

.. code-block:: bash

    POST /annotation

*Body*

.. code-block:: bash

    { 
        "data_product_uid": "1f8250d0-0e2f-2269-1d9a-ad465ae15d5c",
        "annotation_text": "Example annotation text message.",
        "user_principal_name": "test.user@skao.int",
        "timestamp_created": "2024-11-13T14:32:00",
        "timestamp_modified": "2024-11-13T14:32:00"
    }

*Response*

.. code-block:: bash

    [
        {
            "status": "success",
            "message": "New Data Annotation received and successfully saved."
        },
        201
    ]

An example of a POST request body for an update request:

*Request*

.. code-block:: bash

    POST /annotation

*Body*

.. code-block:: bash

    { 
        "annotation_text": "Example annotation text message.",
        "user_principal_name": "test.user@skao.int",
        "timestamp_modified": "2024-11-13T14:32:00",
        "annotation_id": 23
    }

*Response*

.. code-block:: bash

    [
        {
            "status": "success",
            "message": "Data Annotation received and updated successfully."
        },
        200
    ]

An example of a response when PostgresSQL is not available:

*Response*

.. code-block:: bash

    [
        {
            "status": "Received but not processed",
            "message": "PostgresSQL is not available, cannot access data annotations.",
        },
        202
    ]


Annotations GET endpoint
------------------------

.. note:: Annotation functionality is only available if the API is running with a PostgreSQL persistent metadata store.

Sending a GET request to the /annotations endpoint will retrieve a list of the annotations linked to the specified data product uid.
If PostgreSQL is not available, an status code of 202 will be received.

*Request*

.. code-block:: bash

    GET /annotations/1f8250d0-0e2f-2269-1d9a-ad465ae15d5c

*Response*

.. code-block:: bash

    [
        [
            {
                "annotation_id": 21, 
                "data_product_uid": "1f8250d0-0e2f-2269-1d9a-ad465ae15d5c",
                "annotation_text": "Example annotation text message.",
                "user_principal_name": "test.user@skao.int",
                "timestamp_created": "2024-11-13:14:32:00",
                "timestamp_modified": "2024-11-13T14:32:00"
            },
            {
                "annotation_id": 36, 
                "data_product_uid": "1f8250d0-0e2f-2269-1d9a-ad465ae15d5c",
                "annotation_text": "Example annotation text message.",
                "user_principal_name": "test.user@skao.int",
                "timestamp_created": "2024-11-13:14:45:00",
                "timestamp_modified": "2024-11-13T14:45:00"
            }
        ],
        200
    ]
    

An example of a response when PostgresSQL is not available:

*Response*

.. code-block:: bash

    [
        {
            "status": "Received but not processed",
            "message": "PostgresSQL is not available, cannot access data annotations.",
        },
        202
    ]


