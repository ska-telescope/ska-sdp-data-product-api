SKA Data Product API Overview
=============================

This API is used to provide a list of SKA Data Products (files) that are hosted at a configurable storage location <PERSISTENT_STORAGE_PATH>.


Automatic API Documentation
---------------------------

Detailed interactive documentation for the API is available through Swagger UI. Access it at *http://<API URL>/docs* while running the application.


Basic Usage
-----------

.. note:: This API is typically deployed behind a secure layer that encrypts communication (TLS/SSL) and likely requires user authentication through a separate system. When accessing the API through a browser, both the encryption and the authentication will be handled by the browser, but direct access with scripts or notebooks to the API from outside the cluster is currently not supported. To make use of this API directly, the user need to access it from within the cluster where it is hosted.

.. note:: If a data product have been assigned a context.access_group, then that data product will not be available/listed when accessing the api directly with scripts or notebooks. This is due the required access token of an authenticated user that is not available in this mode of operation.

Status endpoint
~~~~~~~~~~~~~~~

Verify the API's status by sending a GET request to the /status endpoint. The response will indicate the API's operational state.

*Request*

.. code-block:: bash

    GET /status

*Response*

.. code-block:: bash


    {
        "api_running": true,
        "api_version": "0.8.0",
        "startup_time": "2024-08-06T21:59:18.333369",
        "last_metadata_update_time": "2024-08-06T21:59:18.333359",
        "metadata_store_status": {
            "store_type": "Persistent PosgreSQL metadata store",
            "host": "localhost",
            "port": 5432,
            "user": "postgres",
            "running": true,
            "schema": "sdp_sdp_dataproduct_dashboard_dev",
            "table_name": "localhost_sdp_dataproduct_dashboard_dev_v1",
            "number_of_dataproducts": 10,
            "postgresql_version": "PostgreSQL 16.3 on x86_64-pc-linux-musl, compiled by gcc (Alpine 13.2.1_git20240309) 13.2.1 20240309, 64-bit"
        },
        "search_store_status": {
            "metadata_store_in_use": "ElasticsearchMetadataStore",
            "url": "https://localhost:9200",
            "user": "elastic",
            "running": true,
            "connection_established_at": "2024-08-06T21:59:18.210017",
            "number_of_dataproducts": 10,
            "indices": "ska-dp-dataproduct-localhost-dev-v1",
            "cluster_info": {
                "name": "46f82bbc7307",
                "cluster_name": "docker-cluster",
                "cluster_uuid": "5nqaD334QZuVZjjMYAFCmQ",
                "version": {
                    "number": "8.14.2",
                    "build_flavor": "default",
                    "build_type": "docker",
                    "build_hash": "2afe7caceec8a26ff53817e5ed88235e90592a1b",
                    "build_date": "2024-07-01T22:06:58.515911606Z",
                    "build_snapshot": false,
                    "lucene_version": "9.10.0",
                    "minimum_wire_compatibility_version": "7.17.0",
                    "minimum_index_compatibility_version": "7.0.0"
                },
                "tagline": "You Know, for Search"
            }
        }
    }



Search endpoint
~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The data product metadata store can be re-indexed but making a get request to the /reindexdataproducts endpoint. This allows the user to update the metadata store if data products or metadata have been added or changed on the data volume since the previous indexing.

*Request*

.. code-block:: bash

    GET /reindexdataproducts

*Response*

.. code-block:: bash

    "Metadata is set to be re-indexed"

Download data product endpoint
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
        "uuid": "a0a2a10f-e382-31ba-0949-9a79204dfcad"
    }

*Response*

A stream response of the specified data product as a tar archive

.. note:: A data product with an execution block id can contain 'sub' data products, that is defined by another metadata file. If the user request to download the product with the execution_block, all the product of that execution block id will be downloaded.

Retrieve metadata of a data product endpoint
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sending a post request to the /dataproductmetadata endpoint will return a Response with the metadata of the data product in a JSON format.

The body of the post request must contain the UUID of the data product. 

For example, the post request body:

*Request*

.. code-block:: bash

    POST /dataproductmetadata

*Body*

.. code-block:: bash

    {
        "uuid": "6a11ddaa-6b45-6759-47e7-a5abd5105b0e"
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
        "uuid": "6a11ddaa-6b45-6759-47e7-a5abd5105b0e"
    }

Ingest new data product
~~~~~~~~~~~~~~~~~~~~~~~

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
            "uuid": "f0b91aa5-d54b-e11a-410e-3e4edca5346f"
        },
        201
    ]

Ingest new metadata endpoint
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
            "uuid": "1f8250d0-0e2f-2269-1d9a-ad465ae15d5c"
        },
        201
    ]

Annotation POST endpoint
~~~~~~~~~~~~~~~~~~~~~~~~

Annotations are used to add notes to specific data products and are stored in the metadata store in a separate table.

Sending a POST request to the /annotation endpoint will parse the supplied JSON data as data product annotation, and add the annotation to the Postgres database.

For example, the POST request body:

*Request*

.. code-block:: bash

    POST /annotation

*Body*

.. code-block:: bash

    { 
        "data_product_uuid": "1f8250d0-0e2f-2269-1d9a-ad465ae15d5c",
        "annotation_text": "Example annotation text message.",
        "user_principal_name": "test.user@skao.int",
        "timestamp_created": "2024-11-13:14:32:00",
        "timestamp_modified": "2024-11-13:14:32:00"
    }

*Response*

.. code-block:: bash

    [
        200
    ]

Annotation GET endpoint
~~~~~~~~~~~~~~~~~~~~~~~~

Sending a GET request to the /annotation endpoint will retrieve the annotation linked to the specified id.

*Request*

.. code-block:: bash

    GET /annotation/21

*Response*

.. code-block:: bash

    [
        {
            "annotation_id": 21, 
            "data_product_uuid": "1f8250d0-0e2f-2269-1d9a-ad465ae15d5c",
            "annotation_text": "Example annotation text message.",
            "user_principal_name": "test.user@skao.int",
            "timestamp_created": "2024-11-13:14:32:00",
            "timestamp_modified": "2024-11-13:14:32:00"
        },
        200
    ]

Annotations GET endpoint
~~~~~~~~~~~~~~~~~~~~~~~~

Sending a GET request to the /annotations endpoint will retrieve a list of the annotations linked to the specified data product uuid.

*Request*

.. code-block:: bash

    GET /annotations/1f8250d0-0e2f-2269-1d9a-ad465ae15d5c

*Response*

.. code-block:: bash

    [
        [
            {
                "annotation_id": 21, 
                "data_product_uuid": "1f8250d0-0e2f-2269-1d9a-ad465ae15d5c",
                "annotation_text": "Example annotation text message.",
                "user_principal_name": "test.user@skao.int",
                "timestamp_created": "2024-11-13:14:32:00",
                "timestamp_modified": "2024-11-13:14:32:00"
            },
            {
                "annotation_id": 36, 
                "data_product_uuid": "1f8250d0-0e2f-2269-1d9a-ad465ae15d5c",
                "annotation_text": "Example annotation text message.",
                "user_principal_name": "test.user@skao.int",
                "timestamp_created": "2024-11-13:14:45:00",
                "timestamp_modified": "2024-11-13:14:45:00"
            }
        ],
        200
    ]

API User
--------

The Data Product Dashboard (DPD) will usually be used via the GUI, for certain systems and users direct access to the API may be useful and desired. This guide will help users get up to speed with the Data Product Dashboard API.

To access the API from within the cluster, you can use the BASE_URL http://<service name>.<namespace>:<port>

Searching for and downloading Data Products
When searching for data products it is important to ensure that the most recent data is available. The cached map for the in-memory solution periodically checks for new product that are available, but there is a way to manually ensure this, namely through the update command:

.. code-block:: python

    import requests
    BASE_URL = "http://<service name>.<namespace>:<port>"
    response = requests.get(f"{BASE_URL}/reindexdataproducts")
    print(response.status_code)
    >>> 202


Searching for a specific product can be done by date or by other metadata fields available.

.. code-block:: python

    data = {
        "start_date": "2001-12-12",
        "end_date": "2032-12-12",
        "key_value_pairs": ["execution_block:eb-m001-20191031-12345"]
    }
    response = requests.post(f"{BASE_URL}/dataproductsearch", json=data)
    products = response.json()
    print(products)
    >>> [{'execution_block': 'eb-m001-20191031-12345', 'date_created': '2019-10-31', 'dataproduct_file': 'eb-m001-20221212-12345', 'metadata_file': 'eb-m001-20221212-12345/ska-data-product.yaml', 'interface': 'http://schema.skao.int/ska-data-product-meta/0.1', 'context.observer': 'AIV_person_1', 'context.intent': 'Experimental run as part of XYZ-123', 'context.notes': 'Running that signal from XX/YY/ZZ through again, things seem a bit flaky', 'config.processing_block': 'pb-m001-20191031-12345', 'config.processing_script': 'receive', 'config.image': 'artefact.skao.int/ska-docker/vis_receive', 'config.version': '0.1.3', 'config.commit': '516fb5a693f9dc9aff5d46192f4e055b582fc025', 'config.cmdline': '-dump /product/eb-m001-20191031-12345/ska-sdp/pb-m001-20191031-12345/vis.ms', 'id': 2}]


Identify the product that should be downloaded and select it. This will be one of the products in the list of returned products:

.. code-block:: python

    product = products[0]

The download endpoint returns a response that can be used to stream the data product into a tarball. This can saved into a local file:

.. code-block:: python

    data = {"execution_block": "eb-notebook-20240201-54576"}

or

.. code-block:: python

    data = {"uuid": "a0a2a10f-e382-31ba-0949-9a79204dfcad"}

    response = requests.post(f"{BASE_URL}/download", json=data)

    with open('product.tar', 'wb') as fd:
        for chunk in response.iter_content(chunk_size=4096):
            fd.write(chunk)

The tarball can then be opened using standard operation software. On linux this can be done using

.. code-block:: bash

    $ tar -xvf ./product.tar
    eb-notebook-20240201-54576/
