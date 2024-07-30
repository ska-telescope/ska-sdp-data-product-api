SDP Data Product API Overview
=============================

This API is used to provide a list of SDP data products (files) that are hosted at a configurable storage location <PERSISTENT_STORAGE_PATH>.


Automatic API Documentation
---------------------------

Detailed interactive documentation for the API is available through Swagger UI. Access it at *http://<API URL>/docs* while running the application.


Basic Usage
-----------

.. note:: This API is typically deployed behind a secure layer that encrypts communication (TLS/SSL) and likely requires user authentication through a separate system. When accessing the API through a browser, both the encryption and the authentication will be handled by the browser, but direct access with scripts or notebooks to the API from outside the cluster is currently not supported. To make use of this API directly, the user need to access it from within the cluster where it is hosted.
 

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
        "startup_time": "2024-07-09T09:33:22.858611",
        "request_count": 0,
        "error_rate": 0,
        "last_metadata_update_time": "2024-07-09T09:33:22.858558",
        "search_metadata_store_status": {
            "metadata_store_in_use": "InMemoryDataproductSearch",
            "indexing": false,
            "indexing_timestamp": 1720510402.8141217,
            "number_of_data_products": 11
        },
        "metadata_store": {
            "host": "localhost",
            "port": 5432,
            "user": "postgres",
            "running": true,
            "postgresql_version": "PostgreSQL 16.3 on x86_64-pc-linux-musl, compiled by gcc (Alpine 13.2.1_git20240309) 13.2.1 20240309, 64-bit",
            "connection_error": ""
        }
    }



Search endpoint
~~~~~~~~~~~~~~~

Use the search endpoint to query your data products. Specify a time range and key-value pairs to filter your results. The response prioritizes products within the timeframe that best match your criteria.

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

The data product metadata store can be re-indexed but making a get request to the reindexdataproducts endpoint. This allows the user to update the metadata store if metadata have been added or changed since the previous indexing.

*Request*

.. code-block:: bash

    GET /reindexdataproducts

*Response*

.. code-block:: bash

    "Metadata is set to be cleared and re-indexed"

Download data product endpoint
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sending a post request to the download endpoint will return a stream response of the specified data product as a tar archive.

The body of the post request must contain the name of the file and the relative path of the file you want to download as listed in the file list response above. 


*Request*

.. code-block:: bash

    POST /download

*Body*

.. code-block:: bash

    {
        "fileName": "eb-test-20200325-00001",
        "relativePathName": "product/eb-test-20200325-00001"
    }

*Response*

A stream response of the specified data product as a tar archive

Retrieve metadata of a data product endpoint
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sending a post request to the dataproductmetadata endpoint will return a Response with the metadata of the data product in a JSON format.

The body of the post request must contain the name of the file "ska-data-product.yaml" and the relative path of the metadata file. 

For example, the post request body:

*Request*

.. code-block:: bash

    POST /dataproductmetadata

*Body*

.. code-block:: bash

    {
        "fileName": "ska-data-product.yaml",
        "relativePathName": "product/eb_id_2/ska-sub-system/scan_id_2/pb_id_2/ska-data-product.yaml"
    }

*Response*

.. code-block:: bash

    {
        "interface": "http://schema.skao.int/ska-data-product-meta/0.1", 
        "execution_block": "eb-m001-20191031-12345", 
        "context": 
        {
            "observer": "AIV_person_1", 
            "intent": "Experimental run as part of XYZ-123", 
            "notes": "Running that signal from XX/YY/ZZ through again, things seem a bit flaky"
        }, 
        "config": 
        {
            "processing_block": "pb-m001-20191031-12345", 
            "processing_script": "receive", 
            "image": "artefact.skao.int/ska-docker/vis_receive", 
            "version": "0.1.3", 
            "commit": "516fb5a693f9dc9aff5d46192f4e055b582fc025", 
            "cmdline": "-dump /product/eb-m001-20191031-12345/ska-sdp/pb-m001-20191031-12345/vis.ms"
        }, 
        "files": 
        [
            {
                "path": "vis.ms", 
                "status": "working", 
                "description": "Raw visibility dump from receive"
            }
        ]
    }

Ingest new data product
~~~~~~~~~~~~~~~~~~~~~~~

Sending a POST request to the ingestnewdataproduct endpoint will load and parse a file at the supplied filename, and add the data product to the metadata store.

.. code-block:: bash

    {
        "fileName": "eb-test-20200325-00001",
        "relativePathName": "product/eb-test-20200325-00001"
    }


Ingest new metadata endpoint
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note:: In this release, ingested metadata is not persistently stored. This means any data you add will be cleared when the API restarts. This functionality will be changed in future releases.

Sending a POST request to the ingestnewmetadata endpoint will parse the supplied JSON data as data product metadata, and add the data product to the metadata store.

For example, the POST request body:

.. code-block:: bash

    {
        "interface": "http://schema.skao.int/ska-data-product-meta/0.1",
        "execution_block": "eb-rest-00000000-99999",
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
            "cmdline": "",
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
            "obs_id": "",
            "obs_publisher_did": "",
            "pol_states": "XX/XY/YX/YY",
            "pol_xel": 0,
            "s_dec": 0,
            "s_ra": 0.0,
            "t_exptime": 5.0,
            "t_max": 57196.962848574476,
            "t_min": 57196.96279070411,
            "t_resolution": 0.9,
            "target_name": "",
        }
    }

API User
--------

The Data Product Dashboard (DPD) will usually be used via the GUI, for certain systems and users direct access to the API may be useful and desired. This guide will help users get up to speed with the Data Product Dashboard API.

DPD API documentation can be found at https://developer.skao.int/projects/ska-sdp-dataproduct-api/en/latest/overview.html#automatic-api-documentation. The DPD API is self documenting and as such the available endpoints can be found at `/docs`

Searching for and Downloading Data Products
When searching for data products it is important to ensure that the most recent data is available. The cached map for the in-memory solution periodically checks for new product that are available, but there is a way to manually ensure this, namely through the update command:

.. code-block:: python

    import requests
    BASE_URL = "http://localhost:8000"
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

    data = {"fileName": product["dataproduct_file"],"relativePathName": product["dataproduct_file"]}
    response = requests.post(f"{BASE_URL}/download", json=data)

    with open('product.tar', 'wb') as fd:
        for chunk in response.iter_content(chunk_size=4096):
            fd.write(chunk)

The tarball can then be opened using standard operation software. On linux this can be done using

.. code-block:: bash

    $ tar -xvf ./product.tar
    eb-m001-20221212-12345/
