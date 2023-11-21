SDP Data Product API Overview
=============================

This API is used to provide a list of SDP data products (files) that are hosted at a configurable storage location <PERSISTANT_STORAGE_PATH>.


Deployment
----------

Local Deployment
~~~~~~~~~~~~~~~~
**Tooling Pre-requisites**
Below are some tools that will be required to work with the data product API:

- Python 3.10 or later versions: Install page URL: https://www.python.org/downloads/
- Poetry 1.2 or later versions: Install page URL: https://python-poetry.org/docs/#installation
- GNU make 4.2 or later versions: Install page URL: https://www.gnu.org/software/make/
- Elasticsearch 8.6.0 or later versions: (optional)

**Installation**

Clone the repository and its submodules:

.. code-block:: bash

    git clone git@gitlab.com:ska-telescope/sdp/ska-sdp-dataproduct-api.git
    git submodule update --init --recursive

**Running the application**

Configure the environmental variables in the .evn file under the root folder according to your requirements and environment. The default values are:

.. code-block:: bash

    REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_URL=http://localhost
    REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_PORT=8100
    PERSISTANT_STORAGE_PATH=./tests/test_files/product
    METADATA_FILE_NAME=ska-data-product.yaml
    METADATA_ES_SCHEMA_FILE=./src/ska_sdp_dataproduct_api/elasticsearch/data_product_metadata_schema.json
    METADATA_JSON_SCHEMA_FILE=./src/ska_sdp_dataproduct_api/core/data_product_metadata_json_schema.json
    ES_HOST=http://localhost:9200
    STREAM_CHUNK_SIZE=65536

*To run the application directly on your host machine:*

.. code-block:: bash

    cd ska-sdp-dataproduct-api
    poetry shell
    poetry install
    uvicorn src.ska_sdp_dataproduct_api.main:app --reload

*To run the application inside a docker container on your host machine:*

NOTE: When running the application in a docker container, the <PERSISTANT_STORAGE_PATH> needs to be accessible from within the container. You can mount the test folder into this location as done below:

.. code-block:: bash

    docker build -t ska-sdp-dataproduct-api .
    docker run -p 8000:8000 -v <YOUR_PROJECT_DIR>/ska-sdp-dataproduct-api/tests:/usr/src/ska_sdp_dataproduct_api/tests ska-sdp-dataproduct-api

Uvicorn will then be running on http://127.0.0.1:8000

Kubernetes Deployment
~~~~~~~~~~~~~~~~~~~~~



The SDP Data Product API is deployed as part of the helm chart of the `SDP Data Product Dashboard <https://gitlab.com/ska-telescope/sdp/ska-sdp-dataproduct-dashboard>`_. In the Kubernetes deployment, the environmental variables are updated from the values files of the deployment and not the .env file in the project. Please see the documentation in the `SDP Data Product Dashboard documentation <https://developer.skao.int/projects/ska-sdp-dataproduct-dashboard/en/latest/?badge=latest>`_ for more information.



Automatic API Documentation
-----
For detailed documentation of the API, see the FastAPI Swagger UI documentation. This interactive API documentation can be accessed at http://127.0.0.1:8000/docs when running the application locally or https://<domain>/<namespace>/api/docs when deployed behind an ingress.

Basic Usage
-----------

Test endpoint
~~~~~~~~~~~~~


To retrieve the status of the API, you can send a get request to the status endpoint and you will get a reply indicating the status of the API and the Search:

.. code-block:: bash

    GET /status

    {"API_running":true,"Search_enabled":false}



Metadata search endpoint
~~~~~~~~~~~~~~~~~~~~~~~~

When an Elasticsearch backend endpoint is available, the dataproductsearch will query the Elasticsearch datastore with the search criteria passed to the API (start_date, end_date and key_pair). The search results will then be returned as a list of data products, with key metadata attributes.

.. code-block:: bash

    POST /dataproductsearch

    {
    "start_date": "2001-12-12",
    "end_date": "2032-12-12",
    "key_pair": "execution_block:eb-m001-20191031-12345"
    }


    [{"id": 1, "execution_block": "eb-test-20230401-12345", "interface": "http://schema.skao.int/ska-data-product-meta/0.1", "date_created": "2023-04-01", "dataproduct_file": "product/eb-test-20230401-12345", "metadata_file": "product/eb-test-20230401-12345/ska-data-product.yaml", "obscore.dataproduct_type": "MS"}, {"id": 2, "interface": "http://schema.skao.int/ska-data-product-meta/0.1", "execution_block": "eb-m004-20191031-12345", "date_created": "2019-10-31", "dataproduct_file": "product/eb-m004-20191031-12345", "metadata_file": "product/eb-m004-20191031-12345/ska-data-product.yaml", "obscore.dataproduct_type": "MS"}]

Metadata list endpoint
~~~~~~~~~~~~~~~~~~~~~~

When an Elasticsearch backend endpoint is not available, the dataproductlist can be used to return all the data products as a list of data products, with key metadata attributes.

.. code-block:: bash

    GET /dataproductlist


    [{"id": 1, "interface": "http://schema.skao.int/ska-data-product-meta/0.1", "execution_block": "eb-m001-20191031-12345", "date_created": "2019-10-31", "dataproduct_file": "product/eb-m001-20221212-12345", "metadata_file": "product/eb-m001-20221212-12345/ska-data-product.yaml"}, {"id": 2, "interface": "http://schema.skao.int/ska-data-product-meta/0.1", "execution_block": "eb-m002-20221212-12345", "date_created": "2022-12-12", "dataproduct_file": "product/eb-m002-20221212-12345", "metadata_file": "product/eb-m002-20221212-12345/ska-data-product.yaml"}]


Re-index data products endpoint
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The data product metadata store can be re-indexed but making a get request to the reindexdataproducts endpoint. This allows the user to update the metadata store if metadata have been added or changed since the previous indexing.

.. code-block:: bash

    GET /reindexdataproducts


    "Metadata store cleared and re-indexed"

Download data product endpoint
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sending a post request to the download endpoint will return a stream response of the specified data product as a tar archive.

The body of the post request must contain the name of the file and the relative path of the file you want to download as listed in the file list response above. 

For example, the post request body:

.. code-block:: bash

    {
        "fileName": "eb-test-20200325-00001",
        "relativePathName": "product/eb-test-20200325-00001"
    }

The post request endpoint: 

.. code-block:: bash

    POST /download


Retrieve metadata of a data product endpoint
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sending a post request to the dataproductmetadata endpoint will return a Response with the metadata of the data product in a JSON format.

The body of the post request must contain the name of the file "ska-data-product.yaml" and the relative path of the metadata file. 

For example, the post request body:

.. code-block:: bash

    {
        "fileName": "ska-data-product.yaml",
        "relativePathName": "product/eb_id_2/ska-sub-system/scan_id_2/pb_id_2/ska-data-product.yaml"
    }

The post request endpoint: 

.. code-block:: bash

    POST /dataproductmetadata

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


API User
-----------

The Data Product Dashboard (DPD) will usually be used via the GUI, for certain systems and users direct access to the API may be useful and desired. This guide will help users get up to speed with the Data Product Dashboard API.

DPD API documentation can be found at https://developer.skao.int/projects/ska-sdp-dataproduct-api/en/latest/overview.html#automatic-api-documentation. The DPD API is self documenting and as such the available endpoints can be found at `/docs`

Data Product Modes of Operation
The Data Product Dashboard has two modes of operation. With an Elastic Search backend available the full functionality is available, without that backend, a degraded experience is given to the user. Due to current architectural decisions that need to be made. The degraded or “in memory” implementation is currently the expected behavior and as such this guide expects the “in memory” mode of operation. Since the API is consistent between the two modes of operation, the guide should still be relevant when the mode is switched across.
The endpoint ‘/status’ will inform which mode of operation is currently activated, Search enabled is expected to be false.

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
        "key_pair": "execution_block:eb-m001-20191031-12345",
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
