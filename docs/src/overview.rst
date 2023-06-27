SDP Data Product API Overview
=============

This API is used to provide a list of SDP data products (files) that are hosted at a configurable storage location <PERSISTANT_STORAGE_PATH>.


Deployment
-----

Local Deployment
~~~~~~~~~~~
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

    REACT_APP_SKA_SDP_DATAPRODUCT_DASHBOARD_URL=http://localhost
    REACT_APP_SKA_SDP_DATAPRODUCT_DASHBOARD_PORT=8100
    PERSISTANT_STORAGE_PATH=./tests/test_files
    METADATE_FILE_NAME=ska-data-product.yaml
    METADATA_ES_SCHEMA_FILE=./src/ska_sdp_dataproduct_api/elasticsearch/data_product_metadata_schema.json
    ES_HOST=http://localhost:9200

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
~~~~~~~~~~~



The SDP Data Product API is deployed as part of the helm chart of the `SDP Data Product Dashboard <https://gitlab.com/ska-telescope/sdp/ska-sdp-dataproduct-dashboard>`_. In the Kubernetes deployment, the environmental variables are updated from the values files of the deployment and not the .env file in the project. Please see the documentation in the `SDP Data Product Dashboard documentation <https://developer.skao.int/projects/ska-sdp-dataproduct-dashboard/en/latest/?badge=latest>`_ for more information.



Automatic API Documentation
-----
For detailed documentation of the API, see the FastAPI Swagger UI documentation. This interactive API documentation can be accessed at http://127.0.0.1:8000/docs after running the application.

Basic Usage
-----

Test endpoint
~~~~~~~~~~~


To retrieve the status of the API, you can send a get request to the status endpoint and you will get a reply indicating the status of the API and the Search:

.. code-block:: bash

    GET /status

    {"API_running":true,"Search_enabled":false}



Metadata search endpoint
~~~~~~~~~~~

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
~~~~~~~~~~~

When an Elasticsearch backend endpoint is not available, the dataproductlist can be used to return all the data products as a list of data products, with key metadata attributes.

.. code-block:: bash

    GET /dataproductlist


    [{"id": 1, "interface": "http://schema.skao.int/ska-data-product-meta/0.1", "execution_block": "eb-m001-20191031-12345", "date_created": "2019-10-31", "dataproduct_file": "product/eb-m001-20221212-12345", "metadata_file": "product/eb-m001-20221212-12345/ska-data-product.yaml"}, {"id": 2, "interface": "http://schema.skao.int/ska-data-product-meta/0.1", "execution_block": "eb-m002-20221212-12345", "date_created": "2022-12-12", "dataproduct_file": "product/eb-m002-20221212-12345", "metadata_file": "product/eb-m002-20221212-12345/ska-data-product.yaml"}]


Re-index data products endpoint
~~~~~~~~~~~

The data product metadata store can be re-indexed but making a get request to the reindexdataproducts endpoint. This allows the user to update the metadata store if metadata have been added or changed since the previous indexing.

.. code-block:: bash

    GET /reindexdataproducts


    "Metadata store cleared and re-indexed"

Download data product endpoint
~~~~~~~~~~~

Sending a post request to the download endpoint will return a response to an in-memory tar file of the selected data product.

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
~~~~~~~~~~~

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