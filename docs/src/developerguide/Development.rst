Developer Guide
~~~~~~~~~~~~~~~

This document complements the guidelines set out in the `SKA telescope developer portal <https://developer.skao.int/en/latest/>`_


Tooling Pre-requisites
======================

Below are some tools that will be required to work with the data product API:

- Python 3.10 or later versions: Install page URL: https://www.python.org/downloads/
- Poetry 1.6 or later versions: Install page URL: https://python-poetry.org/docs/#installation
- GNU make 4.2 or later versions: Install page URL: https://www.gnu.org/software/make/
- Elasticsearch 8.6.0 or later versions: (optional)

Development setup
=================

Clone the repository and its submodules:

.. code-block:: bash

    git clone --recursive git@gitlab.com:ska-telescope/sdp/ska-sdp-dataproduct-api.git

The application make use of two databases; a persistent metadata store implemented with PostgreSQL and a search metadata store, implemented with Elasticsearch. Development instances of the databases can be created in a local Docker environment by running the provided Makefile commands:

.. note:: You will be required to give a developer password for you database instances, that should also be added to the environment variables below.


.. code-block:: bash

    make create-dev-postgres
    make create-dev-elasticsearch
    make cp-dev-elasticsearch-http-ca-cert

Running the application
=======================

Configure the environmental variables in the .env file under the root folder according to your requirements and environment. The default values are:

.. code-block:: bash

    REACT_APP_SKA_SDP_DATAPRODUCT_DASHBOARD_URL=http://localhost
    REACT_APP_SKA_SDP_DATAPRODUCT_DASHBOARD_PORT=8100
    PERSISTENT_STORAGE_PATH=./tests/test_files/product
    METADATA_FILE_NAME=ska-data-product.yaml
    STREAM_CHUNK_SIZE=65536
    SDP_DATAPRODUCT_API_POSTGRESQL_USER=postgres
    SDP_DATAPRODUCT_API_POSTGRESQL_PASSWORD=password
    SDP_DATAPRODUCT_API_ELASTIC_HOST=http://localhost
    SDP_DATAPRODUCT_API_ELASTIC_PORT=9200
    SDP_DATAPRODUCT_API_ELASTIC_USER=elastic
    SDP_DATAPRODUCT_API_ELASTIC_PASSWORD=password
    SDP_DATAPRODUCT_API_ELASTIC_HTTP_CA=http_ca.crt
    SDP_DATAPRODUCT_API_ELASTIC_METADATA_SCHEMA_FILE=./src/ska_sdp_dataproduct_api/elasticsearch/data_product_metadata_schema.json

To run the application directly on your host machine:

.. code-block:: bash

    make run-dev

*To run the application inside a docker container on your host machine:*

.. note:: When running the application in a docker container, the <PERSISTENT_STORAGE_PATH> needs to be accessible from within the container. You can mount the test folder into this location as done below:

.. code-block:: bash

    docker build -t ska-sdp-dataproduct-api .
    docker run -p 8000:8000 -v <YOUR_PROJECT_DIR>/ska-sdp-dataproduct-api/tests:/usr/src/ska_sdp_dataproduct_api/tests ska-sdp-dataproduct-api

Uvicorn will then be running on http://127.0.0.1:8000
