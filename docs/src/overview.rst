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

**Installation**

Clone the repository and its submodules:

.. code-block:: bash

    git clone git@gitlab.com:ska-telescope/sdp/ska-sdp-data-product-api.git
    git submodule update --init --recursive

**Running the application**

Configure the environmental variables in the .evn file under the root folder according to your requirements and environment. The default values are:

.. code-block:: bash

    REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_URL=http://localhost
    REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_PORT=8100
    PERSISTANT_STORAGE_PATH=./tests/test_files

*To run the application directly on your host machine:*

.. code-block:: bash

    cd ska-sdp-data-product-api
    poetry shell
    poetry install
    uvicorn src.ska_sdp_data_product_api.main:app --reload

*To run the application inside a docker container on your host machine:*

NOTE: When running the application in a docker container, the <PERSISTANT_STORAGE_PATH> needs to be accessible from within  the container. This is not configured automatically.

.. code-block:: bash

    docker build -t api-docker .
    docker run -p 8000:8000 api-docker

Uvicorn should then be running on http://127.0.0.1:8000

Kuberneties Deployment
~~~~~~~~~~~

The SDP Data Product API is deployed as part of the helm chart of the SDP Data Product Dashboard (https://gitlab.com/ska-telescope/sdp/ska-sdp-data-product-dashboard). In the Kubernetes deployment, the environmental variables is updated from the values files of the deployment and not the .env file in the project. Please see the chart and other documentation of the SDP Data Product Dashboard for more information (https://developer.skao.int/projects/ska-sdp-data-product-dashboard/en/latest/?badge=latest).


Automatic API Documentation
-----
For detailed documentation of the API, see the FastAPI Swagger UI documentation. This interactive API documentation can be accessed at http://127.0.0.1:8000/docs after running the application.

Basic Usage
-----

Test endpoint
~~~~~~~~~~~


To test if your instance of the API is up and running, you can send a get request to the ping endpoint and you should get the following reply:

.. code-block:: bash

    GET /ping

    {"ping": "The application is running"}

File list endpoint
~~~~~~~~~~~

Sending a get request to the file list endpoint should return a list of all the files in the specified <PERSISTANT_STORAGE_PATH>

.. code-block:: bash

    GET /filelist

    {"id":"root","name":"test_files","relativefilename":".","type":"directory","children":[{"id":1,"name":"product","relativefilename":".","type":"directory","children":[...]}]}

Download data product endpoint
~~~~~~~~~~~

Sending a post request to that download endpoint will return either a FileResponse with the requested file, or a Response with an in-memory zip file.

The body of the post request must contain the name of the file and the relative path of the file you want to download as listed in the file list response above. 

For example the post request body:

.. code-block:: bash

    {
        "fileName": "eb_id_2",
        "relativeFileName": "product/eb_id_2/"
    }

The post request endpoint: 

.. code-block:: bash

    POST /download
