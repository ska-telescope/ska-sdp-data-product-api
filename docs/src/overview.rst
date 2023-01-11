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

Uvicorn will then be running on http://127.0.0.1:8000

Kubernetes Deployment
~~~~~~~~~~~



The SDP Data Product API is deployed as part of the helm chart of the `SDP Data Product Dashboard <https://gitlab.com/ska-telescope/sdp/ska-sdp-data-product-dashboard>`_. In the Kubernetes deployment, the environmental variables is updated from the values files of the deployment and not the .env file in the project. Please see the documentation in the `SDP Data Product Dashboard documentation <https://developer.skao.int/projects/ska-sdp-data-product-dashboard/en/latest/?badge=latest>`_ for more information.



Automatic API Documentation
-----
For detailed documentation of the API, see the FastAPI Swagger UI documentation. This interactive API documentation can be accessed at http://127.0.0.1:8000/docs after running the application.

Basic Usage
-----

Test endpoint
~~~~~~~~~~~


To test if your instance of the API is up and running, you can send a get request to the ping endpoint and you will get the following reply:

.. code-block:: bash

    GET /ping

    {"ping": "The application is running"}

File list endpoint
~~~~~~~~~~~

Sending a get request to the file list endpoint returns a list of all the files in the <PERSISTANT_STORAGE_PATH>

.. code-block:: bash

    GET /filelist

    {
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

Data product list endpoint
~~~~~~~~~~~
A folder is considred a data product if the folder contains a file named <METADATA_FILE_NAME>.
Sending a get request to the data product list endpoint returns a list of all the data products in the path <PERSISTANT_STORAGE_PATH>

.. code-block:: bash

    GET /dataproductlist

    {
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
