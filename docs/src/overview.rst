SDP Data Product API Overview
=============

This is the documentation for the SKA SDP Data Product API.

This API is used to provide a list of SDP data products (files) that is hosted at a persistent storage or other orchestrated location and make them available to download.

Usage
-----

This usage will assume that you have the repo and submodules checked out.

Basic Usage
~~~~~~~~~~~

1. Set the storage path environmental variable in a .env file or export the variable with the path to your Data Products:

.. code-block:: bash

    PERSISTANT_STORAGE_PATH=/path/to/your/data/

2. Enter a Poetry virtual environment, install dependencies and run the code with Uvicorn: 

.. code-block:: bash

    poetry shell
    poetry install
    uvicorn src.ska_sdp_data_product_api.main:app --reload

3. You should now be able to test the API with 

3.1 Test the API

http://127.0.0.1:8000/

{"message":"Hello World"}

3.2 Return the file list

http://127.0.0.1:8000/filelist

{"filelist":[{"id":0,"filename":"Chicken.jpg"},{"id":1,"filename":"Cow.jpg"},{"id":2,"filename":"Duck.jpg"},{"id":3,"filename":"Pig.jpg"},{"id":4,"filename":"Rabbit.jpg"},{"id":5,"filename":"Subfolder1"},{"id":6,"filename":"Subfolder1/Orange.jpg"},{"id":7,"filename":"Subfolder1/Peach.jpg"},{"id":8,"filename":"Subfolder1/Popo.jpg"},{"id":9,"filename":"Subfolder1/SubSubFolder"},{"id":10,"filename":"Subfolder1/SubSubFolder/New Text Document.txt"}]}

3.3 Download a file

http://127.0.0.1:8000/download/{filename}

Will return a FileResponse object to download the file.

Running the application inside a containder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To run the application using docker, build the docker file in the root directory and run the container exposing port 8000.

```
 docker build -t api-docker .
 docker run -p 8000:8000 api-docker
```