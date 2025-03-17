Python GUI Usage
================

While the Data Product Dashboard (DPD) is usually used via the GUI, for certain systems and users direct access to the API may be useful and desired.
This page provides a brief example of how to access the API with the Python requests package, which can be adopted similarly in other languages.

To access the API from within the cluster, you can use the BASE_URL http://<service name>.<namespace>:<port>.

Searching for and downloading Data Products
-------------------------------------------

When searching, it is important to ensure that the most recent data is available.
The cached map for the in-memory solution periodically checks for new product that are available, but there is a way to manually ensure this, namely through the update command:

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

    data = {"uid": "a0a2a10f-e382-31ba-0949-9a79204dfcad"}

    response = requests.post(f"{BASE_URL}/download", json=data)

    with open('product.tar', 'wb') as fd:
        for chunk in response.iter_content(chunk_size=4096):
            fd.write(chunk)

The tarball can then be opened using standard operation software. On linux this can be done using

.. code-block:: bash

    $ tar -xvf ./product.tar
    eb-notebook-20240201-54576/
