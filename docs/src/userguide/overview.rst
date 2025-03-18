API Usage Overview
==================

The Data Product API is used to provide a list of SKA Data Products (files) that are hosted at a configurable storage location <PERSISTENT_STORAGE_PATH>.
Interaction with the API is done like any other RESTful API, by sending HTTP requests to the API's endpoints, and receiving responses (typically in JSON format).
There are many ways to access REST API endpoints, such as a web browser (with a REST client), a command-line tool like wget, an API platform like postman, or inside a Python script.
We won't list all examples here and as there are plenty of resources online.

The Swagger UI documentation provides interactive options to test the API, which is the method we recommend to use, please check out
`Interactive API <userguide/api.html>`_ for more information.
If you are interested to to follow some basic examples using command line , we also have basic usage commands using the classical HTTP methods in `Access via HTTP Method <userguide/http.html>`_.
For integrating the API inside Python, we have provided a simple example in `Python Usage <userguide/client.html>`_.

.. note:: This API is typically deployed behind a secure layer that encrypts communication (TLS/SSL) and likely requires user authentication through a separate system. When accessing the API through a browser, both the encryption and the authentication will be handled by the browser, but direct access with scripts or notebooks to the API from outside the cluster is currently not supported. To make use of this API directly, the user need to access it from within the cluster where it is hosted.

.. note:: If a data product have been assigned a context.access_group, then that data product will not be available/listed when accessing the api directly with scripts or notebooks. This is due the required access token of an authenticated user that is not available in this mode of operation.


