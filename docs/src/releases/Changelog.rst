Changelog
=========


Current Development
-------------------

* `NAL-1128 <https://jira.skatelescope.org/browse/NAL-1128>`_ 

  - [Added] Updated the PostgreSQL persistent metadata store to ingest all the metadata into the database when the application loads or new data products are added with the /ingestnewmetadata endpoint.

* `NAL-1110 <https://jira.skatelescope.org/browse/NAL-1110>`_ 

  - [Added] Added SDP_DATAPRODUCT_API_ELASTIC_INDICES to environment variables to enable specification of Elasticsearch instances in deployments.

* `NAL-1127 <https://jira.skatelescope.org/browse/NAL-1127>`_ 

  - **BREAKING** [Removed] Removed /dataproductlist endpoint. This functionality has been replaced with either the /filterdataproducts which is aligned with the Data Product Dashboard requirements, or the /dataproductsearch endpoint that is a simplified version allowing for search and list of data products.
  - [Changed] Updated the /dataproductsearch endpoint to enable search in both the in memory or Elasticsearch modes of operation.
  - [Changed] Updated the Elasticsearch query_body to be dynamically created based on search criteria from either the search parameters supplied from the dataproductsearch endpoint, the MUI DataGrid filter model or the Data Product Dashboard search panel.
  - [Changed] Restructured the data store to allow better integration between modes of operation.

* `NAL-1115 <https://jira.skatelescope.org/browse/NAL-1115>`_ 

  - [Changed] Updated make file to include options to create a development Docker image with PostgreSQL and Elasticsearch.
  - [Changed] Addition of basic authentication and self signed CA certificate for Elasticsearch developer environment.  


* `NAL-1121 <https://jira.skatelescope.org/browse/NAL-1121>`_ 

  - [Changed] Improved the git repository structure.
  - [Added] Added a class to connect to an instance of PostgreSQL for development.
  - [Changed] Expanded the API status endpoint to include more information.

* `NAL-1093 <https://jira.skatelescope.org/browse/NAL-1093>`_ 

  - **BREAKING** [Changed] This update refactors the data structure used to serve data to the MUI DataGrid component. It now aligns with the structure expected by the MUI DataGrid itself. This brings several improvements:

    - Column Filters and Pagination: You can now leverage built-in MUI DataGrid features like column filters and pagination.
    - Full API Configurability: The table can be fully configured from the API, allowing for more granular control over its behaviors.

* `YAN-1370 <https://jira.skatelescope.org/browse/YAN-1370>`_ 

  - [Added] Introduced a new endpoint: /ingestnewmetadata (POST). This endpoint allows you to ingest data product metadata directly through the REST API. Send a POST request with the contents of your metadata file formatted as JSON. The API will parse the JSON data and add the corresponding data product to the metadata store in use.
 

Released
========

v0.8.0
------

* `NAL-1012 <https://jira.skatelescope.org/browse/NAL-1012>`_ 

  - [Test Evidence] Addition of unit tests for datastore.
  - [Changed] Restructured documentation. 

v0.7.0
------

* `NAL-511 <https://jira.skatelescope.org/browse/NAL-511>`_ 
 
  - [Changed] Update the API search endpoint from the current search for 1x key value pair, to a multiple key value pairs that is all used to create the query for ES.
  - [Added] Added an in-memory search / filter on date range and key value pairs when not using the ES backend.

* `NAL-936 <https://jira.skatelescope.org/browse/NAL-936>`_ 

  - [Changed] The documentation config is updated.
  - [Changed] The documentation is updated with Elasticsearch deployment information.

* `NAL-952 <https://jira.skatelescope.org/browse/NAL-952>`_ 

  - [Changed] This MR removes the condition that Execution Block ID's needed to be unique, as there are sub-products that are part of the EB that share that ID.
  - [Changed] It also sorts the in memory datastore according to date.

* `NAL-952 <https://jira.skatelescope.org/browse/NAL-952>`_ 

  - [Test Evidence] Adds a sample data product with sub products to the tests


v0.6.2
------

* **BREAKING** [Changed] Add indexing status to status endpoint.

* `NAL-858 <https://jira.skatelescope.org/browse/NAL-858>`_ 

  - [Fixed] Fix for load of new data products failures without a refresh.
