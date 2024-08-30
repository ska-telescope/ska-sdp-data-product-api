Changelog
=========


Current Development
-------------------

* `NAL-1157 <https://jira.skatelescope.org/browse/NAL-1157>`_ 

 - [Test Evidence] Improved test coverage
 - [Fixed] Failure to load metadata indo store when running the API with a PostgreSQL datastore.
 

* `NAL-1146 <https://jira.skatelescope.org/browse/NAL-1146>`_ 

  - **BREAKING** [Added] Added the concept of access_group to the metadata. This limits the access to data products if the user, authenticated with MS Entra, has not been assigned to the access_group of the data product. When using the API with the Data Product Dashboard, the user can authenticate with MS Entra. When loading the data products (using the filterdataproducts endpoint) the users access token will be used to retrieve the users assigned user groups, and that will be used as access list to determine which data products the user have access to. 
  Data products that does not have an access_group assigned, will be open access to all users.
  Note: When using the API for scripted access to data products with the dataproductsearch endpoint, only data products with open access will be accessible.
  - **BREAKING** [Added] Integrated the API with the SKA Permissions API to enable it to obtain the users assigned user groups from MS Entra.
  - [Changed] Changed the Elasticsearch schema date_created field to date, and updated the query_body size to 100.
  - [Changed] The Elasticsearch http CA certificate needs to be loaded when deployed from the vault. To maintain the correct formatting, the certificate needs to be Base64 encoded before it is saved in the vault. The DPD API will now load the certificate from the vault into an environment variable, then decode it and save it in the format required for Elasticsearch.


* `NAL-1145 <https://jira.skatelescope.org/browse/NAL-1145>`_ 

  - [Changed] Updated the configuration of the PostgreSQL metadata store to allow specification of a schema.
  - [Changed] Updated PostgreSQL connection to enable connection to an existing DB instance in a cluster. 
  - [Changed] Moved passwords out of the env file into a .secrets file that is excluded from the repository and if found locally, loaded into environment variables.

* `NAL-1132 <https://jira.skatelescope.org/browse/NAL-1132>`_ 

  - [Changed] Updated project structure to align with the component functions, separating metadata stores and search stores.
  - [Changed] The application now loads metadata either from a specified volume or when submitted to the API endpoints then saves it into either a persistent store using PostgreSQL or in memory if the database is not available. The search store then loads the metadata from either of these metadata stores into an Elasticsearch instance or an in-memory search store instance if Elasticsearch is not available.
  - **BREAKING** [Changed] Changed the body or API request to specify the *execution_block* ID instead of *fileName* and *relativePathName*.
  - [Added] Added DataProductMetadata class that contains methods related to handling of metadata in the application.
  - **[Deprecated]** The in-memory store is deprecated and will be removed after all users have access to persistent PostgreSQL deployments. The functionality to load metadata into memory will be maintained to allow all users to configure and migrate to a persistent PostgreSQL DB for metadata storage.

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
