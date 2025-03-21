# Changelog


## Current Development

- [PHX-18](https://jira.skatelescope.org/browse/PHX-18)
    - [Changed] Updated documentation to include more introduction and restructured the content.


## v0.13.0

- [NAL-1363](https://jira.skatelescope.org/browse/NAL-1363)
    - [Added] Updated DPD PostgreSQL interface to load data products from the DLM tables.

- [NAL-1341](https://jira.skatelescope.org/browse/NAL-1341)
    - [Changed] Removed unneeded method call at start of application that ran a background task on startup"

- [NAL-1363](https://jira.skatelescope.org/browse/NAL-1363)
    - [Changed] Added indexing duration to status

- [NAL-1322](https://jira.skatelescope.org/browse/NAL-1322)
    - [Changed] Updated the PostgreSQL search to allow search of annotations saved in the annotation table.

- [NAL-1320](https://jira.skatelescope.org/browse/NAL-1320)
    - [Changed] Updated the annotation endpoint to obtain the user details for the user profile using the auth token.

## v0.12.0

- [NAL-1347](https://jira.skatelescope.org/browse/NAL-1347)
    - [Added] Added a PVDataProduct, PVIndex and PVInterface class, refactoring the PV interface in preparation to ease the integration with the DLM database.
    - [Changed] Removed indexing from the MetadataStore store class, and integrated indexing from the PVInterface class.
    - [Added] Added methods get_folder_size and get_latest_modification_time to the PVDataProduct class.
    - [Changed] Separated the PostgresConnector from the PGMetadataStore.
    - [Added] Added a POSTGRESQL_QUERY_SIZE_LIMIT to limit the number of products returned to a query from the dashboard at a time.
    - [Removed] Removed the ElasticSearch search store.
    - [Changed] Updated PostgreSQL search to only make use of known fields and parameters to prevent SQL injection.

- [NAL-1323](https://jira.skatelescope.org/browse/NAL-1323)
    - [Changed] Added the functionality to allow annotations to be updated.
    - [Changed] Added a check to return nothing if PostgresSQL is not available when attempting to use annotation endpoints.

- [NAL-1309](https://jira.skatelescope.org/browse/NAL-1309)
    - [Changed] Made the Elasticsearch query body size configurable from environment variable SKA_DATAPRODUCT_API_ELASTIC_QUERY_BODY_SIZE.

- [NAL-1296](https://jira.skatelescope.org/browse/NAL-1296)
    - [Added] Added a method to retrieve an annotation based on id from metastore.
    - [Added] Added a new method to retrieve all annotations associated with a data product uuid from metastore.
    - [Added] Added a GET method to get an annotation from metastore by id.
    - [Added] Added a GET method to get all annotations associated with data product uuid from metastore.
    - [Changed] Updated the DataProductAnnotations class to allow for conversion from database results.
    
- [NAL-1279](https://jira.skatelescope.org/browse/NAL-1279)
    - [Changed] Set the default sort order for Elasticsearch to descending based on date created.

- [NAL-1282](https://jira.skatelescope.org/browse/NAL-1282)
    - [Changed] Addition of a MuiDataGridColumn class and reintroduced dynamic addition of columns.

- [NAL-1295](https://jira.skatelescope.org/browse/NAL-1295)
    - [Added] Added a class to represent the new annotations table.
    - [Added] Added a new method to insert annotations into data product metastore annotations table.
    - [Added] Added a new POST method to add new annotation data product metastore.

- [NAL-1294](https://jira.skatelescope.org/browse/NAL-1294)
    - [Added] Added a method to create the annotations table in the Postgres database.
    - [Changed] Updated the connector method to create the new table.

- [NAL-1280](https://jira.skatelescope.org/browse/NAL-1280)
    - [Changed] Updated changelog to match guidelines in developer portal.
    - [Changed] Changed connections to PostgreSQL to make use of [context manager](https://www.psycopg.org/psycopg3/docs/basic/usage.html#connection-context) .
    - [Changed] Updated error handling with the PostgreSQL persistent data store.

## v0.11.0

- [NAL-1254](https://jira.skatelescope.org/browse/NAL-1254)
    - [Changed] Changed the unique ID of data products from the execution_block_id to a UUID. This allows sub products of an execution_block to be loaded as a separate data product on the DPD.
    - [Fixed] Updated the in memory search store that failed to load new data on a re-index of the PV.
    - [Changed] Changed the /dataproductmetadata endpoint to expect a data products UUID instead of a execution_block_id, which is not unique in the case where there are sub products inside a data product.
    - [Changed] Updated the /download endpoint to accept either the execution_block_id or a UUID. The UUID is used to differentiate between sub products inside a data product when downloading from the dashboard. If there are more than one match for data products of an execution_block_id, they will all be downloaded when calling the endpoint with an execution_block_id.
    - [Changed] Updated the response of the /ingestnewdataproduct and /ingestnewmetadata endpoint to reply with an http response and uuid of the product submitted.

## v0.10.0

- [NAL-1228](https://jira.skatelescope.org/browse/NAL-1228)
    - [Changed] Updated the error handling when the ElasticSearch connection fails.


- [NAL-1227](https://jira.skatelescope.org/browse/NAL-1227)
    - BREAKING [Changed] The project have now been renamed and moved out of the SDP Gitlab folder. This was done because the Data Product API is not limited to data products of SDP and it might cause confusion if the name is not more general.
 
## v0.9.0

- [NAL-1186](https://jira.skatelescope.org/browse/NAL-1186)
    - [Changed] To enable better handling of connections and re-connections, a common PostgreSQL query method is added that returns the cursor object. When an operation fails due to a connection failure, it will retry and set the PostgreSQL availability to false on repeated failure.

- [NAL-1157](https://jira.skatelescope.org/browse/NAL-1157)
    - [Test Evidence] Improved test coverage
    - [Fixed] Fixed failure to load metadata into store when running the API with a PostgreSQL datastore.

- [NAL-1146](https://jira.skatelescope.org/browse/NAL-1146)
    - **BREAKING** [Added] Added the concept of access_group to the metadata. This limits the access to data products if the user, authenticated with MS Entra, has not been assigned to the access_group of the data product. When using the API with the Data Product Dashboard, the user can authenticate with MS Entra. When loading the data products (using the filterdataproducts endpoint) the users access token will be used to retrieve the users assigned user groups, and that will be used as access list to determine which data products the user have access to. Data products that does not have an access_group assigned, will be open access to all users. Note: When using the API for scripted access to data products all data products will be accessible as it is assume the the user inside the VPN have access to all the data.
    - **BREAKING** [Added] Integrated the API with the SKA Permissions API to enable it to obtain the users assigned user groups from MS Entra.
    - [Changed] Changed the Elasticsearch schema date_created field to date, and updated the query_body size to 100.
    - [Changed] The Elasticsearch http CA certificate needs to be loaded when deployed from the vault. To maintain the correct formatting, the certificate needs to be Base64 encoded before it is saved in the vault. The DPD API will now load the certificate from the vault into an environment variable, then decode it and save it in the format required for Elasticsearch.

- [NAL-1145](https://jira.skatelescope.org/browse/NAL-1145)
    - [Changed] Updated the configuration of the PostgreSQL metadata store to allow specification of a schema.
    - [Changed] Updated PostgreSQL connection to enable connection to an existing DB instance in a cluster. 
    - [Changed] Moved passwords out of the env file into a .secrets file that is excluded from the repository and if found locally, loaded into environment variables.

- [NAL-1132](https://jira.skatelescope.org/browse/NAL-1132)
    - [Changed] Updated project structure to align with the component functions, separating metadata stores and search stores.
    - [Changed] The application now loads metadata either from a specified volume or when submitted to the API endpoints then saves it into either a persistent store using PostgreSQL or in memory if the database is not available. The search store then loads the metadata from either of these metadata stores into an Elasticsearch instance or an in-memory search store instance if Elasticsearch is not available.
    - **BREAKING** [Changed] Changed the body or API request to specify the *execution_block* ID instead of *fileName* and *relativePathName*.
    - [Added] Added DataProductMetadata class that contains methods related to handling of metadata in the application.
    - **[Deprecated]** The in-memory store is deprecated and will be removed after all users have access to persistent PostgreSQL deployments. The functionality to load metadata into memory will be maintained to allow all users to configure and migrate to a persistent PostgreSQL DB for metadata storage.

- [NAL-1128](https://jira.skatelescope.org/browse/NAL-1128)
    - [Added] Updated the PostgreSQL persistent metadata store to ingest all the metadata into the database when the application loads or new data products are added with the /ingestnewmetadata endpoint.

- [NAL-1110](https://jira.skatelescope.org/browse/NAL-1110)
    - [Added] Added SDP_DATAPRODUCT_API_ELASTIC_INDICES to environment variables to enable specification of Elasticsearch instances in deployments.

- [NAL-1127](https://jira.skatelescope.org/browse/NAL-1127)
    - **BREAKING** [Removed] Removed /dataproductlist endpoint. This functionality has been replaced with either the /filterdataproducts which is aligned with the Data Product Dashboard requirements, or the /dataproductsearch endpoint that is a simplified version allowing for search and list of data products.
    - [Changed] Updated the /dataproductsearch endpoint to enable search in both the in memory or Elasticsearch modes of operation.
    - [Changed] Updated the Elasticsearch query_body to be dynamically created based on search criteria from either the search parameters supplied from the dataproductsearch endpoint, the MUI DataGrid filter model or the Data Product Dashboard search panel.
    - [Changed] Restructured the data store to allow better integration between modes of operation.

- [NAL-1115](https://jira.skatelescope.org/browse/NAL-1115)
    - [Changed] Updated make file to include options to create a development Docker image with PostgreSQL and Elasticsearch.
    - [Changed] Addition of basic authentication and self signed CA certificate for Elasticsearch developer environment.  

- [NAL-1121](https://jira.skatelescope.org/browse/NAL-1121)
    - [Changed] Improved the git repository structure.
    - [Added] Added a class to connect to an instance of PostgreSQL for development.
    - [Changed] Expanded the API status endpoint to include more information.

- [NAL-1093](https://jira.skatelescope.org/browse/NAL-1093)
    - **BREAKING** [Changed] This update refactors the data structure used to serve data to the MUI DataGrid component. It now aligns with the structure expected by the MUI DataGrid itself. This brings several improvements:
    - Column Filters and Pagination: You can now leverage built-in MUI DataGrid features like column filters and pagination.
    - Full API Configurability: The table can be fully configured from the API, allowing for more granular control over its behaviors.

- [YAN-1370](https://jira.skatelescope.org/browse/YAN-1370)
    - [Added] Introduced a new endpoint: /ingestnewmetadata (POST). This endpoint allows you to ingest data product metadata directly through the REST API. Send a POST request with the contents of your metadata file formatted as JSON. The API will parse the JSON data and add the corresponding data product to the metadata store in use.

## v0.8.0

- [NAL-1012](https://jira.skatelescope.org/browse/NAL-1012)
    - [Test Evidence] Addition of unit tests for datastore.
    - [Changed] Restructured documentation. 

## v0.7.0

- [NAL-511](https://jira.skatelescope.org/browse/NAL-511)
    - [Changed] Update the API search endpoint from the current search for 1x key value pair, to a multiple key value pairs that is all used to create the query for ES.
    - [Added] Added an in-memory search / filter on date range and key value pairs when not using the ES backend.

- [NAL-936](https://jira.skatelescope.org/browse/NAL-936)
    - [Changed] The documentation config is updated.
    - [Changed] The documentation is updated with Elasticsearch deployment information.

- [NAL-952](https://jira.skatelescope.org/browse/NAL-952)
    - [Changed] This MR removes the condition that Execution Block ID's needed to be unique, as there are sub-products that are part of the EB that share that ID.
    - [Changed] It also sorts the in memory datastore according to date.
    - [Test Evidence] Adds a sample data product with sub products to the tests


## v0.6.2

* **BREAKING** [Changed] Add indexing status to status endpoint.

- [NAL-858](https://jira.skatelescope.org/browse/NAL-858)
    - [Fixed] Fix for load of new data products failures without a refresh.
