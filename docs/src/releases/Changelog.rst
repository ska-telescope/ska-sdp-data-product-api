Changelog
=========


Current Development
-------------------

* [Added] `NAL-1121 <https://jira.skatelescope.org/browse/NAL-1121>`_ 

  - Refactored the git repository structure.
  - Added initial ska_sdp_dataproduct_postgresql Docker file for the use of Postgresql for dev and test deployments.

* **BREAKING** [Changed] `NAL-1093 <https://jira.skatelescope.org/browse/NAL-1093>`_ 

  - This update refactors the data structure used to serve data to the MUI DataGrid component. It now aligns with the structure expected by the MUI DataGrid itself. This brings several improvements:

    - Column Filters and Pagination: You can now leverage built-in MUI DataGrid features like column filters and pagination.
    - Full API Configurability: The table can be fully configured from the API, allowing for more granular control over its behaviors.

* [Added] `YAN-1370 <https://jira.skatelescope.org/browse/YAN-1370>`_ 

  - Introduced a new endpoint: /ingestnewmetadata (POST). This endpoint allows you to ingest data product metadata directly through the REST API. Send a POST request with the contents of your metadata file formatted as JSON. The API will parse the JSON data and add the corresponding data product to the metadata store in use.
 

Released
========

v0.8.0
------

* [Test Evidence] `NAL-1012 <https://jira.skatelescope.org/browse/NAL-1012>`_ 

  - Addition of unit tests for datastore.
  - Restructured documentation. 

v0.7.0
------

* [Added] `NAL-511 <https://jira.skatelescope.org/browse/NAL-511>`_ 
 
  - Update the API search endpoint from the current search for 1x key value pair, to a multiple key value pairs that is all used to create the query for ES.
  - Added an in-memory search / filter on date range and key value pairs when not using the ES backend.

* [Changed] `NAL-936 <https://jira.skatelescope.org/browse/NAL-936>`_ 

  - The documentation config is updated.
  - The documentation is updated with Elasticsearch deployment information.

* [Changed] `NAL-952 <https://jira.skatelescope.org/browse/NAL-952>`_ 

  - This MR removes the condition that Execution Block ID's needed to be unique, as there are sub-products that are part of the EB that share that ID.
  - It also sorts the in memory datastore according to date.

* [Test Evidence] `NAL-952 <https://jira.skatelescope.org/browse/NAL-952>`_ 

  - Adds a sample data product with sub products to the tests


v0.6.2
------

* **BREAKING** [Changed] Add indexing status to status endpoint.

* [Fixed] `NAL-858 <https://jira.skatelescope.org/browse/NAL-858>`_ 

  - Fix for load of new data products failures without a refresh.
