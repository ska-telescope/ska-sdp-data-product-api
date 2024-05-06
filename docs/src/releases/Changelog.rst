Changelog
=========


Current Development
-------------------



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
