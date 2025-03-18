SKA Data Product API
====================

The `ska-dataproduct-api` repository is an SKA tool used to access and manage SKA Data Products in a storage volume.
The current repository contains mainly the code for the API and client configurations. It is used as the backend for the
`SKA Data Product Dashboard <https://developer.skao.int/projects/ska-dataproduct-dashboard/en/latest/>`_.

The Data Product API uses `Fast API <https://fastapi.tiangolo.com>`_ as the web framework and `starlette <https://www.starlette.io>`_
to set up configurations. It also makes use of a persistent metadata store implemented with `PostgreSQL <https://www.postgresql.org>`_.

If you just want to use the API, check the `User Guide <userguide/overview.html>`_.
To understand the internals, check out the `Developer Guide <developerguide/Development.html>`_.

.. toctree::
  :maxdepth: 1
  :caption: User Guide

  userguide/overview
  userguide/api
  userguide/client

.. toctree::
  :maxdepth: 1
  :caption: Developer Guide

  developerguide/Development
  developerguide/Deployment

.. toctree::
  :maxdepth: 1
  :caption: Releases

  CHANGELOG.md
