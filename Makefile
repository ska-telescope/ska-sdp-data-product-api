# include OCI Images support
include .make/oci.mk

# include k8s support
include .make/k8s.mk

# include Helm Chart support
include .make/helm.mk

# Include Python support
include .make/python.mk

# include raw support
include .make/raw.mk

# include core make support
include .make/base.mk

# include your own private variables for custom deployment configuration
-include PrivateRules.mak

PYTHON_LINE_LENGTH = 99

# Run the application in a virtual environment on the host for development use.
run-dev:
	poetry install; \
	poetry run uvicorn ska_sdp_dataproduct_api.api.main:app --reload --port 8000 --host 0.0.0.0 --app-dir ./src --log-level debug