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

run-dev:
# Proceed with poetry install and uvicorn command
	poetry install; \
	poetry run uvicorn ska_dataproduct_api.api.main:app --reload --port 8001 --host 0.0.0.0 --app-dir ./src --log-level debug

restart-databases-containers:
# Try restarting Docker containers (ignore errors)
	docker restart dpd-postgres-container || true

create-dev-postgres:
# Creates a PostgreSQL Docker container for development use.
	docker run  --detach \
		--name dpd-postgres-container \
		-e POSTGRES_PASSWORD=$(shell bash -c 'read -s -p "Password: " pwd; echo $$pwd') \
		-p 5432:5432 \
		postgres:16.3-alpine
