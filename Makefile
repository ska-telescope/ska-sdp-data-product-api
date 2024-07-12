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
	poetry run uvicorn ska_sdp_dataproduct_api.api.main:app --reload --port 8000 --host 0.0.0.0 --app-dir ./src --log-level debug

restart-databases-containers:
# Try restarting Docker containers (ignore errors)
	docker restart dpd-postgres-container || true
	docker restart dpd-elasticsearch-container || true
	# NOTE: It will take a minute for Elasticsearch to restart before connections to it can be made.

create-dev-postgres:
# Creates a PostgreSQL Docker container for development use.
	docker run  --detach \
		--name dpd-postgres-container \
		-e POSTGRES_PASSWORD=$(shell bash -c 'read -s -p "Password: " pwd; echo $$pwd') \
		-p 5432:5432 \
		postgres:16.3-alpine

create-dev-elasticsearch:
# Creates a Elasticsearch Docker container for development use.
	docker run  --detach \
		--name dpd-elasticsearch-container \
		-e ELASTIC_PASSWORD=$(shell bash -c 'read -s -p "Password: " pwd; echo $$pwd') \
		-p 9200:9200 \
		elasticsearch:8.14.2

cp-dev-elasticsearch-http-ca-cert:
# CP a self signed cert for the Elasticsearch Docker container.
	docker cp dpd-elasticsearch-container:/usr/share/elasticsearch/config/certs/http_ca.crt .
