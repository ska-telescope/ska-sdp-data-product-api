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
	# CP a self-signed cert from the Elasticsearch Docker container, encode it with Base64 and then saves it in an env variable in the .secrets file.
	docker cp dpd-elasticsearch-container:/usr/share/elasticsearch/config/certs/http_ca.crt ./src/ska_dataproduct_api/configuration/
	@cat ./src/ska_dataproduct_api/configuration/http_ca.crt | base64 -w 0 > cert_base64
	@if grep -q "^SKA_DATAPRODUCT_API_ELASTIC_HTTP_CA_BASE64_CERT=" .secrets; then \
		sed -i "s/^SKA_DATAPRODUCT_API_ELASTIC_HTTP_CA_BASE64_CERT=.*/SKA_DATAPRODUCT_API_ELASTIC_HTTP_CA_BASE64_CERT=$$(cat cert_base64)/" .secrets; \
	else \
		echo "SKA_DATAPRODUCT_API_ELASTIC_HTTP_CA_BASE64_CERT=$$(cat cert_base64)" >> .secrets; \
	fi
	@rm cert_base64
