# Using multi stage build to update the requirements.txt from the project.toml.
FROM python:3.9-slim as builder
WORKDIR /usr/src/app
RUN pip install poetry
COPY pyproject.toml poetry.lock ./
RUN poetry export --format requirements.txt --without-hashes > requirements.txt

# pull the base image
FROM python:3.9-slim

# set the working direction
WORKDIR /usr/src/ska_sdp_data_product_api

# add app
COPY src/ska_sdp_data_product_api/ .

# install app dependencies
COPY --from=builder /usr/src/app/requirements.txt .
RUN set -eux \
    && pip install --upgrade pip setuptools wheel \
    && pip install -r requirements.txt \
    && rm -rf /root/.cache/pip

# start app
CMD ["uvicorn", "ska_sdp_data_product_api.main:app", "--reload", "--host", "0.0.0.0", "--port", "8000", "--app-dir", "/usr/src"]
