# Using multi stage build to update the requirements.txt from the project.toml.
FROM python:3.10-slim as builder
WORKDIR /usr/src/app
RUN pip install --no-cache-dir poetry==1.2.0
COPY pyproject.toml poetry.lock ./
RUN poetry export -f requirements.txt --without-hashes --output requirements.txt

# pull the base image
FROM python:3.10-slim

# set the working direction
WORKDIR /usr/src/ska_sdp_data_product_api

# add app
COPY src/ska_sdp_data_product_api/ .

# install app dependencies
COPY --from=builder /usr/src/app/requirements.txt .
RUN set -eux \
    && pip install --no-cache-dir -r requirements.txt \
    && rm -rf /root/.cache/pip

# start app
CMD ["uvicorn", "ska_sdp_data_product_api.main:app", "--reload", "--host", "0.0.0.0", "--port", "8000", "--app-dir", "/usr/src"]
