# Using multi stage build to update the requirements.txt from the project.toml.
FROM artefact.skao.int/ska-sdp-python:0.1.0 as builder
WORKDIR /usr/src/app
RUN pip install --no-cache-dir poetry==1.8.2
COPY pyproject.toml poetry.lock ./
RUN poetry export -f requirements.txt --without-hashes --output requirements.txt

# pull the base image
FROM artefact.skao.int/ska-sdp-python:0.1.0

# set the working direction
WORKDIR /usr/src/ska_dataproduct_api

# add app
COPY src/ska_dataproduct_api/ .

# install app dependencies
COPY --from=builder /usr/src/app/requirements.txt .
RUN set -eux \
    && pip install --no-cache-dir -r requirements.txt \
    && rm -rf /root/.cache/pip

# start app
CMD ["uvicorn", "ska_dataproduct_api.api.main:app", "--reload", "--host", "0.0.0.0", "--port", "8000", "--app-dir", "/usr/src",  "--log-level", "warning"]
