# SKA SDP Data Product API
## Description
This repository contains an SKA SDP Data Product API that is used to provide a list of SDP data products (files) and make them available to download.
##### Badges
[![Documentation Status](https://readthedocs.org/projects/ska-telescope-ska-sdp-data-product-api/badge/?version=latest)](https://developer.skao.int/projects/ska-sdp-data-product-api/en/latest/?badge=latest)  ![Pipeline](https://gitlab.com/ska-telescope/sdp/ska-sdp-data-product-api/badges/main/pipeline.svg)

## Getting Started

## Tooling Pre-requisites

Below are some tools that will be required to work with the data product api:
- Python 3.8 or later versions: Install page URL: https://www.python.org/downloads/
- Poetry 1.1 or later versions: Install page URL: https://python-poetry.org/docs/#installation
- GNU make 4.2 or later versions: Install page URL: https://www.gnu.org/software/make/
<!-- - Docker 20.10 or later versions: Install page URL: https://docs.docker.com/engine/install/ -->

## Installation

Clone the repository and its submodules:

```bash
git clone git@gitlab.com:ska-telescope/sdp/ska-sdp-data-product-api.git
git submodule update --init --recursive
```

## Usage

```bash
poetry shell
poetry install
uvicorn src.ska_sdp_data_product_api.main:app --reload
```
## Running the application inside a containder

To run the application using docker, build the docker file in the root directory and run the container exposing port 8000.

```
 docker build -t api-docker .
 docker run -p 8000:8000 api-docker
```

The API can be teste by requesting the file list at the url http://127.0.0.1:8000/filelist
## Roadmap
This project is in very early development, but the following have already been identified to be added:
[]  Addition of different endpoints for files shared as a directory or with Rucio

## Contributing
Contributions are welcome, please see the SKAO developer portal for guidance. https://developer.skao.int/en/latest/

## Project status
Initial proof of concept working towards a minimum viable product.
