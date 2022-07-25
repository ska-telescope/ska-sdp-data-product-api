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

## Roadmap
This project is in very early development, but the following have already been identified to be added:
[]  Addition of different endpoints for files shared as a directory or with Rucio

## Contributing
Contributions are welcome, please see the SKAO developer portal for guidance. https://developer.skao.int/en/latest/

## Project status
Initial proof of concept working towards a minimum viable product.
