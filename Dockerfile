FROM ubuntu:20.04 AS base

# define venv path for poetry
ENV PATH="/srv/app/.venv/bin:$PATH"

RUN sudo apt-get upgrade && sudo apt-get update && sudo apt-get install --assume-yes --no-install-recommends git python3-venv

FROM base AS builder

ENTRYPOINT [ "dbt" ]