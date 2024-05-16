FROM ubuntu:20.04 AS base

# define venv path for poetry
ENV PATH="/srv/app/.venv/bin:$PATH"

RUN sudo apt-get upgrade && sudo apt-get update && sudo apt-get install --assume-yes --no-install-recommends git python3-venv

FROM base AS builder

# installing poetry
RUN sudo apt-get install --assume-yes --no-install-recommends python3-pip
RUN sudo pip3 install --no-chache-dir poetry==1.8.3

COPY poetry.lock pyproject.toml ./
RUN poetry config virtualenvs.in-project true
RUN poetry install --no-interaction --only main

ENTRYPOINT [ "dbt" ]