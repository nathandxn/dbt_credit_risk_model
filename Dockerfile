FROM ubuntu:20.04 AS base

# define venv path for poetry
ENV PATH="/srv/app/.venv/bin:$PATH"

