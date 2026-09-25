"""HTTP surface of the service, served entirely on localhost."""

from __future__ import annotations

from fastapi import FastAPI


def create_app(service=None) -> FastAPI:
    raise NotImplementedError
