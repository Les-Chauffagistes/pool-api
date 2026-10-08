# coding: utf-8

from typing import Dict, List  # noqa: F401
import importlib
import pkgutil

from pool_api.apis.health_api_base import BaseHealthApi
import pool_api.impl

from fastapi import (  # noqa: F401
    APIRouter,
    Body,
    Cookie,
    Depends,
    Form,
    Header,
    HTTPException,
    Path,
    Query,
    Response,
    Security,
    status,
)

from typing import Any

router = APIRouter()

ns_pkg = pool_api.impl
for _, name, _ in pkgutil.iter_modules(ns_pkg.__path__, ns_pkg.__name__ + "."):
    importlib.import_module(name)


@router.get(
    "/health",
    responses={
        200: {"description": "OK"},
    },
    tags=["health"],
    summary="Health check",
    response_model_by_alias=True,
)
async def get_health(
) -> None:
    if not BaseHealthApi.subclasses:
        raise HTTPException(status_code=500, detail="Not implemented")
    return await BaseHealthApi.subclasses[0]().get_health()
