# coding: utf-8

from typing import Dict, List  # noqa: F401
import importlib
import pkgutil

from pool_api.apis.pool_api_base import BasePoolApi
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

from pool_api.models.node import Node
from chauff_cmn.models import Pool

router = APIRouter()

ns_pkg = pool_api.impl
for _, name, _ in pkgutil.iter_modules(ns_pkg.__path__, ns_pkg.__name__ + "."):
    importlib.import_module(name)


@router.get(
    "/v1/pool",
    responses={
        200: {"model": Pool, "description": "OK"},
    },
    tags=["pool"],
    summary="Pool metrics",
    response_model_by_alias=True,
)
async def get_pool(
) -> Pool:
    if not BasePoolApi.subclasses:
        raise HTTPException(status_code=500, detail="Not implemented")
    return await BasePoolApi.subclasses[0]().get_pool()


@router.get(
    "/v1/node",
    responses={
        200: {"model": Node, "description": "OK"},
    },
    tags=["pool"],
    summary="Node metrics",
    response_model_by_alias=True,
)
async def get_node(
) -> Node:
    if not BasePoolApi.subclasses:
        raise HTTPException(status_code=500, detail="Not implemented")
    return await BasePoolApi.subclasses[0]().get_node()
