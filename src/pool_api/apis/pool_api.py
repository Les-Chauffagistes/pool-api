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

from pool_api.models.get_user_stats404_response import GetUserStats404Response
from pool_api.models.node import Node
from chauff_cmn.models import Pool
from pool_api.models.pool_ping import PoolPing
from pool_api.models.pool_stats import PoolStats
from pool_api.models.pool_top import PoolTop

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


@router.get(
    "/v1/stats/{user}",
    responses={
        200: {"model": PoolStats, "description": "OK"},
        404: {"model": GetUserStats404Response, "description": "Adresse introuvable"},
    },
    tags=["pool"],
    summary="User stats",
    response_model_by_alias=True,
)
async def get_user_stats(
    user: str = Path(..., description=""),
) -> PoolStats:
    if not BasePoolApi.subclasses:
        raise HTTPException(status_code=500, detail="Not implemented")
    return await BasePoolApi.subclasses[0]().get_user_stats(user)


@router.get(
    "/v1/top",
    responses={
        200: {"model": PoolTop, "description": "OK"},
    },
    tags=["pool"],
    summary="Top metrics",
    response_model_by_alias=True,
)
async def get_top(
) -> PoolTop:
    if not BasePoolApi.subclasses:
        raise HTTPException(status_code=500, detail="Not implemented")
    return await BasePoolApi.subclasses[0]().get_top()


@router.get(
    "/v1/pings",
    responses={
        200: {"model": PoolPing, "description": "OK"},
    },
    tags=["pool"],
    summary="Ping metrics",
    response_model_by_alias=True,
)
async def get_pings(
) -> PoolPing:
    if not BasePoolApi.subclasses:
        raise HTTPException(status_code=500, detail="Not implemented")
    return await BasePoolApi.subclasses[0]().get_pings()
