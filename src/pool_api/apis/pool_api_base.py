# coding: utf-8

from typing import ClassVar, Dict, List, Tuple  # noqa: F401

from pool_api.models.get_user_stats404_response import GetUserStats404Response
from pool_api.models.node import Node
from chauff_cmn.models import Pool
from pool_api.models.pool_stats import PoolStats


class BasePoolApi:
    subclasses: ClassVar[Tuple] = ()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        BasePoolApi.subclasses = BasePoolApi.subclasses + (cls,)
    async def get_pool(
        self,
    ) -> Pool:
        ...


    async def get_node(
        self,
    ) -> Node:
        ...


    async def get_user_stats(
        self,
        user: str,
    ) -> PoolStats:
        ...
