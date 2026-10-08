# coding: utf-8

from typing import ClassVar, Dict, List, Tuple  # noqa: F401

from chauff_cmn.models import Pool


class BasePoolApi:
    subclasses: ClassVar[Tuple] = ()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        BasePoolApi.subclasses = BasePoolApi.subclasses + (cls,)
    async def get_pool(
        self,
    ) -> Pool:
        ...
