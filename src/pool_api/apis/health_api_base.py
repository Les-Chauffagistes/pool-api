# coding: utf-8

from typing import ClassVar, Dict, List, Tuple  # noqa: F401

from typing import Any


class BaseHealthApi:
    subclasses: ClassVar[Tuple] = ()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        BaseHealthApi.subclasses = BaseHealthApi.subclasses + (cls,)
    async def get_health(
        self,
    ) -> None:
        ...
