from functools import wraps

import asynch
from asynch.cursors import DictCursor
import os
from random import randint
import sentry_sdk

from rd_chassis.misc.logger import Logger
from rd_chassis.misc.singleton import Singleton
from rd_chassis.misc.helpers import parse_bool
from rd_chassis.misc.measure_took import MeasureTook
from rd_chassis.tasks.metrics.providers.counter import Counter
from rd_chassis.tasks.metrics.providers.avg_value import AvgValue

from typing import TypeVar, SupportsInt, TypedDict


_T = TypeVar("_T", SupportsInt, bool)


def clickhouse_span(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        description = f"{func.__name__}"
        op = f"gateway.clickhouse.{self.__class__.__name__}"
        with sentry_sdk.start_span(description=description, op=op) as span:
            span.set_tag("gateway_type", "ClickhouseGateway")
            span.set_tag("gateway_name", self.__class__.__name__)
            span.set_data("sql", kwargs.get("sql", ""))
            if kwargs.get("args"):
                span.set_data("args", kwargs.get("args"))
            if kwargs.get("data"):
                span.set_data("data", kwargs.get("data"))
            result = await func(self, *args, **kwargs)
        return result

    return wrapper


class ClickhouseGateway(metaclass=Singleton):
    __slots__ = ("logger", "__db_config", "debug", "__pool", "__metrics")

    class Metrics(TypedDict):
        queries_count: Counter
        slow_queries: Counter
        slow_query_time: AvgValue
        query_time: AvgValue
        pool_acquire: AvgValue

    retries: _T

    __metrics: Metrics

    def __init__(self, db_env_prefix: str, /):
        self.logger = Logger(self.__class__.__name__)
        self.__db_config = {
            "host": os.getenv(db_env_prefix + "_HOST"),
            "port": int(os.getenv(db_env_prefix + "_APP_PORT") or 9000),
            "user": os.getenv(db_env_prefix + "_USERNAME")
            or os.getenv(db_env_prefix + "_USER"),  # backward compatibility
            "password": os.getenv(db_env_prefix + "_PASSWORD"),
            "db": os.getenv(db_env_prefix + "_DB"),
        }
        self.debug = parse_bool(os.getenv(db_env_prefix + "_DEBUG"))

        from rd_chassis.tasks.metrics.metrics import Metrics

        m = Metrics()
        __metrics_prefix = db_env_prefix.lower().replace("_clickhouse", "")

        self.__metrics = ClickhouseGateway.Metrics(
            pool_acquire=AvgValue(),
            query_time=AvgValue(),
            slow_query_time=AvgValue(),
            slow_queries=Counter(),
            queries_count=Counter(),
        )

        for name, metric in self.__metrics.items():
            provider_name = f"clickhouse.{__metrics_prefix}.{name}"
            m.add_provider(provider_name, metric)

    async def __aenter__(self):
        if not self.__db_config:
            raise ValueError("self.db_config must be set")
        for option in ("host", "port", "user", "password"):
            if not self.__db_config.get(option, None):
                raise ValueError(f"self.db_config.{option} must be set")

        await self.__create_pool()

    def __pool_acquire_wrapper(self, func):
        class Context:
            __slots__ = ("__context", "__metric")

            def __init__(self, context, metric: AvgValue):
                self.__context = context
                self.__metric = metric

            async def __aenter__(self):
                async with MeasureTook() as took:
                    connection = await self.__context.__aenter__()

                self.__metric += int(took)
                return connection

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                await self.__context.__aexit__(exc_type, exc_val, exc_tb)

        def wrapper(*args, **kwargs):
            return Context(func(*args, **kwargs), self.__metrics["pool_acquire"])

        return wrapper

    async def __create_pool(self):
        self.__pool = await asynch.create_pool(
            host=self.__db_config["host"],
            port=self.__db_config["port"],
            user=self.__db_config["user"],
            password=self.__db_config["password"],
            database=self.__db_config["db"]
        )
        self.__pool.acquire = self.__pool_acquire_wrapper(self.__pool.acquire)

    async def __aexit__(self, exc_type, exc, tb):
        self.__pool.close()
        await self.__pool.wait_closed()

    @clickhouse_span
    async def __execute_cursor(self, cur, sql, args):
        sql = sql.replace("?", "%s")
        logger = None
        if self.debug:
            query_id = randint(10000, 100000000000000000000000)
            logger = self.logger.with_tags(str(query_id))
            logger.debug("--- START SQL QUERY DEBUG ---")
            logger.debug(f"sql: {sql}")
            logger.debug(f"args: {args}")

        async with MeasureTook() as took:
            await cur.execute(query=sql, context=args)
        took = int(took)

        self.__metrics["queries_count"].increment()
        self.__metrics["query_time"] += took
        if took >= 1000:
            self.__metrics["slow_queries"].increment()
            self.__metrics["slow_query_time"] += took

        if self.debug:
            logger.debug(f"Query took: {took / 1000} sec.")
            logger.debug("--- END SQL QUERY DEBUG ---")

    async def __execute(self, sql, args):
        async with self.__pool.acquire() as conn:
            cur = conn.cursor(cursor=DictCursor)
            await self.__execute_cursor(cur=cur, sql=sql, args=args)
            await cur.close()
            return cur

    async def execute(self, sql, args=()):
        return await self.__execute(sql=sql, args=args)

    async def __select(self, sql, args, size):
        async with self.__pool.acquire() as conn:
            cur = conn.cursor(DictCursor)
            await self.__execute_cursor(cur=cur, sql=sql, args=args)

        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        await cur.close()
        return rs

    async def select(self, sql, args=(), size=None):
        return await self.__select(sql=sql, args=args, size=size)

    @clickhouse_span
    async def __executemany_cursor(self, cur, sql: str, data: list[tuple]):
        sql = sql.replace("?", "%s")
        logger = None
        if self.debug:
            query_id = randint(10000, 100000000000000000000000)
            logger = self.logger.with_tags(str(query_id))
            logger.debug("--- START SQL QUERY DEBUG ---")
            logger.debug(f"sql: {sql}")
            logger.debug(f"data: {data=}")

        async with MeasureTook() as took:
            await cur.executemany(query=sql, args=data)
        took = int(took)

        self.__metrics["queries_count"].increment()
        self.__metrics["query_time"] += took
        if took >= 1000:
            self.__metrics["slow_queries"].increment()
            self.__metrics["slow_query_time"] += took

        if self.debug:
            logger.debug(f"Query took: {took / 1000} sec.")
            logger.debug("--- END SQL QUERY DEBUG ---")

    async def __executemany(self, sql: str, data: list[tuple]):
        async with self.__pool.acquire() as conn:
            cur = conn.cursor(cursor=DictCursor)
            await self.__executemany_cursor(cur=cur, sql=sql, data=data)
            await cur.close()
            return cur

    async def executemany(self, sql: str, data: list[tuple]):
        return await self.__executemany(sql, data)
