import httpx
import pytest

from pool_api.clients.prometheus import PrometheusClient


def make_client(handler) -> PrometheusClient:
    return PrometheusClient(httpx.AsyncClient(base_url="http://prom", transport=httpx.MockTransport(handler)))


async def test_query_returns_raw_series_and_sends_promql():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"], seen["query"] = request.url.path, request.url.params["query"]
        return httpx.Response(200, json={"status": "success", "data": {"result": [{"value": [1, "42"]}]}})

    assert await make_client(handler).query("bitcoin_blocks") == [{"value": [1, "42"]}]
    assert seen == {"path": "/api/v1/query", "query": "bitcoin_blocks"}


async def test_query_scalar_returns_the_first_value_as_string():
    body = {"status": "success", "data": {"result": [{"value": [1, "7"]}, {"value": [1, "9"]}]}}
    client = make_client(lambda r: httpx.Response(200, json=body))
    assert await client.query_scalar("x") == "7"


async def test_query_scalar_without_series_raises_lookup_error():
    client = make_client(lambda r: httpx.Response(200, json={"status": "success", "data": {"result": []}}))
    with pytest.raises(LookupError, match="my_metric"):
        await client.query_scalar("my_metric")


async def test_prometheus_error_status_raises_runtime_error():
    client = make_client(lambda r: httpx.Response(200, json={"status": "error", "error": "bad query"}))
    with pytest.raises(RuntimeError, match="bad query"):
        await client.query("x")


async def test_http_error_is_propagated():
    client = make_client(lambda r: httpx.Response(500))
    with pytest.raises(httpx.HTTPStatusError):
        await client.query("x")
