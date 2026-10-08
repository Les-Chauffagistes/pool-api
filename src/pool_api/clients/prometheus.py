import httpx


class PrometheusClient:
    def __init__(self, http: httpx.AsyncClient):
        self.http = http

    async def query(self, promql: str) -> list[dict]:
        """Requête instantanée : renvoie la liste brute des séries (`data.result`)."""
        response = await self.http.get("/api/v1/query", params={"query": promql})
        response.raise_for_status()
        body = response.json()
        if body["status"] != "success":
            raise RuntimeError(f"Requête Prometheus en erreur : {body.get('error')}")
        return body["data"]["result"]

    async def query_scalar(self, promql: str) -> str:
        """Valeur brute (chaîne, telle que renvoyée par Prometheus) de la première série."""
        result = await self.query(promql)
        if not result:
            raise LookupError(f"Aucune série pour la requête Prometheus : {promql}")
        return result[0]["value"][1]
