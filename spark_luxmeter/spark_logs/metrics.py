import aiohttp

spark_metrics_endpoints = {
    "applications": "%(api_path)s/applications/",
    "application_data": "%(api_path)s/applications/%(application_id)s/",
    "jobs": "%(api_path)s/applications/%(application_id)s/jobs",
    "job_data": "%(api_path)s/applications/%(application_id)s/jobs/%(job_id)s",
    "executors": "%(api_path)s/applications/%(application_id)s/executors",
    "executor_data": "%(api_path)s/applications/%(application_id)s/executors/%(executor_id)s",
}


class MetricsClient:
    def __init__(
            self, driver_host: str, driver_port: int = 4040, update_interval=2 * 60
    ):
        self.driver_endpoint = f"http://{driver_host}:{driver_port}"
        self.update_interval = update_interval

    async def start_monitoring(self):
        await self.check_driver_reachable()

    async def check_driver_reachable(self):
        async with aiohttp.ClientSession() as session:
            resp: aiohttp.ClientResponse = await session.request(
                "GET", self.driver_endpoint
            )
            resp.raise_for_status()

    async def get_spark_metrics(self, endpoint, **kwargs):
        expected_kwargs = {"application_id", "job_id", "executor_id"}
        if not kwargs.keys() <= expected_kwargs:
            raise RuntimeError(f"Kwargs contain unexpected element(s): {kwargs.keys() - expected_kwargs}")
        url = spark_metrics_endpoints[endpoint]
        kwargs["api_path"] = f"{self.driver_endpoint}/api/v1"
        async with aiohttp.ClientSession() as session:
            resp: aiohttp.ClientResponse = await session.request(
                "GET", url % kwargs
            )
            return await resp.json()
