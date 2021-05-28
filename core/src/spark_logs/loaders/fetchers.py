import abc
import asyncio

import aiohttp
from yarl import URL

from spark_logs.config import DEFAULT_CONFIG


class BaseFetcher:
    def __init__(self, *, config=None):
        self.fetch_lock = asyncio.Lock()
        self.config = config or DEFAULT_CONFIG

    @abc.abstractmethod
    async def fetch(self, **kwargs):
        pass


class HttpFetcher(BaseFetcher):
    def get_url(self, *, node: str, **data):
        base_url = URL(self.config["base_url"])

        if node == "applications":
            return base_url / "cluster"

        application_id = data.pop("application_id")
        api_url = (
            base_url
            / "proxy"
            / application_id
            / "api"
            / "v1"
            / "applications"
            / application_id
        )
        if node == "application":
            return api_url
        if node == "executors":
            return api_url / "executors"
        if node == "jobs":
            return api_url / "jobs"
        if node == "job":
            job_id = data["job_id"]
            return api_url / "jobs" / job_id
        if node == "stages":
            return api_url / "stages"
        if node == "stage":
            stage_id = data["stage_id"]
            return api_url / "stages" / stage_id
        if node == "tasks":
            stage_id = data["stage_id"]
            attempt_id = data["attempt_id"]
            return api_url / "stages" / stage_id / attempt_id / "taskList"
        if node == "task":
            job_id = data["job_id"]
            task_id = data["task_id"]
            return api_url / "jobs" / job_id / "tasks" / task_id
        if node == "environment":
            return api_url / "environment"
        raise NotImplementedError()

    async def fetch(self, *, node, resp_format, **data):
        async with self.fetch_lock:
            await asyncio.sleep(0.3)  # For not making too much requests per second
            async with aiohttp.client.ClientSession() as session:
                url = self.get_url(node=node, **data)
                print(url)
                response = await session.get(url)
                response.raise_for_status()
                if resp_format is "meta":
                    return response
                if resp_format == "json":
                    return await response.json()
                if resp_format == "html":
                    return await response.text()
                raise NotImplementedError()
