import abc

import requests
from yarl import URL

from spark_luxmeter.spark_logs.config import CONFIG


class BaseFetcher:
    @abc.abstractmethod
    def fetch(self, **kwargs):
        pass


class HttpFetcher(BaseFetcher):
    def get_url(self, *, node: str, **data):
        base_url = URL(CONFIG["base_url"])
        if node == "applications":
            return base_url / "cluster"

        application_id = data.pop("application_id")
        api_url = base_url / "proxy" / application_id / "api" / "v1" / "applications" / application_id
        if node == "application":
            api_url = api_url
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
            job_id = data["job_id"]
            return api_url / "jobs" / job_id / "tasks"
        if node == "task":
            job_id = data["job_id"]
            task_id = data["task_id"]
            return api_url / "jobs" / job_id / "tasks" / task_id

    def fetch(self, *, node, **data) -> requests.Response:
        return requests.get(self.get_url(node=node, **data))
