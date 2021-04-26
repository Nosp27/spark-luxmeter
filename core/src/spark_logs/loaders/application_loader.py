import asyncio
import time

import orjson
from typing import Optional

from aioredis import Redis

from spark_logs import db
from spark_logs.loaders.clients import MetricsClient


class ApplicationLoader:
    def __init__(
        self, metrics_client: MetricsClient, app_id, fetch_last_jobs, timeout=10
    ):
        self.app_id = app_id
        self.metrics_client: MetricsClient = metrics_client
        self.fetch_last_jobs = fetch_last_jobs
        self.redis: Optional[Redis] = None
        self.timeout = timeout

    async def loop_update_app_metrics(self):
        try:
            while True:
                await self.update_app_metrics()
                await asyncio.sleep(self.timeout)
        except Exception:
            import traceback
            traceback.print_exc()
            raise

    async def update_app_metrics(self):
        execution_timestamp = time.time()
        fresh_metrics = await self.fresh_app_metrics()
        await self._report_metrics(fresh_metrics, execution_timestamp)

    async def fresh_app_metrics(self):
        metrics_client = self.metrics_client
        metrics_data = dict()
        executor_metrics = await metrics_client.get_node_metrics(
            node="executors", application_id=self.app_id
        )
        metrics_data["executor_metrics"] = executor_metrics
        jobs = await metrics_client.get_node_metrics("jobs", application_id=self.app_id)
        jobs_to_fetch = jobs
        if self.fetch_last_jobs is not None:
            jobs_to_fetch = jobs_to_fetch[: self.fetch_last_jobs]
        jobs_data = await asyncio.gather(
            *[self.fetch_for_job(job) for job in jobs_to_fetch]
        )
        metrics_data["job"] = dict(jobs_data)
        return metrics_data

    async def _report_metrics(self, fresh_metrics, execution_timestamp):
        if self.redis is None:
            self.redis = await db.connect_with_redis()
        value = orjson.dumps(fresh_metrics, option=orjson.OPT_NON_STR_KEYS)
        await self.redis.zadd(self.app_id, execution_timestamp, value)

    async def fetch_for_job(self, job):
        stage_ids = job["stageIds"]
        job_data = dict()
        job_data["stage"] = dict()
        job_stages = await asyncio.gather(*[self.fetch_for_stage(stage_id) for stage_id in stage_ids])
        job_stages = [stage for stage in job_stages if stage[1]["data"]["status"] not in ("COMPLETE", "RUNNING")]
        job_data["stage"] = dict(job_stages)
        return job["jobId"], job_data

    async def fetch_for_stage(self, stage_id):
        stage_id = str(stage_id)
        stage, tasks = await self.metrics_client.get_node_metrics(
            "stage", application_id=self.app_id, stage_id=stage_id
        )
        return stage_id, {"data": stage, "tasks": tasks}


class AppIdsLoader:
    def __init__(
        self, redis, metrics_client: MetricsClient, timeout=10
    ):
        self.metrics_client: MetricsClient = metrics_client
        self.redis: Redis = redis
        self.timeout = timeout

    async def set_for_app(self, app):
        if app["State"] == "RUNNING":
            await self.redis.zadd("applications", int(app["StartTime"]), app["ID"])

    async def loop_update_app_ids(self):
        while True:
            print("Updating app ids")
            apps = await self.metrics_client.get_node_metrics("applications")
            await asyncio.gather(*[self.set_for_app(app) for app in apps])
            await asyncio.sleep(self.timeout)


