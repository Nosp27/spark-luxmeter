import asyncio
import itertools
import time
from typing import Optional, List, Dict, Tuple

import orjson
from aioredis import Redis

from spark_logs import db, kvstore
from spark_logs.loaders.clients import MetricsClient
from spark_logs.types import Executor, Job, StageTasks, JobStages, ApplicationMetrics


class ApplicationLoader:
    def __init__(
            self, metrics_client: MetricsClient, app_id, fetch_last_jobs, timeout=10
    ):
        self.app_id = app_id
        self.metrics_client: MetricsClient = metrics_client
        self.job_selector = JobSelector(fetch_last_jobs)
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

    async def fresh_app_metrics(self) -> ApplicationMetrics:
        metrics_client = self.metrics_client
        executor_metrics: List[Executor] = await metrics_client.get_node_metrics(
            node="executors", application_id=self.app_id
        )

        jobs: List[Job] = await metrics_client.get_node_metrics(
            "jobs", application_id=self.app_id
        )
        jobs_to_fetch: List[Job] = self.job_selector.select(jobs)
        jobs_stages_list: List[JobStages] = await asyncio.gather(
            *[self.fetch_for_job(job) for job in jobs_to_fetch]
        )
        jobs_data = {js.job.jobId: js for js in jobs_stages_list}

        return ApplicationMetrics(
            executor_metrics=executor_metrics, jobs_stages=jobs_data
        )

    async def _report_metrics(self, fresh_metrics: ApplicationMetrics, execution_timestamp):
        if self.redis is None:
            self.redis = await db.connect_with_redis()
        value = fresh_metrics.dump()
        await self.redis.zadd(self.app_id, execution_timestamp, value)

        if len(fresh_metrics.jobs_stages) > 0:
            args = list(itertools.chain.from_iterable(
                [
                    (int(job_id), job_data.dump())
                    for job_id, job_data in fresh_metrics.jobs_stages.items()
                    if job_data.job.completionTime is not None
                ]
            ))
            await self.redis.zadd(kvstore.sequential_jobs_key(app_id=self.app_id), *args)

    async def fetch_for_job(self, job: Job):
        stage_ids = job.stageIds
        job_stages_list: List[StageTasks] = await asyncio.gather(
            *[self.fetch_for_stage(stage_id) for stage_id in stage_ids]
        )
        job_stages: Dict[str, StageTasks] = {
            stage_and_tasks.stage.stageId: stage_and_tasks
            for stage_and_tasks in job_stages_list
            if stage_and_tasks.stage.status in ("COMPLETE", "RUNNING")
        }
        return JobStages(job=job, stages=job_stages)

    async def fetch_for_stage(self, stage_id):
        stage_and_tasks = await self.metrics_client.get_node_metrics(
            "stage", application_id=self.app_id, stage_id=str(stage_id)
        )
        return stage_and_tasks


class AppIdsLoader:
    def __init__(self, redis, metrics_client: MetricsClient, timeout=10):
        self.metrics_client: MetricsClient = metrics_client
        self.redis: Redis = redis
        self.timeout = timeout

    async def set_for_app(self, app):
        if app["State"] == "RUNNING":
            await self.redis.zadd(
                kvstore.applications_key(), int(app["StartTime"]), app["ID"]
            )

    async def loop_update_app_ids(self):
        while True:
            print("Updating app ids")
            apps = await self.metrics_client.get_node_metrics("applications")
            await asyncio.gather(*[self.set_for_app(app) for app in apps])
            await asyncio.sleep(self.timeout)


class JobSelector:
    def __init__(self, batch_size):
        self.batch_size = batch_size

    def select(self, jobs_data: List[Job]):
        return jobs_data[: self.batch_size]
