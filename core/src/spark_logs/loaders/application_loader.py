import asyncio
import itertools
import time
from typing import Optional, List, Dict, Tuple, Set

import numpy
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

        self.stored_job_ids: Set[int] = set()

    async def loop_update_app_metrics(self):
        self.stored_job_ids = await self.load_stored_jobs()
        try:
            while True:
                await self.update_app_metrics()
                await asyncio.sleep(self.timeout)
        except Exception:
            import traceback

            traceback.print_exc()
            raise

    async def load_stored_jobs(self) -> Set[int]:
        if self.redis is None:
            self.redis = await db.connect_with_redis()
        key = kvstore.sequential_jobs_key(app_id=self.app_id)
        stored_job_ids = {
            score
            for _, score in await self.redis.zrevrangebyscore(key, withscores=True)
        }
        return stored_job_ids

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

        jobs_to_fetch: List[Job] = self.job_selector.select(jobs, self.stored_job_ids)
        jobs_stages_list: List[JobStages] = await asyncio.gather(
            *[self.fetch_for_job(job) for job in jobs_to_fetch]
        )
        jobs_data = {js.job.jobId: js for js in jobs_stages_list}

        return ApplicationMetrics(
            executor_metrics=executor_metrics, jobs_stages=jobs_data
        )

    async def _report_metrics(
        self, fresh_metrics: ApplicationMetrics, execution_timestamp
    ):
        if self.redis is None:
            self.redis = await db.connect_with_redis()

        running_jobs: Dict[str, JobStages] = dict()
        completed_jobs: Dict[str, JobStages] = dict()
        for job_id, job_data in fresh_metrics.jobs_stages.items():
            if job_data.job.completionTime is None:
                running_jobs[job_id] = job_data
            else:
                completed_jobs[job_id] = job_data
        value = ApplicationMetrics(
            executor_metrics=fresh_metrics.executor_metrics, jobs_stages=running_jobs
        ).dump()
        await self.redis.zadd(self.app_id, execution_timestamp, value)
        args = list(
            itertools.chain.from_iterable(
                [
                    (int(job_id), job_data.dump())
                    for job_id, job_data in completed_jobs.items()
                ]
            )
        )
        if len(args) > 0:
            try:
                await self.redis.zadd(
                    kvstore.sequential_jobs_key(app_id=self.app_id), *args
                )
            except Exception as exc:
                raise

        self.stored_job_ids |= {
            int(job_id) for job_id, job_data in fresh_metrics.jobs_stages.items()
        }

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

    def select(self, jobs_data: List[Job], stored_job_ids: Set[int]) -> List[Job]:
        jobs_dict = {int(j.jobId): j for j in jobs_data}
        job_ids_to_load = set(jobs_dict.keys()) - stored_job_ids

        sorted_ids = reversed(sorted(job_ids_to_load))
        to_load: List[Job] = [jobs_dict[i] for i in sorted_ids][: self.batch_size]

        print("Selected: ", [x.jobId for x in to_load])
        return to_load
