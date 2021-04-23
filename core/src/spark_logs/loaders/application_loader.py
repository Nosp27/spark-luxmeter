import asyncio

from spark_logs.loaders.clients import MetricsClient


class ApplicationLoader:
    def __init__(self, metrics_client: MetricsClient, app_id, fetch_last_jobs):
        self.app_id = app_id
        self.metrics_client: MetricsClient = metrics_client
        self.app_metrics = []
        self.offset = dict()

        self.fetch_last_jobs = fetch_last_jobs

    async def append_app_metrics(self):
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
        self.app_metrics.append(metrics_data)

    async def fetch_for_job(self, job):
        stage_ids = job["stageIds"]
        job_data = dict()
        job_data["stage"] = dict()

        job_stages = await asyncio.gather(
            *[self.fetch_for_stage(stage_id) for stage_id in stage_ids]
        )
        job_data["stage"] = dict(job_stages)
        return job["jobId"], job_data

    async def fetch_for_stage(self, stage_id):
        stage_id = str(stage_id)
        stage, tasks = await self.metrics_client.get_node_metrics(
            "stage", application_id=self.app_id, stage_id=stage_id
        )
        return stage_id, {"data": stage, "tasks": tasks}
