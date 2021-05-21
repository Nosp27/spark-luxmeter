import abc
import asyncio
from typing import Dict, List

from aioredis import Redis

from spark_logs import kvstore
from spark_logs.types import ApplicationMetrics, JobStages, StageTasks


class HybridMetricStrategy(abc.ABC):
    test_name = None

    @abc.abstractmethod
    def apply(self, data: StageTasks) -> float:
        pass

    async def loop_app_apply(self, redis: Redis, app_id):
        try:
            while True:
                await self._app_latest_apply(redis, app_id)
                await asyncio.sleep(3)
        except Exception:
            import traceback

            traceback.print_exc()
            raise

    async def _app_latest_apply(self, redis: Redis, app_id):
        data = await redis.zrevrangebyscore(app_id, offset=0, count=1)
        applications_data: List[ApplicationMetrics] = [
            ApplicationMetrics.from_json(d) for d in data
        ]
        for app_data in applications_data:
            jobs: Dict[str, JobStages] = app_data.jobs_stages
            for job_data in jobs.values():
                await asyncio.gather(
                    *[
                        self.write_stage_test(
                            redis, app_id, stage_id, float(self.apply(stage_data))
                        )
                        for stage_id, stage_data in job_data.stages.items()
                    ]
                )

    async def write_stage_test(self, redis: Redis, app_id, stage_id, test_result):
        key = kvstore.hybrid_metric_key(
            app_id=app_id, metric_name=self.test_name, job_id=stage_id
        )
        if not await redis.exists(key):
            await redis.set(key, test_result)
