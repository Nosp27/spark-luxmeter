import abc
import asyncio
from typing import Dict, List

import orjson
from aioredis import Redis
from graphitesend import GraphiteClient

from spark_logs import kvstore
from spark_logs.types import ApplicationMetrics, JobStages, StageTasks


class HybridMetricStrategy(abc.ABC):
    test_name = None

    @abc.abstractmethod
    def apply(self, data: StageTasks) -> float:
        pass

    async def loop_app_apply(self, redis: Redis, graphite: GraphiteClient, app_id):
        try:
            while True:
                await self._app_latest_apply(redis, graphite, app_id)
                await asyncio.sleep(10)
        except asyncio.CancelledError:
            pass
        except Exception:
            import traceback

            traceback.print_exc()
            raise

    async def _app_latest_apply(self, redis: Redis, graphite, app_id):
        data = await redis.zrevrangebyscore(kvstore.sequential_jobs_key(app_id=app_id), count=3, offset=0)
        last_jobs = [JobStages.from_json(d) for d in data]
        for job_data in last_jobs:
            job_group_alias = await self.resolve_job_group(app_id, job_data, redis)
            if job_group_alias is None:
                continue
            job_group_alias = job_group_alias.decode()
            await asyncio.gather(
                *[
                    self.write_stage_test(
                        graphite, app_id, float(self.apply(stage_data)), job_data.job.completionTime, job_group_alias
                    )
                    for stage_id, stage_data in job_data.stages.items()
                ]
            )

    async def resolve_job_group(self, app_id, job_data, redis):
        job_group_raw_key = ",".join(sorted([str(s.stage.numTasks) for s in job_data.stages.values()]))
        job_group_alias = await redis.get(kvstore.job_group_hashes_key(app_id=app_id, group_hash=job_group_raw_key))
        return job_group_alias

    async def write_stage_test(
            self, graphite: GraphiteClient, app_id, test_result, completion_time, job_group_alias
    ):
        key = f"app.{app_id}.job_group.{job_group_alias}.test.{self.test_name}"
        graphite.send(key, test_result, completion_time.timestamp())
