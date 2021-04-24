import abc
import asyncio
from typing import Any, Dict

import orjson
from aioredis import Redis


class HybridMetricStrategy(abc.ABC):
    @abc.abstractmethod
    def apply(self, data: Dict[str, Any]):
        pass

    async def loop_app_apply(self, redis: Redis, app_id):
        while True:
            await self._app_latest_apply(redis, app_id)
            await asyncio.sleep(30)

    async def _app_latest_apply(self, redis: Redis, app_id):
        data = await redis.zrevrangebyscore(app_id, count=1)
        applications_data = [orjson.loads(d) for d in data]
        for app_data in applications_data:
            jobs = app_data["job"]
            for job_data in jobs.values():
                result = self.apply(job_data)
                await asyncio.gather(
                    *[
                        self.write_stage_test(
                            redis, app_id, stage_id, stage_id in result
                        )
                        for stage_id in job_data["stage"]
                    ]
                )

    async def write_stage_test(self, redis: Redis, app_id, stage_id, test_result):
        key = f"{app_id}:{stage_id}"
        if not await redis.exists(key):
            await redis.set(key, test_result)
