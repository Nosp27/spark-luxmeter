import asyncio
from datetime import datetime

import numpy as np
from typing import List, Dict, Tuple
from spark_logs import kvstore

import orjson
from aioredis import Redis
import graphitesend

from spark_logs.anomaly_detection.dataset_extractor import JobGroupedExtractor
from spark_logs.anomaly_detection.features import (
    StageRunTimeFeature,
    StageShuffleReadFeature,
)
from spark_logs.config import DEFAULT_CONFIG


class DetectionProcessor:
    def __init__(self, app_id, detector, batch=5, timeout=10):
        self.detector = detector
        self.timeout = timeout
        self.dataset_extractor = JobGroupedExtractor
        self.app_id = app_id
        self.graphite_server = DEFAULT_CONFIG.get("graphite_server")
        self.graphite_port = DEFAULT_CONFIG.get("graphite_port")
        self._graphite_client = None
        self._batch = batch

    @property
    def graphite_client(self):
        if self._graphite_client is None:
            self._graphite_client = graphitesend.GraphiteClient(
                prefix="anomaly_detection",
                system_name="",
                graphite_server=self.graphite_server,
                graphite_port=self.graphite_port,
                autoreconnect=True,
            )
        return self._graphite_client

    async def loop_process(self, redis: Redis):
        try:
            while True:
                print("Iteration")
                last_score = await self.load_last_job_score(redis)
                jobs_raw, (last_score, *_) = await redis.zrevrangebyscore(
                    kvstore.sequential_jobs_key(app_id=self.app_id),
                    min=last_score,
                    exclude=True,
                    withscores=True,
                    count=self._batch,
                )
                jobs = orjson.loads(jobs_raw)

                extractor = self.dataset_extractor(
                    jobs, features=[StageRunTimeFeature(), StageShuffleReadFeature()],
                )
                grouped_predicts = await self.process_sequential_jobs(extractor)

                await self.save_last_job_score(redis, last_score)

                key_mapping = extractor.group_key_mapping
                for group_key, group_data in grouped_predicts.items():
                    hash_ = key_mapping[group_key]
                    await redis.set(
                        kvstore.job_group_hashes_key(
                            app_id=self.app_id, group_hash=hash_
                        ),
                        group_key,
                    )
                    await self.write_to_graphite(group_data, key_mapping)
                    print("Supplied")
                await asyncio.sleep(self.timeout)
        except Exception:
            import traceback

            traceback.print_exc()
            raise

    def write_to_graphite(self, grouped_predicts, key_mapping):
        client = self.graphite_client
        for key, group in grouped_predicts:
            for (predict, timestamp) in group:
                client.send(f"sequential.{key_mapping[key]}", predict, timestamp)

    async def process_sequential_jobs(
        self, extractor
    ) -> Dict[str, List[Tuple[np.array, datetime]]]:
        grouped_datasets = extractor.extract()
        detector = self.detector
        grouped_predicts = {
            k: detector.detect_anomalies(dataset)
            for k, dataset in grouped_datasets.items()
        }

        ret = dict()
        for group, predicts in grouped_predicts.items():
            timestamps = extractor.get_timestamps(group)
            assert len(timestamps) == len(predicts)
            ret[group] = list(zip(predicts, timestamps))

        return ret

    async def load_last_job_score(self, redis):
        return int(
            await redis.get(kvstore.latest_processed_job_id_key(app_id=self.app_id))
            or 0
        )

    async def save_last_job_score(self, redis, score: int):
        await redis.sadd(kvstore.latest_processed_job_id_key(app_id=self.app_id), score)
