import asyncio
from datetime import datetime
from typing import List, Dict, Tuple, Type

import graphitesend
import numpy as np
from aioredis import Redis

from spark_logs import kvstore
from spark_logs.anomaly_detection.dataset_extractor import JobGroupedExtractor
from spark_logs.anomaly_detection.features import (
    StageRunTimeFeature,
    StageShuffleReadFeature,
)
from spark_logs.config import DEFAULT_CONFIG
from spark_logs.types import JobStages


class DetectionProcessor:
    def __init__(self, app_id, extractor_cls, detector, batch=5, timeout=10):
        self.detector = detector
        self.timeout = timeout
        self.dataset_extractor_cls: Type[JobGroupedExtractor] = extractor_cls
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
                graphite_server="localhost",
                graphite_port=self.graphite_port,
                autoreconnect=True,
            )
        return self._graphite_client

    async def loop_process(self, redis: Redis):
        first_step = True
        try:
            while True:
                if first_step:
                    first_step = False
                else:
                    await asyncio.sleep(self.timeout)
                print("Iteration")
                last_score: int = int(await self.load_last_job_score(redis))
                data = await redis.zrevrangebyscore(
                    kvstore.sequential_jobs_key(app_id=self.app_id),
                    min=last_score,
                    exclude=redis.ZSET_EXCLUDE_BOTH,
                    withscores=True,
                    count=self._batch,
                    offset=0,
                )
                if not data:
                    print("No data")
                    continue
                print(f"Data: {len(data)} lines")
                jobs_raw = [x[0] for x in data]
                last_score = data[0][1]
                jobs: List[JobStages] = [JobStages.from_json(d) for d in jobs_raw]

                extractor = self.dataset_extractor_cls(
                    jobs, features=[StageRunTimeFeature(), StageShuffleReadFeature()],
                )
                group_dataset: Dict[str, np.array] = extractor.extract()
                timestamps: Dict[str, List[datetime]] = extractor.get_all_timestamps()

                # grouped_predicts = await self.process_sequential_jobs(group_dataset, timestamps)

                await self.save_last_job_score(redis, last_score)

                key_mapping = extractor.group_key_aliases
                # for group_key, group_data in grouped_predicts.items():
                #     hash_ = key_mapping[group_key]
                #     await redis.set(
                #         kvstore.job_group_hashes_key(
                #             app_id=self.app_id, group_hash=group_key
                #         ),
                #         hash_,
                #     )
                #     await self.write_to_graphite(group_data, key_mapping)

                features = [f.name for f in extractor.features]
                for group_key, dataset in group_dataset.items():
                    self.write_to_graphite(
                        group_key, dataset, timestamps[group_key], features
                    )
                print("Supplied")
        except Exception:
            import traceback

            traceback.print_exc()
            raise

    def write_to_graphite(
            self, key, data: np.array, timestamps: List[datetime], featurenames: List[str]
    ):
        assert len(data.shape) == 2
        assert data.shape[0] == 1
        data = data.reshape((data.shape[0], -1, len(featurenames)))
        data = np.where(data == None, 0, data).mean(axis=1)
        client = self.graphite_client
        for data_row, timestamp in zip(data, timestamps):
            client.send_dict(
                {
                    f"sequential.{key}.{feature_name}": feature_value
                    for feature_name, feature_value in zip(featurenames, data_row)
                },
                int(timestamp.timestamp()),
            )

    async def process_sequential_jobs(
            self, grouped_datasets, timestamps
    ) -> Dict[str, List[Tuple[np.array, datetime]]]:
        detector = self.detector
        grouped_predicts = {
            k: detector.detect_anomalies(dataset)
            for k, dataset in grouped_datasets.items()
        }

        ret = dict()
        for group, predicts in grouped_predicts.items():
            assert len(timestamps) == len(predicts)
            ret[group] = list(zip(predicts, timestamps))
        return ret

    async def load_last_job_score(self, redis):
        return int(
            await redis.get(kvstore.latest_processed_job_id_key(app_id=self.app_id))
            or 0
        )

    async def save_last_job_score(self, redis, score: int):
        await redis.set(kvstore.latest_processed_job_id_key(app_id=self.app_id), score)
