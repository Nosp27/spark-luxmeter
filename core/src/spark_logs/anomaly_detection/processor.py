import asyncio
import pickle
from abc import abstractmethod
from datetime import datetime
from typing import List, Dict, Type, Set, Iterable

import graphitesend
import numpy as np
from aioredis import Redis

from spark_logs import kvstore
from spark_logs.anomaly_detection.dataset_extractor import JobGroupedExtractor
from spark_logs.anomaly_detection.detectors import Model
from spark_logs.anomaly_detection.features import (
    StageRunTimeFeature,
    StageShuffleReadFeature,
    Feature)
from spark_logs.config import DEFAULT_CONFIG
from spark_logs.types import JobStages


class BaseProcessor:
    def __init__(self, *, timeout):
        self.timeout = timeout

    async def loop_process(self, *args):
        first_step = True
        try:
            while True:
                if first_step:
                    first_step = False
                else:
                    await asyncio.sleep(self.timeout)
                print(f"Iteration {type(self)} #{id(self)}")
                await self.process_iteration(*args)
        except Exception:
            import traceback

            traceback.print_exc()
            raise

    @abstractmethod
    async def process_iteration(self, *args):
        pass


class SequentialJobsProcessor(BaseProcessor):
    @property
    def processor_id(self):
        raise NotImplementedError()

    def __init__(self, app_id, extractor_cls, batch=15, timeout=10):
        super().__init__(timeout=timeout)
        self.timeout = timeout
        self.dataset_extractor_cls: Type[JobGroupedExtractor] = extractor_cls
        self.app_id = app_id
        self.graphite_server = DEFAULT_CONFIG.get("graphite_server")
        self.graphite_port = DEFAULT_CONFIG.get("graphite_port")
        self._graphite_client = None
        self._batch = batch
        self.features: List[Feature] = [StageRunTimeFeature(), StageShuffleReadFeature()]

    @property
    def graphite_client(self):
        if self._graphite_client is None:
            self._graphite_client = graphitesend.GraphiteClient(
                prefix=self.processor_id,
                system_name="",
                graphite_server="localhost",
                graphite_port=self.graphite_port,
                autoreconnect=True,
            )
        return self._graphite_client

    async def process_iteration(self, redis: Redis):
        jobs: List[JobStages] = await self.load_jobs(redis)
        if not jobs:
            return
        extractor = self.dataset_extractor_cls(
            jobs, features=self.features,
        )
        grouped_dataset: Dict[str, np.ndarray] = extractor.extract()
        timestamps: Dict[str, List[datetime]] = extractor.get_all_timestamps()

        for group_key, group_data in grouped_dataset.items():
            await self.process_group(
                group_key, group_data, timestamps[group_key], redis, group_alias=extractor.group_long_aliases[group_key]
            )

        await self.report_jobs(redis, jobs)
        print("Supplied")

    @abstractmethod
    async def process_group(self, group_key, group_data: np.ndarray, timestamps, redis, group_alias):
        pass

    async def load_jobs(self, redis) -> List[JobStages]:
        reported_jobs: Set[int] = await self.load_reported_jobs(redis)

        data = {
            score: job
            for job, score in await redis.zrevrangebyscore(
                kvstore.sequential_jobs_key(app_id=self.app_id), withscores=True
            )
        }
        if not data:
            print("No data")
            return []
        print(f"Data: {len(data)} lines")

        job_ids_to_process: List[int] = sorted(data.keys() - reported_jobs)[-self._batch:]
        print("Job ids: ", job_ids_to_process)

        return [
            JobStages.from_json(data[jid]) for jid in job_ids_to_process
        ]

    async def load_reported_jobs(self, redis) -> Set[int]:
        key = kvstore.time_series_processed_jobs(app_id=self.app_id, processor_id=self.processor_id)
        if not await redis.exists(key):
            return set()
        return {int(x) for x in await redis.smembers(key)}

    async def report_jobs(self, redis, jobs: Iterable[JobStages]):
        if not jobs:
            return
        await redis.sadd(
            kvstore.time_series_processed_jobs(app_id=self.app_id, processor_id=self.processor_id),
            *(job.job.jobId for job in jobs)
        )


class SequentialDetector(SequentialJobsProcessor):
    processor_id = "anomaly_detection"

    def __init__(self, *args, detector_cls):
        super().__init__(*args)
        self.detector_cls = detector_cls
        self._group_detectors = dict()

    async def process_group(self, group_key, group_data: np.ndarray, timestamps, redis, group_alias):
        await redis.set(
            kvstore.job_group_hashes_key(
                app_id=self.app_id, group_hash=group_key
            ),
            group_alias,
        )

        # Write raw features
        featurenames = [f.name for f in self.features]
        raw_features_data = group_data.reshape((group_data.shape[0], -1, len(featurenames)))
        raw_features_data = np.where(raw_features_data == None, 0, raw_features_data).mean(axis=1)
        self.write_features_to_graphite(
            group_key, raw_features_data, timestamps, featurenames
        )

        model_loaded = await redis.get(kvstore.anomaly_model_key(
            app_id=self.app_id, model_name=self.detector_cls.model_name, job_group=group_key
        ))
        if model_loaded is not None:
            model_loaded = pickle.loads(model_loaded)
        self._group_detectors.setdefault(group_key, self.detector_cls())
        detector = self._group_detectors[group_key]
        detector.model = model_loaded
        if detector.ready:
            predicts = detector.detect_anomalies(group_data)
            assert len(timestamps) == len(predicts)
            # Write predicts
            self.write_predicts_to_graphite(
                group_key, predicts, timestamps
            )
        else:
            print("Detector not ready")

    def write_predicts_to_graphite(self, key: str, predicts: np.ndarray, timestamps: List[datetime]):
        assert 1

    def write_features_to_graphite(
            self, key, data: np.ndarray, timestamps: List[datetime], featurenames: List[str]
    ):
        assert data.shape == (len(timestamps), len(featurenames))
        client = self.graphite_client
        for data_row, timestamp in zip(data, timestamps):
            client.send_dict(
                {
                    f"sequential.3.{key}.{feature_name}": feature_value
                    for feature_name, feature_value in zip(featurenames, data_row)
                },
                int(timestamp.timestamp()),
            )


class SequentialJobsFitter(SequentialJobsProcessor):
    processor_id = "model_fitter"

    def __init__(self, *args, detector_cls, **kwargs):
        super().__init__(*args, **kwargs)
        self.detector_cls = detector_cls
        self._group_detectors = dict()

    async def process_group(self, group_key, group_data: np.ndarray, timestamps, redis, group_alias):
        self._group_detectors.setdefault(group_key, self.detector_cls())
        detector: Model = self._group_detectors[group_key]
        model_key = kvstore.anomaly_model_key(
            app_id=self.app_id, model_name=detector.model_name, job_group=group_key
        )
        if not detector.ready:
            model_path = await redis.get(model_key)
            if model_path is not None:
                model = detector.load(model_path)
                detector.model = model
        detector.fit(group_data)

        filepath = detector.save(model_key)
        await redis.set(model_key, filepath)
