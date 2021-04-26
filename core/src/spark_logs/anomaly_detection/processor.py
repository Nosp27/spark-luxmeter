import asyncio
import base64
from hashlib import md5

from aioredis import Redis
import graphitesend

from spark_logs.config import DEFAULT_CONFIG


class DetectionProcessor:
    def __init__(self, app_id, dataset_extractor, detector, timeout=10):
        self.detector = detector
        self.timeout = timeout
        self.dataset_extractor = dataset_extractor
        self.app_id = app_id
        self.graphite_server = DEFAULT_CONFIG.get("graphite_server")
        self.graphite_port = DEFAULT_CONFIG.get("graphite_port")
        self._graphite_client = None

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

    def hash_group_key(self, key):
        return base64.b64encode(md5(key).digest()).decode()

    async def loop_process(self, redis: Redis):
        while True:
            grouped_predicts = await self.process_sequential_jobs(redis)
            for group_key in grouped_predicts:
                hash_ = self.hash_group_key(group_key)
                await redis.set(f"job_group:{self.app_id}:{hash_}", group_key)
                await self.write_to_graphite(grouped_predicts)
            await asyncio.sleep(self.timeout)

    def write_to_graphite(self, grouped_predicts):
        client = self.graphite_client
        for key, group in grouped_predicts:
            for (predict, timestamp) in group:
                client.send(f"sequential.{self.hash_group_key(key)}", predict, timestamp)

    async def process_sequential_jobs(self, redis: Redis):
        app_data = await redis.zrevrangebyscore(self.app_id, count=1)
        grouped_datasets = self.dataset_extractor.extract(app_data)
        detector = self.detector
        grouped_predicts = {k: detector.detect_anomalies(dataset) for k, dataset in grouped_datasets.items()}

        ret = dict()
        for group, predicts in grouped_predicts.items():
            timestamps = self.dataset_extractor.job_group_timestamps[group]
            assert len(timestamps) == len(predicts)
            ret[group] = list(zip(predicts, timestamps))

        return ret
