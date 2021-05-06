import abc
import hashlib
from collections import defaultdict
from datetime import datetime
from typing import NamedTuple, Any, Dict, List

import numpy as np
import pandas as pd

from spark_logs.anomaly_detection.features import StageFeature


class JobGroupElement(NamedTuple):
    key: str
    job_data: Dict[str, Any]
    completion_timestamp: datetime


class DatasetExtractor:
    def __init__(self, features: List[StageFeature]):
        self.features = features

    @abc.abstractmethod
    def extract(self):
        pass


class JobGroupedExtractor(DatasetExtractor):
    def __init__(self, jobs, **kwargs):
        super().__init__(**kwargs)
        self.jobs = jobs
        self._job_groups = None
        self._group_key_mapping = None

    def get_grouping_key(self, jdata) -> str:
        return ",".join([s["data"]["name"] for s in jdata["stage"].values()])

    def get_short_group_key(self, group_key) -> str:
        return self.group_key_mapping[group_key]

    def get_timestamps(self, group_key):
        return [x.completion_timestamp for x in self._job_groups[group_key]]

    @property
    def group_key_mapping(self):
        if self._group_key_mapping is None:
            group_keys = [self.get_grouping_key(j) for j in self.jobs.values()]
            self._group_key_mapping = {
                group: f"job_group_{idx}" for idx, group in enumerate(group_keys)
            }

        return self._group_key_mapping

    def extract(self) -> Dict[str, pd.DataFrame]:
        groups = self.get_groups()
        return {
            group_key: self.extract_for_group(group_jobs)
            for group_key, group_jobs in groups.items()
        }

    def get_groups(self) -> Dict[str, List[JobGroupElement]]:
        if not self._job_groups:
            jobs = self.jobs.values()
            groups = defaultdict(list)
            for job_data in jobs:
                completionTime = job_data.get("completionTime")
                if completionTime is None:
                    continue
                hashed_group_key = self.get_short_group_key(
                    self.get_grouping_key(job_data)
                )
                groups[hashed_group_key].append(
                    JobGroupElement(
                        key=hashed_group_key,
                        job_data=job_data,
                        completion_timestamp=completionTime,
                    )
                )
            self._job_groups = groups
        return self._job_groups

    def extract_for_group(self, group_data: List[JobGroupElement]):
        ret = np.array([self.extract_within_job(job) for job in group_data])
        assert len(ret.shape) == 2
        return ret

    def extract_within_job(self, job: JobGroupElement):
        ret = []
        for stage in job.job_data["stage"].values():
            ret.extend([feature.apply(stage) for feature in self.features])
        return ret
