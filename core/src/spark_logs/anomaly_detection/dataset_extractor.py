import abc
from collections import defaultdict
from datetime import datetime
from typing import NamedTuple, Dict, List, Union

import numpy as np

from spark_logs.anomaly_detection.features import StageFeature
from spark_logs.types import JobStages


class JobGroupElement(NamedTuple):
    key: str
    job_data: JobStages
    completion_timestamp: datetime


class DatasetExtractor:
    def __init__(self, features: List[StageFeature]):
        self.features = features

    @abc.abstractmethod
    def extract(self) -> Dict[str, np.array]:
        pass


class JobGroupedExtractor(DatasetExtractor):
    def __init__(self, jobs, **kwargs):
        super().__init__(**kwargs)
        self.jobs: List[JobStages] = jobs
        self._job_groups = None
        self._group_key_mapping = None

    def get_grouping_key(self, jdata: JobStages) -> str:
        return ",".join(sorted([str(s.stage.numTasks) for s in jdata.stages.values()]))

    def get_short_group_key(self, group_key) -> str:
        return self.group_key_aliases[group_key]

    def get_timestamps(self, group_key) -> List[datetime]:
        return [x.completion_timestamp for x in self._job_groups[group_key]]

    def get_all_timestamps(self) -> Dict[str, List[datetime]]:
        return {k: self.get_timestamps(k) for k in self._job_groups}

    @property
    def group_key_aliases(self) -> Dict[str, str]:
        """Maps long key to short"""
        if self._group_key_mapping is None:
            group_keys = {self.get_grouping_key(j) for j in self.jobs}
            self._group_key_mapping = {
                group: f"job_group_{idx}" for idx, group in enumerate(group_keys)
            }

        return self._group_key_mapping

    def extract(self) -> Dict[str, np.array]:
        groups = self.get_groups()
        return {
            group_key: self.extract_for_group(group_jobs)
            for group_key, group_jobs in groups.items()
        }

    def get_groups(self) -> Dict[str, List[JobGroupElement]]:
        if not self._job_groups:
            jobs = self.jobs
            groups = defaultdict(list)
            for job_data in jobs:
                completionTime = job_data.job.completionTime
                if completionTime is None:
                    continue
                group_alias = self.get_short_group_key(self.get_grouping_key(job_data))
                groups[group_alias].append(
                    JobGroupElement(
                        key=group_alias,
                        job_data=job_data,
                        completion_timestamp=completionTime,
                    )
                )
            self._job_groups = groups
        return self._job_groups

    def extract_for_group(self, group_data: List[JobGroupElement]) -> np.array:
        ret = np.array([self.extract_within_job(job) for job in group_data])
        assert len(ret.shape) == 2
        return ret

    def extract_within_job(self, job: JobGroupElement) -> List[Union[int, float]]:
        ret: List[Union[int, float]] = []
        for stage in job.job_data.stages.values():
            ret.extend([feature.apply(stage) for feature in self.features])
        return ret
