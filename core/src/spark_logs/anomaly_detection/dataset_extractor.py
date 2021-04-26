import abc

import numpy as np


class DatasetExtractor:
    def __init__(self, features):
        self.features = features

    @abc.abstractmethod
    def extract(self):
        pass


class JobGroupedExtractor(DatasetExtractor):
    def __init__(self, jobs, **kwargs):
        super().__init__(**kwargs)
        self.jobs = jobs
        self.job_groups = None
        self.job_group_timestamps = None

    def get_grouping_key(self, j):
        return j["name"]

    def extract(self):
        jobs = self.jobs
        job_grouping_keys = {self.get_grouping_key(j) for j in jobs}
        groups = dict()
        for group_key in job_grouping_keys:
            groups[group_key] = [j for j in jobs if self.get_grouping_key(j) == group_key]

        self.job_groups = groups
        self.job_group_timestamps = {k: [j["completionTime"] for j in js] for k, js in groups.items()}

        group_datasets = dict()
        for key, group in groups:
            ret = np.array([self.extract_within_job(job) for job in jobs])
            assert len(ret.shape) == 2
            group_datasets[key] = ret
        return group_datasets

    def extract_within_job(self, job_data):
        return [
            [feature.apply(stage) for feature in self.features]
            for stage in job_data["stages"].values()
        ]


