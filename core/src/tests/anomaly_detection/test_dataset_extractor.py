import orjson
import pytest

from spark_logs.anomaly_detection.dataset_extractor import JobGroupedExtractor
from spark_logs.anomaly_detection.features import StageFeature


class TestFeature(StageFeature):
    def tasks(self, tasks):
        return len(tasks)


@pytest.fixture(scope="session")
def jobs_data():
    return orjson.loads(open("../../../../sample_data/job_list_sample").read())


def test_job_grouped_extractor(jobs_data):
    extractor = JobGroupedExtractor(jobs_data, features=[TestFeature(), TestFeature()])
    result = extractor.extract()
