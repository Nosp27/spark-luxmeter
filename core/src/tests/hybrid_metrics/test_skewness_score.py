from collections import defaultdict
from unittest.mock import Mock, MagicMock

import pytest

from spark_logs.hybrid_metrics.skewness_score import SkewDetectStrategy
from spark_logs.types import StageTasks, Stage, Node


@pytest.mark.parametrize(
    "metrics, expected_skewness_test",
    [
        (
            [
                {
                    "taskId": 1,
                    "duration": 95,
                    "shuffleReadMetrics": {
                        "remoteBlocksFetched": 200,
                        "localBlocksFetched": 0,
                        "fetchWaitTime": 0,
                        "remoteBytesRead": 31919134,
                        "remoteBytesReadToDisk": 0,
                        "localBytesRead": 0,
                        "recordsRead": 95085,
                    },
                    "shuffleWriteMetrics": {
                        "bytesWritten": 0,
                        "writeTime": 0,
                        "recordsWritten": 0,
                    },
                },
                {
                    "taskId": 2,
                    "duration": 102,
                    "shuffleReadMetrics": {
                        "remoteBlocksFetched": 191,
                        "localBlocksFetched": 9,
                        "fetchWaitTime": 0,
                        "remoteBytesRead": 30357720,
                        "remoteBytesReadToDisk": 0,
                        "localBytesRead": 1439773,
                        "recordsRead": 95084,
                    },
                    "shuffleWriteMetrics": {
                        "bytesWritten": 0,
                        "writeTime": 0,
                        "recordsWritten": 0,
                    },
                },
                {
                    "taskId": 3,
                    "duration": 90,
                    "shuffleReadMetrics": {
                        "remoteBlocksFetched": 200,
                        "localBlocksFetched": 0,
                        "fetchWaitTime": 0,
                        "remoteBytesRead": 31801199,
                        "remoteBytesReadToDisk": 0,
                        "localBytesRead": 0,
                        "recordsRead": 95093,
                    },
                    "shuffleWriteMetrics": {
                        "bytesWritten": 0,
                        "writeTime": 0,
                        "recordsWritten": 0,
                    },
                },
            ],
            False,
        ),
    ],
)
def test_shuffle_score(metrics, expected_skewness_test):
    stage = MagicMock()
    stage.__class__ = Node
    nones = ["index", "attempt", "executorId", "host", "status", "taskLocality", "speculative"]
    tasks = [{none: None for none in nones}] * len(metrics)
    for tasks_el, metrics_el in zip(tasks, metrics):
        tasks_el.update(metrics_el)
    stage_metrics = StageTasks(stage=stage, tasks={v["taskId"]: v for v in tasks})
    skew_detector = SkewDetectStrategy()
    actual_skew_test = skew_detector.apply(stage_metrics)
    assert bool(actual_skew_test) == expected_skewness_test
