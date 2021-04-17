import pytest

from spark_luxmeter.spark_logs.hybrid_metrics.shuffle_score import SkewDetectStrategy


@pytest.mark.parametrize(
    "metrics",
    [
        [
            {
                "taskId": 1,
                "shuffleReadMetrics": {
                    "remoteBlocksFetched": 200,
                    "localBlocksFetched": 0,
                    "fetchWaitTime": 0,
                    "remoteBytesRead": 31919134,
                    "remoteBytesReadToDisk": 0,
                    "localBytesRead": 0,
                    "recordsRead": 95085
                },
                "shuffleWriteMetrics": {
                    "bytesWritten": 0,
                    "writeTime": 0,
                    "recordsWritten": 0
                }
            },
            {
                "taskId": 2,
                "shuffleReadMetrics": {
                    "remoteBlocksFetched": 191,
                    "localBlocksFetched": 9,
                    "fetchWaitTime": 0,
                    "remoteBytesRead": 30357720,
                    "remoteBytesReadToDisk": 0,
                    "localBytesRead": 1439773,
                    "recordsRead": 95084
                },
                "shuffleWriteMetrics": {
                    "bytesWritten": 0,
                    "writeTime": 0,
                    "recordsWritten": 0
                }
            },
            {
                "taskId": 3,
                "shuffleReadMetrics": {
                    "remoteBlocksFetched": 200,
                    "localBlocksFetched": 0,
                    "fetchWaitTime": 0,
                    "remoteBytesRead": 31801199,
                    "remoteBytesReadToDisk": 0,
                    "localBytesRead": 0,
                    "recordsRead": 95093
                },
                "shuffleWriteMetrics": {
                    "bytesWritten": 0,
                    "writeTime": 0,
                    "recordsWritten": 0
                }
            }
        ]
    ]
)
def test_shuffle_score(metrics):
    stage_metrics = {
        "stage": {
            "stage_1": {
                "taskList": metrics
            }
        }
    }
    skew_detector = SkewDetectStrategy()
    x = skew_detector.apply_for_stage(stage_metrics, "stage_1", "recordsRead")
    print("X")
