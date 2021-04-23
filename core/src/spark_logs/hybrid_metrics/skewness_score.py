from typing import Dict, Any

from spark_logs.hybrid_metrics.abc import HybridMetricStrategy
import pandas as pd
import numpy as np


class SkewDetectStrategy(HybridMetricStrategy):
    additional_metrics = ("duration",)
    skew_test_fields = ("recordsRead", "duration")

    def __init__(self, threshold=1.25):
        self.threshold = threshold

    def apply(self, job_data: Dict[str, Any]):
        skewed_stages = []
        for stage_id, stage_data in job_data["stage"].items():
            tasks = stage_data["tasks"]
            tasks_shuffle_metrics = self.get_stage_shuffle_metrics(tasks)
            if any(self.skew_test(tasks_shuffle_metrics[f]) for f in self.skew_test_fields):
                skewed_stages.append(stage_id)
        return skewed_stages

    def get_stage_shuffle_metrics(self, tasks):
        tasks_shuffle_data = {
            task_data["taskId"]: {
                **{metric: task_data[metric] for metric in self.additional_metrics},
                **task_data[f"shuffleReadMetrics"],
                **task_data[f"shuffleWriteMetrics"],
            }
            for task_data in tasks
        }

        for task_id, shuffle_data in tasks_shuffle_data.items():
            colnames = shuffle_data.keys()
            break
        else:
            raise ValueError("No task data")

        return pd.DataFrame.from_dict(
            {k: v.values() for k, v in tasks_shuffle_data.items()},
            orient="index",
            columns=colnames,
        )

    def skew_test(self, arr: np.ndarray):
        assert len(arr.shape) == 1

        if arr.shape[0] == 1:
            return False

        sorted_arr = np.sort(arr)
        shifted = np.array(arr[1:])
        diffs = shifted - sorted_arr[:-1]
        diffs_normalized = diffs / np.linalg.norm(diffs)

        actual_quantile_deviation = np.max(diffs_normalized)
        return actual_quantile_deviation > self.threshold
