from typing import Dict, Any

from spark_logs.hybrid_metrics.abc import HybridMetricStrategy
import pandas as pd
import numpy as np


class SkewDetectStrategy(HybridMetricStrategy):
    metrics = ("duration",)
    test_name = "skewness_score"

    def apply(self, job_data: Dict[str, Any]):
        for stage_id, stage_data in job_data["stage"].items():
            tasks = stage_data["tasks"]
            tasks_shuffle_metrics = self.get_stage_shuffle_metrics(tasks)
            scores = [
                self.skew_test(tasks_shuffle_metrics[metric].values)
                for metric in self.metrics
            ]
            return np.mean(scores)

    def get_stage_shuffle_metrics(self, tasks):
        tasks_shuffle_features = {
            task_data["taskId"]: {
                **{metric: task_data[metric] for metric in self.metrics},
            }
            for task_data in tasks.values()
        }

        for task_id, shuffle_data in tasks_shuffle_features.items():
            colnames = shuffle_data.keys()
            break
        else:
            raise ValueError("No task data")

        return pd.DataFrame.from_dict(
            {k: v.values() for k, v in tasks_shuffle_features.items()},
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
        return actual_quantile_deviation
