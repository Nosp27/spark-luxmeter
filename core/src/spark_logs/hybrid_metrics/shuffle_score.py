from typing import Dict

from spark_logs.hybrid_metrics.abc import HybridMetricStrategy
import pandas as pd
import numpy as np


class SkewDetectStrategy(HybridMetricStrategy):
    def __init__(self, threshold=1.25):
        self.threshold = threshold

    def apply(self, data: Dict[str, pd.DataFrame]):
        stages = [data["stage"][stage_id] for stage_id in data["stages"]]
        pass

    def apply_for_job(self, data, job_id):
        pass

    def apply_for_stage(self, data, stage_id, field):
        tasks = data["stage"][stage_id]["taskList"]

        task_metrics = self.get_task_shuffle_metrics(tasks)
        return self.skew_test(task_metrics[field])

    def get_task_shuffle_metrics(self, tasks):
        tasks_shuffle_data = {
            task_data["taskId"]: {
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

        expected_max_deviation = np.std(diffs_normalized) * self.threshold
        return diffs_normalized.max() - expected_max_deviation > diffs_normalized.min()
