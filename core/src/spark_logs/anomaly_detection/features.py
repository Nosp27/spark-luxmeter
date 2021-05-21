import abc
from typing import List, Union

from spark_logs.types import StageTasks, Stage, Task


class Feature:
    @property
    def name(self):
        raise NotImplementedError()

    def apply(self, data):
        pass


class StageFeature(Feature):
    def apply(self, stage_data: StageTasks) -> Union[int, float]:
        result = self.common(stage_data.stage)
        if result is None:
            result = self.tasks(
                list(stage_data.tasks.values())
            )
        if result is None:
            print("N")
        return result

    @abc.abstractmethod
    def common(self, common: Stage):
        pass

    @abc.abstractmethod
    def tasks(self, tasks: List[Task]):
        pass


class JobFeature(Feature):
    pass


class StageRunTimeFeature(StageFeature):
    @property
    def name(self):
        return "stage_run_time"

    def common(self, common: Stage) -> int:
        return common.executorRunTime


class StageShuffleReadFeature(StageFeature):
    @property
    def name(self):
        return "stage_shuffle_read"

    def common(self, common: Stage) -> int:
        return common.shuffleReadBytes


class StageShuffleWriteFeature(StageFeature):
    @property
    def name(self):
        return "stage_shuffle_write"

    def common(self, common: Stage) -> int:
        return common.shuffleWriteBytes
