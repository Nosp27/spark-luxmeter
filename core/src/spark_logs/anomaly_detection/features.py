class Feature:
    def apply(self, data):
        pass


class StageFeature(Feature):
    def apply(self, stage_data):
        common, tasks = stage_data.values()
        return self.common_process(common) or self.tasks_process(tasks)

    def common_process(self, common):
        pass

    def tasks_process(self, tasks):
        pass


class JobFeature(Feature):
    pass


class StageRunTimeFeature(StageFeature):
    def common(self, common):
        return common["executorRunTime"]


class StageShuffleReadFeature(StageFeature):
    def common(self, common):
        return common["shuffleReadBytes"]


class StageShuffleWriteFeature(StageFeature):
    def common(self, common):
        return common["shuffleWriteBytes"]