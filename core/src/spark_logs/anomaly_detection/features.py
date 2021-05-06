class Feature:
    def apply(self, data):
        pass


class StageFeature(Feature):
    def apply(self, stage_data):
        common, tasks = stage_data.values()
        return self.common(common) or self.tasks(tasks)

    def common(self, common):
        pass

    def tasks(self, tasks):
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
