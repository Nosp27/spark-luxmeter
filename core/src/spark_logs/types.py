from datetime import datetime
from typing import List, Dict, Any, NamedTuple, Optional

import attr
import orjson
from dateutil import parser


def maybe(foo):
    def inner(arg):
        if arg is None:
            return arg
        return foo(arg)
    return inner


def for_each(foo):
    def inner(arg_collection):
        if isinstance(arg_collection, list):
            return [foo(arg) for arg in arg_collection]
        elif isinstance(arg_collection, dict):
            return {k: foo(v) for k, v in arg_collection.items()}
        raise NotImplementedError()
    return inner


class Node:
    @classmethod
    def from_json(cls, payload: bytes):
        return cls.create_from_dict(orjson.loads(payload))

    @classmethod
    def create_from_dict(cls, data: Dict[str, Any]):
        if issubclass(data.__class__, Node):
            return data
        assert isinstance(data, dict), type(data)
        field_names = {f.name for f in attr.fields(cls)}
        kwargs = {x: data[x] for x in field_names & set(data.keys())}
        try:
            d = cls(**kwargs)
            return d
        except Exception as exc:
            raise

    def dump(self):
        def default(o):
            try:
                if issubclass(o.__class__, Node):
                    return o._to_dict()
            except Exception as exc:
                import pudb; pudb.set_trace()
                raise
            raise TypeError()

        try:
            return orjson.dumps(self._to_dict(), default=default)
        except Exception as exc:
            import pudb; pudb.set_trace()
            raise

    def _to_dict(self):
        attr_names = [x.name for x in attr.fields(self.__class__)]
        return dict(zip(attr_names, [getattr(self, x) for x in attr_names]))


@attr.s(kw_only=True)
class Job(Node):
    jobId: str = attr.ib(converter=str)
    name: str = attr.ib()
    submissionTime: datetime = attr.ib(converter=parser.parse)
    completionTime: Optional[datetime] = attr.ib(converter=maybe(parser.parse), default=None)
    stageIds: List[int] = attr.ib()
    status: str = attr.ib()


@attr.s(kw_only=True)
class Executor(Node):
    id: str = attr.ib()
    hostPort: str = attr.ib()
    isActive: bool = attr.ib()
    memoryUsed: int = attr.ib()
    diskUsed: int = attr.ib()
    totalCores: int = attr.ib()
    maxTasks: int = attr.ib()
    maxMemory: int = attr.ib()
    totalShuffleRead: int = attr.ib()
    totalShuffleWrite: int = attr.ib()


@attr.s(kw_only=True)
class Stage(Node):
    status: str = attr.ib()
    stageId: str = attr.ib(converter=str)
    attemptId: int = attr.ib(converter=int)
    numTasks: int = attr.ib(converter=int)
    numActiveTasks: int = attr.ib(converter=int)
    numCompleteTasks: int = attr.ib(converter=int)
    numFailedTasks: int = attr.ib(converter=int)
    numKilledTasks: int = attr.ib(converter=int)
    numCompletedIndices: int = attr.ib(converter=int)
    executorRunTime: int = attr.ib(converter=int)
    executorCpuTime: int = attr.ib(converter=int)
    inputBytes: int = attr.ib(converter=int)
    inputRecords: int = attr.ib(converter=int)
    outputBytes: int = attr.ib(converter=int)
    outputRecords: int = attr.ib(converter=int)
    shuffleReadBytes: int = attr.ib(converter=int)
    shuffleReadRecords: int = attr.ib(converter=int)
    shuffleWriteBytes: int = attr.ib(converter=int)
    shuffleWriteRecords: int = attr.ib(converter=int)
    memoryBytesSpilled: int = attr.ib(converter=int)
    diskBytesSpilled: int = attr.ib(converter=int)
    name: str = attr.ib()


@attr.s(kw_only=True)
class Task(Node):
    taskId: str = attr.ib(converter=str)
    index: int = attr.ib()
    attempt: int = attr.ib()
    executorId: str = attr.ib()
    host: str = attr.ib()
    status: str = attr.ib()
    duration: int = attr.ib()
    taskLocality: str = attr.ib()
    speculative: bool = attr.ib()
    taskMetrics: Optional[Dict[str, Any]] = attr.ib(default=attr.Factory(dict))
    shuffleReadMetrics: Optional[Dict[str, Any]] = attr.ib(default=attr.Factory(dict))
    shuffleWriteMetrics: Optional[Dict[str, Any]] = attr.ib(default=attr.Factory(dict))


@attr.s(kw_only=True)
class StageTasks(Node):
    stage: Stage = attr.ib(converter=Stage.create_from_dict)
    tasks: Dict[str, Task] = attr.ib(converter=for_each(Task.create_from_dict))

    def __str__(self):
        return f"JobStages metrics {id(self)}"


@attr.s(kw_only=True)
class JobStages(Node):
    job: Job = attr.ib(converter=Job.create_from_dict)
    stages: Dict[str, StageTasks] = attr.ib(converter=for_each(StageTasks.create_from_dict))

    def __str__(self):
        return f"JobStages metrics {id(self)}"


@attr.s(kw_only=True)
class ApplicationMetrics(Node):
    executor_metrics: List[Executor] = attr.ib(converter=for_each(Executor.create_from_dict))
    jobs_stages: Dict[str, JobStages] = attr.ib(converter=for_each(JobStages.create_from_dict))

    def __str__(self):
        return f"Application metrics {id(self)}"

