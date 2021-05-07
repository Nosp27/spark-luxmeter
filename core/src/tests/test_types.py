import orjson
import pytest

from spark_logs.types import (
    Job,
    Stage,
    Task,
    JobStages,
    StageTasks,
    ApplicationMetrics,
    Executor,
)


@pytest.fixture
def sample_job():
    return orjson.dumps(
        {
            "jobId": 2718,
            "name": "call at /usr/local/share/spark/python/lib/py4j-0.10.7-src.zip/py4j/java_gateway.py:2381",
            "submissionTime": "2021-05-05T09:55:01.071GMT",
            "completionTime": "2021-05-05T09:56:32.601GMT",
            "stageIds": [9510, 9511, 9512, 9513],
            "status": "SUCCEEDED",
            "numTasks": 239,
            "numActiveTasks": 0,
            "numCompletedTasks": 239,
            "numSkippedTasks": 0,
            "numFailedTasks": 0,
            "numKilledTasks": 0,
            "numCompletedIndices": 239,
            "numActiveStages": 0,
            "numCompletedStages": 4,
            "numSkippedStages": 0,
            "numFailedStages": 0,
            "killedTasksSummary": {},
        }
    )


@pytest.fixture
def sample_stage():
    return orjson.dumps(
        {
            "status": "PENDING",
            "stageId": 9569,
            "attemptId": 0,
            "numTasks": 3,
            "numActiveTasks": 0,
            "numCompleteTasks": 0,
            "numFailedTasks": 0,
            "numKilledTasks": 0,
            "numCompletedIndices": 0,
            "executorRunTime": 0,
            "executorCpuTime": 0,
            "inputBytes": 0,
            "inputRecords": 0,
            "outputBytes": 0,
            "outputRecords": 0,
            "shuffleReadBytes": 0,
            "shuffleReadRecords": 0,
            "shuffleWriteBytes": 0,
            "shuffleWriteRecords": 0,
            "memoryBytesSpilled": 0,
            "diskBytesSpilled": 0,
            "name": """call at / usr / local / share / spark / python / lib / py4j - 0.10
     .7 - src.zip / py4j / java_gateway.py: 2381', 'details': 'org.apache.spark.rdd.RDD.collect(RDD.sca
     la: 944)\norg.apache.spark.api.python.PythonRDD$.collectAndServe(
        PythonRDD.scala: 166)\norg.apache.spark.api.python.PythonRDD.collectAnd
    Serve(PythonRDD.scala)\nsun.reflect.GeneratedMethodAccessor119.invoke(Unknown
    Source)\nsun.reflect.DelegatingMethodAccessorImpl.invoke
    (DelegatingMethodAccessorImpl.java: 43)\njava.lang.reflect.Method.invoke(
        Method.java: 498)\npy4j.reflection.MethodInvoker.invoke(MethodI
    nvoker.java: 244)\npy4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java: 357)\npy4j.Gateway.invoke(
        Gateway.java: 282)\npy4j.comm
    ands.AbstractCommand.invokeMethod(AbstractCommand.java: 132)\npy4j.commands.CallCommand.execute(
        CallCommand.java: 79)\npy4j.GatewayConne
    ction.run(GatewayConnection.java: 238)\njava.lang.Thread.run(Thread.java: 748)""",
            "schedulingPool": "default",
            "rddIds": [83394, 83391, 83392, 83393],
            "accumulatorUpdates": [],
            "executorSummary": {},
            "killedTasksSummary": {},
        }
    )


@pytest.fixture
def sample_task():
    return orjson.dumps(
        {
            "taskId": 422945,
            "index": 10,
            "attempt": 0,
            "launchTime": "2021-05-05T10:35:07.317GMT",
            "duration": 6492,
            "executorId": "11",
            "host": "host.com",
            "status": "SUCCESS",
            "taskLocality": "PROCESS_LOCAL",
            "speculative": False,
            "accumulatorUpdates": [],
            "taskMetrics": {
                "executorDeserializeTime": 7,
                "executorDeserializeCpuTime": 1261000,
                "executorRunTime": 6476,
                "executorCpuTime": 5987652000,
                "resultSize": 2218,
                "jvmGcTime": 54,
                "resultSerializationTime": 0,
                "memoryBytesSpilled": 0,
                "diskBytesSpilled": 0,
                "peakExecutionMemory": 0,
                "inputMetrics": {"bytesRead": 0, "recordsRead": 0},
                "outputMetrics": {"bytesWritten": 0, "recordsWritten": 0},
                "shuffleReadMetrics": {
                    "remoteBlocksFetched": 0,
                    "localBlocksFetched": 0,
                    "fetchWaitTime": 0,
                    "remoteBytesRead": 0,
                    "remoteBytesReadToDisk": 0,
                    "localBytesRead": 0,
                    "recordsRead": 0,
                },
                "shuffleWriteMetrics": {
                    "bytesWritten": 9306865,
                    "writeTime": 72477586,
                    "recordsWritten": 262941,
                },
            },
        }
    )


@pytest.fixture
def data_from_redis():
    return open("../../../sample_data/stored_application.json", "rb").read()


def test_load_from_json(sample_job, sample_stage, sample_task):
    job = Job.from_json(sample_job)
    job.dump()

    stage = Stage.from_json(sample_stage)
    stage.dump()

    task = Task.from_json(sample_task)
    task.dump()

    stage_tasks = StageTasks(stage=stage, tasks={"1": task})
    job_stages = JobStages(job=job, stages={"1": stage_tasks})
    job_stages.dump()
    app_metrics = ApplicationMetrics(executor_metrics={}, jobs_stages=job_stages)
    app_metrics.dump()


def test_restore(data_from_redis):
    app_metrics = ApplicationMetrics.from_json(data_from_redis)
    assert all(isinstance(x, Executor) for x in app_metrics.executor_metrics)
    assert all(isinstance(x, JobStages) for x in app_metrics.jobs_stages.values())
