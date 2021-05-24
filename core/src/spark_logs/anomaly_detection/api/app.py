import asyncio
from collections import defaultdict

import aiohttp
from aiohttp import http_exceptions
from aiohttp import web

from spark_logs import db
from spark_logs.anomaly_detection.dataset_extractor import JobGroupedExtractor
from spark_logs.anomaly_detection.detectors import AutoencoderDetector
from spark_logs.anomaly_detection.processor import (
    SequentialDetector,
    SequentialJobsFitter,
    SequentialFeatureBypass,
)

routes = web.RouteTableDef()


@routes.get("/")
async def hello(request):
    return web.json_response({"status": "healthy"})


@routes.get("/client/ls")
async def ls_tasks(request: aiohttp.web.Request):
    def task_status(task_entry):
        task_name, task = task_entry
        status = "running"
        if task.cancelled():
            status = "cancelled"
        return status

    processors = request.app["APP_METRICS"]
    app_processors = defaultdict(dict)
    for app_id in processors:
        metrics = processors[app_id]
        for metric_name, metric_tasks in metrics.items():
            tasks = dict(
                zip(
                    ["task_fit", "task_predict", "task_bypass"],
                    map(task_status, list(metric_tasks.items())[1:]),
                )
            )
            app_processors[app_id][metric_name] = tasks
    return web.json_response({"applications": app_processors})


@routes.post("/detector/create")
async def client_for_app(request: aiohttp.web.Request):
    app = request.app
    try:
        app_id = request.query["app_id"]
        metric_name = request.query.get("metric_name") or "iforest_processor"
        detector_factory, fitter_factory, bypass_factory = app["METRIC_PROCESSORS"][
            metric_name
        ]
        detector = detector_factory(
            app_id, JobGroupedExtractor, detector_cls=AutoencoderDetector
        )
        fitter = fitter_factory(
            app_id,
            JobGroupedExtractor,
            detector_cls=AutoencoderDetector,
            timeout=120,
            batch=1500,
        )
        bypass = bypass_factory(app_id, JobGroupedExtractor, timeout=10)
    except KeyError as exc:
        raise http_exceptions.HttpBadRequest("No key " + str(exc))
    redis = app["REDIS"]

    task_f = asyncio.create_task(fitter.loop_process(redis))
    task_p = asyncio.create_task(detector.loop_process(redis))
    task_b = asyncio.create_task(bypass.loop_process(redis))

    app["APP_METRICS"][app_id][metric_name] = {
        "processors": (fitter, detector),
        "task_f": task_f,
        "task_p": task_p,
        "task_b": task_b,
    }

    return aiohttp.web.json_response({"status": "created new processor"})


@routes.post("/detector/rm_proc")
async def rm_metric_proc(request: aiohttp.web.Request):
    app = request.app
    try:
        app_id = request.query["app_id"]
        metric_name = request.query["metric_name"]
    except KeyError as exc:
        raise http_exceptions.HttpBadRequest("No key " + str(exc))

    data = app["APP_METRICS"].get(app_id, {}).get(metric_name)
    if data is None:
        return aiohttp.web.json_response({"error": "Processor not found"})

    tasks = [x for x in data.values() if isinstance(x, asyncio.Task)]
    for x in tasks:
        x.cancel()
    del app["APP_METRICS"][app_id][metric_name]
    return aiohttp.web.json_response(
        {"status": f"Deleted processor. Closed {len(tasks)} tasks"}
    )


@routes.post("/detector/rm")
async def rm_metrics_for_app(request: aiohttp.web.Request):
    app = request.app
    try:
        app_id = request.query["app_id"]
    except KeyError as exc:
        raise http_exceptions.HttpBadRequest("No key " + str(exc))

    data = app["APP_METRICS"].get(app_id)
    if data is None:
        return aiohttp.web.json_response({"error": "App not found"})

    total_tasks_closed = 0
    for metric_name, metric_data in data.items():
        tasks = [x for x in metric_data.values() if isinstance(x, asyncio.Task)]
        for x in tasks:
            x.cancel()
        total_tasks_closed += len(tasks)

    del app["APP_METRICS"][app_id]

    return aiohttp.web.json_response(
        {
            "status": f"Deleted processors for {app_id}. Closed {total_tasks_closed} tasks"
        }
    )


async def create_redis_connection(app):
    app["REDIS"] = await db.connect_with_redis()


def start():
    app = aiohttp.web.Application(middlewares=[aiohttp.web.normalize_path_middleware()])
    app["METRIC_PROCESSORS"] = {
        "sequential_processor": (
            SequentialDetector,
            SequentialJobsFitter,
            SequentialFeatureBypass,
        ),
    }
    app["APP_METRICS"] = defaultdict(dict)
    app.router.add_routes(routes)
    app.on_startup.append(create_redis_connection)
    aiohttp.web.run_app(app, port=10000)
