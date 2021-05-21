import asyncio
from collections import defaultdict

import aiohttp
from aiohttp import http_exceptions
from aiohttp import web

from spark_logs import db
from spark_logs.anomaly_detection.dataset_extractor import JobGroupedExtractor
from spark_logs.anomaly_detection.detectors import AutoencoderDetector
from spark_logs.anomaly_detection.processor import SequentialDetector, SequentialJobsFitter

routes = web.RouteTableDef()


@routes.post("/detector/create")
async def client_for_app(request: aiohttp.web.Request):
    app = request.app
    try:
        app_id = request.query["app_id"]
        metric_name = request.query.get("metric_name") or "iforest_processor"
        detector_factory, fitter_factory = app["METRIC_PROCESSORS"][metric_name]
        detector = detector_factory(
            app_id, JobGroupedExtractor, detector_cls=AutoencoderDetector
        )
        fitter = fitter_factory(
            app_id, JobGroupedExtractor, detector_cls=AutoencoderDetector, timeout=120,
        )
    except KeyError as exc:
        raise http_exceptions.HttpBadRequest("No key " + str(exc))
    redis = app["REDIS"]
    task_f = asyncio.create_task(fitter.loop_process(redis))
    task_p = asyncio.create_task(detector.loop_process(redis))

    app["APP_METRICS"][app_id][metric_name] = {
        "processors": (fitter, detector),
        "task_f": task_f,
        "task_p": task_p,
    }

    return aiohttp.web.json_response({"status": "created new processor"})


@routes.post("/detector/rm")
async def rm_metric_for_app(request: aiohttp.web.Request):
    app = request.app
    try:
        app_id = request.query["app_id"]
        metric_name = request.query["metric_name"]
    except KeyError as exc:
        raise http_exceptions.HttpBadRequest("No key " + str(exc))

    data = app["APP_METRICS"].get(app_id, {}).get(metric_name)
    if data is None:
        return aiohttp.web.json_response({"error": "Processor not found"})

    data["task"].cancel()
    del app["APP_METRICS"][app_id][metric_name]
    return aiohttp.web.json_response({"status": "Deleted processor"})


@routes.post("/detector/rm_app")
async def rm_metrics_for_app(request: aiohttp.web.Request):
    app = request.app
    try:
        app_id = request.query["app_id"]
    except KeyError as exc:
        raise http_exceptions.HttpBadRequest("No key " + str(exc))

    data = app["APP_METRICS"].get(app_id)
    if data is None:
        return aiohttp.web.json_response({"error": "App not found"})

    for key, item in data.items():
        item["task"].cancel()
        del data[key]
    return aiohttp.web.json_response({"status": f"Deleted processors for {app_id}"})


async def create_redis_connection(app):
    app["REDIS"] = await db.connect_with_redis()


def start():
    app = aiohttp.web.Application(middlewares=[aiohttp.web.normalize_path_middleware()])
    app["METRIC_PROCESSORS"] = {
        "sequential_processor": (SequentialDetector, SequentialJobsFitter),
    }
    app["APP_METRICS"] = defaultdict(dict)
    app.router.add_routes(routes)
    app.on_startup.append(create_redis_connection)
    aiohttp.web.run_app(app, port=10000)
