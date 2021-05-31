import asyncio
from collections import defaultdict

import aiohttp
from aiohttp import http_exceptions
from aiohttp import web

from spark_logs import db
from spark_logs.hybrid_metrics import skewness_score
from spark_logs.task_tools import task_status

routes = web.RouteTableDef()


@routes.get("/")
async def hello(request):
    return web.json_response({"status": "healthy"})


@routes.get("/client/ls")
async def ls_tasks(request: aiohttp.web.Request):
    app = request.app
    running_processors = app["APP_METRICS"]
    ret = defaultdict(dict)
    for app_id, app_processor in running_processors.items():
        for metric_name, processor in app_processor.items():
            ret[app_id][metric_name] = {"task": task_status((processor["task"]))}
        return web.json_response({"applications": ret})


@routes.post("/metric/create")
async def client_for_app(request: aiohttp.web.Request):
    app = request.app
    try:
        app_id = request.query["app_id"]
        metric_name = request.query.get("metric_name") or "skewness_score"
        processor = app["METRIC_PROCESSORS"][metric_name]()
    except KeyError as exc:
        raise http_exceptions.HttpBadRequest("No key " + str(exc))
    redis = app["REDIS"]
    task = asyncio.create_task(processor.loop_app_apply(redis, app["GRAPHITE"], app_id))

    app["APP_METRICS"][app_id][metric_name] = {
        "processor": processor,
        "task": task,
    }

    return aiohttp.web.json_response({"status": "created new processor"})


@routes.post("/metric/rm_proc")
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


@routes.post("/metric/rm")
async def rm_metrics_for_app(request: aiohttp.web.Request):
    app = request.app
    try:
        app_id = request.query["app_id"]
    except KeyError as exc:
        raise http_exceptions.HttpBadRequest("No key " + str(exc))

    data = app["APP_METRICS"].get(app_id)
    if data is None:
        return aiohttp.web.json_response({"error": "App not found"})

    keys_to_delete = set()
    for key, item in data.items():
        item["task"].cancel()
        keys_to_delete.add(key)
    for k in keys_to_delete:
        del data[k]
    return aiohttp.web.json_response({"status": f"Deleted processors for {app_id}"})


async def create_redis_connection(app):
    app["REDIS"] = await db.connect_with_redis()
    app["GRAPHITE"] = db.connect_with_graphtie("hybrid_metrics")


def start():
    app = aiohttp.web.Application(middlewares=[aiohttp.web.normalize_path_middleware()])
    app["METRIC_PROCESSORS"] = {
        "skewness_score": skewness_score.SkewDetectStrategy,
    }
    app["APP_METRICS"] = defaultdict(dict)
    app.router.add_routes(routes)
    app.on_startup.append(create_redis_connection)
    aiohttp.web.run_app(app, port=10111)
