import asyncio
import json
import aiohttp
import orjson
from aiohttp import web
from aiohttp.http_exceptions import HttpBadRequest
from aioredis import Redis

from spark_logs import db, kvstore
from spark_logs.loaders import application_loader, clients
from spark_logs.loaders.application_loader import AppIdsLoader

routes = web.RouteTableDef()


@routes.get("/hello")
async def hello(request):
    return web.json_response({"status": "healthy"})


@routes.get("/client/list_apps")
async def client_for_app(request: aiohttp.web.Request):
    redis: Redis = request.app["REDIS"]
    applications = await redis.zrevrangebyscore(kvstore.applications_key())
    applications = [app.decode() for app in applications]
    return web.json_response({"applications": applications})


@routes.post("/client/create")
async def client_for_app(request: aiohttp.web.Request):
    app_id = request.query["app_id"]
    print(app_id)
    app = request.app
    if app_id in app["LOADERS"]:
        return aiohttp.web.json_response({"status": "client exists"}, status=302)

    loader = application_loader.ApplicationLoader(
        app["METRICS_CLIENT"], app_id, fetch_last_jobs=2
    )
    task = asyncio.create_task(loader.loop_update_app_metrics())
    app["LOADERS"][app_id] = loader
    app["LOADER_TASKS"][app_id] = task
    return aiohttp.web.json_response({"status": "created new client"})


@routes.post("/client/rm/")
async def rm_client_for_app(request: aiohttp.web.Request):
    try:
        app_id = request.query["app_id"]
    except KeyError as exc:
        raise HttpBadRequest("No key " + str(exc))

    app = request.app
    if app_id not in app["LOADERS"]:
        return aiohttp.web.json_response({"error": "client does not exist"}, status=404)

    del app["LOADERS"][app_id]
    app["LOADER_TASKS"][app_id].cancel()
    del app["LOADER_TASKS"][app_id]
    return aiohttp.web.json_response({"status": f"client for {app_id} deleted"})


async def create_redis_connection(app):
    app["REDIS"] = await db.connect_with_redis()


async def init_loaders(app):
    app["LOADERS"] = dict()
    app["LOADER_TASKS"] = dict()

    app_ids_loader = AppIdsLoader(app["REDIS"], app["METRICS_CLIENT"], timeout=30)
    task = asyncio.create_task(app_ids_loader.loop_update_app_ids())
    app["LOADERS"]["IDS"] = app_ids_loader
    app["LOADER_TASKS"]["IDS"] = task


def start():
    app = aiohttp.web.Application(middlewares=[aiohttp.web.normalize_path_middleware()])
    app.router.add_routes(routes)
    app["METRICS_CLIENT"] = clients.MetricsClient(inactive_jobs_only=True)
    app.on_startup.append(create_redis_connection)
    app.on_startup.append(init_loaders)
    aiohttp.web.run_app(app, port=8001)
