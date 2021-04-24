import aioredis

from spark_logs.config import DEFAULT_CONFIG


async def connect_with_redis() -> aioredis.Redis:
    redis_host = DEFAULT_CONFIG["redis_host"]
    redis_port = DEFAULT_CONFIG["redis_port"]
    return await aioredis.create_redis_pool(f"redis://{redis_host}:{redis_port}")
