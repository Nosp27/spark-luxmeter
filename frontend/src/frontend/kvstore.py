import redis

from spark_logs.config import DEFAULT_CONFIG

port = 1234

client = redis.Redis(port=DEFAULT_CONFIG["redis_port"])
