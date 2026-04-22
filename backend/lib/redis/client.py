import os

from redis.asyncio.client import Redis

from backend.lib.utils import none_throws


class RedisClient:
    def __init__(self) -> None:
        self.client = Redis(
            host=none_throws(os.getenv("REDIS_HOST")),
            port=int(none_throws(os.getenv("REDIS_PORT"))),
            decode_responses=True,
            username=none_throws(os.getenv("REDIS_USERNAME")),
            password=none_throws(os.getenv("REDIS_PASSWORD")),
        )
