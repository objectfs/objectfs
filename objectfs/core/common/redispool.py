import redis
from objectfs.util.singletontype import SingletonType
from objectfs.settings import Settings
settings = Settings()

class RedisPool(object):
    __metaclass__ = SingletonType

    blocking_pool = redis.BlockingConnectionPool(host = settings.REDIS_HOST,
                                                 port = settings.REDIS_PORT,
                                                 db = settings.REDIS_DB,
                                                 max_connections = settings.REDIS_MAX_CONNECTIONS)
