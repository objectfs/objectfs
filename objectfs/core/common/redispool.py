# Copyright 2017 IBM Corporation
# Copyright 2017 The Johns Hopkins University
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
