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

from __future__ import print_function, absolute_import
import redis
from objectfs.settings import Settings
settings = Settings()

FS_DELIMITER = '%'
NAME_DELIMITER = '@'
QUEUE_NAME = 'MERGE'

class MergeQueue(object):

    def __init__(self, fs_name):
        try:
            self._fs_name = fs_name
            self._client = redis.StrictRedis(host=settings.REDIS_HOST, port=6379, db=0)
        except redis.ConntectionError as e:
            print("Cannot connect to Redis server")
            raise e
    
    def delete_queue(self, inode_id):
        """Delete the queue"""
        self._client.delete(self._queue_key(inode_id))

    def _queue_key(self, inode_id):
        return '{}{}{}{}{}'.format(self._fs_name, FS_DELIMITER, inode_id, NAME_DELIMITER, QUEUE_NAME)
    
    def insert(self, inode_id, fragment_index):
        try:
            self._client.rpush(self._queue_key(inode_id), fragment_index)
        except Exception as e:
            raise e

    def fetch(self, inode_id, start=0, end=5):
        try:
            log_object_list = self._client.lrange(self._queue_key(inode_id), start, end)
            for log_object in log_object_list:
                yield log_object
        except Exception as e:
            raise e

    def remove(self, inode_id, fragment, count=1):
        try:
            self._client.lrem(self._queue_key(inode_id), count, fragment)
        except Exception as e:
            raise e
