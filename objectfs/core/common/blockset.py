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
from time import time
from sortedcontainers import SortedKeyList
from operator import neg
from objectfs.settings import Settings
settings = Settings()

FS_DELIMITER = '%'
NAME_DELIMITER = '@'

class BlockSet(object):

    def __init__(self, fs_name, cache_type):
        try:
            self._fs_name = fs_name
            self._cache_type = cache_type
            self._client = redis.StrictRedis(host=settings.REDIS_HOST, port=6379, db=0)
        except redis.ConntectionError as e:
            print("Cannot connect to Redis server")
            raise e


    def _set_key(self, inode_id):
        """Key for storing the set"""
        return '{}{}{}{}{}'.format(self._fs_name, FS_DELIMITER, inode_id, NAME_DELIMITER, self._cache_type)
    
    def delete_set(self, inode_id):
        """Delete set"""
        try:
            self._client.delete(self._set_key(inode_id))
        except Exception as e:
            raise e

    def add(self, inode_id, block_id_list):
        """Add members to the set"""
        try:
            self._client.sadd(self._set_key(inode_id), *block_id_list)
        except Exception as e:
            raise e

    def get(self, inode_id):
        """Get block ids from the set"""
        try:
            block_list = self._client.smembers(self._set_key(inode_id))
            for block in block_list:
                yield int(block)
        except Exception as e:
            raise e
    
    def remove(self, inode_id, block_id_list):
        """Remove block ids from the set"""
        try:
            self._client.srem(self._set_key(inode_id), *block_id_list)
        except Exception as e:
            raise e


class DirtySet(BlockSet):

    def __init__(self, fs_name):
        super(self.__class__, self).__init__(fs_name, 'DIRTY')

class CleanSet(BlockSet):

    def __init__(self, fs_name):
        super(self.__class__, self).__init__(fs_name, 'CLEAN')
