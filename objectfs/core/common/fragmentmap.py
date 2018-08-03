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
from sets import Set
from time import time
from sortedcontainers import SortedKeyList
from operator import neg
from objectfs.settings import Settings
settings = Settings()

FS_DELIMITER = '%'
NAME_DELIMITER = '@'
LOG_DELIMITER = '_'

class FragmentMap(object):

    def __init__(self, fs_name):
        try:
            self._fs_name = fs_name
            self._client = redis.StrictRedis(host=settings.REDIS_HOST, port=6379, db=0)
        except redis.ConntectionError as e:
            print("Cannot connect to Redis server")
            raise e

    def _block_key(self, inode_id, block_id):
        return '{}{}{}{}{}'.format(self._fs_name, FS_DELIMITER, inode_id, NAME_DELIMITER, block_id)
    
    def _cache_key(self, inode_id, block_id):
        """Return the cache frgament index"""
        return self._block_key(inode_id, block_id)

    def _log_key(self, inode_id, block_id_list, flush_time):
        """Return the log fragment index"""
        return '{}{}{}'.format(LOG_DELIMITER.join(map(str, [flush_time]+block_id_list)), NAME_DELIMITER,inode_id)
    
    def _decode_log_key(self, inode, log_object_name):
        """Return the block id list given a log object name"""
        block_id_list = log_object_name.split(NAME_DELIMITER)[0].split(LOG_DELIMITER)
        return block_id_list[1:]

    def _remove_block(self, inode_id, block_id):
        """Remove a single block"""
        try:
            self._client.delete(self._block_key(inode_id, block_id))
        except Exception as e:
            raise e

    def remove_fragment_map(self, inode_id, block_id_list):
        """Remove fragment maps for give block ids"""
        try:
            block_key_list = []
            for block_id in block_id_list:
                block_key_list.append(self._block_key(inode_id, block_id))
            self._client.delete(*block_key_list)
        except Exception as e:
            raise e

    def _add_fragment(self, inode_id, block_id, fragment_time, value):
        try:
            self._client.zadd(self._block_key(inode_id, block_id), fragment_time, value)
        except Exception as e:
            raise e
    
    def add_cache_fragment(self, inode_id, block_id, time=time()):
        """Add an index for a cache fragment"""
        return self._add_fragment(inode_id, block_id, int(time), self._cache_key(inode_id, block_id))
    
    def remove_cache_fragment(self, inode_id, block_id_list, flush_time):
        """Remove indexes for cache fragments"""
        try:
            for block_id in block_id_list:
                fragment_list = self.get_fragment(inode_id, block_id, max_score=int(flush_time))
                fragment_removal_list = []
                for fragment, time in fragment_list:
                    if fragment == self._cache_key(inode_id, block_id):
                        fragment_removal_list.append(fragment)
                self._remove_fragments(inode_id, block_id, fragment_removal_list)
        except Exception as e:
            raise e
                

    def add_log_fragment(self, inode_id, block_id_list, flush_time):
        """Add an index for a log fragment"""
        try:
            log_key = self._log_key(inode_id, block_id_list, int(flush_time))
            for block_id in block_id_list:
                self._add_fragment(inode_id, block_id, int(flush_time), log_key)
        except Exception as e:
            raise e
    
    def remove_log_fragment(self, inode_id, block_id_list, flush_time):
        """Remove indexes for log fragments"""
        try:
            for block_id in block_id_list:
                fragment_list = self.get_fragment(inode_id, block_id, max_score=int(flush_time))
                fragment_removal_list = []
                for fragment, time in fragment_list:
                    if LOG_DELIMITER in fragment:
                        fragment_removal_list.append(fragment)
                self._remove_fragments(inode_id, block_id, fragment_removal_list)
        except Exception as e:
            raise e

    def get_fragment(self, inode_id, block_id, max_score='+inf', min_score='-inf'):
        try:
            fragment_list = self._client.zrevrangebyscore(self._block_key(inode_id, block_id), max_score, min_score, withscores=True)
            for fragment, time in fragment_list:
                yield (fragment, time)
        except Exception as e:
            raise e

    def _remove_fragments(self, inode_id, block_id, fragment_list):
        try:
            if not fragment_list:
                return
            else:
                self._client.zrem(self._block_key(inode_id, block_id), *fragment_list)
        except Exception as e:
            raise e
        
# class FragmentMap(object):

    # def __init__(self):
        # self._map = SortedKeyList(key=lambda x:neg(x[0]))
        # # self._map = SortedKeyList(key=neg)
    
    # @property
    # def map(self):
        # return self._map

    # def add(self, value):
        # self.map.add((time(), value))

    # def range(self, min_key=None, max_key=None):
        # yield self._map.range(min_key, max_key)
