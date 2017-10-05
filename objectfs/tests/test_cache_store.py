from __future__ import absolute_import, print_function
import os
import sys
import pytest
import stat
sys.path.append('..')
from objectfs.core.cache.cachestore import CacheStore
from objectfs.settings import Settings
settings = Settings()
from config import CACHE_STORE_LIST

@pytest.mark.parametrize('cache_store', CACHE_STORE_LIST)
def test_object(cache_store):
    cach = Cache_Store_Test(cache_store)
    cach.test_put_get_remove_exists_inode()
    cach.test_write_read_inode()

class Cache_Store_Test:

    def __init__(self, cache_store):
        self._cache_store = CacheStore.load('test_fs', cache_store)
        self._inode_id = 99
        self._data_string = 'hello world'
    
    def __del__(self):
        self._cache_store.clean_inode(self._inode_id)
    
    def test_put_get_remove_exists_inode(self):
        """Test put, get, exists and remove inode data from cache"""
        self._cache_store.put_inode(self._inode_id, self._data_string)
        assert(self._cache_store.exists_inode(self._inode_id) is True)
        response = self._cache_store.get_inode(self._inode_id)
        assert(response == self._data_string)
        self._cache_store.remove_inode(self._inode_id)
        response = self._cache_store.exists_inode(self._inode_id)
        assert(response is False)

    def test_write_read_inode(self):
        """Test writing inode data to cache and check it reads correctly"""
        self._cache_store.write_inode(self._inode_id, 0, self._data_string)
        response = self._cache_store.read_inode(self._inode_id, 0, len(self._data_string))
        assert(response == self._data_string)
