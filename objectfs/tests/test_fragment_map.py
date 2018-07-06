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

from __future__ import absolute_import, print_function
import sys
import pytest
from time import time
sys.path.append('..')
from objectfs.core.common.fragmentmap import FragmentMap
from objectfs.settings import Settings
settings = Settings()
from config import META_STORE_LIST

@pytest.mark.parametrize('meta_store', META_STORE_LIST)
def test_fragment(meta_store):
    frag = FragmentMap_Test(meta_store)
    frag.test_add_cache_fragment()
    frag.test_add_log_fragment()
    frag.test_remove_cache_fragment()
    frag.test_remove_log_fragment()

class FragmentMap_Test:

    def __init__(self, meta_store):
        # create a fragment map
        self.fragment_map = FragmentMap('test_fs')
        self.inode_id = 1
        self.block_id_list = [10, 12, 14, 15]
    
    def __del__(self):
        self.fragment_map.remove_fragment_map(self.inode_id, self.block_id_list)
    
    def _check_fragment_index(self, block_id, fragment_key):
        for fragment, time in self.fragment_map.get_fragment(self.inode_id, block_id):
            if fragment_key == fragment:
                return True
        return False

    def test_add_cache_fragment(self):
        """Test addition of index for cache fragment"""
        self.fragment_map.add_cache_fragment(self.inode_id, self.block_id_list[0])
        assert(self._check_fragment_index(self.block_id_list[0], self.fragment_map._cache_key(self.inode_id, self.block_id_list[0])))
    
    def test_add_log_fragment(self):
        """Test addition of index for log fragment"""
        self.flush_time = int(time())
        self.fragment_map.add_log_fragment(self.inode_id, self.block_id_list, self.flush_time)
        for block_id in self.block_id_list:
            assert(self._check_fragment_index(self.block_id_list[0], self.fragment_map._log_key(self.inode_id, self.block_id_list, self.flush_time)))
    
    def test_remove_cache_fragment(self):
        """Test removal of index for cache fragment"""
        self.fragment_map.remove_cache_fragment(self.inode_id, [self.block_id_list[0]], self.flush_time)
        assert(not self._check_fragment_index(self.block_id_list[0], self.fragment_map._cache_key(self.inode_id, self.block_id_list[0])))

    def test_remove_log_fragment(self):
        """Test removal of index for log fragment"""
        self.fragment_map.remove_log_fragment(self.inode_id, self.block_id_list, self.flush_time)
        for block_id in self.block_id_list:
            assert(not self._check_fragment_index(self.block_id_list[0], self.fragment_map._log_key(self.inode_id, self.block_id_list, self.flush_time)))
