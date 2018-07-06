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
from objectfs.core.common.mergequeue import MergeQueue
from objectfs.settings import Settings
settings = Settings()
from config import META_STORE_LIST

@pytest.mark.parametrize('meta_store', META_STORE_LIST)
def test_queue(meta_store):
    mque = MergeQueue_Test(meta_store)
    mque.test_insert_merge_queue()
    mque.test_order_merge_queue()
    mque.test_remove_merge_queue()

class MergeQueue_Test:

    def __init__(self, meta_store):
        # create a fragment map
        self.merge_queue = MergeQueue('test_fs')
        self.inode_id = 1
        self.fragment_index_1 = '1000_10_12_14'
        self.fragment_index_2 = '900_12_13'
    
    def __del__(self):
        self.merge_queue.delete_queue(self.inode_id)
    
    def _check_fragment_index(self, fragment_key, end=1):
        fragment_list = []
        for fragment in self.merge_queue.fetch(self.inode_id, end=end):
            fragment_list.append(fragment)
        
        if fragment_key in fragment_list:
            return True
        else:
            return False

    def test_insert_merge_queue(self):
        """Test insertion of index to merge queue"""
        self.merge_queue.insert(self.inode_id, self.fragment_index_1)
        assert(self._check_fragment_index(self.fragment_index_1))
    
    def test_order_merge_queue(self):
        """Test fetching of index from merge queue"""
        self.merge_queue.insert(self.inode_id, self.fragment_index_2)
        assert(self._check_fragment_index(self.fragment_index_1))
        assert(self._check_fragment_index(self.fragment_index_1, end=2))

    
    def test_remove_merge_queue(self):
        """Test removal of index from merge queue"""
        self.merge_queue.remove(self.inode_id, self.fragment_index_1)
        assert(not self._check_fragment_index(self.fragment_index_1))
        self.merge_queue.remove(self.inode_id, self.fragment_index_2)
        assert(not self._check_fragment_index(self.fragment_index_2))
