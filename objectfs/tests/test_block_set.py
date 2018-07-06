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
from objectfs.core.common.blockset import DirtySet, CleanSet
from objectfs.settings import Settings
settings = Settings()
from config import META_STORE_LIST

@pytest.mark.parametrize('meta_store', META_STORE_LIST)
def test_block(meta_store):
    bset = BlockSet_Test(meta_store)
    bset.test_insert_block_set()
    bset.test_remove_block_set()

class BlockSet_Test:

    def __init__(self, meta_store):
        # create a block set
        self.block_set = DirtySet('test_fs')
        self.block_id_list = [10, 12, 14]
        self.inode_id = '2'
    
    def __del__(self):
        self.block_set.delete_set(self.inode_id)
    
    def _check_block_set(self, block_id_list):
        fetched_block_id_list = []
        for block_id in self.block_set.get(self.inode_id):
            fetched_block_id_list.append(block_id)

        if set(fetched_block_id_list) == set(block_id_list):
            return True
        else:
            return False
        

    def test_insert_block_set(self):
        """Test insertion of block id to set"""
        self.block_set.add(self.inode_id, self.block_id_list[0])
        assert(self._check_block_set([self.block_id_list[0]]))
        self.block_set.add(self.inode_id, self.block_id_list[1])
        self.block_set.add(self.inode_id, self.block_id_list[2])
        assert(self._check_block_set(self.block_id_list))
    
    
    def test_remove_block_set(self):
        """Test removal of block id from set"""
        self.block_set.remove(self.inode_id, [self.block_id_list[0]])
        assert(self._check_block_set(self.block_id_list[1:]))
        self.block_set.remove(self.inode_id, self.block_id_list[1:])
        assert(self._check_block_set([]))
