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
import os
import sys
import pytest
import llfuse
import stat
sys.path.append('..')
from objectfs.core.metadata.superblock import SuperBlock
from objectfs.settings import Settings
settings = Settings()
from config import META_STORE_LIST, FS_SIZE, FS_BLOCK_SIZE, FS_NUM_INODES

@pytest.mark.parametrize('meta_store', META_STORE_LIST)
def test_object(meta_store):
    super_test = Super_Block_Test(meta_store)
    super_test.test_max_inodes()
    super_test.test_block_size()
    super_test.test_total_size()
    super_test.test_inode_counter()
    super_test.test_free_inode_id()
    super_test.test_used_size()
    super_test.test_keys_exists()
    super_test.test_delete_superblock()

class Super_Block_Test:

    def __init__(self, meta_store):
        self._superblock = SuperBlock('test_fs')
        self._superblock.init_superblock()
    
    def __del__(self):
        self._superblock.delete_superblock()

    def test_max_inodes(self):
        """Test max inodes value"""
        self._superblock.max_inodes = FS_NUM_INODES
        assert(self._superblock.max_inodes == FS_NUM_INODES)
    
    def test_block_size(self):
        """Test max inodes value"""
        self._superblock.block_size = FS_BLOCK_SIZE
        assert(self._superblock.block_size == FS_BLOCK_SIZE)
    
    def test_total_size(self):
        """Test total size"""
        self._superblock.total_size = FS_SIZE
        assert(self._superblock.total_size == FS_SIZE)
    
    def test_inode_counter(self):
        """Test the inode counter"""
        assert(self._superblock.inode_counter == 1)
        self._superblock.incr_inode_counter()
        assert(self._superblock.inode_counter == 2)
        self._superblock.incr_inode_counter()
        assert(self._superblock.inode_counter == 3)
        self._superblock.incr_inode_counter()
        self._superblock.incr_inode_counter()
        assert(self._superblock.inode_counter == 5)
        with pytest.raises(llfuse.FUSEError, message='No space left on device') as e:
            self._superblock.incr_inode_counter()
        self._superblock.decr_inode_counter()
        assert(self._superblock.inode_counter == 4)
        self._superblock.decr_inode_counter()
        self._superblock.decr_inode_counter()
        assert(self._superblock.inode_counter == 2)
    
    def test_free_inode_id(self):
        """Test the free inode id"""
        assert(self._superblock.fetch_free_inode_id() == 2)
        assert(self._superblock.fetch_free_inode_id() == 3)
        assert(self._superblock.fetch_free_inode_id() == 4)

    def test_used_size(self):
        """Test max inodes value"""
        assert(self._superblock.used_size == 0)
        self._superblock.incr_used_size(FS_SIZE)
        assert(self._superblock.used_size == FS_SIZE)
        with pytest.raises(llfuse.FUSEError, message='No space left on device') as e:
            self._superblock.incr_used_size(10)
        self._superblock.decr_used_size(FS_SIZE // 2 )
        assert(self._superblock.used_size == (FS_SIZE // 2))
    
    def test_keys_exists(self):
        assert(self._superblock.exists())
    
    def test_delete_superblock(self):
        """Test if the superblock was cleaned up correctly"""
        self._superblock.delete_superblock()
        assert(self._superblock.used_size is None)
        assert(self._superblock.total_size is None)
        assert(self._superblock.inode_counter is None)
        assert(self._superblock.free_inode_id is None)
        assert(self._superblock.max_inodes is None)
