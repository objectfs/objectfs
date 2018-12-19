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
import llfuse
import errno
from objectfs.core.metadata.metastore import MetaStoreFactory
from objectfs.settings import Settings
settings = Settings()
import logging
logger = logging.getLogger(__name__)

# super-block delimiter
DELIMITER = '%'

class SuperBlock(object):

    def __init__(self, name):
        """Init the class object"""
        self._name = name
        self._meta_store = MetaStoreFactory.create_store(name)
        self._max_inodes = self._wrap_with_delimiter(settings.SB_MAX_INODES)
        self._inode_counter = self._wrap_with_delimiter(settings.SB_INODE_COUNTER)
        self._block_size = self._wrap_with_delimiter(settings.SB_BLOCK_SIZE)
        self._free_inode_id = self._wrap_with_delimiter(settings.SB_FREE_INODE_ID)
        self._total_size = self._wrap_with_delimiter(settings.SB_TOTAL_SIZE)
        self._used_size = self._wrap_with_delimiter(settings.SB_USED_SIZE)
    
    def init_superblock(self, used_size=0, free_inode_id=1, inode_counter=1, block_size=0):
        """Init the superblock with pre-set values"""
        logger.debug("Init superblock with values")
        self.used_size = used_size
        # starts at 1 for root inode
        self.free_inode_id = free_inode_id
        # starts at 1 for root inode
        self.inode_counter = inode_counter
        self.block_size = block_size
    
    def _wrap_with_delimiter(self, superblock_key):
        return '{}{}{}{}{}'.format(DELIMITER, self._name, DELIMITER, superblock_key, DELIMITER)

    def _generate_key_list(self):
        return [self._max_inodes, self._inode_counter, self._block_size, self._free_inode_id, self._total_size, self._used_size]

    def delete_superblock(self):
        self._meta_store.delete_superblock_key(self._generate_key_list())
    
    def exists(self):
        """Check if all the values have been intialized or not"""
        for key_name in self._generate_key_list():
            if self._meta_store.exists(key_name):
                continue
            else:
                return False
        return True 

    @property
    def name(self):
        return self._name

    @property
    def max_inodes(self):
        return self._meta_store.get_superblock_key(self._max_inodes)

    @max_inodes.setter
    def max_inodes(self, value):
        logger.debug("Set max inodes as {}".format(value))
        self._meta_store.set_superblock_key(self._max_inodes, value)

    @property
    def free_inode_id(self):
        return self._meta_store.get_superblock_key(self._free_inode_id)
    
    @free_inode_id.setter
    def free_inode_id(self, value):
        logger.debug("Set free inode id as {}".format(value))
        self._meta_store.set_superblock_key(self._free_inode_id, value)

    def fetch_free_inode_id(self):
        # call this here since we have to fetch an unique id everytime we create one
        self.incr_inode_counter()
        logger.debug("Fetch next free inode number")
        return self._meta_store.incr_superblock_key(self._free_inode_id, 1)

    @property
    def inode_counter(self):
        return self._meta_store.get_superblock_key(self._inode_counter)
    
    @inode_counter.setter
    def inode_counter(self, value):
        self._meta_store.set_superblock_key(self._inode_counter, value)

    def incr_inode_counter(self):
        if self.inode_counter + 1 <= self.max_inodes:
            logger.debug("Increment inode counter")
            self._meta_store.incr_superblock_key(self._inode_counter, 1)
        else:
            logger.warn("No space left on device")
            raise llfuse.FUSEError(errno.ENOSPC)

    def decr_inode_counter(self):
        logger.debug("Decrement inode counter")
        self._meta_store.decr_superblock_key(self._inode_counter, 1)

    @property
    def block_size(self):
        return self._meta_store.get_superblock_key(self._block_size)

    @block_size.setter
    def block_size(self, value):
        logger.debug("Set the block size to {}".format(value))
        self._meta_store.set_superblock_key(self._block_size, value)

    @property
    def total_size(self):
        total_size = self._meta_store.get_superblock_key(self._total_size)
        return total_size if total_size else 0

    @total_size.setter
    def total_size(self, value):
        logger.debug("Set the total size to {}".format(value))
        self._meta_store.set_superblock_key(self._total_size, value)

    @property
    def used_size(self):
        used_size = self._meta_store.get_superblock_key(self._used_size)
        return used_size if used_size else 0
    
    @used_size.setter
    def used_size(self, value):
        logger.debug("Set used size to {}".format(value))
        self._meta_store.set_superblock_key(self._used_size, value)
    
    def incr_used_size(self, value):
        if self.used_size + value <= self.total_size:
            logger.debug("Increase used size by {}".format(value))
            self._meta_store.incr_superblock_key(self._used_size, value)
        else:
            logger.warn("No space left on device")
            raise llfuse.FUSEError(errno.ENOSPC)
    
    def decr_used_size(self, value):
        logger.debug("Decrease used size by {}".format(value))
        self._meta_store.decr_superblock_key(self._used_size, value)
