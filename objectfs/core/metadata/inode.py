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
import os
import llfuse
import stat
import cPickle as pickle
from time import time
import logging
logger = logging.getLogger(__name__)

class Inode(object):

    def __init__(self, fs_name, node_id, mode, object_name=None, parent_inode_id=llfuse.ROOT_INODE, size=0, uid=os.getuid(), gid=os.getgid(), atime=None, mtime=None, ctime=None, target=None, rdev=0, nlink=1):
        self._id = node_id
        self._mode = mode
        self._fs_name = fs_name
        self._name = object_name
        self._parent_inode_id = parent_inode_id
        self._size = size
        self._open_count = 0
        self._lookup_count = 0
        self._uid = uid
        self._gid = gid
        self._atime = self._get_time()
        self._mtime = self._get_time()
        self._ctime = self._get_time()
        self._rdev = rdev
        self._target = target
        self._nlink = nlink
        self._inode_id_list = []
    
    @staticmethod
    def from_string(object_string):
        """Serialization method. Converts object to string"""
        return pickle.loads(object_string)

    @staticmethod
    def to_string(inode_object):
        """Deserialization method. Converts string to object"""
        return pickle.dumps(inode_object)
    
    def __getstate__(self):
        """Called to pickle class"""
        return self.__dict__

    def __setstate__(self, state):
        """Called to unpickle class"""
        self.__dict__.update(state)
    
    def _get_time(self):
        """Return the time since epoch as expected by FUSE"""
        return int(time() * 1e9)
    
    @property
    def fs_name(self):
        return self._fs_name

    @property
    def id(self):
        return self._id

    @property
    def mode(self):
        return self._mode
    
    @mode.setter
    def mode(self, value):
        logger.debug("Set mode as {}".format(value))
        self._mode = value
        self.update()

    @property
    def name(self):
        return self._name
    
    @name.setter
    def name(self, value):
        logger.debug("Set name as {}".format(value))
        self._name = value
        self.update()

    @property
    def parent_inode_id(self):
        return self._parent_inode_id
    
    @parent_inode_id.setter
    def parent_inode_id(self, value):
        logger.debug("Set parent inode id as {}".format(value))
        self._parent_inode_id = value
        self.update()

    @property
    def size(self):
        return self._size
    
    @size.setter
    def size(self, value):
        self._size = value
        logger.debug("Set size as {}".format(value))
        self.update()

    @property
    def open_count(self):
        return self._open_count

    @open_count.setter
    def open_count(self, value):
        logger.debug("Set open count as {}".format(value))
        self._open_count = value
        self.update()
    
    @property
    def lookup_count(self):
        return self._lookup_count

    @lookup_count.setter
    def lookup_count(self, value):
        logger.debug("Set lookup count as {}".format(value))
        self._lookup_count = value
        self.update()

    @property
    def uid(self):
        return self._uid

    @uid.setter
    def uid(self, value):
        logger.debug("Set uid as {}".format(value))
        self._uid = value
        self.update()

    @property
    def gid(self):
        return self._gid

    @gid.setter
    def gid(self, value):
        logger.debug("Set gid as {}".format(value))
        self._gid = value
        self.update()

    @property
    def atime(self):
        return self._atime

    @atime.setter
    def atime(self, value):
        logger.debug("Set atime as {}".format(value))
        self._atime = value
        self.update()
    
    @property
    def mtime(self):
        return self._mtime

    @mtime.setter
    def mtime(self, value):
        logger.debug("Set mtime as {}".format(value))
        self._mtime = value
        self.update()
    
    @property
    def ctime(self):
        return self._ctime

    @ctime.setter
    def ctime(self, value):
        logger.debug("Set ctime as {}".format(value))
        self._ctime = value
        self.update()
    
    @property
    def target(self):
        return self._target

    @target.setter
    def target(self, value):
        self._target = value
        self.update()

    @property
    def rdev(self):
        return self._rdev

    @rdev.setter
    def rdev(self, value):
        self._rdev = value
        self.update()
    
    @property
    def nlink(self):
        return self._nlink

    @nlink.setter
    def nlink(self, value):
        self._nlink = value
        self.update()

    @property
    def inode_id_list(self):
        from objectfs.core.metadata.metastore import MetaStore
        return MetaStore.load(self.fs_name).get_inode_id_list(self.id)
        # return self._inode_id_list
    
    @property
    def length_inode_id_list(self):
        from objectfs.core.metadata.metastore import MetaStore
        return MetaStore.load(self.fs_name).length_inode_id_list(self.id)

    def add_id_to_list(self, new_id):
        # KL TODO check that this only happens for DIR
        from core.filesystem.metadata import MetaStore
        logger.debug("Add inode no {} to list of {}".format(new_id, self.id))
        MetaStore.load(self.fs_name).add_inode_id_to_list(self.id, new_id)
        # return self._inode_id_list.append(new_id)

    def remove_id_from_list(self, existing_id):
        # KL TODO check that this only happens for DIR
        from core.filesystem.metadata import MetaStore
        logger.debug("Remove inode no {} from list of {}".format(new_id, self.id))
        MetaStore.load(self.fs_name).remove_inode_id_from_list(self.id, existing_id)

    def update(self):
        """Save the changed object to memory store"""
        from objectfs.core.metadata.metastore import MetaStore
        MetaStore.load(self.fs_name).update_inode(self)
