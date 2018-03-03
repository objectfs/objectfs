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

from __future__ import print_function, absolute_import, division
import sys
sys.path.append('..')
import os
import errno
import llfuse
import stat
import copy
import math
from time import time
from llfuse import FUSEError
from argparse import ArgumentParser
from objectfs.core.data.objectstore import ObjectStore
from objectfs.core.metadata.metastore import MetaStore
from objectfs.core.cache.cachestore import CacheStore
from objectfs.core.metadata.inode import Inode
from objectfs.core.metadata.superblock import SuperBlock
from objectfs.core.cache.cachequeue import CacheQueue
from objectfs.settings import Settings
settings = Settings()
import logging
logger = logging.getLogger(__name__)

DIR_MODE = (stat.S_IFDIR | 0777)
# FILE_MODE = (stat.S_IFREG | 0644)
FS_FILE_NAME_LENGTH = 255

class ObjectFsOperations(llfuse.Operations):

    def __init__(self, fs_name, cache_flag=True, multipart_flag=False):
        """Setup the filesystem"""
        super(ObjectFsOperations, self).__init__()
        self._fs_name = fs_name
        # loading the meta-store
        self._meta_store = MetaStore.load(self.fs_name)
        # loading the superblock
        self._super_block = SuperBlock(self.fs_name)
        # loading the data-store aka object-store
        self._data_store = ObjectStore.load(self.fs_name)
        # loading the cache store
        self._cache_store = CacheStore.load(self.fs_name)
        # load the cache queue
        self._cache_queue = CacheQueue(self.fs_name)
        # cache flag
        self._cache_flag = cache_flag
        # multi part flag
        self._multipart_flag = multipart_flag
    
    @property
    def fs_name(self):
        return self._fs_name
    
    def setup_root_inode(self):
        """Setup root inode"""
        logger.debug("Setting up ROOT inode")
        root_inode = Inode(self.fs_name, llfuse.ROOT_INODE, DIR_MODE, parent_inode_id=llfuse.ROOT_INODE)
        self._meta_store.add_inode_id_to_list(llfuse.ROOT_INODE, llfuse.ROOT_INODE, '..')
        self._meta_store.add_inode_id_to_list(llfuse.ROOT_INODE, llfuse.ROOT_INODE, '.')
        self._meta_store.put_inode(root_inode)

    def destroy(self):
        """Called when filesystem exits"""
        pass
        # KL TODO remove this after testing
        # cleaning memory store for now
        # self._meta_store._clean_store()
        # delete all the objects from the object store
        # super(ObjectFs, self).__del__()

    def getattr(self, inode_id, ctx=None):
        """Return attr"""
        logger.debug("GETATTR for inode:{}".format(inode_id))
        inode = self._meta_store.get_inode(inode_id)
        entry = llfuse.EntryAttributes()
        # unique inode number
        entry.st_ino = inode.id
        entry.st_mode = inode.mode
        entry.st_gid = inode.gid
        entry.st_uid = inode.uid
        entry.st_size = inode.size
        entry.st_atime_ns = inode.atime
        entry.st_ctime_ns = inode.ctime
        entry.st_mtime_ns = inode.mtime
        entry.st_rdev = inode.rdev
        entry.st_nlink = inode.nlink
        entry.st_blksize = self._super_block.block_size
        entry.st_blocks = math.ceil(entry.st_size / entry.st_blksize)
        # generation number for this entry
        # KL TODO this might be an issue for NFS
        entry.generation = 0
        # validity timout in seconds for the name
        entry.entry_timeout = 10
        # validity timout in seconds for attributes
        entry.attr_timeout = 10
        return entry
    
    def setattr(self, inode_id, attr, fields, fh, ctx):
        """Change the attributes of an inode"""
        logger.debug("SETATTR inode:{}".format(inode_id))
        inode = self._meta_store.get_inode(inode_id)
        if fields.update_size:
            if self._cache_flag:
                # KL TODO this needs to be fixed
                data = self._cache_store.get_inode(inode_id, 0)
            else:
                data = self._data_store.get_dnode(inode_id)
            if data is None:
                data = b''
            if len(data) < attr.st_size:
                data = data + b'\0' * (attr.st_size - len(data))
            else:
                data = data[:attr.st_size]
            # update size of the file in inode-map
            inode.size = attr.st_size
            # update the object in the object store
            if self._cache_flag:
                # KL TODO fix this as well for new struct
                self._cache_store.put_inode(inode_id, 0, data)
            else:
                self._data_store.put_dnode(inode_id, data)
        
        if fields.update_mode:
            inode.mode = attr.st_mode

        if fields.update_uid:
            inode.uid = attr.st_uid

        if fields.update_gid:
            inode.gid = attr.st_gid
        
        if fields.update_atime:
            inode.atime = attr.st_atime_ns

        if fields.update_mtime:
            inode.mtime = attr.st_mtime_ns
        
        # update ctime
        inode.ctime = self._get_time()

        return self.getattr(inode.id)

    def lookup(self, parent_inode_id, name, ctx=None):
        """Lookup the file using name and parent inode"""
        logger.debug("LOOKUP entry for parent inode:{},name:{}".format(parent_inode_id, name))
        if name == '.':
            inode_id = parent_inode_id
        elif name == '..':
            # inode_id = self._meta_store.get_inode(parent_inode_id).parent_inode_id
            inode_id = self._meta_store.get_inode(parent_inode_id).id
        else: 
            inode_id = self._meta_store.get_inode_id(parent_inode_id, name)
            if inode_id is None:
                logger.error("Entry not found for parent inode:{},name:{} in LOOKUP".format(parent_inode_id, name))
                raise llfuse.FUSEError(errno.ENOENT)
            self._meta_store.get_inode(inode_id).lookup_count += 1
        # increment lookup counter when we lookup file/folder
        return self.getattr(inode_id, ctx)
    
    def opendir(self, inode_id, ctx):
        logger.debug("OPENDIR inode:{}".format(inode_id))
        return inode_id
    
    def open(self, inode_id, flags, ctx):
        logger.debug("OPEN inode:{}".format(inode_id))
        # increment open counter when we open file
        self._meta_store.get_inode(inode_id).open_count += 1
        # increment lookup counter when we open file
        # self._meta_store.get_inode(inode_id).lookup_count += 1
        if self._cache_flag and not self._multipart_flag:
            if self._cache_store.exists_inode(inode_id) is False:
                data = self._data_store.get_dnode(inode_id)
                self._cache_store.put_inode(inode_id, data)
        return inode_id
    
    def create(self, parent_inode_id, name, mode, flags, ctx):
        """Create a file with permissions mode and open with flags"""
        logger.debug("CREATE parent inode:{},name:{}".format(parent_inode_id, name))
        entry = self._create(parent_inode_id, name, mode, flags, ctx)
        return (entry.st_ino, entry)
    
    def _create(self, parent_inode_id, name, mode, ctx, rdev=0, target=None):
        """Common create function"""
        # check if file/dir exists already
        if self._meta_store.get_inode_id(parent_inode_id, name):
            raise llfuse.FUSEError(errno.EEXIST)
        # check if the inode has a parent node or not
        if self.getattr(parent_inode_id).st_nlink == 0:
            logger.error("Attempting to create entry {} with unlinked parent node {}".format(name, parent_inode_id))
            raise FUSEError(errno.EINVAL)
        # get a new node id
        new_inode_id = self._super_block.fetch_free_inode_id()
        # creating entry in inode map
        new_inode = Inode(self.fs_name, new_inode_id, mode, name, parent_inode_id=parent_inode_id, rdev=0, target=target)
        self._meta_store.put_inode(new_inode)
        # add the inode in the base directory list
        self._meta_store.add_inode_id_to_list(parent_inode_id, new_inode.id, name)
        # check if the inode is a file or a directory
        if stat.S_ISREG(new_inode.mode):
            # creating an empty object
            if self._cache_flag:
                self._cache_store.put_inode(new_inode_id, 0, '')
            else:
                self._data_store.put_dnode(new_inode_id, '')
            # increment open counter when we create file
            self._meta_store.get_inode(new_inode_id).open_count += 1
        if stat.S_ISDIR(new_inode.mode):
            # insert . and .. in the inode id list
            self._meta_store.add_inode_id_to_list(new_inode_id, new_inode_id, '.')
            self._meta_store.add_inode_id_to_list(new_inode_id, parent_inode_id, '..')
        # increment lookup counter when we create, symlink, mkdir, mknod file/folder
        self._meta_store.get_inode(new_inode_id).lookup_count += 1
        return self.getattr(new_inode.id)
    
    def symlink(self, parent_inode_id, name, target, ctx):
        """Create a symbolic link"""
        mode = (stat.S_IFLNK | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
                stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP | stat.S_IROTH |
                stat.S_IWOTH | stat.S_IXOTH)
        return self._create(parent_inode_id, name, mode, ctx, target=target)

    def readdir(self, inode_id, off):
        """Returns name, attr, next"""
        logger.debug("READDIR inode:{}, offset:{}".format(inode_id, off))
        # iterate over the inode_id_list to return all the inode id's in the root directory
        for (list_offset, (inode_child_id, file_name)) in enumerate(self._meta_store.get_inode_id_list(inode_id, offset=off)):
            logger.debug("READDIR yield: file:{}, child inode:{}, offset:{}".format(file_name, inode_child_id, list_offset+off+1))
            yield(file_name, self.getattr(inode_child_id), list_offset+off+1)
    
    def mkdir(self, parent_inode_id, name, mode, ctx):
        """Create a directory"""
        logger.debug("MKDIR parent inode:{}, name:{}".format(parent_inode_id, name))
        if len(name) > FS_FILE_NAME_LENGTH:
            raise llfuse.FUSEError(errno.ENAMETOOLONG)
        return self._create(parent_inode_id, name, mode, ctx)
    
    def rmdir(self, parent_inode_id, name, ctx):
        """Remove directory"""
        logger.debug("RMDIR parent inode {}, name:{}".format(parent_inode_id, name))
        entry = self._get_entry(parent_inode_id, name, ctx)
        if len(name) > FS_FILE_NAME_LENGTH:
            logger.error("File name exceeds the length limit {} for file names".format(FS_FILE_NAME_LENGTH))
            return llfuse.FUSEError(errno.ENAMETOOLONG)
        if not stat.S_ISDIR(entry.st_mode):
            raise llfuse.FUSEError(errno.ENOTDIR)
        self._remove(parent_inode_id, name, entry)
    
    def _remove(self, parent_inode_id, name, entry):
        """Common remove function"""
        # count the number of nodes which this node points to
        if self._meta_store.length_inode_id_list(entry.st_ino) > 2:
            raise llfuse.FUSEError(errno.ENOTEMPTY)
        
        # if entry.st_nlink > 1 and self._meta_store.get_inode(entry.st_ino).open_count == 0:
            # self._meta_store.clean_index(parent_inode_id, name)
            # # KL TODO check if this works
            # self.memory_store.get_inode(entry.st_ino).nlink -= 1

        # remove inode_id from the inode_list of the parent_inode
        self._meta_store.remove_inode_id_from_list(parent_inode_id, entry.st_ino, name)
        # remove the name from the lookup index
        self._meta_store.clean_index(parent_inode_id, name)
        self._meta_store.get_inode(entry.st_ino).nlink -= 1
        
        # only remove the inode when there is a single nlink to the inode
        if entry.st_nlink == 1 and self._meta_store.get_inode(entry.st_ino).lookup_count == 0:
            inode = self._meta_store.get_inode(entry.st_ino)
            # fetch parent inode id of the inode
            parent_inode_id = inode.parent_inode_id
            
            # remove object from the object-store if it is a file
            if stat.S_ISREG(inode.mode):
                self._data_store.delete_dnode(inode.id)
                if self._cache_flag:
                    self._cache_store.remove_inode(inode.id)
            # remove inode from memory store
            self._meta_store.delete_inode(entry.st_ino)
            # decrease the size of the inode removed from used size
            self._super_block.decr_used_size(inode.size)
            # reduce the inode counter
            self._super_block.decr_inode_counter()
        

    def read(self, inode_id, off, size):
        """Read a file"""
        logger.debug("READ inode:{}, offset:{}, size:{}".format(inode_id, off, size))
        # fetching the inode for the object name
        # inode = self._meta_store.get_inode(inode_id)
        # fetch object from object-store and read object
        if self._cache_flag:
            data = self._cache_store.read_inode(inode_id, 0, off, off+size-1)
            if data:
                return data
            else:
                return b''
        elif self._multipart_flag:
            # list_object_block_ids = range(off//settings.DATA_BLOCK_SIZE, (off+size)//settings.DATA_BLOCK_SIZE+0 if (off+size)%settings.DATA_BLOCK_SIZE==0 else (off+size)//settings.DATA_BLOCK_SIZE+1)
            # for object_block_id in list_object_block_ids:
                # self._cache_queue.publish_work(inode_id, object_block_id)
            # self._cache_queue.subscribe_reply()
            # for (inode_id, object_block_id) in self._cache_queue.get_reply_message():
                # list_object_block_ids.remove(object_block_id)
                # if not list_object_block_ids:
                    # break
            # list_object_block_ids = range(off//settings.DATA_BLOCK_SIZE, (off+size)//settings.DATA_BLOCK_SIZE+0 if (off+size)%settings.DATA_BLOCK_SIZE==0 else (off+size)//settings.DATA_BLOCK_SIZE+1)
            # data = b''
            # old_off = off
            # for object_block_id in list_object_block_ids:
                # data += self._cache_store.read_inode(inode_id, object_block_id, off%((object_block_id+1)*settings.DATA_BLOCK_SIZE), off%((object_block_id+1)*settings.DATA_BLOCK_SIZE)+(min(size, (object_block_id+1)*settings.DATA_BLOCK_SIZE))-1)
                # off = (object_block_id+1)*settings.DATA_BLOCK_SIZE
            inode = self._meta_store.get_inode(inode_id)
            object_block_id = off // settings.DATA_BLOCK_SIZE
            new_off = off - (object_block_id*settings.DATA_BLOCK_SIZE)
            if off >= inode.size:
                return b''
            # check if object block exists in cache
            if self._cache_store.exists_inode(inode_id, object_block_id):
                data = self._cache_store.read_inode(inode_id, object_block_id, new_off, new_off+size-1)
            else:
                # enqueue a job to fetch the object block
                job = self._cache_queue.enqueue_download_task(inode_id, object_block_id, new_off, size-1)
                for i in range(object_block_id+1, inode.size/settings.DATA_BLOCK_SIZE, 1):
                    self._cache_queue.enqueue_prefetch_task(inode_id, i)
                while True:
                    if job.status == 'finished':
                        data = job.result
                        break
                    elif job.status == 'failed':
                        break
            if data:
                return data
            else:
                return b''

            # data = self._cache_store.read_inode(inode_id, off, off+size-1)
            # if data:
                # return data
            # else:
                # return b''
        else:
            data = self._data_store.get_dnode(inode_id)

            if data is None:
                return b''
            return data[off:off+size]
    
    def readlink(self, inode_id, ctx):
        """Read the link for a file"""
        return self._meta_store.get_inode(inode_id).target

    def write(self, inode_id, offset, buf):
        """Write a file"""
        logger.debug("WRITE inode:{}, offset:{}, buffer length:{}".format(inode_id, offset, len(buf)))
        inode = self._meta_store.get_inode(inode_id)
        if self._cache_flag:
            data_size = inode.size
            self._cache_store.write_inode(inode_id, 0, offset, buf)
            inode.size = max(data_size, len(buf)+offset)
            self._super_block.incr_used_size(inode.size-data_size)
        elif self._multipart_flag:
            object_block_id = offset // settings.DATA_BLOCK_SIZE
            new_offset = offset - (object_block_id*settings.DATA_BLOCK_SIZE)
            
            data_size = inode.size
            self._cache_store.write_inode(inode_id, object_block_id, new_offset, buf)
            inode.size = max(data_size, len(buf)+offset)
            self._super_block.incr_used_size(inode.size-data_size)
        else:
            data = self._data_store.get_dnode(inode_id)
            if data is None:
                data = b''
            if len(data)< offset:
                data = data + b'\x00'*(offset-len(data)) + buf
            else:
                data = data[:offset] + buf + data[offset+len(buf):]
            self._data_store.put_dnode(inode_id, data)
            # update the used size
            self._super_block.incr_used_size(len(data)-inode.size)
            # update inode size
            inode.size = len(data)
        return len(buf)

    def release(self, inode_id):
        """Relase a file"""
        logger.debug("RELEASE inode:{}".format(inode_id))
        # decrement inode open_count
        inode = self._meta_store.get_inode(inode_id)
        inode.open_count -= 1
        
        # check if the file is open or not
        if inode.open_count == 0:
            
            if self._multipart_flag:
              inode = self._meta_store.get_inode(inode_id)
              # contains the list of jobs which we have launched
              job_list = []
              # containes the etag to part mapping for every part upload
              etag_part_list = []
              multi_part_obj = self._data_store.container.object(inode_id).initiate_multipart_upload()
              # iterate over the size of the file and upload each object block individually
              for object_block_id in range(inode.size // settings.DATA_BLOCK_SIZE,-1,-1):
                  # append the launched jobs for book-keeping
                  job_list.append(self._cache_queue.enqueue_multipart_upload_task(inode_id, object_block_id, multi_part_obj.id))
              # wait for all jobs in the list to finish
              while job_list:
                  for job in job_list:
                      if job.status == 'finished':
                          # save part etag mapping for completion
                          # import pdb; pdb.set_trace()
                          etag_part_list.append({'ETag': job.result[0], 'PartNumber': job.result[1]})
                          # remove job from list
                          job_list.remove(job)
              # complete the multipart upload
              self._data_store.container.object(str(inode_id)).complete_multipart_upload(multi_part_obj.id, etag_part_list)
              print("Finished upload")
            elif self._cache_flag:
                data = self._cache_store.get_inode(inode_id)
                self._data_store.put_dnode(inode_id, data)
                self._cache_store.remove_inode(inode_id)
            else:
              # do nothing since the writes are being directly written to object storage
              pass
            
            # # self._meta_store.get_inode(inode_id).nlink -= 1
            # # KL TODO not sure if this is correct
            # # self._meta_store.clean_index(inode.parent_inode_id, inode.name)
            # if self.getattr(inode_id).st_nlink == 0:
                # self._meta_store.delete_inode(inode.id)
                # # TODO remove from object-store??
    
    def link(self, inode_id, new_parent_inode_id, new_name, ctx):
        logger.debug("LINK inode:{}, new parent:{}, new name:{}".format(inode_id, new_parent_inode_id, new_name))
        parent_entry = self.getattr(new_parent_inode_id)
        if parent_entry.st_nlink == 0:
            logger.error('Attempted to create entry {} with unlinked parent {}'.format(new_name, parent_inode_id))
            raise FUSEError(errno.EINVAL)
        # add the lookup index for the new name
        self._meta_store.build_index(new_parent_inode_id, inode_id, new_name)
        self._meta_store.add_inode_id_to_list(new_parent_inode_id, inode_id, new_name)
        self._meta_store.get_inode(inode_id).nlink +=1
        # incremeting lookup count
        self._meta_store.get_inode(inode_id).lookup_count += 1
        return self.getattr(inode_id)

    def unlink(self, parent_inode_id, name, ctx):
        logger.debug("UNLINK parent inode{}, name:{}".format(parent_inode_id, name))
        entry = self._get_entry(parent_inode_id, name, ctx)
        if stat.S_ISDIR(entry.st_mode):
            logger.error("Attempting to unlink a directory parent inode {} and name {}".format(parent_inode_id, name))
            raise llfuse.FUSEError(errno.EISDIR)
        self._remove(parent_inode_id, name, entry)
    
    def _get_entry(self, parent_inode_id, name, ctx):
        """Returns an entry for an inode"""
        inode_id = self._meta_store.get_inode_id(parent_inode_id, name)
        if inode_id is None:
            logger.error("Entry not found for parent inode:{},name:{} in LOOKUP".format(parent_inode_id, name))
            raise llfuse.FUSEError(errno.ENOENT)
        return self.getattr(inode_id, ctx)

    def rename(self, old_parent_inode_id, old_name, new_parent_inode_id, new_name, ctx):
        """Rename directories"""
        logger.debug("RENAME parent inode:{}, name:{} to parent inode:{}, name:{}".format(old_parent_inode_id, old_name, new_parent_inode_id, new_name))
        old_entry = self._get_entry(old_parent_inode_id, old_name, ctx)
        try:
            new_entry = self._get_entry(new_parent_inode_id, new_name, ctx)
        except llfuse.FUSEError as e:
            if e.errno != errno.ENOENT:
                raise
            target_exists = False
        else:
            target_exists = True

        if target_exists:
            self._replace(old_parent_inode_id, old_name, new_parent_inode_id, new_name, old_entry, new_entry)
        else:
            # remove inode from old parent id list
            self._meta_store.remove_inode_id_from_list(old_parent_inode_id, old_entry.st_ino, old_name)
            # add inode to new parent id list
            self._meta_store.add_inode_id_to_list(new_parent_inode_id, old_entry.st_ino, new_name)
            # update the name stored in inode
            inode = self._meta_store.get_inode(old_entry.st_ino)
            inode.name = new_name
            # remove old lookup key
            self._meta_store.clean_index(old_parent_inode_id, old_name)
            # insert the new lookup key
            self._meta_store.build_index(new_parent_inode_id, old_entry.st_ino, new_name)

    def _replace(self, old_parent_inode_id, old_name, new_parent_inode_id, new_name, old_entry, new_entry):
        # check if the destination is empty for not
        # check if the destination inode has child inodes or not
        if next(self._meta_store.get_inode_id_list(new_entry.st_ino), None):
            logger.error("RENAME inode:{} not empty".format(new_entry.st_ino))
            raise llfuse.FUSEError(errno.ENOTEMPTY)
        
        # KL TODO optimize when both the parents are the same
        # intialize the old and new inodes
        # old_inode = self._meta_store.get_inode(old_entry.st_ino)
        new_inode = self._meta_store.get_inode(new_entry.st_ino)
        # reduce the nlink to new_inode
        new_inode.nlink -= 1
        # remove old_inode_id from old_parent_id_list
        self._meta_store.remove_inode_id_from_list(old_parent_inode_id, old_entry.st_ino, old_name)
        # remove new_inode_id from new_parent_id_list
        self._meta_store.remove_inode_id_from_list(new_parent_inode_id, new_entry.st_ino, new_name)
        # add old_inode_id to new_parent_id_list
        self._meta_store.add_inode_id_to_list(new_parent_inode_id, old_entry.st_ino, old_name)
        # remove lookup key for new_inode_id
        self._meta_store.clean_index(new_parent_inode_id, new_name)
        # remove old lookup key for old_inode_id
        self._meta_store.clean_index(old_parent_inode_id, old_name)
        # # remove existing object from the object store
        # self._data_store.delete_dnode(new_inode.id)
        if self._cache_flag:
            # remove inode data from cache
            self._cache_store.remove_inode(new_inode.id)
        # # delete the new node from memory store
        # self._meta_store.delete_inode(new_entry.st_ino)
        # insert new lookup key for old_inode_id
        self._meta_store.build_index(new_parent_inode_id, old_entry.st_ino, old_name)

    def statfs(self, ctx):
        """Return stats on the file-system"""
        logger.debug("STATFS called")
        stat_fs = llfuse.StatvfsData()
        # file system block size
        stat_fs.f_bsize = self._super_block.block_size
        # fragment size / fundamental file system block size
        stat_fs.f_frsize = stat_fs.f_bsize
        
        # fetch total and used size from superblock
        total_size = self._super_block.total_size
        used_size = self._super_block.used_size
        # total number of blocks
        stat_fs.f_blocks = total_size // stat_fs.f_frsize
        # total number of free blocks 
        stat_fs.f_bfree = (total_size - used_size) // stat_fs.f_frsize
        # tota number of free blocks avaliable to non-privileged processes
        stat_fs.f_bavail = stat_fs.f_bfree
        
        # total of file nodes/inodes on the system
        stat_fs.f_files = self._super_block.max_inodes
        # total of free file nodes/inodes
        stat_fs.f_ffree = (stat_fs.f_files - self._super_block.inode_counter)
        # total number of avaliable of free nodes/inodes avaliable to non-privileged processes
        stat_fs.f_favail = stat_fs.f_ffree

        return stat_fs
    
    def _get_time(self):
        return int(time() * 1e9)

    def forget(self, inode_list):
        """Forget the inode"""
        for (inode_id, lookup_count) in inode_list:
            logger.debug("FORGET inode:{} with lookup_count:{}".format(inode_id, lookup_count))
            inode = self._meta_store.get_inode(inode_id)
            if inode.nlink == 0 and inode.lookup_count - lookup_count == 0:
                logger.debug("Deleting inode:{}".format(inode_id))
                # fetch parent inode id of the inode
                parent_inode_id = inode.parent_inode_id
                # remove object from the object-store if it is a file
                if stat.S_ISREG(inode.mode):
                    self._data_store.delete_dnode(inode.id)
                    if self._cache_flag:
                        for object_block_id in range(inode.size // settings.DATA_BLOCK_SIZE):
                            self._cache_store.remove_inode(inode.id, object_block_id)
                # remove inode from memory store
                self._meta_store.delete_inode(inode.id)
                # decrease the size of the inode removed from used size
                self._super_block.decr_used_size(inode.size)
                # reduce the inode counter
                self._super_block.decr_inode_counter()
            # else:
                # # evict the inode from cache
                # logger.debug("Evicting inode:{} from cache".format(inode_id))
                # self._cache_store.remove_inode(inode_id)

    def mknod(self, parent_inode_id, name, mode, rdev, ctx):
        logger.debug("MKNOD parent inode:{}, name:{}, rdev:{}".format(parent_inode_id, name, rdev))
        return self._create(parent_inode_id, name, mode, ctx, rdev=rdev)

    def access(self, inode_id, mode, ctx):
        """Check if requesting process has mode rights on the inode"""
        logger.debug("ACCESS for inode:{} with mode:{}".format(inode_id, mode))
        return True

    # def fsync(self, fh, datasync):
        # """Flush buffers for open file"""
        # logger.debug("FSYNC for inode:{}".format(fh))
        # # KL TODO what happens if the cache is turned off 
        # if self._cache_flag:
            # data = self._cache_store.get_inode(fh)
            # self._data_store.put_dnode(fh, data)
    
    # def flush(self, fh):
        # return NotImplemented

    # def fsyncdir(self, fh, datasync):
        # return NotImplemented

    # def stacktrace(self, ctx):
        # return NotImplemented

    # def setxattr(self, inode_id, name, value, ctx):
        # return NotImplemented

    # def getxattr(self, inode_id, name, ctx):
        # return NotImplemented

    # def listxattr(self, inode_id, name, ctx):
        # return NotImplemented

    # def removexattr(self, inode_id, name, ctx):
        # return NotImplemented
