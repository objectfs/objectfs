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
import subprocess
import pytest
import stat
import time
sys.path.append('..')
from objectfs.settings import Settings
settings = Settings()
from config import FS_MODE_LIST

DEVNULL = open(os.devnull, 'w')

@pytest.mark.parametrize('fs_mode', FS_MODE_LIST)
def test_file_system(fs_mode):
    fs = File_System_Test(fs_mode)
    fs.test_mk_dir()
    # mem.test_inode_id_list()
    # mem.test_delete_inode()

def wait_for_mount():
    elapsed = 0
    while elapsed < 30:
        if os.path.ismount('/data/testfs'):
            return True
        time.sleep(0.1)
        elapsed += 1

def delete_file_system():

    cmd_line = ['../objectfs_cli', 'delete', 'kunalfs']
    delete_process = subprocess.Popen(cmd_line, stdin=DEVNULL, universal_newlines=True)

class File_System_Test:

    def __init__(self, fs_mode):
        import pdb; pdb.set_trace()
        cmd_line = ['../objectfs_cli', 'mount', 'kunalfs', '/data/testfs']
        mount_process = subprocess.Popen(cmd_line, stdin=DEVNULL, universal_newlines=True)
        wait_for_mount()
    
    def __del__(self):
        
        delete_file_system()
    
    def test_mk_dir(self):
        """Test mkdir command"""
        dir_path = '/data/testfs/test_dir'
        os.mkdir(dir_path)
        fstat = os.stat(dir_path)
        assert stat.S_ISDIR(fstat.st_mode)
        # response = self._meta_store.put_inode(self.inode)
        # inode = self._meta_store.get_inode(self.inode.id)
        # assert(inode.id == self.inode.id)
        # assert(inode.name == self.inode.name)
        # inode_id = self._meta_store.get_inode_id(self.inode.parent_inode_id, self.inode.name)
        # assert(self.inode.id == inode_id)
    
    def test_inode_id_list(self):
        """Test the insertion and removal of inode ids from inode id list"""
        self._meta_store.add_inode_id_to_list(self.inode.id, 5, 'five')
        self._meta_store.add_inode_id_to_list(self.inode.id, 6, 'six')
        inode_list = []
        for (inode_id, file_name) in self._meta_store.get_inode_id_list(self.inode.id):
            inode_list.append((inode_id, file_name))
        assert((5, 'five') in inode_list)
        assert((6, 'six') in inode_list)
        self._meta_store.remove_inode_id_from_list(self.inode.id, 5, 'five')
        inode_list = []
        for (inode_id, file_name) in self._meta_store.get_inode_id_list(self.inode.id):
            inode_list.append((inode_id, file_name))
        assert((5, 'five') not in inode_list)
        assert((6, 'six') in inode_list)
        self._meta_store.remove_inode_id_from_list(self.inode.id, 6, 'six')
        inode_list = []
        for (inode_id, file_name) in self._meta_store.get_inode_id_list(self.inode.id):
            inode_list.append((inode_id, file_name))
        assert((6, 'six') not in inode_list)
    
    def test_delete_inode(self):
        """Test inode deletion"""
        self._meta_store.delete_inode(self.inode.id)
        response = self._meta_store.get_inode(self.inode.id)
        assert(response is None)
