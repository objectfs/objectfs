from __future__ import absolute_import, print_function
import os
import sys
import pytest
import stat
sys.path.append('..')
from objectfs.core.metadata.metastore import MetaStore
from objectfs.core.metadata.inode import Inode
from objectfs.settings import Settings
settings = Settings()
from config import META_STORE_LIST

@pytest.mark.parametrize('meta_store', META_STORE_LIST)
def test_object(meta_store):
    mem = Meta_Store_Test(meta_store)
    mem.test_put_get_inode()
    mem.test_inode_id_list()
    mem.test_delete_inode()

class Meta_Store_Test:

    def __init__(self, meta_store):
        self._meta_store = MetaStore.load('test_fs', meta_store)
        self.inode = Inode(4, stat.S_IFREG | 644, 'test_inode_name')  
    
    def __del__(self):
        self._meta_store.delete_inode_id_list(self.inode.id)
    
    def test_put_get_inode(self):
        """Test inode put and get. Check if index is built correctly"""
        response = self._meta_store.put_inode(self.inode)
        inode = self._meta_store.get_inode(self.inode.id)
        assert(inode.id == self.inode.id)
        assert(inode.name == self.inode.name)
        inode_id = self._meta_store.get_inode_id(self.inode.parent_inode_id, self.inode.name)
        assert(self.inode.id == inode_id)
    
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
