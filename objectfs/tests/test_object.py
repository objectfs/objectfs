from __future__ import absolute_import, print_function
import os
import sys
import stat
import copy
import pytest
sys.path.append('..')
from objectfs.core.data.objectstore import ObjectStore
from objectfs.settings import Settings
settings = Settings()
from config import OBJECT_STORE_LIST

@pytest.mark.parametrize('object_store', OBJECT_STORE_LIST)
def test_object(object_store):
    obj = Object_Test(object_store)
    obj.test_create_object()
    obj.test_get_object()
    obj.test_list_objects()
    # obj.test_move_object()
    obj.test_delete_object()
    obj.test_delete_all_objects()

class Object_Test:

    def __init__(self, object_store):
        # load and create the container
        self.container = ObjectStore.load('test_fs', object_store).container
        self.container.create()
        # load the object
        self.object = self.container.object('test_object')
        self.second_object = self.container.object('test_second_object')
        self._object_string = 'hello'
        self._new_object_name = 'new_test_object'
    
    def __del__(self):
        self.container.delete()
    
    def test_create_object(self):
        """Test object creation"""
        self.object.put(self._object_string)
        assert(self.object.head())
    
    def test_get_object(self):
        """Test object fetch"""
        contents = self.object.get()
        assert(contents == self._object_string)
    
    def test_list_objects(self):
        """Test object list"""
        for object_item in self.container.list_objects():
            if object_item.name == 'test_object':
                return
        assert(False)

    def test_move_object(self):
        """Test object move"""
        assert(True)
        self.object.move(self._new_object_name)
        # loading new object for the new object name
        self.object = self.container.load_object(self._new_object_name)
        assert(self.object.head())

    def test_delete_object(self):
        """Test object deletion"""
        self.object.delete()
        assert(self.object.head() is False)

    def test_delete_all_objects(self):
        """Test all object deletion inside container"""
        self.object.put(self._object_string)
        self.second_object.put(self._object_string)
        assert(self.second_object.head())
        self.container.delete_all_objects()
        assert(self.second_object.head() is False)
        assert(self.object.head() is False)

