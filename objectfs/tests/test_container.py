from __future__ import absolute_import, print_function
import sys
sys.path.append('..')
import pytest
from objectfs.core.data.objectstore import ObjectStore
from objectfs.settings import Settings
settings = Settings()
from config import OBJECT_STORE_LIST

@pytest.mark.parametrize('object_store', OBJECT_STORE_LIST)
def test_container(object_store):
    container = Container_Test(object_store)
    container.test_create_container()
    # skipping S3 delete test for now
    if object_store == 'Swift':
        container.test_delete_container()

class Container_Test:

    # def setup_class(self, param):
    def __init__(self, object_store):
        self.container = ObjectStore.load('test_fs', object_store).container
    
    def teardown_class(self):
        pass
    
    def test_create_container(self):
        self.container.create()
        container_list = self.container.list()
        for container in container_list:
            if self.container.name == container.name:
                return
        assert(False)
    
    def test_delete_container(self):
        response = self.container.delete()
        container_list = self.container.list()
        for container in container_list:
            if self.container.name == container.name:
                assert(False)
