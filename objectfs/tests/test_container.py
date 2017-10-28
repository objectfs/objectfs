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
