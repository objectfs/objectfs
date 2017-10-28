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
from swiftclient.service import SwiftError
from core.objectstore.store import Store

class Container(object):

    def __init__(self, container_name):
        # create swift service
        self._object_store = Store.load()
        self._name = container_name

    @property
    def name(self):
        return self._name

    def create(self):
        """Create a container"""
        
        try:
            self._object_store._service.post(self.name)
        except SwiftError as e:
            print(e)
            raise e
    
    def update(container_name):
        """Update a container metadata"""
        return NotImplemented        

    def delete(self):
        """Delete a container"""

        try:
            response = self._object_store._service.delete(container=self.name)
            for page in response:
                continue
        except SwiftError as e:
            print(e)
            raise e

    @staticmethod
    def list():
        """List containers"""
        
        object_store = Store.load()
        try:
            response = object_store._service.list()
        except SwiftError as e:
            print(e)
            raise e
        for page in response:
            for container in page['listing']:
                yield Container(container['name'])
