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
from swiftclient.service import SwiftError, SwiftUploadObject
from core.objectstore.store import Store
from core.container import Container

class Object(object):

    def __init__(self, container_obj, object_name):
        # create swift connection
        self._object_store = Store.load()
        self._container = container_obj
        self._name = object_name
    
    @property
    def container(self):
        return self._container

    @property
    def name(self):
        return self._name
    
    def _read_file(self):
        with open(self._object_name, 'r') as object_handle:
            return object_handle.read()

    def put(self):
        """Put an object"""
        try:
            upload_object = SwiftUploadObject(self.name, object_name=self.name)
            response = self._object_store._service.upload(self.container.name, [upload_object])
            for item in response:
                continue
        except SwiftError as e:
            print(e)
            raise(e)
    
    def update(self):
        """Update an object"""
        return NotImplemented

    def get(self):
        """Get an object"""
        try:
            response = self._object_store._service.download(self.container.name, [self.name])
            for item in response:
                return item['path']
        except SwiftError as e:
            print(e)
            raise(e)
    
    @staticmethod
    def list(container):
        """List existing objects"""
        
        object_store = Store.load()
        try:
            response = object_store._service.list(container=container.name)
            for page in response:
                for item in page['listing']:
                    yield item['name']
        except SwiftError as e:
            print(e)
            raise(e)

    def delete(self):
        """Delete an object"""
        try:
            response = self._object_store._service.delete(container=self.container.name, objects=[self.name])
            for page in response:
                for item in page:
                    continue
        except SwiftError as e:
            print(e)
            raise(e)
