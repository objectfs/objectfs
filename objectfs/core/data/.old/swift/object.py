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
