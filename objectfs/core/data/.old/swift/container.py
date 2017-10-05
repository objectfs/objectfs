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
