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

from __future__ import print_function
from __future__ import absolute_import
import os
import sys
import logging
from logging.config import fileConfig
try:
    from ConfigParser import SafeConfigParser
except:
    from configparser import ConfigParser as SafeConfigParser

class Settings(object):

    def __init__(self, file_name='settings.ini'):
        self.parser = SafeConfigParser()
        # reading the settings file
        self._read_settings_file(file_name)
        self._set_path()
    
    def _read_settings_file(self, file_name):
        """Read the settings file"""
        try:
            self._file_name = os.path.join(os.path.dirname('/etc/'), file_name)
            self.parser.read(self._file_name)
            self._load_logging_config()
        except Exception as e:
            try:
                self._file_name = os.path.join(os.path.dirname(__file__), file_name)
                self.parser.read(self._file_name)
                self._load_logging_config()
            except Exception as e:
                raise e

    def _load_logging_config(self):
        """Load the logging config from settings file"""
        fileConfig(self._file_name)

    def _set_path(self):
        """Add modules to path"""
        return
        # BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), self.parser.get('path', 'BASE_PATH')))
    
    def _convert_list(self, values_list):
        """Converts the values to a list"""
        return [value.strip() for value in values_list.split(',')]

    @property
    def SWIFT_AUTH_USER(self):
        return self.parser.get('swift-auth', 'user')

    @property
    def SWIFT_AUTH_KEY(self):
        return self.parser.get('swift-auth', 'key')

    @property
    def SWIFT_AUTH_URL(self):
        return self.parser.get('swift-auth', 'url')
        
    @property
    def S3_AWS_REGION(self):
        return self.parser.get('s3-auth', 'aws_region')

    @property
    def S3_ENDPOINT(self):
        end_point = self.parser.get('s3-auth', 'endpoint')
        if end_point:
          return end_point
        else:
          return None

    @property
    def AWS_ACCESS_KEY_ID(self):
        return self.parser.get('s3-auth', 'aws_access_key_id')

    @property
    def AWS_SECRET_ACCESS_KEY(self):
        return self.parser.get('s3-auth', 'aws_secret_access_key')
    
    @property
    def GOOGLE_SERVICE_ACCOUNT_JSON_PATH(self):
      return self.parser.get('google-auth', 'service_account_json_path')

    @property 
    def OBJECT_STORE(self):
        return self.parser.get('store', 'name')
    
    @property
    def OBJECT_STORES_SUPPORTED(self):
        """List of supported object stores"""
        return self._convert_list(self.parser.get('store', 'object_stores_supported'))

    @property
    def CACHE_STORE(self):
        return self.parser.get('cache', 'name')
    
    @property
    def CACHE_STORES_SUPPORTED(self):
        """List of supported cache stores"""
        return self._convert_list(self.parser.get('cache', 'cache_stores_supported'))
    
    @property
    def META_STORE(self):
        return self.parser.get('meta', 'name')

    @property
    def META_STORES_SUPPORTED(self):
        """List of supported meta stores"""
        return self._convert_list(self.parser.get('meta', 'meta_stores_supported'))

    @property
    def NUM_THREADS(self):
        return self.parser.getint('store', 'num_threads')
    
    @property
    def DATA_BLOCK_SIZE(self):
        return self.parser.getint('store', 'block_size')
    
    @property
    def FS_OPERATION_MODES_SUPPORTED(self):
        return self._convert_list(self.parser.get('file-system-mode', 'operation_modes_supported'))

    @property
    def FS_OPERATION_MODE(self):
        return self.parser.get('file-system-mode', 'operation_mode')

    @property
    def FS_MOUNT_POINT(self):
        return self.parser.get('file-system-mount', 'mount_point')
    
    @property
    def FS_NUM_INODES(self):
        return self.parser.getint('file-system-make', 'num_inodes')

    @property
    def FS_BLOCK_SIZE(self):
        return self.parser.getint('file-system-make', 'block_size')

    @property
    def FS_NUM_THREADS(self):
        return self.parser.getint('file-system-make', 'num_threads')

    @property
    def FS_SIZE(self):
        return self.parser.getint('file-system-make', 'total_size')

    @property
    def REDIS_HOST(self):
        return self.parser.get('redis', 'host')

    @property
    def REDIS_PORT(self):
        return self.parser.getint('redis', 'port')

    @property
    def REDIS_DB(self):
        return self.parser.getint('redis', 'db')

    @property
    def REDIS_MAX_CONNECTIONS(self):
        return self.parser.getint('redis', 'max_connections')
    
    @property
    def FILE_CACHE_MOUNT_POINT(self):
        return self.parser.get('file-cache', 'mount_point')

    @property
    def SB_INODE_COUNTER(self):
        return self.parser.get('file-system-superblock', 'inode_counter')

    @property
    def SB_MAX_INODES(self):
        return self.parser.get('file-system-superblock', 'max_inodes')

    @property
    def SB_BLOCK_SIZE(self):
        return self.parser.get('file-system-superblock', 'block_size')

    @property
    def SB_TOTAL_SIZE(self):
        return self.parser.get('file-system-superblock', 'total_size')

    @property
    def SB_USED_SIZE(self):
        return self.parser.get('file-system-superblock', 'used_size')

    @property
    def SB_FREE_INODE_ID(self):
        return self.parser.get('file-system-superblock', 'free_inode_id')
