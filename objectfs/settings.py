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
        return self.parser.get('s3-auth', 'endpoint')

    @property
    def AWS_ACCESS_KEY_ID(self):
        return self.parser.get('s3-auth', 'aws_access_key_id')

    @property
    def AWS_SECRET_ACCESS_KEY(self):
        return self.parser.get('s3-auth', 'aws_secret_access_key')
    
    @property 
    def OBJECT_STORE(self):
        return self.parser.get('store', 'name')
    
    @property
    def CACHE_STORE(self):
        return self.parser.get('cache', 'name')

    @property
    def NUM_THREADS(self):
        return self.parser.getint('store', 'num_threads')
    
    @property
    def DATA_BLOCK_SIZE(self):
        return self.parser.getint('store', 'block_size')

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
