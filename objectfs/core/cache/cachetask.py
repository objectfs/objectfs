from __future__ import print_function, absolute_import
from time import time
from rq.decorators import job
from objectfs.core.data.objectstore import ObjectStore
from objectfs.core.cache.cachestore import CacheStore
from objectfs.settings import Settings
settings = Settings()
import logging
logger = logging.getLogger(__name__)

def download_object_block(fs_name, inode_id, object_block_id, offset, size):
    """"Download object block task"""
    data_store = ObjectStore.load(fs_name)
    cache_store = CacheStore.load(fs_name)
    # check if the object block already exists
    if cache_store.exists_inode(inode_id, object_block_id):
        logger.debug("Worker reading from cache since inode {} object-block {} exists".format(inode_id, object_block_id))
        return cache_store.read_inode(inode_id, object_block_id, offset, offset+size-1)
    else:
        logger.debug("Starting download for inode {} object-block {}".format(inode_id, object_block_id))
        data = data_store.get_dnode(inode_id, object_block_id)
        cache_store.put_inode(inode_id, object_block_id, data)
        logger.debug("Finished with downloading inode {} object-block {}".format(inode_id, object_block_id))
        return data[offset:offset+size]

def prefetch_object_block(fs_name, inode_id, object_block_id):
    """Prefetch object block task"""
    data_store = ObjectStore.load(fs_name)
    cache_store = CacheStore.load(fs_name)
    # check if the object block already exists in cache
    if cache_store.exists_inode(inode_id, object_block_id):
        logger.debug("Returning from prefetch task as inode {} object-block {} exists".format(inode_id, object_block_id))
        return
    else:
        # read data from object store
        logger.debug("Starting the prefetch task for inode {}, object_block {}".format(inode_id, object_block_id))
        data = data_store.get_dnode(inode_id, object_block_id)
        # put data in cache store
        cache_store.put_inode(inode_id, object_block_id, data)
        logger.debug("Finished with prefetch task for inode {}, object_block {}".format(inode_id, object_block_id))

def multipart_upload_object_block(fs_name, inode_id, object_block_id, multipart_id):
    """Multipart upload task"""
    data_store = ObjectStore.load(fs_name)
    cache_store = CacheStore.load(fs_name)
    logger.debug("Starting multipart_upload task for inode {} object-block {} multi-part {}".format(inode_id, object_block_id, multipart_id))
    # read data from cache
    data = cache_store.read_inode(inode_id, object_block_id, 0, settings.DATA_BLOCK_SIZE-1)
    # upload data to object store
    return data_store.multipart_upload_dnode(inode_id, object_block_id, multipart_id, data)
    logger.debug("Finished with multipart_upload task for inode {} object-block {} multi-part {}".format(inode_id, object_block_id, multipart_id))
