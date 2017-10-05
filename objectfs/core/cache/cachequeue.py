from __future__ import print_function, absolute_import
import redis
from rq import Queue
from objectfs.core.cache.cachetask import download_object_block, prefetch_object_block, multipart_upload_object_block
from objectfs.settings import Settings
settings = Settings()
import logging
logger = logging.getLogger(__name__)

class CacheQueue(object):

    def __init__(self, fs_name, async_status=False):
        try:
            self._fs_name = fs_name
            self._client = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0)
            self._queue = Queue(connection=self._client, async=async_status)
        except redis.ConnectionError as e:
            logger.error("Cannot connect to Redis Server")
            raise e
    
    def enqueue_download_task(self, inode_id, object_block_id, offset, size):
        logger.debug("Enqueueing download task for inode {} object-block {}".format(inode_id, object_block_id))
        return self._queue.enqueue_call(func=download_object_block, args=(self._fs_name, inode_id, object_block_id, offset, size), timeout=5000)
    
    def enqueue_prefetch_task(self, inode_id, object_block_id):
        logger.debug("Enqueueing prefetch task for inode {} object-block {}".format(inode_id, object_block_id))
        return self._queue.enqueue_call(func=prefetch_object_block, args=(self._fs_name, inode_id, object_block_id), timeout=5000)
    
    def enqueue_multipart_upload_task(self, inode_id, object_block_id, multipart_id):
        logger.debug("Enqueueing multipart upload task for inode {} object-bock {} multi-part {}".format(inode_id, object_block_id, multipart_id))
        return self._queue.enqueue_call(func=multipart_upload_object_block, args=(self._fs_name, inode_id, object_block_id, multipart_id), timeout=5000)
