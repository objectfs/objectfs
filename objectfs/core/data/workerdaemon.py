from __future__ import print_function, absolute_import
import argparse
from objectfs.core.data.objectstore import ObjectStore
from objectfs.core.cache.cachestore import CacheStore
from objectfs.core.cache.cachequeue import CacheQueue

TOTAL_WORKERS = 2

class WorkerDaemon(object):

    def __init__(self, fs_name, worker_id, workers):
        self._fs_name = fs_name
        self._worker_id = worker_id
        self._total_workers = workers
        self._cache_queue = CacheQueue(self._fs_name)
        self._data_store = ObjectStore.load(self._fs_name)
        self._cache_store = CacheStore.load(self._fs_name)
    
    def _fetch_object_block(self, inode_id, object_block_id):
        if not self._cache_store.exists_inode(inode_id, object_block_id):
            print("Fetching the object", inode_id, object_block_id)
            data = self._data_store.get_dnode(inode_id, object_block_id)
            self._cache_store.put_inode(inode_id, object_block_id, data)

    def run(self):
        self._cache_queue.subscribe_work()
        for (inode_id, object_block_id) in self._cache_queue.get_work_message():
            if object_block_id % self._total_workers == self._worker_id:
                self._fetch_object_block(inode_id, object_block_id)
                self._cache_queue.publish_reply(inode_id, object_block_id) 


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('worker_id', type=int, help='Worked ID')
    parser.add_argument('-t', '--workers', type=int, default=TOTAL_WORKERS, help='Total number of workers')
    result = parser.parse_args()
    worker_daemon = WorkerDaemon('testfs', result.worker_id, result.workers)
    worker_daemon.run()
