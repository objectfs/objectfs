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

from __future__ import print_function, absolute_import
import redis
from time import time
from sets import Set
from rq.decorators import job
from objectfs.core.data.objectstore import ObjectStoreFactory
from objectfs.core.cache.cachestore import CacheStoreFactory
from objectfs.core.common.mergequeue import MergeQueue
from objectfs.core.common.fragmentmap import FragmentMap
from objectfs.settings import Settings
settings = Settings()
import logging
logger = logging.getLogger(__name__)

redis_client = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0)

@job('default', connection=redis_client, timeout=100)
def download_object_block(fs_name, inode_id, object_block_id, offset, size):
    """"Download object block task"""
    data_store = ObjectStoreFactory.create_store(fs_name)
    cache_store = CacheStoreFactory.create_store(fs_name)
    # check if the object block already exists
    if False:
    # if cache_store.exists_inode(inode_id, object_block_id):
        logger.debug("Worker reading from cache since inode {} object-block {} exists".format(inode_id, object_block_id))
        return cache_store.read_inode(inode_id, offset, offset+size-1, object_block_id)
    else:
        logger.debug("Starting download for inode {} object-block {}".format(inode_id, object_block_id))
        data = data_store.get_dnode(inode_id, object_block_id)
        logger.debug("Data fetched from object storage")
        cache_store.put_inode(inode_id, data, offset=offset, object_block_id=object_block_id)
        logger.debug("Data inserted in cache")
        logger.debug("Finished with downloading inode {} object-block {}".format(inode_id, object_block_id))
        return data[offset:offset+size]

# @job('default', connection=redis_client, timeout=100)
def prefetch_object_block((fs_name, inode_id, object_block_id)):
    """Prefetch object block task"""
    data_store = ObjectStoreFactory.create_store(fs_name)
    cache_store = CacheStoreFactory.create_store(fs_name)
    # check if the object block already exists in cache
    if False:
    # if cache_store.exists_inode(inode_id, object_block_id):
        logger.debug("Returning from prefetch task as inode {} object-block {} exists".format(inode_id, object_block_id))
        return
    else:
        # read data from object store
        logger.debug("Starting the prefetch task for inode {}, object_block {}".format(inode_id, object_block_id))
        data = data_store.get_dnode(inode_id, object_block_id)
        # put data in cache store
        cache_store.put_inode(inode_id, data, object_block_id=object_block_id)
        logger.debug("Finished with prefetch task for inode {}, object_block {}".format(inode_id, object_block_id))

def multipart_upload_object_block((fs_name, inode_id, object_block_id, multipart_id, log_object_name)):
    """Multipart upload task"""
    data_store = ObjectStoreFactory.create_store(fs_name)
    cache_store = CacheStoreFactory.create_store(fs_name)
    logger.debug("Starting multipart_upload task for inode {} object-block {} multi-part {}".format(inode_id, object_block_id, multipart_id))
    # read data from cache
    data = cache_store.read_inode(inode_id, 0, settings.DATA_BLOCK_SIZE-1, object_block_id)
    # remove data from cache
    cache_store.remove_inode(inode_id, object_block_id)
    # upload data to object store
    return data_store.multipart_upload_dnode(inode_id, object_block_id, multipart_id, data, log_object_name)
    logger.debug("Finished with multipart_upload task for inode {} object-block {} multi-part {}".format(inode_id, object_block_id, multipart_id))

@job('default', connection=redis_client, timeout=100)
def merge_log_objects(fs_name, inode_id):
    data_store = ObjectStoreFactory.create_store(fs_name)
    cache_store = CacheStoreFactory.create_store(fs_name)
    merge_queue = MergeQueue(fs_name)
    fragment_map = FragmentMap(fs_name)
    
    log_object_list = []
    for log_object in merge_queue.fetch(inode_id):
        log_object_list.append(log_object)    
    
    object_id_set = Set([])
    etag_part_list = []
    # start multipart upload
    base_obj = data_store.container.object(inode_id).initiate_multipart_upload()

    for log_object in log_object_list:
        block_id_list = fragment_map._decode_log_key(inode_id, log_object)
        block_id_list = map(int, block_id_list)
        log_object_set = Set(block_id_list)

        for block_id in log_object_set.difference(object_id_set):
            # fetch from block_index since it determines where the block lives in that log object
            block_index = block_id_list.index(block_id)
            print("Log:", block_id)
            data = data_store.get_dnode(inode_id, int(block_index), log_object)
            job_result = data_store.multipart_upload_dnode(inode_id, int(block_id), base_obj.id, data)
            # job_result = data_store.multipart_copy_dnode(inode_id, int(block_id), base_obj.id, log_object, int(block_index))
            etag_part_list.append({'ETag': job_result[0], 'PartNumber': job_result[1]})

        object_id_set = object_id_set.union(log_object_set)
    
    # upload the remaining from the base object
    base_obj_set = Set(range(data_store.dnode_size(inode_id)//settings.DATA_BLOCK_SIZE))
    for block_id in base_obj_set.symmetric_difference(object_id_set):
        data = data_store.get_dnode(inode_id, int(block_id))
        print("Base:", block_id)
        job_result = data_store.multipart_upload_dnode(inode_id, int(block_id), base_obj.id, data)
        # job_result = data_store.multipart_copy_dnode(inode_id, int(block_id), base_obj.id, inode_id, int(block_id))
        etag_part_list.append({'ETag': job_result[0], 'PartNumber': job_result[1]})

    etag_part_list.sort()
    # from operator import itemgetter
    # etag_part_list = sorted(etag_part_list, key=itemgetter('PartNumber'))
    # complete multipart upload
    data_store.container.object(inode_id).complete_multipart_upload(base_obj.id, etag_part_list)
    # remove log object from object store and merge queue
    for log_object in log_object_list:
        data_store.delete_dnode(inode_id, log_object)
        merge_queue.remove(inode_id, log_object)

def merge_log_object_parallel_worker((fs_name, inode_id, block_index, block_id, multipart_id, log_object)):
    data_store = ObjectStoreFactory.create_store(fs_name)
    cache_store = CacheStoreFactory.create_store(fs_name)
    merge_queue = MergeQueue(fs_name)
    fragment_map = FragmentMap(fs_name)
    
    if block_index is None:
        # data = data_store.get_dnode(inode_id, int(block_id), log_object)
        return data_store.multipart_copy_dnode(inode_id, int(block_id), multipart_id, inode_id, int(block_index))
    else:
        # data = data_store.get_dnode(inode_id, int(block_index), log_object)
        return data_store.multipart_copy_dnode(inode_id, int(block_id), multipart_id, inode_id, int(block_id))
    # return data_store.multipart_upload_dnode(inode_id, int(block_id), multipart_id, data)
    

@job('default', connection=redis_client, timeout=100)
def merge_log_objects_parallel(fs_name, inode_id, num_threads=4):
    data_store = ObjectStoreFactory.create_store(fs_name)
    cache_store = CacheStoreFactory.create_store(fs_name)
    merge_queue = MergeQueue(fs_name)
    fragment_map = FragmentMap(fs_name)
    
    log_object_list = []
    for log_object in merge_queue.fetch(inode_id):
        log_object_list.append(log_object)    
    
    object_id_set = Set([])
    args_list = []
    etag_part_list = []
    # start multipart upload
    base_obj = data_store.container.object(inode_id).initiate_multipart_upload()

    import pdb; pdb.set_trace() 
    for log_object in log_object_list:
        block_id_list = fragment_map._decode_log_key(inode_id, log_object)
        block_id_list = map(int, block_id_list)
    	log_object_set = Set(block_id_list)
        print(log_object)

        for block_id in log_object_set.difference(object_id_set):
            # fetch from block_index since it determines where the block lives in that log object
            block_index = block_id_list.index(block_id)
            args_list.append((fs_name, inode_id, int(block_index), int(block_id), base_obj.id, log_object))

        object_id_set = object_id_set.union(log_object_set)
    
    # upload the remaining from the base object
    base_obj_set = Set(range(data_store.dnode_size(inode_id)//settings.DATA_BLOCK_SIZE))
    for block_id in base_obj_set.symmetric_difference(object_id_set):
        args_list.append((fs_name, inode_id, None, int(block_id), base_obj.id, None))
    
    import multiprocessing
    pool = multiprocessing.Pool(num_threads)
    print("threads:", num_threads)
    job_result_list = pool.map(merge_log_object_parallel_worker, args_list)
    pool.close()
    for job_result in job_result_list:
        etag_part_list.append({'ETag': job_result[0], 'PartNumber': job_result[1]})
    
    # etag_part_list.sort()
    from operator import itemgetter
    etag_part_list = sorted(etag_part_list, key=itemgetter('PartNumber'))
    # complete multipart upload
    data_store.container.object(inode_id).complete_multipart_upload(base_obj.id, etag_part_list)
    # remove log object from object store and merge queue
    for log_object in log_object_list:
        data_store.delete_dnode(inode_id, log_object)
        merge_queue.remove(inode_id, log_object)
