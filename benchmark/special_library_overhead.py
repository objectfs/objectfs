from __future__ import absolute_import, print_function
import argparse
import multiprocessing
import time
import csv
from objectfs.core.data.objectstore import ObjectStoreFactory
from objectfs.core.metadata.metastore import MetaStoreFactory
from objectfs.core.metadata.superblock import SuperBlock

BUCKET_NAME = 'kunalfs'
NUM_PROCS = 2
INODE_NUMBER = 500
INODE_OFFSET = 1
PATH_PREFIX = '/'

def fetch_mapping((fs_name, file_path)):
    """Fetch object to file mapping"""
    data_store = ObjectStoreFactory.create_store(fs_name)
    meta_store = MetaStoreFactory.create_store(fs_name)
    start_time = time.time()
    inode_id = 1
    file_path = file_path.split('/')
    for name in file_path:
        inode_id = meta_store.get_inode_id(inode_id, name)
    data = data_store.get_dnode(inode_id)
    return time.time()-start_time


def put_dnode((fs_name, file_name, object_name)):
    """Upload object and populate file_path"""
    data_store = ObjectStoreFactory.create_store(fs_name)
    meta_store = MetaStoreFactory.create_store(fs_name)
    super_block = SuperBlock(fs_name)
    inode_id = super_block.fetch_free_inode_id()
    data_store.put_dnode(inode_id, '')
    

class SpecialLibraryBenchmark(object):

  def __init__(self):
      self.parse_args = self._parse_args()
      self.pool = multiprocessing.Pool(processes=self.parse_args.proc_num)

  def run(self):
      # populate the object store
      args_list = []
      for inode_number in range(self.parse_args.inode_num):
          file_path = self.parse_args.path_prefix + str(inode_number+self.parse_args.inode_offset)
          args_list.append((self.parse_args.fs_name, file_path))
      # fetch object to file mapping
      import pdb; pdb.set_trace()
      time_values = self.pool.map(fetch_mapping, args_list)
      self.pool.close()
  
  def _parse_args(self):
      parser = argparse.ArgumentParser(description='Special library benchmark')
      parser.add_argument('-b', '--fs_name', type=str, default=BUCKET_NAME)
      parser.add_argument('-n', '--proc_num', type=int, default=NUM_PROCS)
      parser.add_argument('-i', '--inode_num', type=int, default=INODE_NUMBER)
      parser.add_argument('-o', '--inode_offset', type=int, default=INODE_OFFSET)
      parser.add_argument('-p', '--path_prefix', type=str, default=PATH_PREFIX)
      return parser.parse_args()


if __name__ == '__main__':
    special_library =  SpecialLibraryBenchmark()
    special_library.run()
    del special_library
