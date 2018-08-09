#!/usr/bin/env python
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

# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function
import os
import argparse
import csv
import random
import time
import multiprocessing
from sets import Set
from operator import truediv, add
from objectfs.core.data.objectstore import ObjectStoreFactory
from objectfs.core.common.fragmentmap import FragmentMap
from objectfs.core.common.mergequeue import MergeQueue
from objectfs.core.cache.cachetask import merge_log_objects, merge_log_objects_parallel
from procnetdev import ProcNetDev

BASE_SIZE = 400
LOG_SIZE = 40
NUM_ITER = 5
FS_NAME = 'kunalfs2'
NUM_THREADS = 1
INODE_ID_LIST = range(71, 150, 1)
DISPERSIVE_INDEX = 4
NUM_LOG_OBJECTS = 1

def merge_log_objects_wrapper((fs_name, inode_id)):
    merge_log_objects(fs_name, inode_id)

def populate_files_woker((fs_name, inode_id, log_object_index, data)):
    # create data store
    data_store = ObjectStoreFactory.create_store(fs_name)
    data_store.put_dnode(inode_id, data, log_object_index)

class MergeBenchmark(object):

    def __init__(self):
        self.parse_args = self._parse_args()
        self.data_store = ObjectStoreFactory.create_store(self.fs_name)
        self.fragment_map = FragmentMap(self.fs_name)
        self.merge_queue = MergeQueue(self.parse_args.fs_name)
        self.base_data = self.read_input_file(self.parse_args.base_size)
        # cleaning queue
        self.clean_queue()
        self.pnd = ProcNetDev()
    
    @property
    def inode_id_list(self):
        return INODE_ID_LIST

    @property
    def fs_name(self):
        return self.parse_args.fs_name
  
    def clean_queue(self):
        """Clean queue"""
        for inode_id in self.inode_id_list:
            self.merge_queue.delete_queue(inode_id)
        

    def populate_files(self, inode_id):
        """Populate the files"""
        block_set = Set(range(self.parse_args.base_size//10))
        args_list = []
        # total_log_size = self.parse_args.dispersive_index*10
        for num_object in range(self.parse_args.num_log_objects):
            # this for mismatch log size with increasing degree
            # if total_log_size < self.parse_args.log_size:
                # self.parse_args.log_size = total_log_size
                # self.log_data = self.read_input_file(self.parse_args.log_size)
            block_id_list = []
            for block_iter in range(self.parse_args.log_size[num_object]//10):
                block_id = random.choice(list(block_set))
                block_id_list.append(block_id)
                block_set.remove(block_id)
            log_object_index = self.fragment_map._log_key(inode_id, block_id_list, int(time.time()))
            self.merge_queue.insert(inode_id, log_object_index)
	    print(inode_id, log_object_index)
            self.log_data = self.read_input_file(self.parse_args.log_size[num_object])
            args_list.append((self.fs_name, inode_id, log_object_index, self.log_data))
            # self.data_store.put_dnode(inode_id, self.log_data, log_object_index)
            # keeping track of log sizes written
            # total_log_size = total_log_size - self.parse_args.log_size
        pool = multiprocessing.Pool(16)
        pool.map(populate_files_woker, args_list)
        pool.close()
    
    
    def run(self):
        self.merge(self.parse_args.iter)
        # self.parallel_merge(self.parse_args.iter)
    
    def parallel_merge(self, iter_num):
        """Run the parallel merge benchmark"""
        run_time_list = []
        recvd_bytes_list = []
        transmit_bytes_list = []
        for i in range(iter_num):
            merge_list = []
            inode_id = self.inode_id_list[0]
            self.data_store.put_dnode(inode_id, self.base_data)
            self.populate_files(inode_id)
            
            recvd_bytes = self.pnd['ens5']['receive']['bytes']
            transmit_bytes = self.pnd['ens5']['transmit']['bytes']
            start_time = time.time()
            merge_log_objects_parallel(self.fs_name, inode_id, self.parse_args.num_threads) 
            run_time_list.append(time.time()-start_time)
            recvd_bytes_list.append(self.pnd['ens5']['receive']['bytes']-recvd_bytes)
            transmit_bytes_list.append(self.pnd['ens5']['transmit']['bytes']-transmit_bytes)
            
            # cleaning queue
            self.clean_queue()
        
        # write to csv file
        self.write_time(run_time_list)
        self.write_obps(map(truediv, [1]*self.parse_args.iter, run_time_list))
        self.write_netbw([float(value)/(1024*1024) for value in recvd_bytes_list])
        self.write_netbw([float(value)/(1024*1024) for value in transmit_bytes_list])
        self.write_netbw([float(value)/(1024*1024) for value in map(add, recvd_bytes_list, transmit_bytes_list)])
            

    def merge(self, iter_num):
        """Run the merge benchmark"""
        run_time_list = []
        recvd_bytes_list = []
        transmit_bytes_list = []
        for i in range(iter_num):
            merge_list = []
            for num_thread in range(self.parse_args.num_threads):
                inode_id = self.inode_id_list[num_thread]
                self.data_store.put_dnode(inode_id, self.base_data)
                self.populate_files(inode_id)
                merge_list.append((self.fs_name, inode_id))
            
            pool = multiprocessing.Pool(self.parse_args.num_threads)
            recvd_bytes = self.pnd['ens5']['receive']['bytes']
            transmit_bytes = self.pnd['ens5']['transmit']['bytes']
            start_time = time.time()
            pool.map(merge_log_objects_wrapper, merge_list)
            run_time_list.append(time.time()-start_time)
            recvd_bytes_list.append(self.pnd['ens5']['receive']['bytes']-recvd_bytes)
            transmit_bytes_list.append(self.pnd['ens5']['transmit']['bytes']-transmit_bytes)
            pool.close()
            # cleaning queue
            self.clean_queue()
        # write to csv file
        self.write_time(run_time_list)
        self.write_obps(map(truediv, [self.parse_args.num_threads]*self.parse_args.iter, run_time_list))
        self.write_netbw([float(value)/(1024*1024) for value in recvd_bytes_list])
        self.write_netbw([float(value)/(1024*1024) for value in transmit_bytes_list])
        self.write_netbw([float(value)/(1024*1024) for value in map(add, recvd_bytes_list, transmit_bytes_list)])
    
    def read_input_file(self, file_name):
        """Read input data file"""
        input_file = open('{}'.format(file_name), 'r')
        return input_file.read()
    
    def write_obps(self, values):
        """Write to objects per second file"""
        file_name = "{}_obps".format(self.parse_args.num_log_objects)
        self.write_csv(file_name, [self.parse_args.dispersive_index*10]+values)

    def write_netbw(self, values):
        """Write to network bandwidth file"""
        file_name = "{}_netbw".format(self.parse_args.num_log_objects)
        self.write_csv(file_name, [self.parse_args.dispersive_index*10]+values)

    def write_time(self, values):
        """Write to timing file"""
        file_name = "{}_time".format(self.parse_args.num_log_objects)
        self.write_csv(file_name, [self.parse_args.dispersive_index*10]+values)
        
    def write_csv(self, file_name, values):
        """Write to csv file"""
        with open("{}.csv".format(file_name), 'a+') as csv_file:
            value_writer = csv.writer(csv_file, delimiter='\t')
            value_writer.writerow(values)
        

    def _parse_args(self):
        """Parse arguments"""

        parser = argparse.ArgumentParser(description='ObjectFS benchmark utils')
        parser.add_argument('-i', '--iter', type=int, default=NUM_ITER, help='Iterations to run for the test. Default:{}'.format(NUM_ITER)) 
        parser.add_argument('-n', '--fs_name', type=str, default=FS_NAME, help='File system name. Default:{}'.format(FS_NAME)) 
        parser.add_argument('-b', '--base_size', type=int, default=BASE_SIZE, help='Size of base object. Default:{}'.format(BASE_SIZE))
        parser.add_argument('-l', '--log_size', nargs='+', type=int, default=LOG_SIZE, help='Size of log object. Default:{}'.format(LOG_SIZE))
        parser.add_argument('-o', '--num_log_objects', type=int, default=NUM_LOG_OBJECTS, help='Number of log objects. Default:{}'.format(NUM_LOG_OBJECTS))
        parser.add_argument('-t', '--num_threads', type=int, default=NUM_THREADS, help='Number of threads. Default:{}'.format(NUM_THREADS))
        parser.add_argument('-f', '--inode_id', type=list, default=INODE_ID_LIST, help='Inode id. Default:{}'.format(INODE_ID_LIST))
        parser.add_argument('-d', '--dispersive_index', type=int, default=DISPERSIVE_INDEX, help='Dispersive Index. Default:{}'.format(DISPERSIVE_INDEX))

        return parser.parse_args()

def main():

    merge_benchmark = MergeBenchmark()
    merge_benchmark.run()

if __name__ == '__main__':
    main()
