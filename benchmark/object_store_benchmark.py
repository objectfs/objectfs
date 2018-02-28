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
import time
import multiprocessing
from objectfs.core.data.objectstore import ObjectStore
import csv

FILE_SIZE = 10
NUM_ITER = 5
NUM_PROC = 1
BUCKET_NAME = 'kunalfs'

def func_wrapper(args):
    data_store = ObjectStore.load('testfs')
    data_store.get_dnode(*args)


class ObjectStoreBenchmark(object):

    def __init__(self):
        self.parse_args = self._parse_args()
        self.data_store = ObjectStore.load(self.parse_args.bucket)
        # self._data_store = ObjectStore.load('testfs')
        for iter_num in range(self.parse_args.iter):
          self.parse_args.func(iter_num)
    
    def read(self, iter_num):
        """Run the read object benchmark"""
        
        #data = self.read_input_file()
        #data_store.put_dnode(self.parse_args.object_name, data)
        start_time = time.time()
        self.data_store.get_dnode(self.parse_args.object_name+str(iter_num))
        end_time = time.time()
        print("ITER:{}, TIME:{}".format(iter_num, end_time-start_time))
        self.write_csv([end_time-start_time])
        time.sleep(5)
        #data_store.delete_dnode(self.parse_args.object_name)

        # process_pool = multiprocessing.Pool(self.parse_args.num)
        # inode_arg_list = []
        # for block_id in range(self.parse_args.num):
            # inode_arg_list.append((self.parse_args.inode, block_id))
        # process_pool.map(func_wr__init__apper, inode_arg_list)
    
    def write(self, iter_num):
        """Run the write object benchmark"""

        data = self.read_input_file()
        start_time = time.time()
        self.data_store.put_dnode(self.parse_args.object_name+str(iter_num), data)
        end_time = time.time()
        print("ITER:{}, TIME:{}".format(iter_num, end_time-start_time))
        self.write_csv([end_time-start_time])
        #data_store.delete_dnode(self.parse_args.object_name)
        time.sleep(5)
    
    def rename(self, iter_num):
        """Run the rename object benchmark"""
        
        start_time = time.time()
        self.data_store.container.object(self.parse_args.object_name+str(iter_num)).move(self.parse_args.object_name+str(iter_num)+'new')
        end_time = time.time()
        print("ITER:{}, TIME:{}".format(iter_num, end_time-start_time))
        self.write_csv([end_time-start_time])
        time.sleep(5)

    def read_input_file(self):
        """Reading the input file"""
        input_file = open("{}".format(self.parse_args.size), 'r')
        return input_file.read()

    def write_csv(self, values):
        """Write to csv file"""
        
        with open("{}{}.csv".format(self.parse_args.func.__name__, self.parse_args.size), 'a+') as csv_file:
           value_writer = csv.writer(csv_file, delimiter=' ')
           value_writer.writerow(values)

    def _parse_args(self):
        """Parse arguments"""

        parser = argparse.ArgumentParser(description='Object Store benchmark utils')
        # parser.add_argument('server_url', type=str, help='Server URL to run the benchmark')
        parser.add_argument('-i', '--iter', type=int, default=NUM_ITER, help='Iterations to run for the test. Default:{}'.format(NUM_ITER)) 
        parser.add_argument('-n', '--num', type=int, default=NUM_PROC, help='Processes to run for the test. Default:{}'.format(NUM_PROC)) 
        parser.add_argument('-b', '--bucket', type=str, default=BUCKET_NAME, help='Bucket name. Default:{}'.format(BUCKET_NAME)) 
        sub_parsers = parser.add_subparsers(help='commands')

        # read objects
        read_parser = sub_parsers.add_parser('read', help='Run and read workload')
        read_parser.add_argument('object_name', type=str, help='Object name to run the test against')
        read_parser.add_argument('-s', '--size', type=int, default=FILE_SIZE, help='Size of files to read and write. Default:{}'.format(FILE_SIZE))
        read_parser.set_defaults(func=self.read)
        
        # write objects
        write_parser = sub_parsers.add_parser('write', help='Run the write workload')
        write_parser.add_argument('object_name', type=str, help='Object name to run the test against')
        write_parser.add_argument('-s', '--size', type=int, default=FILE_SIZE, help='Size of files to read and write. Default:{}'.format(FILE_SIZE))
        write_parser.set_defaults(func=self.write)
        
        # rename objects
        rename_parser = sub_parsers.add_parser('rename', help='Run the rename workload')
        rename_parser.add_argument('object_name', type=str, help='Object name to run the test against')
        rename_parser.add_argument('-s', '--size', type=int, default=FILE_SIZE, help='Size of files to read and write. Default:{}'.format(FILE_SIZE))
        rename_parser.set_defaults(func=self.rename)

        return parser.parse_args()

def main():

    os_benchmark = ObjectStoreBenchmark()

if __name__ == '__main__':
    main()
