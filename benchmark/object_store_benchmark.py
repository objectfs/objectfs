#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function
import os
import argparse
import time
import multiprocessing
from objectfs.core.data.objectstore import ObjectStore

FILE_SIZE = 500
NUM_ITER = 5
NUM_PROC = 4

def func_wrapper(args):
    data_store = ObjectStore.load('testfs')
    data_store.get_dnode(*args)


class ObjectStoreBenchmark(object):

    def __init__(self):
        self.parse_args = self._parse_args()
        # self._data_store = ObjectStore.load('testfs')
        for iter_num in range(self.parse_args.iter):
            self.parse_args.func(iter_num)
    
    def read(self, iter_num):
        """Run the read write benchmark"""
        
        process_pool = multiprocessing.Pool(self.parse_args.num)
        inode_arg_list = []
        for block_id in range(self.parse_args.num):
            inode_arg_list.append((self.parse_args.inode, block_id))
        start = time.time()
        process_pool.map(func_wrapper, inode_arg_list)
        print("ITER:{}, TIME:{}".format(iter_num, time.time()-start))
    

    def _parse_args(self):
        """Parse arguments"""

        parser = argparse.ArgumentParser(description='Object Store benchmark utils')
        # parser.add_argument('server_url', type=str, help='Server URL to run the benchmark')
        parser.add_argument('-i', '--iter', type=int, default=NUM_ITER, help='Iterations to run for the test. Default:{}'.format(NUM_ITER)) 
        parser.add_argument('-n', '--num', type=int, default=NUM_PROC, help='Processes to run for the test. Default:{}'.format(NUM_PROC)) 
        sub_parsers = parser.add_subparsers(help='commands')

        # read files
        read_parser = sub_parsers.add_parser('read', help='Run and read-write workload')
        read_parser.add_argument('inode', type=int, help='Inode number to run the test against')
        read_parser.add_argument('-s', '--size', type=int, default=FILE_SIZE, help='Size of files to read and write. Default:{}'.format(FILE_SIZE))
        read_parser.set_defaults(func=self.read)

        return parser.parse_args()

def main():

    os_benchmark = ObjectStoreBenchmark()

if __name__ == '__main__':
    main()
