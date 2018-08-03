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
import subprocess
import json
from sets import Set
from operator import truediv, add
from objectfs.core.data.objectstore import ObjectStoreFactory
from objectfs.core.common.fragmentmap import FragmentMap
from objectfs.core.common.mergequeue import MergeQueue
from objectfs.core.cache.cachetask import merge_log_objects
from procnetdev import ProcNetDev

NUM_ITER = 5
FS_NAME = 'kunalfs'
RANDOM_PERCENT = 100

def merge_log_objects_wrapper((fs_name, inode_id)):
    merge_log_objects(fs_name, inode_id)


class WriteBenchmark(object):

    def __init__(self):
        self.parse_args = self._parse_args()
        self.data_store = ObjectStoreFactory.create_store(self.fs_name)
        self.pnd = ProcNetDev()
        # cleaning cache
        self.clean_cache()
    
    @property
    def inode_id_list(self):
        return INODE_ID_LIST

    @property
    def fs_name(self):
        return self.parse_args.fs_name
  
    def clean_cache(self):
        """Clean cache"""
        subprocess.call(["rm", "-rf", "/data/ramdisk/{}/".format(self.fs_name)])
    
    def run(self):
        self.write(self.parse_args.iter)

    def write(self, iter_num):
        """Run the write benchmark"""
        for random_percent in  range(10, 110, 10):
            recvd_bytes_list = []
            transmit_bytes_list = []
            io_bw_list = []
            for i in range(iter_num):
                recvd_bytes = self.pnd['eth0']['receive']['bytes']
                transmit_bytes = self.pnd['eth0']['transmit']['bytes']
                p = subprocess.Popen(["fio", "fio_write_{}".format(random_percent), "--output-format=json"], stdout=subprocess.PIPE)
                output, err = p.communicate()
                io_bw_list.append(float(json.loads(output)['jobs'][0]['write']['bw'])/1024)
                recvd_bytes_list.append(self.pnd['eth0']['receive']['bytes']-recvd_bytes)
                transmit_bytes_list.append(self.pnd['eth0']['transmit']['bytes']-transmit_bytes)
                self.clean_cache()
            # write to csv file
            self.write_io(io_bw_list)
            self.write_netbw([float(value)/(1024*1024) for value in recvd_bytes_list])
            self.write_netbw([float(value)/(1024*1024) for value in transmit_bytes_list])
            self.write_netbw([float(value)/(1024*1024) for value in map(add, recvd_bytes_list, transmit_bytes_list)])
            print(random_percent, ":" , io_bw_list)
            print(random_percent, ":", map(add, recvd_bytes_list, transmit_bytes_list))
    
    def read_input_file(self, file_name):
        """Read input data file"""
        input_file = open('{}'.format(file_name), 'r')
        return input_file.read()
    
    def write_io(self, values):
        """Write to io bw file"""
        file_name = "io_bw"
        self.write_csv(file_name, values)

    def write_netbw(self, values):
        """Write to network bandwidth file"""
        file_name = "netbw"
        self.write_csv(file_name, values)

    def write_time(self, values):
        """Write to timing file"""
        file_name = "{}_{}_time".format(self.parse_args.num_threads, self.parse_args.num_log_objects)
        self.write_csv(file_name, [self.parse_args.log_size]+values)
        
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
        parser.add_argument('-r', '--random_percent', type=int, default=RANDOM_PERCENT, help='Random percent. Default:{}'.format(RANDOM_PERCENT))

        return parser.parse_args()

def main():

    write_benchmark = WriteBenchmark()
    write_benchmark.run()

if __name__ == '__main__':
    main()
