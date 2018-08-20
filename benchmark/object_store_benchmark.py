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
from procnetdev import ProcNetDev
from operator import add
import boto3.s3.transfer as transfer
from objectfs.core.data.objectstore import ObjectStoreFactory
import csv

FILE_SIZE = 16
NUM_ITER = 5
NUM_PROC = 4
BUCKET_NAME = 'kunalfs'
NUM_FILES = 50

def multipart_upload_task((fs_name, object_name, block_id, multipart_id, data)):
    data_store = ObjectStoreFactory.create_store(fs_name)
    # start_time = time.time()
    value = data_store.multipart_upload_dnode(object_name, block_id, multipart_id, data)
    # print(time.time()-start_time)
    return value

class ObjectStoreBenchmark(object):

    def __init__(self):
        self.parse_args = self._parse_args()
        self.data_store = ObjectStoreFactory.create_store(self.parse_args.bucket)
        self.pnd = ProcNetDev()
        values = []
        for iter_num in range(self.parse_args.iter):
          values.append(self.parse_args.func(iter_num))
          time.sleep(3)
        self.write_csv(values)

    
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
    
    def multipart_download(self, iter_num):
        """Multi-part download"""
        
        block_size = (self.parse_args.size*1024*1024)/self.parse_args.num
        config = transfer.TransferConfig(multipart_threshold = 1, 
                                max_concurrency = self.parse_args.num,
                                multipart_chunksize = block_size,
                                max_io_queue = 1000,
                                io_chunksize = block_size,
                                use_threads = True)
        start_time = time.time()
        self.data_store.container.object(self.parse_args.object_name+str(iter_num)).download_fileobj(config=config)
        end_time = time.time()
        print("{}".format(iter_num, end_time-start_time))
        self.write_csv([end_time-start_time])
    
    def multipart_upload(self, iter_num):
        """Multi-part upload"""
        
        pool = multiprocessing.Pool(processes=self.parse_args.num)
        # block_size = (self.parse_args.size*1024*1024)/self.parse_args.num
        block_size = (16777216*1)
        data = self.read_input_file()
        # config = transfer.TransferConfig(multipart_threshold = 1, 
                                # max_concurrency = self.parse_args.num,
                                # multipart_chunksize = block_size,
                                # max_io_queue = 1000,
                                # io_chunksize = block_size,
                                # use_threads = True)
        recvd_bytes = self.pnd['ens5']['receive']['bytes']
        transmit_bytes = self.pnd['ens5']['transmit']['bytes']
        start_time = time.time()
        # self.data_store.container.object(self.parse_args.object_name+str(iter_num)).upload_fileobj(data, config=config)
        for i in range(self.parse_args.num_files):
            etag_part_list = []
            args_list = []
            object_name = str(iter_num)+'_'+str(i)+'_'+self.parse_args.object_name
            multi_part_obj = self.data_store.container.object(object_name).initiate_multipart_upload()
            for block_id in range(0, (self.parse_args.size*1024*1024)//block_size, 1):
                args_list.append((self.parse_args.bucket, object_name, block_id, multi_part_obj.id, data))
            job_result_list = pool.map(multipart_upload_task, args_list)
            for job_result in job_result_list:
                etag_part_list.append({'ETag': job_result[0], 'PartNumber': job_result[1]})
            self.data_store.container.object(object_name).complete_multipart_upload(multi_part_obj.id, etag_part_list)
        end_time = time.time()
        pool.close()
        self.pnd['ens5']['receive']['bytes']-recvd_bytes
        self.pnd['ens5']['transmit']['bytes']-transmit_bytes
        print("{}".format(end_time-start_time))
        return (end_time-start_time, float(self.pnd['ens5']['receive']['bytes']-recvd_bytes)/(1024*1024), float(self.pnd['ens5']['transmit']['bytes']-transmit_bytes)/(1024*1024))
  
    def read_input_file(self):
        """Reading the input file"""
        # input_file = open("{}".format(self.parse_args.size), 'r')
        input_file = open("{}".format(16), 'r')
        return input_file.read()
    
    def write_csv(self, values):
        """Write to csv file"""
        
        time_values = [x[0] for x in values]
        io_values = [float(self.parse_args.size)*self.parse_args.num_files/x for x in time_values]
        recvd_values = [x[1] for x in values]
        transmit_values = [x[2] for x in values]
        total_net_values = map(add, recvd_values, transmit_values)

        with open("{}_io.csv".format(self.parse_args.func.__name__), 'a+') as csv_file:
           value_writer = csv.writer(csv_file, delimiter='\t')
           value_writer.writerow(io_values)
        
        with open("{}_net.csv".format(self.parse_args.func.__name__), 'a+') as csv_file:
           value_writer = csv.writer(csv_file, delimiter='\t')
           value_writer.writerow(recvd_values)
           value_writer.writerow(transmit_values)
           value_writer.writerow(total_net_values)

    def _parse_args(self):
        """Parse arguments"""

        parser = argparse.ArgumentParser(description='Object Store benchmark utils')
        # parser.add_argument('server_url', type=str, help='Server URL to run the benchmark')
        parser.add_argument('-i', '--iter', type=int, default=NUM_ITER, help='Iterations to run for the test. Default:{}'.format(NUM_ITER)) 
        parser.add_argument('-b', '--bucket', type=str, default=BUCKET_NAME, help='Bucket name. Default:{}'.format(BUCKET_NAME)) 
        parser.add_argument('-s', '--size', type=int, default=FILE_SIZE, help='Size of files to read and write. Default:{}'.format(FILE_SIZE))
        parser.add_argument('object_name', type=str, help='Object name to run the test against')
        sub_parsers = parser.add_subparsers(help='commands')

        # read objects
        read_parser = sub_parsers.add_parser('read', help='Run and read workload')
        read_parser.set_defaults(func=self.read)
        
        # write objects
        write_parser = sub_parsers.add_parser('write', help='Run the write workload')
        write_parser.set_defaults(func=self.write)
        
        # rename objects
        rename_parser = sub_parsers.add_parser('rename', help='Run the rename workload')
        rename_parser.set_defaults(func=self.rename)

        # multipart download
        download_parser = sub_parsers.add_parser('download', help='Run the multipart download')
        download_parser.add_argument('-n', '--num', type=int, default=NUM_PROC, help='Processes to run for the test. Default:{}'.format(NUM_PROC)) 
        download_parser.set_defaults(func=self.multipart_download)
        
        # multipart upload
        upload_parser = sub_parsers.add_parser('upload', help='Run the multipart upload')
        upload_parser.add_argument('-n', '--num', type=int, default=NUM_PROC, help='Processes to run for the test. Default:{}'.format(NUM_PROC)) 
        upload_parser.add_argument('-m', '--num_files', type=int, default=NUM_FILES, help='Number of files to upload. Default:{}'.format(NUM_PROC)) 
        upload_parser.set_defaults(func=self.multipart_upload)
        
        return parser.parse_args()

def main():

    os_benchmark = ObjectStoreBenchmark()

if __name__ == '__main__':
    main()
