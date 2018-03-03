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
import subprocess
import llfuse
from objectfs.core.objectfs_operations import ObjectFsOperations

FILE_SIZE = 1
NUM_ITER = 1
FS_NAME = 'kunalfs'
MOUNT_POINT = '/data/'
DIR_NAME = 'test'
NUM_THREADS = 1

class FSBenchmark(object):

    def __init__(self):
        self.parse_args = self._parse_args()
        try:
            for iter_num in range(self.parse_args.iter):
                self.mount_fs()
                self.parse_args.func(iter_num)
                self.unmount_fs()
        except Exception as e:
            self.unmount_fs()
    
    def read_write(self, iter_num):
        """Run the read write benchmark"""
        file_des = os.open('{}/{}'.format(self.parse_args.dir_path, iter_num), os.O_RDWR)
    
    def rename(self, iter_num):
        """Run the rename benchmark"""
        
        subprocess.call(['mkdir', '{}{}'.format(self.parse_args.mount_point, DIR_NAME)])
        for i in range(self.parse_args.num_files):
          subprocess.call(['cp', '{}'.format(self.parse_args.size), '{}{}'.format(self.parse_args.mount_point, self.parse_args.file_name+'_',str(i))])
        import pdb; pdb.set_trace()
        start_time = time.time()
        subprocess.call(['mv', '{}{}'.format(self.parse_args.mount_point, DIR_NAME), '{}{}'.format(self.parse_args.mount_point, DIR_NAME+'new')])
        end_time = time.time()
        # subprocess.call(['rm','-rf', '{}'.format(self.parse_args.dir_name)])
        print("ITER:{}, TIME:{}".format(iter_num, end_time-start_time))
        self.write_csv([end_time-start_time])
    
    def mount_fs(self):
        """Mount the file system"""
        
        subprocess.Popen(['../objectfs/objectfs_cli','mount','{}'.format(self.parse_args.name),'{}'.format(self.parse_args.mount_point)], stdout=subprocess.PIPE)
        # fuse_options = set(llfuse.default_options)
        # fuse_options.add('fsname={}'.format(self.parse_args.name))
        # fuse_options.add('nosuid')
        # fuse_options.discard('default_permissions')
        # object_fs_operations = ObjectFsOperations(self.parse_args.name, 
                                                  # cache_flag=self.parse_args.cache,
                                                  # multipart_flag=self.parse_args.multipart)
        # llfuse.init(object_fs_operations, self.parse_args.mount_point, fuse_options)
        # try:
            # llfuse.main(workers=self.parse_args.num_threads)
        # except Exception as e:
            # llfuse.close(unmount=True)
            # raise

        # llfuse.close()

    def unmount_fs(self):
        """Unmount the file system"""
        pass

    def write_csv(self, values):
        """Write to csv file"""
        
        with open("{}{}.csv".format(self.parse_args.func.__name__, self.parse_args.size), 'a+') as csv_file:
            value_writer = csv.writer(csv_file, delimiter=' ')
            value_writer.writerow(values)
        

    def _parse_args(self):
        """Parse arguments"""

        parser = argparse.ArgumentParser(description='ObjectFS benchmark utils')
        parser.add_argument('-i', '--iter', type=int, default=NUM_ITER, help='Iterations to run for the test. Default:{}'.format(NUM_ITER)) 
        parser.add_argument('-n', '--name', type=str, default=FS_NAME, help='File system name. Default:{}'.format(FS_NAME)) 
        parser.add_argument('-c', '--cache', action='store_true', default=False, help='Cache mode. Default:{}'.format('False')) 
        parser.add_argument('-m', '--multipart', action='store_true', default=False, help='Multipart mode. Default:{}'.format('False')) 
        parser.add_argument('-d', '--mount_point', type=str, default=MOUNT_POINT, help='Mount point. Default:{}'.format(MOUNT_POINT))
        parser.add_argument('-s', '--size', type=int, default=FILE_SIZE, help='Size of files. Default:{}'.format(FILE_SIZE))
        parser.add_argument('-t', '--num_threads', type=int, default=NUM_THREADS, help='Number of threads. Default:{}'.format(NUM_THREADS))
        sub_parsers = parser.add_subparsers(help='commands')

        # rename workload
        rename_parser = sub_parsers.add_parser('rename', help='Run the rename benchmark')
        rename_parser.add_argument('num_files', type=int, help='Number of files in directory')
        rename_parser.add_argument('file_name', type=str, help='File name')
        rename_parser.set_defaults(func=self.rename)
          
        # read and write files
        rw_parser = sub_parsers.add_parser('rw', help='Run and read-write workload')
        rw_parser.set_defaults(func=self.read_write)

        return parser.parse_args()

def main():

    fs_benchmark = FSBenchmark()

if __name__ == '__main__':
    main()
