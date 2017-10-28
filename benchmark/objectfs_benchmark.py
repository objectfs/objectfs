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

FILE_SIZE = 500
NUM_ITER = 5

class FsBenchmark(object):

    def __init__(self):
        self.parse_args = self._parse_args()
        for iter_num in self.parse_args.iter:
            self.parse_args.func(iter_num)
    
    def read_write(self, iter_num):
        """Run the read write benchmark"""
        file_des = os.open('{}/{}'.format(self.parse_args.dir_path, iter_num), os.O_RDWR)
    

    def _parse_args(self):
        """Parse arguments"""

        parser = argparse.ArgumentParser(description='ObjectFS benchmark utils')
        parser.add_argument('dir_path', type=str, help='Directory path to run the benchmark')
        parser.add_argument('-i', '--iter', type=int, default=NUM_ITER, help='Iterations to run for the test. Default:{}'.format(NUM_ITER)) 
        sub_parsers = parser.add_subparsers(help='commands')

        # read and write files
        rw_parser = sub_parsers.add_parser('rw', help='Run and read-write workload')
        rw_parser.add_argument('-s', '--size', type=int, default=FILE_SIZE, help='Size of files to read and write. Default:{}'.format(FILE_SIZE))
        rw_parser.set_defaults(func=self.read_write)

def main():

    fs_benchmark = FsBenchmark()

if __name__ == '__main__':
    main()
