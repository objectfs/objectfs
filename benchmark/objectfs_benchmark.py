#!/usr/bin/env python
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
