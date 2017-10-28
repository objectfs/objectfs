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
import docker

HOST = '127.0.0.1'

class ObjectFsDocker(object):

    def __init__(self):
        self._client = docker.DockerClient('tcp://{}:1234'.format(HOST))
        self.parser_args = self._parse_args()
        self.parser_args.func()

    def run(self, container_name):
        self._client.run(container_name)

    def start(self):
        return NotImplemented

    def stop(self):
        return NotImplemented

    def _parse_args(self):
        """Parse arguments"""

        parser = argparse.ArgumentParser(description='ObjectFS Docker command line utils')
        sub_parsers = parser.add_subparsers(help='commands')

        start_parser = sub_parsers.add_parser('start', help='Start ObjectFS docker')
        start_parser.add_argument('name', type=str, help='Name of container')
        start_parser.set_defaults(func=self.start)

def main():
    object_fs_docker = ObjectFsDocker()

if __name__ == '__main__':
    main()
