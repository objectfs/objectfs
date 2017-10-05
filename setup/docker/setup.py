#!/usr/bin/env python
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
