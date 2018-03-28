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

from setuptools import setup, find_packages
from os import path
from codecs import open
from objectfs import __version__

ROOT = path.abspath(path.dirname(__file__))

# read the requirements file
with open(path.join(ROOT, 'setup', 'requirements.txt'), encoding='utf-8') as file_handle:
    install_requires = file_handle.read().split('\n')

# read the long description readme file
with open(path.join(ROOT, 'README.md'), encoding='utf-8') as file_handle:
    long_description = file_handle.read()

# run setup
setup(
        name = 'objectfs',
        version = __version__,
        description = 'A file system with the power of an object store',
        long_description = long_description,
        url = 'https://github.com/objectfs/objectfs',
        author = 'Kunal Lillaney',
        author_email = 'lillaney@jhu.edu',
        maintainer = 'Kunal Lillaney',
        maintainer_email = 'lillaney@jhu.edu',
        license = 'Apache 2.0',
        scripts = ['objectfs/objectfs_cli'],
        data_files = [
            ('/etc/', ['objectfs/settings.ini'])
        ],
        packages = find_packages(exclude=['docs', 'tests', 'benchmark', 'util']),
        include_package_data = True,
        install_requires = install_requires
)
