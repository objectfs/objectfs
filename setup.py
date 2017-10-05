from setuptools import setup, find_packages
from os import path
from codecs import open
from objectfs import __version__

current_directory = path.abspath(path.dirname(__file__))

# with open(path.join(current_directory, 'setup/requirements.txt'), encoding='utf-8') as file_handle:
    # all_requirements = file_handle.split('\n')

setup(
        name = 'objectfs',
        version = __version__,
        description = 'A file system with the power of an object store',
        url = 'https://github.ibm.com/Kunal-Lillaney/objectfs.git',
        author = 'Kunal Lillaney',
        author_email = 'Kunal.Lillaney@ibm.com',
        license = 'Apache 2.0',
        scripts = ['objectfs/objectfs_cli'],
        data_files = [
            ('/etc/', ['objectfs/settings.ini'])
        ],
        packages = find_packages(exclude=['docs', 'tests', 'benchmark', 'util']),
        include_package_data = True,
        install_requires = [
            'redis',
            'boto3',
            'llfuse',
            'python-swiftclient'
        ]
)
