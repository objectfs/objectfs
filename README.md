# ObjectFS

A file system with the power of an object store. 

[![Hex.pm](https://img.shields.io/hexpm/l/plug.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Build Status](https://travis-ci.org/objectfs/objectfs.svg?branch=master)](https://travis-ci.org/objectfs/objectfs)

ObjectFS allows you to mount a generic object store as a file system. It is POSIX complete and compatible with AWS S3, Google Cloud Storage, OpenStack Swift and other S3-based object stores.

## Usage
* Make file-system
```console
./objectfs_cli make <filesystem-name>
```

* Mount file-system
```console
./objectfs_cli mount <filesystem-name> <mount-point>
```

* List all file-systems
```console
./objectfs_cli list
```

* Delete file-system
```console
./objectfs_cli delete <filesystem-name>
```

## Architecture
ObjectFS is a file system which uses object storage as a backend. It's goal is to provide:

* Access data via both file system **and** object interfaces
* Support multiple object stores
* Portable across multiple operating systems

The high-level architecture of ObjectFS:
![Overview](./docs/images/overview.png)

A stack overview of ObjectFS:
![Client](./docs/images/client.png)

A file in ObjectFS:
![File Structure](./docs/images/file.png)

## Contributing

Please submit bug reports as git issues and label them as a *bug*. If you have feature requests, questions please free to open git issues for the same and label them appropriately. You can also contribute code or bug fixes by opening a pull request.
