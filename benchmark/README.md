## Usage

The benchmark script currently supports 3 basic workloads. These workloads have been inspired based on the ones supported by [goofys](https://github.com/kahing/goofys).

1. Create and Remove files
2. Read and Write files 
3. Time to first byte
4. Lookup files

### Create and Remove files
* Run 1 file 1 thread
```console
./objectfs_benchmark.sh create-remove 1 1
```
* Run 1 file 1 thread for 100 threads (creates a total of 100 files)
```console
./objectfs_benchmark.sh create-remove 1 100
```

### Read and Write files
* Run 1 file 1 threads
```console
./objectfs_benchmark.sh write-read 1 1
```
* Run 1 file 1 threads for 100 threads (read and writes to 100 files in parallel)
```console
./objectfs_benchmark.sh write-read 1 100
```

### Time to first byte
* Run 1 file 1 threads
```console
./objectfs_benchmark.sh first 1
```

### Lookup files
* Run lookup in current directory
```console
./objectfs_benchmark.sh lookup
```
