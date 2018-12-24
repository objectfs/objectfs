#!/bin/bash

for file_size in {128..1280..128}
do
  python object_store_benchmark.py -i 5 -b kunalfs -s $file_size test download -n 4
done
