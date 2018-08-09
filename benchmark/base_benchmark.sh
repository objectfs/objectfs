#!/bin/bash

for file_size in {40..400..40}
do
  python object_store_benchmark.py -i 10 -b kunalfs2 -s $file_size test upload -n 4
done
