#!/bin/bash

for file_size in {40..400..40}
do
  python object_store_benchmark.py -i 10 -b kunalfs -s $file_size test upload -n 4
done

  #for log_size in {1..10..1}
  #do
    #python merge_benchmark.py -d $log_size -l $(( log_size * 10 )) -i 5 -o 1 -t $num_thread
  #done
#done
#python merge_benchmark.py -d 10 -l 100 -i 5 -o 1 -t $value
