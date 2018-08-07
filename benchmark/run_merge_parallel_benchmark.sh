#!/bin/bash

base_size=400
iter_num=5
disrupt=10

for num_thread in 1 2 4 8 16 32
do
  # degree = 1
  python merge_benchmark.py -b $base_size -d $disrupt -l 400 -i $iter_num -o 1 -t $num_thread
  # degree = 2
  python merge_benchmark.py -b $base_size -d $disrupt -l 200 200 -i $iter_num -o 2 -t $num_thread
  # degree = 3
  python merge_benchmark.py -b $base_size -d $disrupt -l 200 100 100 -i $iter_num -o 3 -t $num_thread
  # degree = 4
  python merge_benchmark.py -b $base_size -d $disrupt -l 100 100 100 100 -i $iter_num -o 4 -t $num_thread
  # degree = 5
  python merge_benchmark.py -b $base_size -d $disrupt -l 100 100 100 50 50 -i $iter_num -o 5 -t $num_thread
  # degree = 6
  python merge_benchmark.py -b $base_size -d $disrupt -l 100 100 50 50 50 50 -i $iter_num -o 6 -t $num_thread
  # degree = 7
  python merge_benchmark.py -b $base_size -d $disrupt -l 100 50 50 50 50 50 50 -i $iter_num -o 7 -t $num_thread
  # degree = 8
  python merge_benchmark.py -b $base_size -d $disrupt -l 50 50 50 50 50 50 50 50 -i $iter_num -o 8 -t $num_thread
  # degree = 9
  python merge_benchmark.py -b $base_size -d $disrupt -l 50 50 50 50 50 50 50 30 20 -i $iter_num -o 9 -t $num_thread
  # degree = 10
  python merge_benchmark.py -b $base_size -d $disrupt -l 50 50 50 50 50 50 30 30 20 20 -i $iter_num -o 10 -t $num_thread
  # degree = 20
  python merge_benchmark.py -b $base_size -d $disrupt -l 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 -i $iter_num -o 20 -t $num_thread
done
