#!/bin/bash

base_size=1600
iter_num=1
disrupt=10

for num_thread in 32 16 8 4 2 1
do
  # degree = 10
  # python merge_benchmark.py -b $base_size -d $disrupt -l 160 160 160 160 160 160 160 160 160 160 -i $iter_num -o 10 -t $num_thread
  # degree = 20
  # python merge_benchmark.py -b $base_size -d $disrupt -l 80 80 80 80 80 80 80 80 80 80 80 80 80 80 80 80 80 80 80 80 -i $iter_num -o 20 -t $num_thread
  # degree = 30
  # python merge_benchmark.py -b $base_size -d $disrupt -l 96 96 96 96 96 96 96 96 96 96 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 -i $iter_num -o 30 -t $num_thread
  # degree = 40
  # python merge_benchmark.py -b $base_size -d $disrupt -l 96 96 96 96 96 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 -i $iter_num -o 40 -t $num_thread
  # degree = 50
  # python merge_benchmark.py -b $base_size -d $disrupt -l 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 -i $iter_num -o 50 -t $num_thread
  # degree = 60
  # python merge_benchmark.py -b $base_size -d $disrupt -l 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 -i $iter_num -o 60 -t $num_thread
  # degree = 70
  # python merge_benchmark.py -b $base_size -d $disrupt -l 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 -i $iter_num -o 70 -t $num_thread
  # degree = 80
  # python merge_benchmark.py -b $base_size -d $disrupt -l 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 -i $iter_num -o 80 -t $num_thread
  # degree = 90
  # python merge_benchmark.py -b $base_size -d $disrupt -l 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 32 32 32 32 32 32 32 32 32 32 -i $iter_num -o 90 -t $num_thread
  # degree = 100
  python merge_benchmark.py -b $base_size -d $disrupt -l 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 -i $iter_num -o 100 -t $num_thread
done
