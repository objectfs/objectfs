#!/bin/bash

base_size=400
iter_num=5
disrupt=10

for num_thread in 16 32
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
  # degree = 11
  python merge_benchmark.py -b $base_size -d $disrupt -l 50 50 50 50 50 30 30 30 20 20 20 -i $iter_num -o 11 -t $num_thread
  # degree = 12
  python merge_benchmark.py -b $base_size -d $disrupt -l 50 50 50 50 30 30 30 30 20 20 20 20 -i $iter_num -o 12 -t $num_thread
  # degree = 13
  python merge_benchmark.py -b $base_size -d $disrupt -l 50 50 50 30 30 30 30 30 20 20 20 20 20 -i $iter_num -o 13 -t $num_thread
  # degree = 14
  python merge_benchmark.py -b $base_size -d $disrupt -l 50 50 30 30 30 30 30 30 20 20 20 20 20 20 -i $iter_num -o 14 -t $num_thread
  # degree = 15
  python merge_benchmark.py -b $base_size -d $disrupt -l 50 30 30 30 30 30 30 30 20 20 20 20 20 20 20 -i $iter_num -o 15 -t $num_thread
  # degree = 16
  python merge_benchmark.py -b $base_size -d $disrupt -l 30 30 30 30 30 30 30 30 20 20 20 20 20 20 20 20 -i $iter_num -o 16 -t $num_thread
  # degree = 17
  python merge_benchmark.py -b $base_size -d $disrupt -l 30 30 30 30 30 30 20 20 20 20 20 20 20 20 20 20 20 -i $iter_num -o 17 -t $num_thread
  # degree = 18
  python merge_benchmark.py -b $base_size -d $disrupt -l 30 30 30 30 20 20 20 20 20 20 20 20 20 20 20 20 20 20 -i $iter_num -o 18 -t $num_thread
  # degree = 19
  python merge_benchmark.py -b $base_size -d $disrupt -l 30 30 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 -i $iter_num -o 19 -t $num_thread
  # degree = 20
  python merge_benchmark.py -b $base_size -d $disrupt -l 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 -i $iter_num -o 20 -t $num_thread
  # degree = 21
  python merge_benchmark.py -b $base_size -d $disrupt -l 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 10 10 -i $iter_num -o 21 -t $num_thread
  # degree = 22
  python merge_benchmark.py -b $base_size -d $disrupt -l 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 10 10 10 10 -i $iter_num -o 22 -t $num_thread
  # degree = 23
  python merge_benchmark.py -b $base_size -d $disrupt -l 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 10 10 10 10 10 10 -i $iter_num -o 23 -t $num_thread
  # degree = 24
  python merge_benchmark.py -b $base_size -d $disrupt -l 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 10 10 10 10 10 10 10 10 -i $iter_num -o 24 -t $num_thread
  # degree = 25
  python merge_benchmark.py -b $base_size -d $disrupt -l 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 10 10 10 10 10 10 10 10 10 10 -i $iter_num -o 25 -t $num_thread
done

  #for log_size in {1..10..1}
  #do
    #python merge_benchmark.py -d $log_size -l $(( log_size * 10 )) -i 5 -o 1 -t $num_thread
  #done
#done
#python merge_benchmark.py -d 10 -l 100 -i 5 -o 1 -t $value
