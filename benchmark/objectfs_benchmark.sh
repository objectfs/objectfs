#!/bin/bash

# create files
create_files() {
  for i in $(seq 1 $1); do
    touch file$2$i
  done
}

# remove files
remove_files() {
  for i in $(seq 1 $1); do
    rm file$2$i >&/dev/null || true
  done
}

# create files in parellel
create_files_parallel() {
  (for i in $(seq 1 $2); do
    create_files $1 $i & true
  done
  wait)
}

# remove files in parallel
remove_files_parallel(){
  (for i in $(seq 1 $2); do
    remove_files $1 $i & true
  done
  wait)
}

# lookup files
lookup_files() {
  ls > /dev/null
}

# write file
write_file() {
  dd if=/dev/zero of=$2large_file bs=1MB count=$1 oflag=nocache status=none
}

# write file serial
write_file_serial(){
  (for i in $(seq 1 $2); do
    write_file $1 $i
  done
  wait)
}

# write file in parallel
write_file_parallel(){
  (for i in $(seq 1 $2); do
    write_file $1 $i & true
  done
  wait)
}

# read file in serial
read_file_serial(){
  (for i in $(seq 1 $2); do
    read_file $1 $i
  done
  wait)
}

# read file in parallel
read_file_parallel(){
  (for i in $(seq 1 $2); do
    read_file $1 $i & true
  done
  wait)
}

# read file
read_file() {
  dd if=$2large_file of=/dev/null bs=1MB iflag=nocache status=none
}

# read first byte
read_first_byte() {
  dd if=$2large_file of=/dev/null bs=1 count=1 iflag=nocache status=none
}

case "$1" in
  create-remove)
          time create_files_parallel $2 $3
          time remove_files_parallel $2 $3
          ;;
  lookup)
          time lookup_files
          ;;
  first)
          time write_file $2 1
          time read_first_byte $2 1
          ;;
  write-read)
          time write_file_parallel $2 $3
          time read_file_parallel $2 $3
          ;;
  create)
          time create_files_parallel $2 $3
          ;;
  remove)
          time remove_files_parallel $2 $3
          ;;
  write)
          time write_file_serial $2 $3
          ;;
  read)
          time read_file_serial $2 $3
          ;;
  *)
          echo "Usage: $0 (create-remove|write-read|lookup|first) num_files_per_thread num_threads"
          echo "Benchmark for ObjectFS"
esac
