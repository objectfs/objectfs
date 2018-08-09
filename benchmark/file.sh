#!/bin/bash

for value in {10..100..10}
do
  dd of=$value if=/dev/zero bs=1M count=$value
done  
