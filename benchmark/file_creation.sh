#!/bin/bash

for value in {1..10}
do
  dd of=/data/kunalfs/test$value if=/dev/zero bs=1M count=400
done  
