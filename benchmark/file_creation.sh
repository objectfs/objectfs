#!/bin/bash

base=test

for value in {1..25}
do
  dd of=/data/kunalfs/$value$base if=/dev/zero bs=1M count=1280
done  
