#!/bin/bash

for value in {160..1600..160}
do
  dd of=$value if=/dev/zero bs=1M count=$value
done  
