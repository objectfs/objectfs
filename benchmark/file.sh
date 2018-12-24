#!/bin/bash

base=test

for value in {0..1000..1}
do
  touch /data/kunalfs/$value$base
done  
