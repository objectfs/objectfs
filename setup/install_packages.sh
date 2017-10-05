#!/bin/bash
# installation script for objectfs packages

sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install -y libfuse-dev libattr1-dev pkg-config
