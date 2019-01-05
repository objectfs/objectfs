#!/bin/bash

# run ffmpeg benchmark
ffmpeg_benchmark(){
for i in /data/ramdisk/*.mpeg;
do
  ffmpeg -i "$i" -acodec copy copy -vcodec copy "${i%.*}.mp4"
done
}


# clean up tmpfs of all data
# rm -rf /data/ramdisk/
# set concurrent threads to 4 since we use this for other experiments
aws configure set default.s3.max_concurrent_requests 4
# download data from s3 to tmpfs
aws s3 cp --recursive s3://kunalfs/ /data/ramdisk/
# run ffmpeg
ffmpeg_benchmark
# upload data from tmpfs to s3
aws s3 cp /data/ramdisk/*.mp4 s3://kunalfs/
