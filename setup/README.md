## Setup Instructions

### Swift
* Install swift in a docker container. [Link](https://medium.com/technoetics/setting-up-openstack-swift-single-instance-in-docker-ab9cb1d7ca6)

### S3
* Install a developmental S3 mimic called [fakes3](https://github.com/jubos/fake-s3)
* Install gem first
```console
sudo yum install gem
```
* Install fakes3
```console
gem install fakes3
```
* Invoke fakes3
```console
fakes3 -r /data/test/ -p 4567
```

### S3Proxy
* Download S3Proxy. [Latest Release](https://github.com/andrewgaul/s3proxy/releases)
* Compile and make S3Proxy
```console
mvn package
```
* Set the correct permissions
```console
chmod +x s3proxy
```
* Run s3proxy
```console
s3proxy --properties s3proxy.conf
```
* The s3proxy.conf files are located in setup/s3proxy.

### Redis
* Download Redis. [Latest Stable Release](http://download.redis.io/releases/redis-3.2.9.tar.gz)
* Untar the download
```console
tar -xvf redis.tar.gz
```
* Make, test and install
```console
make && make test
make install
```
* Run redis in non-daemon mode for now.
```console
./src/redis-server
```

### Pip packages
* Install all required packages
```console
pip install -r requirements.txt
```

### Create the log directory
* All messages are logged to /var/log/objectfs/
```console
sudo mkdir /var/log/objectfs/
sudo chown <user-name>:<group-name> -R /var/log/objectfs/
```
