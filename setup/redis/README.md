## Setup Redis
**Note**: Assume that the repository is in your home directory
* Symlink systemd service to redis.service
```console
sudo ln -s /etc/systemd/system/redis.service ~/objectfs/setup/redis/redis.service 
```
* Symlink redis.conf for redis service
```console
sudo mkdir /etc/redis
sudo ln -s /etc/redis/redis.conf ~/objectfs/setup/redis/redis.conf
```
