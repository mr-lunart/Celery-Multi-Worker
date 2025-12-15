# Celery Multi Worker

This POC is designed to manage distributed task using celery framework.

To run this project :
1. Install Redis
2. Install PIP Requirement
3. Prepare .env
4. Run Celery Worker
5. Start task starter


## Install Redis

For Linux (RPM):
1. Create the file /etc/yum.repos.d/redis.repo with the following contents.
For Rocky Linux 9 and AlmaLinux 9
```
[Redis]
name=Redis
baseurl=http://packages.redis.io/rpm/rockylinux9
enabled=1
gpgcheck=1
```
For Rocky Linux 8 and AlmaLinux 8
```
[Redis]
name=Redis
baseurl=http://packages.redis.io/rpm/rockylinux8
enabled=1
gpgcheck=1
```
2. Run the following commands:
```bash
curl -fsSL https://packages.redis.io/gpg > /tmp/redis.key
sudo rpm --import /tmp/redis.key
sudo yum install redis
```

