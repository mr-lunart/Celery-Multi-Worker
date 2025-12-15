# Celery Multi Worker

This POC is designed to manage distributed task using celery framework.

To run this project :
1. Install Redis
2. Install PIP Requirement
3. Prepare .env
4. Run Celery Worker
5. Start task starter


## Install Redis
**For Windows (RPM):**

The easiest way to install redis on windows is using [Memurai](https://redis.io/partners/memurai/) Development edition.
For easy managing and develop with redis database, use [Redis Insight](https://redis.io/insight/)

Ensure the port used in Memurai matches the one defined in scrapper.py after installation.
the url format should be
```
redis://:password@hostname:port/db_number
```
```python
app.conf.broker_url = 'redis://127.0.0.1:6379/0'
app.conf.result_backend = 'redis://127.0.0.1:6379/0'
```

**For Linux (RPM):**
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

## Install PIP Requirements
Install python package dependences
```bash
pip install -R requirements.txt
```

## Prepare .env
```.env
AWS_ACCESS_KEY_ID = A**************
AWS_SECRET_ACCESS_KEY = x**************
```

## Run Celery Worker
Run celery worker first. there 2 celery worker, the first worker handle 2 task pulling data from sqs and remove it after processing. the seconds is for processing step and call the first worker after finish processing. the first worker run with 10 concurrency and the second run with 20 concurrency
```
--loglevel=INFO
```
for debugging you can add this command for detailed logging
```bash
celery -A scrapper worker -Q consumer_sqs --concurrency=10 --pool=threads -n worker1@%h
celery -A scrapper worker --loglevel=INFO --concurrency=20 --pool=threads -n worker2@%h
```

<img width="1424" height="570" alt="image" src="https://github.com/user-attachments/assets/a296b312-21a5-41fd-b12e-e1b733aad58b" />


## Start Task Starter
run starter.py, this code would sent 20 queue of task to sqs and kickstart celery worker
```bash
python starter.py
```
