#!/usr/bin/env python

import json                        # noqa F401
import math                        # noqa F401
import sys                         # noqa F401
import os                          # noqa F401

from urllib.parse import urlparse

import boto3
import boto.s3.connection          # noqa F401
from boto.s3.key import Key        # noqa F401
# from filechunkio import FileChunkIO

# AWS_SHARED_CREDENTIALS_FILE = ~/.aws/credentials
# AWS_CONFIG_FILE = ~/.aws/config

access_key = ""
secret_key = ""

hostname = 's3dfrgw.slac.stanford.edu'
port = 443
is_secure = True


def get_endpoint_bucket_key(surl):
    parsed = urlparse(surl)
    endpoint = parsed.scheme + '://' + parsed.netloc
    full_path = parsed.path
    while "//" in full_path:
        full_path = full_path.replace('//', '/')

    parts = full_path.split('/')
    bucket = parts[1]
    key = '/'.join(parts[2:])
    return endpoint, bucket, key


s3_resource = boto3.resource(service_name='s3',
                             endpoint_url='https://s3dfrgw.slac.stanford.edu',
                             aws_access_key_id=access_key,
                             aws_secret_access_key=secret_key
                             )
print(s3_resource)

rs = s3_resource.buckets.all()
for b in rs:
    print(b)
    print(b.name)

s3 = boto3.client(service_name='s3',
                  endpoint_url='https://s3dfrgw.slac.stanford.edu',
                  aws_access_key_id=access_key,
                  aws_secret_access_key=secret_key
                  )

print(s3)

bucket_name = 'rubin-usdf-panda-logs'
# rt = s3.get_bucket_policy(Bucket='rubin-usdf-panda-logs')
# print(rt)
# print(rt['Policy'])


rt = s3.upload_file("/bin/hostname", 'rubin-usdf-panda-logs', 'hostname')
print(rt)

my_bucket = s3_resource.Bucket('rubin-usdf-panda-logs')
for obj in my_bucket.objects.all():
    print(obj)

rt = s3.get_object_acl(Bucket=bucket_name, Key='hostname')
print(rt)

full_url = 'https://s3dfrgw.slac.stanford.edu/rubin-usdf-panda-logs/SLAC_TEST/PandaJob_56998520/pilotlog.txt'
endpoint, bucket_name, object_name = get_endpoint_bucket_key(full_url)
print(endpoint, bucket_name, object_name)

response = s3.generate_presigned_url('get_object',
                                     Params={'Bucket': bucket_name,
                                             'Key': object_name},
                                     ExpiresIn=3600)
print(response)

rt = s3.download_file(bucket_name, object_name, '/tmp/pilotlog.txt')
print(rt)
