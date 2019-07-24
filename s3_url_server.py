"""
Token-based auth for S3 buckets, handing out presigned urls.

ENV Args:
    S3_ADDRESS = S3 endpoint address
    ACCESS_KEY = S3 access key
    SECRET_KEY = S3 secret key
    EXPIRE_DEFAULT = default url expiration in seconds (default: 1 hour)
    EXPIRE_LIMIT = limit for url expiration in seconds (default: 1 day)
    AUTH_SECRET = token auth secret
    AUTH_ISSUER = token auth issuer (default: IceCube token service)
    AUTH_ALGORITHM = token auth algorithm (default: RS512)
    ADDRESS = local address to server from (default: all interfaces)
    PORT = local port to serve from (default: 8080)
    LOGLEVEL = log level (default: INFO)
"""

import os
import json
import logging
from functools import partial

from rest_tools.client import json_decode
from rest_tools.server import RestServer, RestHandler, RestHandlerSetup, scope_role_auth
from tornado.web import HTTPError
from tornado.ioloop import IOLoop
import boto3

class PresignedURL:
    """Handle S3 access and creating presigned urls"""
    def __init__(self, address, access_key, secret_key, expiration, expiration_limit):
        #self.address = address
        #self.access_key = access_key
        #self.secret_key = secret_key
        self.expiration = expiration
        self.expiration_limit = expiration_limit

        self.s3 = boto3.client('s3','us-east-1',
            endpoint_url=address,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key)

    def get(self, bucket, key, expiration=None, method='GET'):
        if not expiration:
            expiration = self.expiration
        if (not isinstance(expiration, int)) or expiration < 1 or expiration > self.expiration_limit:
            raise Exception('invalid expiration time')

        params = {
            'Bucket': bucket,
            'Key': key,
        }
        client_method = 'get_object'
        if method == 'PUT':
            client_method = 'put_object'
        elif method == 'DELETE':
            client_method = 'delete_object'
        url = self.s3.generate_presigned_url(
            ClientMethod=client_method,
            Params=params,
            ExpiresIn=expiration,
            HttpMethod=method,
        )
        return url


### now do the http server

role_auth = partial(scope_role_auth, prefix='s3-url')

class MyHandler(RestHandler):
    def initialize(self, s3=None, **kwargs):
        super(MyHandler, self).initialize(**kwargs)
        self.s3 = s3

class S3Object(MyHandler):
    async def helper(self, bucket, key, method):
        if not bucket:
            raise HTTPError(404, reason='bad bucket name')
        if not key:
            raise HTTPError(404, reason='bad object name')
        try:
            req = json_decode(self.request.body)
        except Exception:
            req = {}
        expiration = req.get('expiration', None)
        url = self.s3.get(bucket, key, expiration=expiration, method=method)
        self.write(url)

    @role_auth(roles=['read'])
    async def get(self, bucket, key):
        await self.helper(bucket, key, 'GET')

    @role_auth(roles=['write'])
    async def put(self, bucket, key):
        await self.helper(bucket, key, 'PUT')

    @role_auth(roles=['write'])
    async def delete(self, bucket, key):
        await self.helper(bucket, key, 'DELETE')


### now configure

def configs():
    config = {
        's3': {
            'address': os.environ.get('S3_ADDRESS'),
            'access_key': os.environ.get('ACCESS_KEY'),
            'secret_key': os.environ.get('SECRET_KEY'),
            'expiration': int(os.environ.get('EXPIRE_DEFAULT', 3600)),
            'expiration_limit': int(os.environ.get('EXPIRE_LIMIT', 86400)),
        },
        'auth': {
            'secret': os.environ.get('AUTH_SECRET'),
            'issuer': os.environ.get('AUTH_ISSUER', 'https://tokens.icecube.wisc.edu'),
            'algorithm': os.environ.get('AUTH_ALGORITHM', 'RS512'),
        },
        'address': os.environ.get('ADDRESS', ''),
        'port': int(os.environ.get('PORT', '8080')),
        'loglevel': os.environ.get('LOGLEVEL', 'INFO'),
    }
    return config

def app(config):
    kwargs = RestHandlerSetup(config)
    kwargs.update({'s3': PresignedURL(**config['s3'])})
    server = RestServer()
    server.add_route(r'/(?P<bucket>[^\?]+)/(?P<key>[^\?]+)', S3Object, kwargs)
    return server

def main():
    config = configs()
    logging.basicConfig(level=config['loglevel'])
    server = app(config)
    server.startup(address=config['address'], port=config['port'])
    IOLoop.current().start()

if __name__ == '__main__':
    main()
