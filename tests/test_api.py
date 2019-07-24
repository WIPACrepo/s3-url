import os
import asyncio

import pytest
import requests
import tornado.web
from tornado.httpclient import HTTPRequest
import tornado.testing
from rest_tools.client import AsyncSession
import boto3

import s3_url_server


CONFIG = {
    'AUTH_ISSUER': None,
    'AUTH_SECRET': 'secret',
}
for k in CONFIG:
    if k in os.environ:
        CONFIG[k] = os.environ[k]

@pytest.fixture
def http_server_port():
    """
    Port used by `http_server`.
    """
    return tornado.testing.bind_unused_port()[-1]

@pytest.fixture
def boto3_mock(mocker):
    return mocker.patch('boto3.client')

@pytest.fixture
async def rest(monkeypatch, http_server_port):
    """Provide RestClient as a test fixture."""
    monkeypatch.setenv("AUTH_ALGORITHM", "HS512")
    if CONFIG['AUTH_ISSUER']:
        monkeypatch.setenv("AUTH_ISSUER", CONFIG['AUTH_ISSUER'])
    monkeypatch.setenv("AUTH_SECRET", CONFIG['AUTH_SECRET'])
    monkeypatch.setenv("ADDRESS", "localhost")
    monkeypatch.setenv("PORT", str(http_server_port))

    c = s3_url_server.configs()
    server = s3_url_server.app(c)
    server.startup(port=http_server_port)

    def client(role='read', timeout=0.1):
        if CONFIG['AUTH_ISSUER']:
            r = requests.get(CONFIG['AUTH_ISSUER']+'/token',
                             params={'scope': f's3-url:{role}'})
            r.raise_for_status()
            t = r.json()['access']
        else:
            raise Exception('testing token service not defined')
        print(t)
        session = AsyncSession(retries=0)
        async def req(method, url, args=None):
            r = await asyncio.wrap_future(session.request(method,
                    f'http://localhost:{http_server_port}'+url,
                    data=args, headers={'Authorization': f'bearer {t}'}, timeout=1))
            r.raise_for_status()
            return r.text
        return req

    yield client
    server.stop()
    await asyncio.sleep(0.01)

@pytest.mark.asyncio
async def test_get(boto3_mock, rest):
    boto3_mock.return_value.generate_presigned_url.return_value = 'url'
    r = rest()
    ret = await r('GET', '/bucket/object')
    assert ret == 'url'

    # try bad auth
    r = rest('blah')
    with pytest.raises(requests.exceptions.HTTPError):
        ret = await r('GET', '/bucket/object')

    # try bad url
    with pytest.raises(requests.exceptions.HTTPError):
        ret = await r('GET', '/bucket')

@pytest.mark.asyncio
async def test_put(boto3_mock, rest):
    boto3_mock.return_value.generate_presigned_url.return_value = 'url'
    r = rest('write')
    ret = await r('PUT', '/bucket/object')
    assert ret == 'url'

    # try bad auth
    r = rest('read')
    with pytest.raises(requests.exceptions.HTTPError):
        ret = await r('PUT', '/bucket/object')

    # try bad url
    with pytest.raises(requests.exceptions.HTTPError):
        ret = await r('PUT', '/bucket')

@pytest.mark.asyncio
async def test_delete(boto3_mock, rest):
    boto3_mock.return_value.generate_presigned_url.return_value = 'url'
    r = rest('write')
    ret = await r('DELETE', '/bucket/object')
    assert ret == 'url'

    # try bad auth
    r = rest('read')
    with pytest.raises(requests.exceptions.HTTPError):
        ret = await r('DELETE', '/bucket/object')

    # try bad url
    with pytest.raises(requests.exceptions.HTTPError):
        ret = await r('DELETE', '/bucket')
            