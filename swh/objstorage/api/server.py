# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import asyncio
import aiohttp.web
import os

from swh.core.config import read as config_read
from swh.core.api_async import (SWHRemoteAPI, decode_request,
                                encode_data_server as encode_data)
from swh.model import hashutil
from swh.objstorage import get_objstorage
from swh.objstorage.exc import ObjNotFoundError


@asyncio.coroutine
def index(request):
    return aiohttp.web.Response(body="SWH Objstorage API server")


@asyncio.coroutine
def check_config(request):
    req = yield from decode_request(request)
    return encode_data(request.app['objstorage'].check_config(**req))


@asyncio.coroutine
def contains(request):
    req = yield from decode_request(request)
    return encode_data(request.app['objstorage'].__contains__(**req))


@asyncio.coroutine
def add_bytes(request):
    req = yield from decode_request(request)
    return encode_data(request.app['objstorage'].add(**req))


@asyncio.coroutine
def add_batch(request):
    req = yield from decode_request(request)
    return encode_data(request.app['objstorage'].add_batch(**req))


@asyncio.coroutine
def get_bytes(request):
    req = yield from decode_request(request)
    try:
        ret = request.app['objstorage'].get(**req)
    except ObjNotFoundError:
        ret = {
            'error': 'object_not_found',
            'request': req,
        }
        return encode_data(ret, status=404)
    else:
        return encode_data(ret)


@asyncio.coroutine
def get_batch(request):
    req = yield from decode_request(request)
    return encode_data(request.app['objstorage'].get_batch(**req))


@asyncio.coroutine
def check(request):
    req = yield from decode_request(request)
    return encode_data(request.app['objstorage'].check(**req))


@asyncio.coroutine
def delete(request):
    req = yield from decode_request(request)
    return encode_data(request.app['objstorage'].delete(**req))


# Management methods

@asyncio.coroutine
def get_random_contents(request):
    req = yield from decode_request(request)
    return encode_data(request.app['objstorage'].get_random(**req))


# Streaming methods

@asyncio.coroutine
def add_stream(request):
    hex_id = request.match_info['hex_id']
    obj_id = hashutil.hash_to_bytes(hex_id)
    check_pres = (request.query.get('check_presence', '').lower() == 'true')
    objstorage = request.app['objstorage']

    if check_pres and obj_id in objstorage:
        return encode_data(obj_id)

    with objstorage.chunk_writer(obj_id) as write:
        # XXX (3.5): use 'async for chunk in request.content.iter_any()'
        while not request.content.at_eof():
            chunk = yield from request.content.readany()
            write(chunk)

    return encode_data(obj_id)


@asyncio.coroutine
def get_stream(request):
    hex_id = request.match_info['hex_id']
    obj_id = hashutil.hash_to_bytes(hex_id)
    response = aiohttp.web.StreamResponse()
    yield from response.prepare(request)
    for chunk in request.app['objstorage'].get_stream(obj_id, 2 << 20):
        response.write(chunk)
        yield from response.drain()
    return response


def make_app(config):
    """Initialize the remote api application.

    """
    app = SWHRemoteAPI()
    # retro compatibility configuration settings
    app['config'] = config
    _cfg = config['objstorage']
    app['objstorage'] = get_objstorage(_cfg['cls'], _cfg['args'])

    client_max_size = config.get('client_max_size', 1024 * 1024 * 1024)
    if client_max_size:
        app._client_max_size = client_max_size

    app.router.add_route('GET', '/', index)
    app.router.add_route('POST', '/check_config', check_config)
    app.router.add_route('POST', '/content/contains', contains)
    app.router.add_route('POST', '/content/add', add_bytes)
    app.router.add_route('POST', '/content/add/batch', add_batch)
    app.router.add_route('POST', '/content/get', get_bytes)
    app.router.add_route('POST', '/content/get/batch', get_batch)
    app.router.add_route('POST', '/content/get/random', get_random_contents)
    app.router.add_route('POST', '/content/check', check)
    app.router.add_route('POST', '/content/delete', delete)
    app.router.add_route('POST', '/content/add_stream/{hex_id}', add_stream)
    app.router.add_route('GET', '/content/get_stream/{hex_id}', get_stream)
    return app


def load_and_check_config(config_file):
    """Check the minimal configuration is set to run the api or raise an
       error explanation.

    Args:
        config_file (str): Path to the configuration file to load
        type (str): configuration type. For 'local' type, more
                    checks are done.

    Raises:
        Error if the setup is not as expected

    Returns:
        configuration as a dict

    """
    if not config_file:
        raise EnvironmentError('Configuration file must be defined')

    if not os.path.exists(config_file):
        raise FileNotFoundError('Configuration file %s does not exist' % (
            config_file, ))

    cfg = config_read(config_file)

    if 'objstorage' not in cfg:
        raise KeyError(
            "Invalid configuration; missing objstorage config entry")

    missing_keys = []
    vcfg = cfg['objstorage']
    for key in ('cls', 'args'):
        v = vcfg.get(key)
        if v is None:
            missing_keys.append(key)

    if missing_keys:
        raise KeyError(
            "Invalid configuration; missing %s config entry" % (
                ', '.join(missing_keys), ))

    cls = vcfg.get('cls')
    if cls == 'pathslicing':
        args = vcfg['args']
        for key in ('root', 'slicing'):
            v = args.get(key)
            if v is None:
                missing_keys.append(key)

        if missing_keys:
            raise KeyError(
                "Invalid configuration; missing args.%s config entry" % (
                    ', '.join(missing_keys), ))

    return cfg


def make_app_from_configfile():
    """Load configuration and then build application to run

    """
    config_file = os.environ.get('SWH_CONFIG_FILENAME')
    config = load_and_check_config(config_file)
    return make_app(config=config)


if __name__ == '__main__':
    print('Deprecated. Use swh-objstorage')
