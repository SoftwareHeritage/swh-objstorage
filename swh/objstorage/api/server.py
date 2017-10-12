# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import asyncio
import aiohttp.web
import click

from swh.core import config
from swh.core.api_async import (SWHRemoteAPI, decode_request,
                                encode_data_server as encode_data)
from swh.model import hashutil
from swh.objstorage import get_objstorage


DEFAULT_CONFIG_PATH = 'objstorage/server'
DEFAULT_CONFIG = {
    'cls': ('str', 'pathslicing'),
    'args': ('dict', {
        'root': '/srv/softwareheritage/objects',
        'slicing': '0:2/2:4/4:6',
    }),
    'client_max_size': ('int', 1024 * 1024 * 1024),
}


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
def get_bytes(request):
    req = yield from decode_request(request)
    return encode_data(request.app['objstorage'].get(**req))


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


def make_app(config, **kwargs):
    if 'client_max_size' in config:
        kwargs['client_max_size'] = config['client_max_size']

    app = SWHRemoteAPI(**kwargs)
    app.router.add_route('GET', '/', index)
    app.router.add_route('POST', '/check_config', check_config)
    app.router.add_route('POST', '/content/contains', contains)
    app.router.add_route('POST', '/content/add', add_bytes)
    app.router.add_route('POST', '/content/get', get_bytes)
    app.router.add_route('POST', '/content/get/batch', get_batch)
    app.router.add_route('POST', '/content/get/random', get_random_contents)
    app.router.add_route('POST', '/content/check', check)
    app.router.add_route('POST', '/content/delete', delete)
    app.router.add_route('POST', '/content/add_stream/{hex_id}', add_stream)
    app.router.add_route('GET', '/content/get_stream/{hex_id}', get_stream)
    app.update(config)
    app['objstorage'] = get_objstorage(app['cls'], app['args'])
    return app


def make_app_from_configfile(config_path=DEFAULT_CONFIG_PATH, **kwargs):
    return make_app(config.read(config_path, DEFAULT_CONFIG), **kwargs)


@click.command()
@click.argument('config-path', required=1)
@click.option('--host', default='0.0.0.0', help="Host to run the server")
@click.option('--port', default=5003, type=click.INT,
              help="Binding port of the server")
@click.option('--debug/--nodebug', default=True,
              help="Indicates if the server should run in debug mode")
def launch(config_path, host, port, debug):
    app = make_app_from_configfile(config_path, debug=bool(debug))
    aiohttp.web.run_app(app, host=host, port=int(port))


if __name__ == '__main__':
    launch()
