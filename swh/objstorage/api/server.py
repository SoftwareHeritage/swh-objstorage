# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

import aiohttp.web

from swh.core.config import read as config_read
from swh.core.api.asynchronous import (SWHRemoteAPI, decode_request,
                                       encode_data_server as encode_data)


from swh.core.api.serializers import msgpack_loads, SWHJSONDecoder

from swh.model import hashutil
from swh.objstorage import get_objstorage
from swh.objstorage.objstorage import DEFAULT_LIMIT
from swh.objstorage.exc import ObjNotFoundError
from swh.core.statsd import statsd


def timed(f):
    async def w(*a, **kw):
        with statsd.timed(
                'swh_objstorage_request_duration_seconds',
                tags={'endpoint': f.__name__}):
            return await f(*a, **kw)
    return w


@timed
async def index(request):
    return aiohttp.web.Response(body="SWH Objstorage API server")


@timed
async def check_config(request):
    req = await decode_request(request)
    return encode_data(request.app['objstorage'].check_config(**req))


@timed
async def contains(request):
    req = await decode_request(request)
    return encode_data(request.app['objstorage'].__contains__(**req))


@timed
async def add_bytes(request):
    req = await decode_request(request)
    statsd.increment('swh_objstorage_in_bytes_total',
                     len(req['content']),
                     tags={'endpoint': 'add_bytes'})
    return encode_data(request.app['objstorage'].add(**req))


@timed
async def add_batch(request):
    req = await decode_request(request)
    return encode_data(request.app['objstorage'].add_batch(**req))


@timed
async def get_bytes(request):
    req = await decode_request(request)
    try:
        ret = request.app['objstorage'].get(**req)
    except ObjNotFoundError:
        ret = {
            'error': 'object_not_found',
            'request': req,
        }
        return encode_data(ret, status=404)
    else:
        statsd.increment('swh_objstorage_out_bytes_total',
                         len(ret),
                         tags={'endpoint': 'get_bytes'})
        return encode_data(ret)


@timed
async def get_batch(request):
    req = await decode_request(request)
    return encode_data(request.app['objstorage'].get_batch(**req))


@timed
async def check(request):
    req = await decode_request(request)
    return encode_data(request.app['objstorage'].check(**req))


@timed
async def delete(request):
    req = await decode_request(request)
    return encode_data(request.app['objstorage'].delete(**req))


# Management methods

@timed
async def get_random_contents(request):
    req = await decode_request(request)
    return encode_data(request.app['objstorage'].get_random(**req))


# Streaming methods

@timed
async def add_stream(request):
    hex_id = request.match_info['hex_id']
    obj_id = hashutil.hash_to_bytes(hex_id)
    check_pres = (request.query.get('check_presence', '').lower() == 'true')
    objstorage = request.app['objstorage']

    if check_pres and obj_id in objstorage:
        return encode_data(obj_id)

    # XXX this really should go in a decode_stream_request coroutine in
    # swh.core, but since py35 does not support async generators, it cannot
    # easily be made for now
    content_type = request.headers.get('Content-Type')
    if content_type == 'application/x-msgpack':
        decode = msgpack_loads
    elif content_type == 'application/json':
        decode = lambda x: json.loads(x, cls=SWHJSONDecoder)  # noqa
    else:
        raise ValueError('Wrong content type `%s` for API request'
                         % content_type)

    buffer = b''
    with objstorage.chunk_writer(obj_id) as write:
        while not request.content.at_eof():
            data, eot = await request.content.readchunk()
            buffer += data
            if eot:
                write(decode(buffer))
                buffer = b''

    return encode_data(obj_id)


@timed
async def get_stream(request):
    hex_id = request.match_info['hex_id']
    obj_id = hashutil.hash_to_bytes(hex_id)
    response = aiohttp.web.StreamResponse()
    await response.prepare(request)
    for chunk in request.app['objstorage'].get_stream(obj_id, 2 << 20):
        await response.write(chunk)
    await response.write_eof()
    return response


@timed
async def list_content(request):
    last_obj_id = request.query.get('last_obj_id')
    if last_obj_id:
        last_obj_id = bytes.fromhex(last_obj_id)
    limit = int(request.query.get('limit', DEFAULT_LIMIT))
    response = aiohttp.web.StreamResponse()
    response.enable_chunked_encoding()
    await response.prepare(request)
    for obj_id in request.app['objstorage'].list_content(
            last_obj_id, limit=limit):
        await response.write(obj_id)
    await response.write_eof()
    return response


def make_app(config):
    """Initialize the remote api application.

    """
    client_max_size = config.get('client_max_size', 1024 * 1024 * 1024)
    app = SWHRemoteAPI(client_max_size=client_max_size)
    # retro compatibility configuration settings
    app['config'] = config
    _cfg = config['objstorage']
    app['objstorage'] = get_objstorage(_cfg['cls'], _cfg['args'])

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
    app.router.add_route('GET', '/content', list_content)
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
