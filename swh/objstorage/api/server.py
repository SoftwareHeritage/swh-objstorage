# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import os

import aiohttp.web

from swh.core.api.asynchronous import RPCServerApp, decode_request
from swh.core.api.asynchronous import encode_data_server as encode_data
from swh.core.api.serializers import SWHJSONDecoder, msgpack_loads
from swh.core.config import read as config_read
from swh.core.statsd import statsd
from swh.model import hashutil
from swh.objstorage.exc import Error, ObjNotFoundError
from swh.objstorage.factory import get_objstorage
from swh.objstorage.objstorage import DEFAULT_LIMIT


def timed(f):
    async def w(*a, **kw):
        with statsd.timed(
            "swh_objstorage_request_duration_seconds", tags={"endpoint": f.__name__}
        ):
            return await f(*a, **kw)

    return w


@timed
async def index(request):
    return aiohttp.web.Response(body="SWH Objstorage API server")


@timed
async def check_config(request):
    req = await decode_request(request)
    return encode_data(request.app["objstorage"].check_config(**req))


@timed
async def contains(request):
    req = await decode_request(request)
    return encode_data(request.app["objstorage"].__contains__(**req))


@timed
async def add_bytes(request):
    req = await decode_request(request)
    statsd.increment(
        "swh_objstorage_in_bytes_total",
        len(req["content"]),
        tags={"endpoint": "add_bytes"},
    )
    return encode_data(request.app["objstorage"].add(**req))


@timed
async def add_batch(request):
    req = await decode_request(request)
    return encode_data(request.app["objstorage"].add_batch(**req))


@timed
async def get_bytes(request):
    req = await decode_request(request)

    ret = request.app["objstorage"].get(**req)

    statsd.increment(
        "swh_objstorage_out_bytes_total", len(ret), tags={"endpoint": "get_bytes"}
    )
    return encode_data(ret)


@timed
async def get_batch(request):
    req = await decode_request(request)
    return encode_data(request.app["objstorage"].get_batch(**req))


@timed
async def check(request):
    req = await decode_request(request)
    return encode_data(request.app["objstorage"].check(**req))


@timed
async def delete(request):
    req = await decode_request(request)
    return encode_data(request.app["objstorage"].delete(**req))


# Management methods


@timed
async def get_random_contents(request):
    req = await decode_request(request)
    return encode_data(request.app["objstorage"].get_random(**req))


# Streaming methods


@timed
async def add_stream(request):
    hex_id = request.match_info["hex_id"]
    obj_id = hashutil.hash_to_bytes(hex_id)
    check_pres = request.query.get("check_presence", "").lower() == "true"
    objstorage = request.app["objstorage"]

    if check_pres and obj_id in objstorage:
        return encode_data(obj_id)

    # XXX this really should go in a decode_stream_request coroutine in
    # swh.core, but since py35 does not support async generators, it cannot
    # easily be made for now
    content_type = request.headers.get("Content-Type")
    if content_type == "application/x-msgpack":
        decode = msgpack_loads
    elif content_type == "application/json":
        decode = lambda x: json.loads(x, cls=SWHJSONDecoder)  # noqa
    else:
        raise ValueError("Wrong content type `%s` for API request" % content_type)

    buffer = b""
    with objstorage.chunk_writer(obj_id) as write:
        while not request.content.at_eof():
            data, eot = await request.content.readchunk()
            buffer += data
            if eot:
                write(decode(buffer))
                buffer = b""

    return encode_data(obj_id)


@timed
async def get_stream(request):
    hex_id = request.match_info["hex_id"]
    obj_id = hashutil.hash_to_bytes(hex_id)
    response = aiohttp.web.StreamResponse()
    await response.prepare(request)
    for chunk in request.app["objstorage"].get_stream(obj_id, 2 << 20):
        await response.write(chunk)
    await response.write_eof()
    return response


@timed
async def list_content(request):
    last_obj_id = request.query.get("last_obj_id")
    if last_obj_id:
        last_obj_id = bytes.fromhex(last_obj_id)
    limit = int(request.query.get("limit", DEFAULT_LIMIT))
    response = aiohttp.web.StreamResponse()
    response.enable_chunked_encoding()
    await response.prepare(request)
    for obj_id in request.app["objstorage"].list_content(last_obj_id, limit=limit):
        await response.write(obj_id)
    await response.write_eof()
    return response


def make_app(config):
    """Initialize the remote api application.

    """
    client_max_size = config.get("client_max_size", 1024 * 1024 * 1024)
    app = RPCServerApp(client_max_size=client_max_size)
    app.client_exception_classes = (ObjNotFoundError, Error)

    # retro compatibility configuration settings
    app["config"] = config
    app["objstorage"] = get_objstorage(**config["objstorage"])

    app.router.add_route("GET", "/", index)
    app.router.add_route("POST", "/check_config", check_config)
    app.router.add_route("POST", "/content/contains", contains)
    app.router.add_route("POST", "/content/add", add_bytes)
    app.router.add_route("POST", "/content/add/batch", add_batch)
    app.router.add_route("POST", "/content/get", get_bytes)
    app.router.add_route("POST", "/content/get/batch", get_batch)
    app.router.add_route("POST", "/content/get/random", get_random_contents)
    app.router.add_route("POST", "/content/check", check)
    app.router.add_route("POST", "/content/delete", delete)
    app.router.add_route("GET", "/content", list_content)
    app.router.add_route("POST", "/content/add_stream/{hex_id}", add_stream)
    app.router.add_route("GET", "/content/get_stream/{hex_id}", get_stream)
    return app


def load_and_check_config(config_file):
    """Check the minimal configuration is set to run the api or raise an
       error explanation.

    Args:
        config_file (str): Path to the configuration file to load

    Raises:
        Error if the setup is not as expected

    Returns:
        configuration as a dict

    """
    if not config_file:
        raise EnvironmentError("Configuration file must be defined")

    if not os.path.exists(config_file):
        raise FileNotFoundError("Configuration file %s does not exist" % (config_file,))

    cfg = config_read(config_file)
    return validate_config(cfg)


def validate_config(cfg):
    """Check the minimal configuration is set to run the api or raise an
       explanatory error.

    Args:
        cfg (dict): Loaded configuration.

    Raises:
        Error if the setup is not as expected

    Returns:
        configuration as a dict

    """
    if "objstorage" not in cfg:
        raise KeyError("Invalid configuration; missing objstorage config entry")

    missing_keys = []
    vcfg = cfg["objstorage"]
    if "cls" not in vcfg:
        raise KeyError("Invalid configuration; missing cls config entry")

    cls = vcfg["cls"]
    if cls == "pathslicing":
        # Backwards-compatibility: either get the deprecated `args` from the
        # objstorage config, or use the full config itself to check for keys
        args = vcfg.get("args", vcfg)
        for key in ("root", "slicing"):
            v = args.get(key)
            if v is None:
                missing_keys.append(key)

        if missing_keys:
            raise KeyError(
                "Invalid configuration; missing %s config entry"
                % (", ".join(missing_keys),)
            )

    return cfg


def make_app_from_configfile():
    """Load configuration and then build application to run

    """
    config_file = os.environ.get("SWH_CONFIG_FILENAME")
    config = load_and_check_config(config_file)
    return make_app(config=config)


if __name__ == "__main__":
    print("Deprecated. Use swh-objstorage")
