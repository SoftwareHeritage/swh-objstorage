# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import logging

from flask import Flask, g, request

from swh.core import config
from swh.objstorage import get_objstorage

from swh.objstorage.api.common import (BytesRequest, decode_request,
                                       error_handler,
                                       encode_data_server as encode_data)

DEFAULT_CONFIG = {
    'cls': ('str', 'pathslicing'),
    'args': ('dict', {
        'root': '/srv/softwareheritage/objects',
        'slicing': '0:2/2:4/4:6',
    })
}

app = Flask(__name__)
app.request_class = BytesRequest


@app.errorhandler(Exception)
def my_error_handler(exception):
    return error_handler(exception, encode_data)


@app.before_request
def before_request():
    g.objstorage = get_objstorage(app.config['cls'], app.config['args'])


@app.route('/')
def index():
    return "SWH Objstorage API server"


@app.route('/content')
def content():
    return str(list(g.storage))


@app.route('/content/contains', methods=['POST'])
def contains():
    return encode_data(g.objstorage.__contains__(**decode_request(request)))


@app.route('/content/add', methods=['POST'])
def add_bytes():
    return encode_data(g.objstorage.add(**decode_request(request)))


@app.route('/content/get', methods=['POST'])
def get_bytes():
    return encode_data(g.objstorage.get(**decode_request(request)))


@app.route('/content/get/batch', methods=['POST'])
def get_batch():
    return encode_data(g.objstorage.get_batch(**decode_request(request)))


@app.route('/content/get/random', methods=['POST'])
def get_random_contents():
    return encode_data(
        g.objstorage.get_random(**decode_request(request))
    )


@app.route('/content/check', methods=['POST'])
def check():
    return encode_data(g.objstorage.check(**decode_request(request)))


def run_from_webserver(environ, start_response):
    """Run the WSGI app from the webserver, loading the configuration.

    """
    config_path = '/etc/softwareheritage/storage/objstorage.ini'

    app.config.update(config.read(config_path, DEFAULT_CONFIG))

    handler = logging.StreamHandler()
    app.logger.addHandler(handler)

    return app(environ, start_response)


@click.command()
@click.argument('config-path', required=1)
@click.option('--host', default='0.0.0.0', help="Host to run the server")
@click.option('--port', default=5000, type=click.INT,
              help="Binding port of the server")
@click.option('--debug/--nodebug', default=True,
              help="Indicates if the server should run in debug mode")
def launch(config_path, host, port, debug):
    app.config.update(config.read(config_path, DEFAULT_CONFIG))
    app.run(host, port=int(port), debug=bool(debug))


if __name__ == '__main__':
    launch()