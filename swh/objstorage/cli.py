# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import logging
import time

import click
import aiohttp.web

from swh.core.cli import CONTEXT_SETTINGS

from swh.objstorage import get_objstorage
from swh.objstorage.api.server import load_and_check_config, make_app


@click.group(name='objstorage', context_settings=CONTEXT_SETTINGS)
@click.option('--config-file', '-C', default=None,
              type=click.Path(exists=True, dir_okay=False,),
              help="Configuration file.")
@click.pass_context
def cli(ctx, config_file):
    '''Software Heritage Objstorage tools.
    '''
    ctx.ensure_object(dict)
    cfg = load_and_check_config(config_file)
    ctx.obj['config'] = cfg


@cli.command('rpc-serve')
@click.option('--host', default='0.0.0.0',
              metavar='IP', show_default=True,
              help="Host ip address to bind the server on")
@click.option('--port', '-p', default=5003, type=click.INT,
              metavar='PORT', show_default=True,
              help="Binding port of the server")
@click.pass_context
def serve(ctx, host, port):
    '''Run a standalone objstorage server.

    This is not meant to be run on production systems.
    '''
    app = make_app(ctx.obj['config'])
    if ctx.obj['log_level'] == 'DEBUG':
        app.update(debug=True)
    aiohttp.web.run_app(app, host=host, port=int(port))


@cli.command('import')
@click.argument('directory', required=True, nargs=-1)
@click.pass_context
def import_directories(ctx, directory):
    '''Import a local directory in an existing objstorage.
    '''
    objstorage = get_objstorage(**ctx.obj['config']['objstorage'])
    nobj = 0
    volume = 0
    t0 = time.time()
    for dirname in directory:
        for root, _dirs, files in os.walk(dirname):
            for name in files:
                path = os.path.join(root, name)
                with open(path, 'rb') as f:
                    objstorage.add(f.read())
                    volume += os.stat(path).st_size
                    nobj += 1
    click.echo('Imported %d files for a volume of %s bytes in %d seconds' %
               (nobj, volume, time.time()-t0))


@cli.command('fsck')
@click.pass_context
def fsck(ctx):
    '''Check the objstorage is not corrupted.
    '''
    objstorage = get_objstorage(**ctx.obj['config']['objstorage'])
    for obj_id in objstorage:
        try:
            objstorage.check(obj_id)
        except objstorage.Error as err:
            logging.error(err)


def main():
    return cli(auto_envvar_prefix='SWH_OBJSTORAGE')


if __name__ == '__main__':
    main()
