# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import logging
import time

import click
import aiohttp.web

from swh.objstorage import get_objstorage
from swh.objstorage.api.server import load_and_check_config, make_app

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--config-file', '-C', default=None,
              type=click.Path(exists=True, dir_okay=False,),
              help="Configuration file.")
@click.option('--log-level', '-l', default='INFO',
              type=click.Choice(logging._nameToLevel.keys()),
              help="Log level (default to INFO)")
@click.pass_context
def cli(ctx, config_file, log_level):
    ctx.ensure_object(dict)
    logging.basicConfig(level=log_level)
    cfg = load_and_check_config(config_file)
    ctx.obj['config'] = cfg
    ctx.obj['log_level'] = log_level


@cli.command('serve')
@click.option('--host', default='0.0.0.0', help="Host to run the server")
@click.option('--port', '-p', default=5003, type=click.INT,
              help="Binding port of the server")
@click.pass_context
def serve(ctx, host, port):
    app = make_app(ctx.obj['config'])
    if ctx.obj['log_level'] == 'DEBUG':
        app.update(debug=True)
    aiohttp.web.run_app(app, host=host, port=int(port))


@cli.command('import')
@click.argument('directory', required=True, nargs=-1)
@click.pass_context
def import_directories(ctx, directory):
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
