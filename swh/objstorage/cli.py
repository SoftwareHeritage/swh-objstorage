# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import aiohttp.web

from swh.objstorage.api.server import load_and_check_config, make_app


@click.command()
@click.argument('config-path', required=1)
@click.option('--host', default='0.0.0.0', help="Host to run the server")
@click.option('--port', default=5007, type=click.INT,
              help="Binding port of the server")
@click.option('--debug/--nodebug', default=True,
              help="Indicates if the server should run in debug mode")
def main(config_path, host, port, debug):
    cfg = load_and_check_config(config_path)
    app = make_app(cfg)
    app.update(debug=bool(debug))
    aiohttp.web.run_app(app, host=host, port=int(port))


if __name__ == '__main__':
    main()
