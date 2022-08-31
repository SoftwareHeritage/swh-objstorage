# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import os
import time

import click

from swh.core.cli import CONTEXT_SETTINGS
from swh.core.cli import swh as swh_cli_group


@swh_cli_group.group(name="objstorage", context_settings=CONTEXT_SETTINGS)
@click.option(
    "--config-file",
    "-C",
    default=None,
    type=click.Path(
        exists=True,
        dir_okay=False,
    ),
    help="Configuration file.",
)
@click.pass_context
def objstorage_cli_group(ctx, config_file):
    """Software Heritage Objstorage tools."""
    from swh.core import config

    if not config_file:
        config_file = os.environ.get("SWH_CONFIG_FILENAME")

    if config_file:
        if not os.path.exists(config_file):
            raise ValueError("%s does not exist" % config_file)
        conf = config.read(config_file)
    else:
        conf = {}

    ctx.ensure_object(dict)

    ctx.obj["config"] = conf


# for BW compat
cli = objstorage_cli_group


@objstorage_cli_group.command("rpc-serve")
@click.option(
    "--host",
    default="0.0.0.0",
    metavar="IP",
    show_default=True,
    help="Host ip address to bind the server on",
)
@click.option(
    "--port",
    "-p",
    default=5003,
    type=click.INT,
    metavar="PORT",
    show_default=True,
    help="Binding port of the server",
)
@click.option(
    "--debug/--no-debug",
    default=True,
    help="Indicates if the server should run in debug mode",
)
@click.pass_context
def serve(ctx, host, port, debug):
    """Run a standalone objstorage server.

    This is not meant to be run on production systems.
    """
    from swh.objstorage.api.server import app, validate_config

    if "log_level" in ctx.obj:
        logging.getLogger("werkzeug").setLevel(ctx.obj["log_level"])
    validate_config(ctx.obj["config"])
    app.config.update(ctx.obj["config"])
    app.run(host, port=int(port), debug=bool(debug))


@objstorage_cli_group.command("import")
@click.argument("directory", required=True, nargs=-1)
@click.pass_context
def import_directories(ctx, directory):
    """Import a local directory in an existing objstorage."""
    from swh.objstorage.factory import get_objstorage
    from swh.objstorage.objstorage import compute_hash

    objstorage = get_objstorage(**ctx.obj["config"]["objstorage"])
    nobj = 0
    volume = 0
    t0 = time.time()
    for dirname in directory:
        for root, _dirs, files in os.walk(dirname):
            for name in files:
                path = os.path.join(root, name)
                with open(path, "rb") as f:
                    content = f.read()
                objstorage.add(content, obj_id=compute_hash(content))
                volume += os.stat(path).st_size
                nobj += 1
    click.echo(
        "Imported %d files for a volume of %s bytes in %d seconds"
        % (nobj, volume, time.time() - t0)
    )


@objstorage_cli_group.command("fsck")
@click.pass_context
def fsck(ctx):
    """Check the objstorage is not corrupted."""
    from swh.objstorage.factory import get_objstorage

    objstorage = get_objstorage(**ctx.obj["config"]["objstorage"])
    for obj_id in objstorage:
        try:
            objstorage.check(obj_id)
        except objstorage.Error as err:
            logging.error(err)


def main():
    return cli(auto_envvar_prefix="SWH_OBJSTORAGE")


if __name__ == "__main__":
    main()
