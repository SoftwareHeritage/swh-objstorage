# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import os
import time
from types import FrameType
from typing import Optional

import click

from swh.core.cli import CONTEXT_SETTINGS
from swh.core.cli import swh as swh_cli_group

logger = logging.getLogger(__name__)


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
            raise click.ClickException(
                "Configuration file %s does not exist" % config_file
            )
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


@objstorage_cli_group.group("winery")
@click.pass_context
def winery(ctx):
    "Winery related commands"
    config = ctx.obj["config"]["objstorage"]
    if config["cls"] != "winery":
        raise click.ClickException("winery packer only works on a winery objstorage")


@winery.command("packer")
@click.option("--stop-after-shards", type=click.INT, default=None)
@click.pass_context
def winery_packer(ctx, stop_after_shards: Optional[int] = None):
    """Run a winery packer process"""
    import signal

    from swh.objstorage.backends.winery.objstorage import shard_packer
    from swh.objstorage.backends.winery.roshard import (
        DEFAULT_IMAGE_FEATURES_UNSUPPORTED,
    )

    config = ctx.obj["config"]["objstorage"]

    signal_received = False

    def stop_packing(num_shards: int) -> bool:
        """Stop packing when a signal is received, or when stop_after_shards is reached."""
        return signal_received or (
            stop_after_shards is not None and num_shards >= stop_after_shards
        )

    def set_signal_received(signum: int, _stack_frame: Optional[FrameType]) -> None:
        nonlocal signal_received
        logger.warning("Received signal %s, exiting", signal.strsignal(signum))
        signal_received = True

    base_dsn = config["base_dsn"]
    shard_dsn = config["shard_dsn"]
    shard_max_size = config["shard_max_size"]
    throttle_read = config.get("throttle_read", 200 * 1024 * 1024)
    throttle_write = config.get("throttle_write", 200 * 1024 * 1024)
    output_dir = config.get("output_dir")
    rbd_pool_name = config.get("rbd_pool_name", "shards")
    rbd_data_pool_name = config.get("rbd_data_pool_name")
    rbd_use_sudo = config.get("rbd_use_sudo", True)
    rbd_image_features_unsupported = tuple(
        config.get("rbd_image_features_unsupported", DEFAULT_IMAGE_FEATURES_UNSUPPORTED)
    )

    signal.signal(signal.SIGINT, set_signal_received)
    signal.signal(signal.SIGTERM, set_signal_received)

    ret = shard_packer(
        base_dsn=base_dsn,
        shard_dsn=shard_dsn,
        shard_max_size=shard_max_size,
        throttle_read=throttle_read,
        throttle_write=throttle_write,
        rbd_pool_name=rbd_pool_name,
        rbd_data_pool_name=rbd_data_pool_name,
        rbd_image_features_unsupported=rbd_image_features_unsupported,
        rbd_use_sudo=rbd_use_sudo,
        output_dir=output_dir,
        stop_packing=stop_packing,
    )

    logger.info("Packed %s shards", ret)


@winery.command("rbd")
@click.option("--stop-instead-of-waiting", is_flag=True)
@click.pass_context
def winery_rbd(ctx, stop_instead_of_waiting: bool = False):
    """Run a winery RBD image manager process"""
    import signal

    from swh.objstorage.backends.winery.roshard import (
        DEFAULT_IMAGE_FEATURES_UNSUPPORTED,
        Pool,
    )
    from swh.objstorage.backends.winery.sleep import sleep_exponential

    config = ctx.obj["config"]["objstorage"]

    stop_on_next_iteration = False

    def stop_running() -> bool:
        """Stop running when a signal is received, or when there's nothing to do."""
        return stop_on_next_iteration

    def wait_for_image(attempt: int):
        nonlocal stop_on_next_iteration
        if stop_instead_of_waiting:
            stop_on_next_iteration = True
            return

        return sleep_exponential(
            min_duration=1,
            max_duration=60,
            factor=2,
            message="No new RBD images",
        )(attempt)

    def set_signal_received(signum: int, _stack_frame: Optional[FrameType]) -> None:
        nonlocal stop_on_next_iteration
        logger.warning("Received signal %s, exiting", signal.strsignal(signum))
        stop_on_next_iteration = True

    base_dsn = config["base_dsn"]
    shard_max_size = config["shard_max_size"]
    rbd_pool_name = config.get("rbd_pool_name", "shards")
    rbd_data_pool_name = config.get("rbd_data_pool_name")
    rbd_use_sudo = config.get("rbd_use_sudo", True)
    rbd_image_features_unsupported = tuple(
        config.get("rbd_image_features_unsupported", DEFAULT_IMAGE_FEATURES_UNSUPPORTED)
    )
    rbd_manage_rw_images = config.get("rbd_manage_rw_images", True)

    signal.signal(signal.SIGINT, set_signal_received)
    signal.signal(signal.SIGTERM, set_signal_received)

    pool = Pool(
        shard_max_size=shard_max_size,
        rbd_pool_name=rbd_pool_name,
        rbd_data_pool_name=rbd_data_pool_name,
        rbd_use_sudo=rbd_use_sudo,
        rbd_image_features_unsupported=rbd_image_features_unsupported,
    )

    pool.manage_images(
        base_dsn=base_dsn,
        manage_rw_images=rbd_manage_rw_images,
        wait_for_image=wait_for_image,
        stop_running=stop_running,
    )

    logger.info("Image manager exiting")


@winery.command("rw-shard-cleaner")
@click.option("--stop-after-shards", type=click.INT, default=None)
@click.option("--stop-instead-of-waiting", is_flag=True)
@click.option(
    "--min-mapped-hosts",
    type=click.INT,
    default=1,
    help="Number of hosts on which the image should be mapped read-only before cleanup",
)
@click.pass_context
def winery_rw_shard_cleaner(
    ctx,
    stop_after_shards: Optional[int] = None,
    stop_instead_of_waiting: bool = False,
    min_mapped_hosts: int = 1,
):
    """Run a winery RBD image manager process"""
    import signal

    from swh.objstorage.backends.winery.objstorage import rw_shard_cleaner
    from swh.objstorage.backends.winery.sleep import sleep_exponential

    config = ctx.obj["config"]["objstorage"]

    stop_on_next_iteration = False

    def stop_cleaning(num_shards: int) -> bool:
        """Stop running when requested, or when the max number of shards was reached."""
        return (
            stop_after_shards is not None and num_shards >= stop_after_shards
        ) or stop_on_next_iteration

    def wait_for_shard(attempt: int):
        nonlocal stop_on_next_iteration
        if stop_instead_of_waiting:
            stop_on_next_iteration = True
            return

        return sleep_exponential(
            min_duration=1,
            max_duration=60,
            factor=2,
            message="No shards to clean up",
        )(attempt)

    def set_signal_received(signum: int, _stack_frame: Optional[FrameType]) -> None:
        nonlocal stop_on_next_iteration
        logger.warning("Received signal %s, exiting", signal.strsignal(signum))
        stop_on_next_iteration = True

    base_dsn = config["base_dsn"]
    shard_dsn = config["base_dsn"]

    signal.signal(signal.SIGINT, set_signal_received)
    signal.signal(signal.SIGTERM, set_signal_received)

    ret = rw_shard_cleaner(
        base_dsn=base_dsn,
        shard_dsn=shard_dsn,
        min_mapped_hosts=min_mapped_hosts,
        stop_cleaning=stop_cleaning,
        wait_for_shard=wait_for_shard,
    )

    logger.info("RW shard cleaner exiting, %d shards cleaned", ret)


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
