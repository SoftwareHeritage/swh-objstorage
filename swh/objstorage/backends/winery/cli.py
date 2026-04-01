# Copyright (C) 2015-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from types import FrameType
from typing import Optional

import click

from swh.objstorage.cli import objstorage_cli_group

# WARNING: do not import unnecessary things here to keep cli startup time under
# control


logger = logging.getLogger(__name__)


@objstorage_cli_group.group("winery")
@click.pass_context
def winery(ctx):
    "Winery related commands"
    config = ctx.obj["config"]["objstorage"]
    if config["cls"] != "winery":
        raise click.ClickException("winery packer only works on a winery objstorage")

    from swh.objstorage.backends.winery.settings import (
        SETTINGS,
        populate_default_settings,
    )

    ctx.obj["winery_settings"] = populate_default_settings(
        **{k: v for k, v in config.items() if k in SETTINGS}
    )


@winery.command("packer")
@click.option("--stop-after-shards", type=click.INT, default=None)
@click.pass_context
def winery_packer(ctx, stop_after_shards: Optional[int] = None):
    """Run the winery packer process

    This process is in charge of creating (packing) shard files when a winery
    writer has accumulated enough file objects to reach the shard's `max_size`
    size.

    When a shard becomes full, it gets locked by this packer service. The shard
    creation can then occur either as part of the packing step (within this
    process) when `create_images` configuration option is set, or waited for
    (in this case, the shard creation processing is delegated to the shard
    managenent tool, aka `swh objstorage winery rdb`).

    When the shard file is ready, the shard gets packed.

    If `clean_immediately` is set, the write shard is immediately removed and
    the shard moved to the `readonly` state.

    Note: when using a `cls: directory` type for `shards_pool` configuration,
    it is advisable to set `create_images` to True; the `rdb` management
    process is then unnecessary (when writing directly in shard files, there is
    no need for provisionning the RDB volume).

    """
    import signal

    from swh.objstorage.backends.winery.housekeeping import shard_packer

    settings = ctx.obj["winery_settings"]

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

    signal.signal(signal.SIGINT, set_signal_received)
    signal.signal(signal.SIGTERM, set_signal_received)

    ret = shard_packer(**settings, stop_packing=stop_packing)

    logger.info("Packed %s shards", ret)


@winery.command("rbd")
@click.option("--stop-instead-of-waiting", is_flag=True)
@click.option("--manage-rw-images", is_flag=True)
@click.option("--only-prefix")
@click.pass_context
def winery_rbd(
    ctx,
    stop_instead_of_waiting: bool = False,
    manage_rw_images: bool = True,
    only_prefix: Optional[str] = None,
):
    """Run a winery RBD image manager process

    This process is in charge of creating and mapping image files for shards.
    This is required for `shards_pool` of type `cls: rbd`. It will:

      - Map all `readonly` shards (if need be).

      - If `manage_rw_images` is true, provision a new RBD image in the Ceph
        cluster each time a shard appears in the `standby` or `writing` state.

      - When a shard packing completes (shrd status becomes one of `packed`,
        `cleaning`, `readonly`), the image is mapped read-only.

      - Record mapping event in the database.


    """
    import signal

    from swh.objstorage.backends.winery.roshard import manage_images, pool_from_settings
    from swh.objstorage.backends.winery.sleep import sleep_exponential

    settings = ctx.obj["winery_settings"]

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

    signal.signal(signal.SIGINT, set_signal_received)
    signal.signal(signal.SIGTERM, set_signal_received)

    pool = pool_from_settings(
        shards_settings=settings["shards"],
        shards_pool_settings=settings["shards_pool"],
    )

    manage_images(
        pool=pool,
        base_dsn=settings["database"]["db"],
        manage_rw_images=manage_rw_images,
        wait_for_image=wait_for_image,
        only_prefix=only_prefix,
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
    """Run the winery database image manager process

    This process is responsible for cleaning winery DB tables for shards that
    have been packed.

    It performs clean up of the `packed` read-write shards, as soon as they are
    recorded as mapped on enough (`--min-mapped-hosts`) hosts (when using a rbd
    shards pool). They get locked in the `cleaning` state, the database cleanup
    is performed, then the shard gets moved in the final `readonly` state.

    This process should run continuously as a background process if the winery
    setup is configured with `clean_immediately=false`.

    """
    import signal

    from swh.objstorage.backends.winery.housekeeping import rw_shard_cleaner
    from swh.objstorage.backends.winery.sleep import sleep_exponential

    settings = ctx.obj["winery_settings"]

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

    signal.signal(signal.SIGINT, set_signal_received)
    signal.signal(signal.SIGTERM, set_signal_received)

    ret = rw_shard_cleaner(
        database=settings["database"],
        min_mapped_hosts=min_mapped_hosts,
        stop_cleaning=stop_cleaning,
        wait_for_shard=wait_for_shard,
    )

    logger.info("RW shard cleaner exiting, %d shards cleaned", ret)


@winery.command("clean-deleted-objects")
@click.pass_context
def winery_clean_deleted_objects(ctx):
    """Clean deleted objects from Winery"""
    import signal

    from swh.objstorage.backends.winery.housekeeping import deleted_objects_cleaner
    from swh.objstorage.backends.winery.roshard import pool_from_settings
    from swh.objstorage.backends.winery.sharedbase import SharedBase

    settings = ctx.obj["winery_settings"]

    stop_on_next_iteration = False

    def stop_running() -> bool:
        """Stop running when a signal is received, or when there's nothing to do."""
        return stop_on_next_iteration

    def set_signal_received(signum: int, _stack_frame: Optional[FrameType]) -> None:
        nonlocal stop_on_next_iteration
        logger.warning("Received signal %s, exiting", signal.strsignal(signum))
        stop_on_next_iteration = True

    signal.signal(signal.SIGINT, set_signal_received)
    signal.signal(signal.SIGTERM, set_signal_received)

    base = SharedBase(base_dsn=settings["database"]["db"])

    pool = pool_from_settings(
        shards_settings=settings["shards"],
        shards_pool_settings=settings["shards_pool"],
    )

    deleted_objects_cleaner(base, pool, stop_running)
