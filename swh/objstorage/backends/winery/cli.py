# Copyright (C) 2015-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import timedelta
import logging
import re
from types import FrameType
from typing import TYPE_CHECKING, Callable, Dict, List, Tuple

import click

from swh.objstorage.cli import objstorage_cli_group

if TYPE_CHECKING:
    from datetime import datetime

    from .sharedbase import ShardState

# WARNING: do not import unnecessary things here to keep cli startup time under
# control


logger = logging.getLogger(__name__)


def install_signal_handlers(signal_handler: Callable[[int, FrameType | None], None]):
    """Install the signal handler for SIGINT and SIGTERM"""
    import os

    # This is critical for tests not to hang... See, the way ServerTestFixture
    # -- used in rpc/api tests -- is shutting down the flask server on teardown
    # is by calling process.terminate()... So if some other test (e.g. in
    # test_objstorage_winery) call this command BEFORE executing rpc/api tests,
    # then the SIGTERM aiming at the Flask server will be caught by this very
    # 'set_signal_received' hook if we install it here...
    if os.environ.get("PYTEST_VERSION") is not None:
        return

    import signal

    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, signal_handler)


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
@click.option("--stop-instead-of-waiting", is_flag=True)
@click.option(
    "--pool-name",
    default=None,
    help=(
        "Pool name to pack shards for (overriding the config entry "
        "'shards_active_pool'). If set to 'all', do pack for all configured pools"
    ),
)
@click.pass_context
def winery_packer(
    ctx, stop_instead_of_waiting: bool = False, pool_name: str | None = None
):
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

    Note: when using a `cls: directory` type for `shards_pool` configuration,
    it is advisable to set `create_images` to True; the `rdb` management
    process is then unnecessary (when writing directly in shard files, there is
    no need for provisionning the RDB volume).

    """
    import signal

    from swh.objstorage.backends.winery.housekeeping import AbortOperation, shard_packer
    from swh.objstorage.backends.winery.sleep import sleep_exponential

    settings = ctx.obj["winery_settings"]

    signal_stop = False
    signal_abort = False

    def stop_packing(num_shards: int) -> bool:
        """Stop packing when a signal is received or when stop_after_shards is reached"""
        return signal_stop

    def abort_packing(num_shards: int) -> bool:
        """Abort packing when a signal is received."""
        return signal_abort

    def wait_for_shard(attempt: int):
        nonlocal signal_stop
        if stop_instead_of_waiting:
            signal_stop = True
            return

        return sleep_exponential(
            min_duration=1,
            max_duration=60,
            factor=2,
            message="No new image to pack",
        )(attempt)

    def set_signal_received(signum: int, _stack_frame: FrameType | None) -> None:
        nonlocal signal_abort
        nonlocal signal_stop
        if signum == signal.SIGTERM:
            signal_abort = True
        else:
            signal_stop = True
        logger.warning(
            "Received signal %s, %s",
            signal.strsignal(signum),
            signal_stop and "exiting" or "aborting",
        )

    install_signal_handlers(set_signal_received)

    logger.info("Image packer starting")
    if pool_name:
        if pool_name == "all":
            settings["shards_active_pool"] = None
        else:
            settings["shards_active_pool"] = pool_name

    try:
        ret = shard_packer(
            **settings,
            stop_packing=stop_packing,
            abort_packing=abort_packing,
            wait_for_shard=wait_for_shard,
        )

        logger.info("Packed %s shards", ret)
    except AbortOperation:
        logger.warning("Packing aborted, exiting")


@winery.command("rbd")
@click.option("--stop-instead-of-waiting", is_flag=True)
@click.option("--manage-rw-images", is_flag=True)
@click.option("--only-prefix")
@click.option("--active-pool")
@click.pass_context
def winery_rbd(
    ctx,
    stop_instead_of_waiting: bool = False,
    manage_rw_images: bool = True,
    only_prefix: str | None = None,
    active_pool: str | None = None,
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

    from swh.objstorage.backends.winery.pools import pool_from_settings
    from swh.objstorage.backends.winery.roshard import manage_images
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

    def set_signal_received(signum: int, _stack_frame: FrameType | None) -> None:
        nonlocal stop_on_next_iteration
        logger.warning("Received signal %s, exiting", signal.strsignal(signum))
        stop_on_next_iteration = True

    install_signal_handlers(set_signal_received)

    if not active_pool:
        active_pool = settings.get("shards_active_pool")
    if not active_pool:
        raise click.ClickException("No active pool has been defined")
    for pool_cfg in settings["shards_pools"]:
        if pool_cfg["pool_name"] == active_pool:
            pool = pool_from_settings(
                shards_settings=settings["shards"],
                shards_pool_settings=pool_cfg,
            )
            break
    else:
        raise click.ClickException(
            "Active pool not found in the list of configured shards pools"
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

    This process should run continuously as a background process.

    """
    import signal

    from swh.objstorage.backends.winery.housekeeping import rw_shard_cleaner
    from swh.objstorage.backends.winery.sleep import sleep_exponential

    settings = ctx.obj["winery_settings"]

    stop_on_next_iteration = False

    def stop_cleaning(num_shards: int) -> bool:
        """Stop running when requested, or when the max number of shards was reached."""
        return stop_on_next_iteration

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

    def set_signal_received(signum: int, _stack_frame: FrameType | None) -> None:
        nonlocal stop_on_next_iteration
        logger.warning("Received signal %s, exiting", signal.strsignal(signum))
        stop_on_next_iteration = True

    install_signal_handlers(set_signal_received)

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
    from swh.objstorage.backends.winery.pools import pool_from_settings
    from swh.objstorage.backends.winery.sharedbase import SharedBase

    settings = ctx.obj["winery_settings"]

    stop_on_next_iteration = False

    def stop_running() -> bool:
        """Stop running when a signal is received, or when there's nothing to do."""
        return stop_on_next_iteration

    def set_signal_received(signum: int, _stack_frame: FrameType | None) -> None:
        nonlocal stop_on_next_iteration
        logger.warning("Received signal %s, exiting", signal.strsignal(signum))
        stop_on_next_iteration = True

    install_signal_handlers(set_signal_received)

    base = SharedBase(base_dsn=settings["database"]["db"])

    pools = [
        pool_from_settings(
            shards_settings=settings["shards"],
            shards_pool_settings=shards_pool,
        )
        for shards_pool in settings["shards_pools"]
    ]
    for pool in pools:
        if stop_running():
            break
        deleted_objects_cleaner(base, pool, stop_running)


# Stolen from pypi's click_pendulum 0.2.1 package:
# swh:1:rel:51dbe55356f79c1d87662fe77a40ee5c28074e67;
#     origin=https://pypi.org/project/click-pendulum/;
#     visit=swh:1:snp:9ff3b835b09011a44c27baec70e3e3f8cb9e114a
# author: Dawson Reid (@ddaws), MIT License.
# Adapted to straight datetime.
# Note: Could probably migrate to swh-core at some point.
class Duration(click.ParamType):
    """A Duration object.

    The pattern used for matching must include the following named groups:
    - years: Matches the number of years.
    - weeks: Matches the number of weeks.
    - days: Matches the number of days.
    - hours: Matches the number of hours.
    - minutes: Matches the number of minutes.
    - seconds: Matches the number of seconds.

    Each group is optional, but the pattern must be structured to capture these
    groups if present.

    """

    name = "duration"

    DEFAULT_PATTERN = (
        r"(?:(?P<years>\d+)\s*y(?:ears?)?)?\s*"
        r"(?:(?P<weeks>\d+)\s*w(?:eeks?)?)?\s*"
        r"(?:(?P<days>\d+)\s*d(?:ays?)?)?\s*"
        r"(?:(?P<hours>\d+)\s*h(?:ours?)?)?\s*"
        r"(?:(?P<minutes>\d+)\s*m(?:inutes?)?)?\s*"
        r"(?:(?P<seconds>\d+)\s*s(?:econds?)?)?"
    )

    _pattern: re.Pattern

    def __init__(self, pattern: str = DEFAULT_PATTERN):
        self._pattern = re.compile(pattern)

    def convert(self, value: str | None, param, ctx) -> timedelta | None:
        if value is None:
            return value

        try:
            match = self._pattern.match(value)
            if not match or not match.group(0).strip():
                raise ValueError("Invalid duration format: no matches found")

            duration_kwargs = {
                "weeks": int(match.group("weeks") or 0),
                "days": int(match.group("days") or 0),
                "hours": int(match.group("hours") or 0),
                "minutes": int(match.group("minutes") or 0),
                "seconds": int(match.group("seconds") or 0),
            }
            return timedelta(**duration_kwargs)
        except ValueError as ex:
            self.fail(
                f'Could not parse duration string "{value}" ({ex})',
                param,
                ctx,
            )


def shards_by_locker(
    shards: List[Tuple[str, "ShardState", "datetime", str | None]],
) -> List[Tuple[str, List[Tuple[str, "ShardState", "datetime"]]]]:
    by_locker: Dict[str, List[Tuple[str, "ShardState", "datetime"]]] = {}
    for name, state, locker_ts, locker in shards:
        if locker is None:
            locker = "N/A"
        by_locker.setdefault(locker, []).append((name, state, locker_ts))
    return sorted(by_locker.items())


@winery.command("list-open-shards")
@click.option(
    "--state",
    "-s",
    type=click.Choice(["standby", "writing", "full", "packing", "packed", "cleaning"]),
    help="Only list shards in the given state (rather than all non-readonly shards)",
    default=None,
)
@click.option(
    "--long",
    "-l",
    is_flag=True,
    help="Long output (can be slow)",
    default=False,
)
@click.option(
    "--humanize/--no-humanize",
    "humanize_results",
    is_flag=True,
    help="Do / do not humalize results",
    default=True,
)
@click.pass_context
def winery_list_open_shards(ctx, state, long, humanize_results):
    """List open shards"""
    from datetime import UTC, datetime

    from humanize import intcomma, naturaldelta, naturalsize

    from swh.objstorage.backends.winery.rwshard import RWShard
    from swh.objstorage.backends.winery.sharedbase import ShardState, SharedBase

    settings = ctx.obj["winery_settings"]
    base = SharedBase(base_dsn=settings["database"]["db"])
    max_size = settings["shards"]["max_size"]

    shardstate = ShardState(state) if state is not None else None

    shards = list(base.list_open_shards(state=shardstate))
    if shards:
        click.echo("Open shards:")
        for locker, entries in shards_by_locker(shards):
            click.echo(f"{locker}:")
            for name, state, locker_ts in entries:
                since = ""
                if locker_ts is not None:
                    since = f" since {naturaldelta(datetime.now(UTC) - locker_ts)}"
                extra = ""
                if long:
                    try:
                        rwshard = RWShard(
                            name=name,
                            base_dsn=base.dsn,
                            readonly=True,
                            shard_max_size=0,
                        )
                        n = rwshard.obj_count.count
                        size = rwshard.obj_count.volume
                        if size >= max_size:
                            full = "full"
                        else:
                            full = f"{size / max_size * 100.0:.2f}%"
                        if humanize_results:
                            extra = (
                                f", N={intcomma(n)}, size={naturalsize(size)} ({full})"
                            )
                        else:
                            extra = f", N={n}, size={size} ({full})"
                    except Exception:
                        logger.warning(
                            f"Failed to retrieve detailed information on {name}"
                        )
                click.echo(f"  {name}: {state.name}{extra}{since}")
    else:
        if state is not None:
            click.echo(f"No shard in the state '{state}'")
        else:
            click.echo("No open shard")


@winery.command("list-stale-shards")
@click.option(
    "--duration",
    "-d",
    type=Duration(),
    help="How long the shard must have been stuck in its state to be considered as stale",
    default="48h",
)
@click.option(
    "--humanize/--no-humanize",
    "humanize_results",
    is_flag=True,
    help="Do / do not humalize results",
    default=True,
)
@click.pass_context
def winery_list_stale_shards(ctx, duration, humanize_results):
    """List open shards that look stale for some reason"""
    from datetime import UTC, datetime

    from humanize import naturaldelta

    from swh.objstorage.backends.winery.sharedbase import SharedBase

    settings = ctx.obj["winery_settings"]
    base = SharedBase(base_dsn=settings["database"]["db"])

    shards = list(base.list_stale_shards(delay=duration))
    if shards:
        click.echo("Potentially stale shards:")
        for locker, entries in shards_by_locker(shards):
            click.echo(f"{locker}:")

            for name, state, locker_ts in entries:
                since = datetime.now(UTC) - locker_ts
                if humanize_results:
                    click.echo(f"  {name}: {state.name} since {naturaldelta(since)}")
                else:
                    click.echo(f"  {name}: {state.name} since {since}")
    else:
        click.echo("No identified stale shard")


@winery.command("release-stale-shards")
@click.option("--shard", "shard_ids", help="shard name to release", multiple=True)
@click.option(
    "--locker",
    "lockers",
    help="limit shards to release to this locker ID",
    multiple=True,
)
@click.option(
    "--duration",
    "-d",
    help="How long the shard must have been stuck in its state to be considered as stale",
    type=Duration(),
    default="48h",
)
@click.option(
    "--dry-run", help="Do not perform the state change", is_flag=True, default=False
)
@click.pass_context
def winery_release_stale_shards(ctx, shard_ids, lockers, duration, dry_run):
    """Release WRITING shards that look stale"""
    from datetime import UTC, datetime

    from humanize import naturaldelta

    from swh.objstorage.backends.winery.sharedbase import ShardState, SharedBase

    settings = ctx.obj["winery_settings"]
    base = SharedBase(base_dsn=settings["database"]["db"])
    dst_state = {
        ShardState.WRITING: ShardState.STANDBY,
        ShardState.PACKING: ShardState.FULL,
        ShardState.CLEANING: ShardState.PACKED,
    }

    shards = list(base.list_stale_shards(delay=duration))
    if lockers:
        shards = [shard for shard in shards if str(shard[3]) in lockers]
    if shard_ids:
        shards = [shard for shard in shards if shard[0] in shard_ids]

    if shards:
        if dry_run:
            click.echo("Would release (dry run):")
        else:
            click.echo("Releasing:")
        for locker, entries in shards_by_locker(shards):
            click.echo(f"{locker}:")
            for name, state, locker_ts in entries:
                since = naturaldelta(datetime.now(UTC) - locker_ts)
                if state not in dst_state:
                    click.echo(f"  {name} is in unexpected state {state}, ignoring")
                    continue
                dst = dst_state[state]
                if not dry_run:
                    click.echo(
                        f"  {name} stuck in {state.name} for {since} --> {dst.name}"
                    )
                    base.set_shard_state(new_state=dst, name=name)
                else:
                    click.echo(
                        f"  {name} stuck in {state.name} for {since} --> {dst.name}"
                    )

    else:
        click.echo("No identified stale shards to release")


@winery.command("import-shards")
@click.option(
    "--pool-name",
    "-n",
    "poolnames",
    default=None,
    multiple=True,
    help=(
        "Pool name to import images files; if not specified, will look for "
        "image files in all configured pools. Can be specified several times."
    ),
)
@click.pass_context
def winery_import_shards(ctx, poolnames):
    """Populate the winery database from existing shard files"""
    from swh.objstorage.backends.winery.housekeeping import import_ro_shards
    from swh.objstorage.backends.winery.pools import pool_from_settings
    from swh.objstorage.backends.winery.sharedbase import SharedBase

    settings = ctx.obj["winery_settings"]
    pool_cfgs = settings["shards_pools"]
    pool_cfgs = [pool_cfg for pool_cfg in pool_cfgs if pool_cfg["type"] == "directory"]
    if not pool_cfgs:
        raise click.ClickException(
            "No directory shard pools found in this configuration"
        )

    for pool_cfg in pool_cfgs:
        if poolnames and pool_cfg["pool_name"] not in poolnames:
            continue
        base = SharedBase(
            base_dsn=settings["database"]["db"], active_pool_name=pool_cfg["pool_name"]
        )
        pool = pool_from_settings(
            shards_settings=settings["shards"],
            shards_pool_settings=pool_cfg,
        )
        n_obj, n_shard = import_ro_shards(base, pool)
        if n_obj:
            click.echo(
                "Pool %s: imported %s objects from %s shards"
                % (pool.pool_name, n_obj, n_shard)
            )
        else:
            click.echo("Pool %s: nothing to do" % (pool.pool_name,))


@winery.command("prepare-upgrade")
@click.option(
    "--pool-name",
    help=(
        "pool name to set as value for the added 'pool_name' "
        "column in the 'shards' table"
    ),
)
@click.option(
    "--assume-yes",
    is_flag=True,
    default=False,
    help=("Do not ask for confirmation before executing pre-migration steps"),
)
@click.pass_context
def winery_prepare_upgrade(ctx, pool_name, assume_yes):
    """Prepare upgrade the winery DB

    Some DB migration need a preparatory step before being able to be properly
    done. This command will apply this prep step.

    Pre-migration steps:

    3->4: Add the pool_name column to the shards table and fill it with
          configured/given pool name.
    """
    from swh.core.db.db_utils import connect_to_conninfo, swh_db_version

    settings = ctx.obj["winery_settings"]
    if not pool_name:
        pool_name = settings.get("shards_active_pool")
    if not pool_name:
        raise click.ClickException("You must specify the pool name to set shards in")

    conninfo = settings["database"]["db"]
    if swh_db_version(conninfo) == 3:
        click.echo(
            "Migration of the database may be required. It will set "
            f"the pool name for all shards to {pool_name}. "
        )
        if assume_yes or click.confirm("Is it OK?"):
            with connect_to_conninfo(conninfo) as db:
                with db.cursor() as c:
                    query = (
                        "ALTER TABLE shards "
                        "ADD COLUMN IF NOT EXISTS pool_name text NOT NULL "
                        "DEFAULT %s"
                    )
                    c.execute(query, (pool_name,))
                    db.commit()
