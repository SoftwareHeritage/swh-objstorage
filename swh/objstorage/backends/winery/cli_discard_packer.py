# Copyright (C) 2015-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Module meant to be called as a `python -m …` command, out of the standard CLI path
to keep safe(r) from mistakes."""

import logging
from pathlib import Path

import click

from swh.core.cli import setup_config
from swh.core.logging import logging_configure
from swh.objstorage.backends.winery.housekeeping import discard_packer
from swh.objstorage.backends.winery.settings import SETTINGS, populate_default_settings

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--yes-i-am-certain",
    help=(
        "This flag must be passed to confirm that you really want to discard shards "
        "as they become ready"
    ),
    is_flag=True,
)
@click.option(
    "--pool-name",
    "-n",
    default=None,
    help=(
        "Pool name to discard shards for (overriding the config entry "
        "'shards_active_pool'). If set to 'all', do pack for all configured pools"
    ),
)
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
@click.option(
    "--log-config",
    default=None,
    type=click.Path(exists=True, readable=True),
    envvar="SWH_LOG_CONFIG",
    help="Python yaml logging configuration file.",
)
@click.pass_context
def winery_discard_packer(
    ctx,
    yes_i_am_certain: bool = False,
    pool_name: str | None = None,
    config_file: Path | None = None,
    log_config: Path | None = None,
):
    """Run the winery discard-packer process

    This process is meant for testing only, and will discard RW shards as they become
    full, doing the job of both the packer and the cleaner. This is useful when you do
    not want to instantiate a shard storage backend, but expect to write large amounts
    of data to Winery.
    """
    setup_config(ctx, config_file)
    logging_configure(log_config=log_config)
    config = ctx.obj["config"]["objstorage"]
    if config["cls"] != "winery":
        raise click.ClickException("winery packer only works on a winery objstorage")
    ctx.obj["winery_settings"] = populate_default_settings(
        **{k: v for k, v in config.items() if k in SETTINGS}
    )

    settings = ctx.obj["winery_settings"]

    if not yes_i_am_certain:
        raise click.UsageError(
            "If you really want to run a discard packer, which will DISCARD shards' "
            "data as they become ready for packing, please pass the --yes-i-am-certain "
            "CLI flag."
        )

    logger.info("Image discard packer starting")
    if not pool_name or pool_name == "all":
        settings["shards_active_pool"] = None
    else:
        settings["shards_active_pool"] = pool_name

    ret = discard_packer(**settings)
    logger.info("Discarded %s shards", ret)


if __name__ == "__main__":
    winery_discard_packer()
