# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy

import pytest
import yaml

from swh.objstorage.api.server import load_and_check_config


def prepare_config_file(tmpdir, content, name="config.yml"):
    """Prepare configuration file in `$tmpdir/name` with content `content`.

    Args:
        tmpdir (LocalPath): root directory
        content (str/dict): Content of the file either as string or as a dict.
                            If a dict, converts the dict into a yaml string.
        name (str): configuration filename

    Returns
        path (str) of the configuration file prepared.

    """
    config_path = tmpdir / name
    if isinstance(content, dict):  # convert if needed
        content = yaml.dump(content)
    config_path.write_text(content, encoding="utf-8")
    # pytest on python3.5 does not support LocalPath manipulation, so
    # convert path to string
    return str(config_path)


def test_load_and_check_config_no_configuration():
    """Inexistent configuration files raises"""
    with pytest.raises(EnvironmentError, match="Configuration file must be defined"):
        load_and_check_config(None)

    config_path = "/indexer/inexistent/config.yml"
    with pytest.raises(FileNotFoundError, match=f"{config_path} does not exist"):
        load_and_check_config(config_path)


def test_load_and_check_config_invalid_configuration_toplevel(tmpdir):
    """Invalid configuration raises"""
    config = {"something": "useless"}
    config_path = prepare_config_file(tmpdir, content=config)
    with pytest.raises(KeyError, match="missing objstorage config entry"):
        load_and_check_config(config_path)


def test_load_and_check_config_invalid_configuration(tmpdir):
    """Invalid configuration raises"""
    config_path = prepare_config_file(
        tmpdir, content={"objstorage": {"something": "useless"}}
    )
    with pytest.raises(KeyError, match="missing cls config entry"):
        load_and_check_config(config_path)


def test_load_and_check_config_invalid_configuration_level2(tmpdir):
    """Invalid configuration at 2nd level raises"""
    config = {
        "objstorage": {
            "cls": "pathslicing",
            "args": {"root": "root", "slicing": "slicing",},
            "client_max_size": "10",
        }
    }
    for key in ("root", "slicing"):
        c = copy.deepcopy(config)
        c["objstorage"]["args"].pop(key)
        config_path = prepare_config_file(tmpdir, c)
        with pytest.raises(KeyError, match=f"missing {key} config entry"):
            load_and_check_config(config_path)


@pytest.mark.parametrize(
    "config",
    [
        pytest.param(
            {
                "objstorage": {
                    "cls": "pathslicing",
                    "args": {"root": "root", "slicing": "slicing"},
                }
            },
            id="pathslicing-bw-compat",
        ),
        pytest.param(
            {
                "objstorage": {
                    "cls": "pathslicing",
                    "root": "root",
                    "slicing": "slicing",
                }
            },
            id="pathslicing",
        ),
        pytest.param(
            {"client_max_size": "10", "objstorage": {"cls": "memory", "args": {}}},
            id="empty-args-bw-compat",
        ),
        pytest.param(
            {"client_max_size": "10", "objstorage": {"cls": "memory"}}, id="empty-args"
        ),
    ],
)
def test_load_and_check_config(tmpdir, config):
    """pathslicing configuration fine loads ok"""
    config_path = prepare_config_file(tmpdir, config)
    cfg = load_and_check_config(config_path)
    assert cfg == config
