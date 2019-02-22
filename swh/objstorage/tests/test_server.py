# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import pytest
import yaml

from swh.objstorage.api.server import load_and_check_config


def prepare_config_file(tmpdir, content, name='config.yml'):
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
    config_path.write_text(content, encoding='utf-8')
    # pytest on python3.5 does not support LocalPath manipulation, so
    # convert path to string
    return str(config_path)


def test_load_and_check_config_no_configuration():
    """Inexistant configuration files raises"""
    with pytest.raises(EnvironmentError) as e:
        load_and_check_config(None)

    assert e.value.args[0] == 'Configuration file must be defined'

    config_path = '/indexer/inexistant/config.yml'
    with pytest.raises(FileNotFoundError) as e:
        load_and_check_config(config_path)

    assert e.value.args[0] == 'Configuration file %s does not exist' % (
        config_path, )


def test_load_and_check_config_invalid_configuration_toplevel(tmpdir):
    """Invalid configuration raises"""
    config = {
        'something': 'useless'
    }
    config_path = prepare_config_file(tmpdir, content=config)
    with pytest.raises(KeyError) as e:
        load_and_check_config(config_path)

    assert (
        e.value.args[0] ==
        'Invalid configuration; missing objstorage config entry'
    )


def test_load_and_check_config_invalid_configuration(tmpdir):
    """Invalid configuration raises"""
    for data, missing_keys in [
            ({'objstorage': {'something': 'useless'}}, ['cls', 'args']),
            ({'objstorage': {'cls': 'something'}}, ['args']),
    ]:
        config_path = prepare_config_file(tmpdir, content=data)
        with pytest.raises(KeyError) as e:
            load_and_check_config(config_path)

        assert (
            e.value.args[0] ==
            'Invalid configuration; missing %s config entry' % (
                ', '.join(missing_keys), )
        )


def test_load_and_check_config_invalid_configuration_level2(tmpdir):
    """Invalid configuration at 2nd level raises"""
    config = {
        'objstorage': {
            'cls': 'pathslicing',
            'args': {
                'root': 'root',
                'slicing': 'slicing',
            },
            'client_max_size': '10',
        }
    }
    for key in ('root', 'slicing'):
        c = copy.deepcopy(config)
        c['objstorage']['args'].pop(key)
        config_path = prepare_config_file(tmpdir, c)
        with pytest.raises(KeyError) as e:
            load_and_check_config(config_path)

        assert (
            e.value.args[0] ==
            "Invalid configuration; missing args.%s config entry" % key
        )


def test_load_and_check_config_fine(tmpdir):
    """pathslicing configuration fine loads ok"""
    config = {
        'objstorage': {
            'cls': 'pathslicing',
            'args': {
                'root': 'root',
                'slicing': 'slicing',
            }
        }
    }

    config_path = prepare_config_file(tmpdir, config)
    cfg = load_and_check_config(config_path)
    assert cfg == config


def test_load_and_check_config_fine2(tmpdir):
    config = {
        'client_max_size': '10',
        'objstorage': {
            'cls': 'remote',
            'args': {}
        }
    }
    config_path = prepare_config_file(tmpdir, config)
    cfg = load_and_check_config(config_path)
    assert cfg == config
