import pytest

pytest_plugins = ["swh.objstorage.pytest_plugin"]


def pytest_configure(config):
    config.addinivalue_line("markers", "shard_max_size: winery backend")
    config.addinivalue_line(
        "markers", "pack_immediately(bool): whether winery should pack immediately"
    )
    config.addinivalue_line(
        "markers",
        "clean_immediately(bool): whether the winery packer should clean rw "
        "shards immediately",
    )
    config.addinivalue_line(
        "markers",
        (
            "all_compression_methods: "
            "test all compression methods instead of only the most common ones"
        ),
    )
    config.addinivalue_line(
        "markers", "skip_on_cloud: skip test on cloud implementations"
    )


def pytest_addoption(parser):
    parser.addoption(
        "--all-compression-methods",
        action="store_true",
        default=False,
        help="Test all compression methods",
    )


def pytest_runtest_setup(item):
    if item.get_closest_marker("all_compression_methods"):
        if not item.config.getoption("--all-compression-methods"):
            pytest.skip("`--all-compression-methods` has not been specified")
    if item.get_closest_marker("skip_on_cloud"):
        if item.parent and "Cloud" in item.parent.name:
            pytest.skip("skipping on cloud implementation")
