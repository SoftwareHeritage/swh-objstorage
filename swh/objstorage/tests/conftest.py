import sys


def pytest_configure(config):
    config.addinivalue_line("markers", "shard_max_size: winery backend")
    config.addinivalue_line(
        "markers", "pack_immediately(bool): whether winery should pack immediately"
    )
    config.addinivalue_line(
        "markers", "use_benchmark_flags: use the --winery-bench-* CLI flags"
    )


def pytest_addoption(parser):
    if sys.version_info >= (3, 9):
        import argparse

        action = argparse.BooleanOptionalAction
        default = True
    else:
        action = "store_true"
        default = False

    parser.addoption(
        "--winery-bench-pack-immediately",
        action=action,
        help="Pack objects synchronously in benchmark",
        default=default,
    )

    parser.addoption(
        "--winery-bench-remove-pool",
        action=action,
        help="Remove Ceph pool before and after tests",
        default=default,
    )

    parser.addoption(
        "--winery-bench-remove-images",
        action=action,
        help="Remove Ceph images after tests",
        default=default,
    )

    parser.addoption(
        "--winery-bench-rbd-pool",
        help="RBD pool for benchmark",
        default="winery-benchmark-shards",
    )

    parser.addoption(
        "--winery-bench-output-directory",
        help="Directory in which the performance results are stored",
        default="/tmp/winery",
    )
    parser.addoption(
        "--winery-bench-rw-workers",
        type=int,
        help="Number of Read/Write workers",
        default=1,
    )
    parser.addoption(
        "--winery-bench-ro-workers",
        type=int,
        help="Number of Readonly workers",
        default=1,
    )
    parser.addoption(
        "--winery-bench-pack-workers",
        type=int,
        help="Number of Pack workers",
        default=1,
    )
    parser.addoption(
        "--winery-bench-duration",
        type=int,
        help="Duration of the benchmarks in seconds",
        default=1,
    )
    parser.addoption(
        "--winery-bench-shard-max-size",
        type=int,
        help="Size of the shard in bytes",
        default=10 * 1024 * 1024,
    )
    parser.addoption(
        "--winery-bench-ro-worker-max-request",
        type=int,
        help="Number of requests a ro worker performs",
        default=1,
    )
    parser.addoption(
        "--winery-bench-throttle-read",
        type=int,
        help="Maximum number of bytes per second read",
        default=100 * 1024 * 1024,
    )
    parser.addoption(
        "--winery-bench-throttle-write",
        type=int,
        help="Maximum number of bytes per second write",
        default=100 * 1024 * 1024,
    )
