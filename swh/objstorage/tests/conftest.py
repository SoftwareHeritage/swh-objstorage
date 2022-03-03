def pytest_configure(config):
    config.addinivalue_line("markers", "shard_max_size: winery backend")


def pytest_addoption(parser):
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
        "--winery-bench-duration",
        type=int,
        help="Duration of the benchmarks in seconds",
        default=1,
    )
    parser.addoption(
        "--winery-shard-max-size",
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
