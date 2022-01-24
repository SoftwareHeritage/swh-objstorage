# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import sys

from matplotlib import pyplot as plt
from matplotlib.ticker import FormatStrFormatter
import pandas as pd


def human(size, unit):
    if size < 1024:
        return f"{int(size)} {unit}/s"
    elif size / 1024 < 1024:
        return f"{round(size/1024, 1)} K{unit}/s"
    elif size / (1024 * 1024) < 1024:
        return f"{round(size / (1024 * 1024), 1)} M{unit}/s"
    elif size / (1024 * 1024 * 1024) < 1024:
        return f"{round(size / (1024 * 1024 * 1024), 1)} G{unit}/s"


def read_stats(stats):
    dfs = []
    files = os.listdir(stats)
    for file in files:
        f = f"{stats}/{file}"
        if not os.path.isfile(f):
            continue
        dfs.append(pd.read_csv(f))
    df = pd.concat(dfs)
    df.set_index("time")
    return df.sort_values(by=["time"])


def main(stats):
    df = read_stats(stats)
    print(df)
    t = df["time"].to_numpy()
    sec = t[-1] - t[0]
    a = df.sum() / sec
    print(f'Bytes write   {human(a["bytes_write"], "B")}')
    print(f'Objects write {human(a["object_write_count"], "object")}')
    print(f'Bytes read    {human(a["bytes_read"], "B")}')
    print(f'Objects read  {human(a["object_read_count"], "object")}')

    df["date"] = pd.to_datetime(df["time"], unit="s")

    p = df.plot(x="time", y=["bytes_write", "bytes_read"])
    p.set_xlabel("Time")
    p.yaxis.set_major_formatter(FormatStrFormatter("%.0f"))
    p.set_ylabel("B/s")
    plt.show()


if __name__ == "__main__":
    main(sys.argv[1])
