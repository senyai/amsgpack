#!/usr/bin/env python3
import json
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict
from amsgpack import __version__ as amsgpack_version
from msgpack import version as msgpack_version_
from ormsgpack import __version__ as ormsgpack_version
from msgspec import __version__ as msgspec_version_

msgpack_version = ".".join(map(str, msgpack_version_))
msgspec_version = msgspec_version_.partition("+")[0]

plt.rcParams["font.size"] = 16

with open("./msgpack_benchmark.json") as fin:
    benchmarks = json.load(fin)["benchmarks"]


data = defaultdict(lambda: defaultdict(dict))
file_names = set()
module_names = set()
for benchmark in benchmarks:
    if not benchmark["name"].endswith("_median"):
        continue
    benchmark_name = benchmark["name"].partition("/")[0]
    module_name, file_name = benchmark["label"].split("(")
    file_name = file_name.rstrip(")")
    data[benchmark_name][module_name][file_name] = benchmark[
        "bytes_per_second"
    ] / (1024 * 1024 * 1024)
    module_names.add(module_name)
    file_names.add(file_name)

width = 0.15  # the width of the bars
multiplier = 0
fig, (ax1, ax2) = plt.subplots(
    nrows=2, ncols=1, figsize=(10, 6), dpi=96, layout="constrained"
)
ax1.grid(axis="y")
ax1.set_axisbelow(True)
ax2.grid(axis="y")
ax2.set_axisbelow(True)
x = np.arange(len(file_names))

colors_and_version = {
    "msgpack": ("#8d3e88", msgpack_version),
    "amsgpack": ("#439987", amsgpack_version),
    "ormsgpack": ("#f5557d", ormsgpack_version),
    # "umsgpack": ("#aaaac0",),
    "msgspec": ("#fe8e61", msgspec_version),
}

from pprint import pprint

pprint(data)

for module_name in ("msgpack", "ormsgpack", "msgspec", "amsgpack"):
    offset = width * multiplier
    color, version = colors_and_version[module_name]
    label = f"$\\bf{{{module_name}}}$ $\\mathrm{{{version}}}$"

    bunch = data["pack_benchmark"][module_name]
    measurement = [bunch[file_name] for file_name in sorted(file_names)]
    rects = ax1.bar(x + offset, measurement, width, label=label, color=color)

    bunch = data["unpack_benchmark"][module_name]
    measurement = [bunch[file_name] for file_name in sorted(file_names)]
    rects = ax2.bar(x + offset, measurement, width, label=label, color=color)

    # ax1.bar_label(rects, padding=6, fmt="%0.2f GiB/s")
    multiplier += 1

ax1.set_title("$packb$ speed")
ax1.set_ylabel("GiB / Second")
ax1.set_xticks(x + width, sorted(file_names))
ax1.legend(loc="upper left", ncols=2, prop={"size": 13})

ax2.set_title("$unpackb$ speed")
ax2.set_ylabel("GiB / Second")
ax2.set_xticks(x + width, sorted(file_names))
ax2.legend(loc="upper left", ncols=2, prop={"size": 13})

filename = f"benchmark-{amsgpack_version}.svg"
plt.savefig(filename)
print(f"saved {filename}")
