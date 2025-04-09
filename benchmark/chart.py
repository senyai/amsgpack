#!/usr/bin/env python3
import json
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict

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

colors = {
    "msgpack": "#8d3e88",
    "amsgpack": "#439987",
    "ormsgpack": "#f5557d",
}

from pprint import pprint

pprint(data)

for module_name in ("msgpack", "ormsgpack", "amsgpack"):
    offset = width * multiplier
    color = colors[module_name]

    bunch = data["pack_benchmark"][module_name]
    measurement = [bunch[file_name] for file_name in sorted(file_names)]
    rects = ax1.bar(
        x + offset, measurement, width, label=module_name, color=color
    )

    bunch = data["unpack_benchmark"][module_name]
    measurement = [bunch[file_name] for file_name in sorted(file_names)]
    rects = ax2.bar(
        x + offset, measurement, width, label=module_name, color=color
    )

    # ax1.bar_label(rects, padding=6, fmt="%0.2f GiB/s")
    multiplier += 1

ax1.set_title("$packb$ speed")
ax1.set_ylabel("GiB / Second")
ax1.set_xticks(x + width, sorted(file_names))
ax1.legend(loc="upper left", ncols=1)

ax2.set_title("$unpackb$ speed")
ax2.set_ylabel("GiB / Second")
ax2.set_xticks(x + width, sorted(file_names))
ax2.legend(loc="upper left", ncols=1)

plt.savefig("benchmark-0.0.7.svg")
