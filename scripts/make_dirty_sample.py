"""Create a deliberately dirty copy of group 1 for a demo/test run.

This is optional and does not alter the supplied source data. It demonstrates
how the pipeline behaves when a sensor misses an hour, sends a null, or emits an
implausible reading.
"""

import argparse
import csv
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("source", nargs="?", default="data/data_group_1.csv")
    parser.add_argument("target", nargs="?", default="data_dirty/data_group_1.csv")
    args = parser.parse_args()

    source = Path(args.source)
    target = Path(args.target)
    target.parent.mkdir(parents=True, exist_ok=True)

    with source.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
        fieldnames = rows[0].keys()

    output = []
    for row in rows:
        key = (row["timestamp"], row["turbine_id"])

        # Simulate a completely missed sensor entry.
        if key == ("2022-03-05 12:00:00", "1"):
            continue

        # Simulate a null reading.
        if key == ("2022-03-05 13:00:00", "2"):
            row["power_output"] = ""

        # Simulate extreme/invalid readings.
        if key == ("2022-03-05 14:00:00", "3"):
            row["wind_speed"] = "100"
        if key == ("2022-03-05 15:00:00", "4"):
            row["power_output"] = "-5"
        if key == ("2022-03-05 16:00:00", "5"):
            row["wind_direction"] = "999"

        output.append(row)

    with target.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output)

    print(f"Wrote dirty sample to {target}")


if __name__ == "__main__":
    main()
