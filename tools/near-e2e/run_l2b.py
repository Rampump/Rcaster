#!/usr/bin/env python3
"""Full-table local NEAR capacity and sampling check."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import run_e2e


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--binary", type=Path, default=Path("target/release/rcaster")
    )
    parser.add_argument(
        "--snapshot",
        type=Path,
        default=Path(__file__).parent / "data" / "crtk-str-20261125.json",
    )
    parser.add_argument(
        "--capture",
        type=Path,
        default=Path("target/com3-rtcm3-capture.bin"),
    )
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()
    args.binary = args.binary.resolve()
    args.snapshot = args.snapshot.resolve()
    args.capture = args.capture.resolve()
    output = Path(__file__).parent / "runs" / datetime.now().strftime(
        "%Y%m%d-%H%M%S-l2b"
    )
    output.mkdir(parents=True, exist_ok=True)
    harness = run_e2e.Harness(args)
    evidence: list[dict] = []
    try:
        harness.start()
        all_records = harness.all_records
        points = harness.start_sources(all_records, timeout=180)
        station_map = {point.station_id: point.mountpoint for point in points}
        anchors = [
            all_records[round(index * (len(all_records) - 1) / 24)]
            for index in range(25)
        ]
        pairs: list[tuple[dict, dict, float]] = []
        for anchor in anchors:
            nearest = min(
                (
                    (
                        run_e2e.haversine_m(
                            float(anchor["latitude"]),
                            float(anchor["longitude"]),
                            float(candidate["latitude"]),
                            float(candidate["longitude"]),
                        ),
                        candidate,
                    )
                    for candidate in all_records
                    if candidate is not anchor
                ),
                key=lambda item: item[0],
            )
            pairs.append((anchor, nearest[1], nearest[0]))
        samples: list[dict] = []
        for index, (left, right, distance) in enumerate(pairs[:15]):
            latitude = (float(left["latitude"]) + float(right["latitude"])) / 2
            longitude = (float(left["longitude"]) + float(right["longitude"])) / 2
            samples.append(
                {
                    "label": f"midpoint:{left['mountpoint']}:{right['mountpoint']}",
                    "latitude": latitude,
                    "longitude": longitude,
                    "pair_distance_m": distance,
                }
            )
        for index, (left, right, distance) in enumerate(pairs[10:25]):
            latitude = (float(left["latitude"]) + float(right["latitude"])) / 2
            longitude = (float(left["longitude"]) + float(right["longitude"])) / 2
            offset = ((index % 2) * 2 - 1) * 50.0 / 111_320.0
            samples.append(
                {
                    "label": f"offset50:{left['mountpoint']}:{right['mountpoint']}:{index}",
                    "latitude": latitude + offset,
                    "longitude": longitude,
                    "pair_distance_m": distance,
                }
            )
        for index, (left, right, distance) in enumerate(pairs[:20]):
            fraction = 0.25 if index % 2 == 0 else 0.75
            latitude, longitude = run_e2e.interpolate(
                (float(left["latitude"]), float(left["longitude"])),
                (float(right["latitude"]), float(right["longitude"])),
                fraction,
            )
            samples.append(
                {
                    "label": f"fraction{fraction}:{left['mountpoint']}:{right['mountpoint']}",
                    "latitude": latitude,
                    "longitude": longitude,
                    "pair_distance_m": distance,
                }
            )

        def check(sample: dict) -> tuple[dict, list[dict]]:
            expected = run_e2e.nearest(
                points, sample["latitude"], sample["longitude"]
            )
            log: list[dict] = []
            seen = run_e2e.collect_client(
                harness.port,
                lambda _t, query=sample: (
                    query["latitude"],
                    query["longitude"],
                    0.9,
                ),
                4.0,
                log,
                station_map,
            )
            actual = seen[-1][1] if seen else None
            matched = (
                expected is None and actual is None
            ) or (
                expected is not None and actual == expected.station_id
            )
            return (
                {
                    "query": sample["label"],
                    "latitude": sample["latitude"],
                    "longitude": sample["longitude"],
                    "expected_station": expected.station_id if expected else None,
                    "actual_station": actual,
                    "pass": matched,
                },
                log,
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
            results = list(pool.map(check, samples))
        for result, log in results:
            evidence.extend(log)
            evidence.append({"event": "case_result", **result})
        passed = sum(1 for result, _ in results if result["pass"])
        ratio = passed / len(results)
        peak_metrics = harness.metrics.refresh()
        str_table = harness.str_table()
        harness.stop_sources()
        cleanup_metrics = harness.metrics.refresh()
        harness.stderr.flush()
        stderr_size = harness.stderr_path.stat().st_size
        summary = {
            "active_sources": len(points),
            "samples": len(results),
            "passed": passed,
            "ratio": ratio,
            "threshold": 0.95,
            "pass": ratio >= 0.95,
            "cleanup": {
                "active_sources": int(
                    cleanup_metrics.get("rcaster_active_sources", -1)
                ),
                "stderr_bytes": stderr_size,
            },
            "results": [result for result, _ in results],
        }
        summary["pass"] = (
            summary["pass"]
            and summary["cleanup"]["active_sources"] == 0
            and summary["cleanup"]["stderr_bytes"] == 0
        )
        run_e2e.write_jsonl(output / "l2b.jsonl", evidence)
        run_e2e.write_json(output / "summary.json", summary)
        run_e2e.write_json(output / "metrics.json", peak_metrics)
        run_e2e.write_json(output / "cleanup-metrics.json", cleanup_metrics)
        (output / "str-table.txt").write_text(str_table, encoding="utf-8")
        print(json.dumps(summary, indent=2))
        return 0 if summary["pass"] else 2
    finally:
        harness.stop()
        if not args.keep:
            shutil.rmtree(harness.temp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
