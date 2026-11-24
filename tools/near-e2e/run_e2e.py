#!/usr/bin/env python3
"""Standalone NEAR end-to-end evidence harness.

This script uses only local sources and the local Caster. The CRTK snapshot is
read from disk and is never contacted during the run.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import os
import re
import selectors
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import rtcm


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6_371_000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dlat = p2 - p1
    dlon = math.radians(lon2 - lon1)
    value = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    return 2.0 * radius * math.asin(math.sqrt(value))


def offset(lat: float, lon: float, distance_m: float, bearing_rad: float) -> tuple[float, float]:
    north = distance_m * math.cos(bearing_rad) / 111_320.0
    east = distance_m * math.sin(bearing_rad) / (
        111_320.0 * max(0.01, math.cos(math.radians(lat)))
    )
    return lat + north, lon + east


def interpolate(a: tuple[float, float], b: tuple[float, float], fraction: float) -> tuple[float, float]:
    return a[0] + (b[0] - a[0]) * fraction, a[1] + (b[1] - a[1]) * fraction


def nmea_coord(value: float, latitude: bool) -> str:
    absolute = abs(value)
    degrees = int(absolute)
    minutes = (absolute - degrees) * 60.0
    width = 2 if latitude else 3
    return f"{degrees:0{width}d}{minutes:07.4f}"


def gga_sentence(lat: float, lon: float, hdop: float = 0.9, quality: int = 1) -> str:
    now = datetime.now(timezone.utc)
    body = (
        f"GPGGA,{now:%H%M%S}.00,{nmea_coord(lat, True)},"
        f"{'N' if lat >= 0 else 'S'},{nmea_coord(lon, False)},"
        f"{'E' if lon >= 0 else 'W'},{quality},12,{hdop:.1f},50.0,M,0.0,M,,"
    )
    checksum = 0
    for byte in body.encode("ascii"):
        checksum ^= byte
    return f"${body}*{checksum:02X}\r\n"


@dataclass(frozen=True)
class Point:
    mountpoint: str
    latitude: float
    longitude: float
    station_id: int


class Metrics:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.values: dict[str, float] = {}

    def refresh(self) -> dict[str, float]:
        with urllib.request.urlopen(f"{self.base_url}/metrics", timeout=3) as response:
            text = response.read().decode("utf-8")
        values: dict[str, float] = {}
        for line in text.splitlines():
            if line.startswith("#") or not line.strip():
                continue
            match = re.match(r"([^\s]+)\s+([0-9.eE+-]+)$", line)
            if match:
                values[match.group(1)] = float(match.group(2))
        self.values = values
        return values

    def near(self, event: str) -> float:
        return self.values.get(
            f'rcaster_near_events_total{{event="{event}"}}', 0.0
        )

    def active_sources(self) -> float:
        return self.values.get("rcaster_active_sources", 0.0)


class SourceStream:
    def __init__(self, point: Point, port: int, payload: bytes):
        self.point = point
        self.port = port
        self.payload = payload
        self.pause = threading.Event()
        self.stop = threading.Event()
        self.error: str | None = None
        self.sock: socket.socket | None = None
        self.thread = threading.Thread(target=self.run, name=f"source-{point.mountpoint}")

    def start(self) -> None:
        sock = socket.create_connection(("127.0.0.1", self.port), timeout=5)
        request = (
            f"SOURCE change-me /{self.point.mountpoint}\r\n"
            "Source-Agent: RCaster-NEAR-e2e/1.0\r\n\r\n"
        ).encode("ascii")
        sock.sendall(request)
        header = recv_header(sock)
        if b" 200 " not in header.split(b"\r\n", 1)[0]:
            sock.close()
            raise RuntimeError(f"source {self.point.mountpoint} rejected: {header!r}")
        self.sock = sock
        self.thread.start()

    def run(self) -> None:
        try:
            sock = self.sock
            if sock is None:
                raise RuntimeError("source socket is not connected")
            for _ in range(4):
                sock.sendall(self.payload)
            while not self.stop.is_set():
                if not self.pause.is_set():
                    sock.sendall(self.payload)
                self.stop.wait(1.0)
        except Exception as error:  # noqa: BLE001 - evidence must retain the failure
            self.error = f"{type(error).__name__}: {error}"
        finally:
            if self.sock is not None:
                self.sock.close()

    def pause_stream(self) -> None:
        self.pause.set()

    def close(self) -> None:
        self.stop.set()

    def join(self) -> None:
        self.thread.join(timeout=5)


def recv_header(sock: socket.socket, limit: int = 16384) -> bytes:
    data = bytearray()
    while b"\r\n\r\n" not in data and len(data) < limit:
        chunk = sock.recv(1024)
        if not chunk:
            break
        data.extend(chunk)
    return bytes(data)


def parse_1005(buffer: bytearray) -> list[int]:
    stations: list[int] = []
    while True:
        start = buffer.find(b"\xD3")
        if start < 0:
            buffer.clear()
            return stations
        if start:
            del buffer[:start]
        if len(buffer) < 6:
            return stations
        length = ((buffer[1] & 0x03) << 8) | buffer[2]
        end = 3 + length + 3
        if len(buffer) < end:
            return stations
        raw = bytes(buffer[:end])
        if rtcm.crc24q(raw[:-3]) != int.from_bytes(raw[-3:], "big"):
            del buffer[0]
            continue
        frame = rtcm.frames(raw)
        if frame:
            station = rtcm.station_id(frame[0])
            if station is not None:
                stations.append(station)
        del buffer[:end]


def open_near(port: int, lat: float, lon: float) -> tuple[socket.socket, bytearray]:
    sock = socket.create_connection(("127.0.0.1", port), timeout=5)
    request = (
        "GET /NEAR HTTP/1.0\r\n"
        f"Host: 127.0.0.1:{port}\r\n"
        "User-Agent: RCaster-NEAR-e2e/1.0\r\n"
        f"Ntrip-GGA: {gga_sentence(lat, lon).strip()}\r\n\r\n"
    ).encode("ascii")
    sock.sendall(request)
    header = recv_header(sock)
    first = header.split(b"\r\n", 1)[0]
    if b" 200 " not in first:
        sock.close()
        raise RuntimeError(f"NEAR rejected: {header!r}")
    marker = header.find(b"\r\n\r\n")
    return sock, bytearray(header[marker + 4 :] if marker >= 0 else b"")


def collect_client(
    port: int,
    plan,
    duration: float,
    log: list[dict],
    station_map: dict[int, str] | None = None,
) -> list[tuple[float, int]]:
    started = time.monotonic()
    lat, lon, hdop = plan(0.0)
    sock, buffer = open_near(port, lat, lon)
    log.append({"t": 0.0, "event": "connect", "gga": gga_sentence(lat, lon).strip()})
    sock.setblocking(False)
    selector = selectors.DefaultSelector()
    selector.register(sock, selectors.EVENT_READ)
    seen: list[tuple[float, int]] = []
    last_station: int | None = None
    next_gga = started
    try:
        while True:
            now = time.monotonic()
            elapsed = now - started
            if elapsed >= duration:
                break
            if now >= next_gga:
                current = plan(elapsed)
                if current is not None:
                    lat, lon, hdop = current
                    sentence = gga_sentence(lat, lon, hdop)
                    sock.sendall(sentence.encode("ascii"))
                    log.append(
                        {
                            "t": round(time.monotonic() - started, 3),
                            "event": "gga",
                            "gga": sentence.strip(),
                        }
                    )
                next_gga = now + 1.0
            if not selector.select(0.05):
                continue
            try:
                chunk = sock.recv(65536)
            except BlockingIOError:
                continue
            if not chunk:
                log.append(
                    {
                        "t": round(time.monotonic() - started, 3),
                        "event": "closed",
                    }
                )
                break
            buffer.extend(chunk)
            log.append(
                {
                    "t": round(time.monotonic() - started, 3),
                    "event": "bytes",
                    "bytes": len(chunk),
                }
            )
            for station in parse_1005(buffer):
                if station != last_station:
                    last_station = station
                    seen.append((time.monotonic() - started, station))
                    log.append(
                        {
                            "t": round(time.monotonic() - started, 3),
                            "event": "switch" if seen[:-1] else "first_frame",
                            "station_id": station,
                            "mountpoint": (station_map or {}).get(station),
                        }
                    )
    finally:
        selector.close()
        sock.close()
    return seen


class Harness:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.root = Path(__file__).resolve().parents[2]
        self.temp = Path(tempfile.mkdtemp(prefix="rcaster-near-e2e-"))
        self.port = free_port()
        self.health_port = free_port()
        self.metrics = Metrics(f"http://127.0.0.1:{self.health_port}")
        self.process: subprocess.Popen | None = None
        self.sources: list[SourceStream] = []
        self.current_points: list[Point] = []
        self.capture = Path(args.capture)
        self.epoch = rtcm.first_epoch(self.capture.read_bytes())

    def start(self) -> None:
        password_hash = subprocess.run(
            [str(self.args.binary), "hash-password"],
            input=b"change-me",
            stdout=subprocess.PIPE,
            check=True,
        ).stdout.decode("ascii").strip()
        snapshot = json.loads(self.args.snapshot.read_text(encoding="utf-8"))
        self.all_records = [
            record
            for record in snapshot["records"]
            if record["mountpoint"] != "NEAR"
        ]
        self.selected_records = select_sources(self.all_records)
        accounts = {
            "users": [
                {
                    "schema_version": 1,
                    "mod_revision": 1,
                    "user_id": "owner",
                    "username": "owner",
                    "password_hash": password_hash,
                    "auth_version": 1,
                    "enabled": True,
                    "ntrip_enabled": True,
                    "web_enabled": True,
                    "role": "user",
                    "created_at": "2026-11-25T00:00:00Z",
                    "updated_at": "2026-11-25T00:00:00Z",
                }
            ],
            "mountpoints": [
                {
                    "schema_version": 1,
                    "mod_revision": 1,
                    "mountpoint": record["mountpoint"],
                    "owner_user_id": "owner",
                    "enabled": True,
                    "ntrip1_upload_password_hash": password_hash,
                    "advertise": True,
                    "region": "near-e2e",
                    "created_at": "2026-11-25T00:00:00Z",
                    "updated_at": "2026-11-25T00:00:00Z",
                }
                for record in self.all_records
            ],
        }
        accounts_path = self.temp / "accounts.json"
        accounts_path.write_text(json.dumps(accounts, indent=2), encoding="utf-8")
        database = self.temp / "near.redb"
        subprocess.run(
            [
                str(self.args.binary),
                "import-accounts",
                "--database",
                str(database),
                "--input",
                str(accounts_path),
            ],
            check=True,
        )
        config = {
            "bind": f"127.0.0.1:{self.port}",
            "health_bind": f"127.0.0.1:{self.health_port}",
            "account_db": str(database),
            "client_auth_required": False,
            "client_auth_require_tls": False,
            "source_auth_require_tls": False,
            "max_sources": 1400,
            "max_validating_sources": 128,
            "validation_buffer_max_bytes": 1073741824,
            "max_connections": 4000,
            "max_subscribers": 4000,
            "max_subscribers_per_source": 200,
            "auth_max_concurrency": 20,
            "accept_rate_per_second": 2000,
            "session_limits": {
                "default_max_sessions": 2000,
                "default_max_ip_sessions": 500,
            },
            "source_admission": {
                "mode": "observe",
                "allowed_formats": ["rtcm3"],
                "require_correction_data": True,
                "min_valid_frames": 4,
                "max_bytes": 8388608,
                "max_frames": 2000000,
                "timeout_ms": 5000,
            },
            "near": {
                "enabled": True,
                "mountpoint": "NEAR",
                "advertise": True,
                "max_recompute_interval_ms": 1000,
                "position_ttl_s": 3,
                "no_source_timeout_ms": 10000,
            },
        }
        config_path = self.temp / "caster.json"
        config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
        self.stderr_path = self.temp / "caster.stderr"
        self.stdout_path = self.temp / "caster.stdout"
        self.stderr = self.stderr_path.open("wb")
        self.stdout = self.stdout_path.open("wb")
        self.process = subprocess.Popen(
            [str(self.args.binary), "serve", "--config", str(config_path)],
            cwd=self.temp,
            stdout=self.stdout,
            stderr=self.stderr,
        )
        wait_until(
            lambda: self.metrics.refresh() is not None,
            10,
            "health listener",
        )

    def point_for(self, record: dict) -> Point:
        index = next(
            i
            for i, item in enumerate(self.all_records)
            if item["mountpoint"] == record["mountpoint"]
        )
        return Point(
            mountpoint=record["mountpoint"],
            latitude=float(record["latitude"]),
            longitude=float(record["longitude"]),
            station_id=index + 1,
        )

    def start_sources(self, records: list[dict], timeout: float = 15.0) -> list[Point]:
        self.stop_sources()
        self.current_points = [self.point_for(record) for record in records]
        self.sources = []
        for point in self.current_points:
            payload = rtcm.payload_for_station(
                self.epoch, point.station_id, point.latitude, point.longitude
            )
            source = SourceStream(point, self.port, payload)
            try:
                source.start()
            except Exception as error:
                for started in self.sources:
                    started.close()
                    started.join()
                self.sources = []
                raise RuntimeError(str(error)) from error
            self.sources.append(source)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            failures = [source.error for source in self.sources if source.error]
            if failures:
                raise RuntimeError(f"source failures: {failures}")
            self.metrics.refresh()
            if self.metrics.active_sources() >= len(records):
                return self.current_points
            time.sleep(0.1)
        failures = [source.error for source in self.sources if source.error]
        raise RuntimeError(
            f"timeout waiting for active sources: active={self.metrics.active_sources()} "
            f"expected={len(records)} failures={failures}"
        )

    def stop_sources(self) -> None:
        for source in self.sources:
            source.close()
        for source in self.sources:
            source.join()
        self.sources = []
        if self.process and self.process.poll() is None:
            try:
                wait_until(
                    lambda: self.metrics.refresh() is not None
                    and self.metrics.active_sources() == 0,
                    15,
                    "source cleanup",
                )
            except RuntimeError:
                pass

    def stop(self) -> None:
        self.stop_sources()
        if self.process and self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        for handle in getattr(self, "stdout", None), getattr(self, "stderr", None):
            if handle:
                handle.close()

    def str_table(self) -> str:
        with socket.create_connection(("127.0.0.1", self.port), timeout=5) as sock:
            sock.sendall(
                b"GET / HTTP/1.0\r\nUser-Agent: RCaster-NEAR-e2e/1.0\r\n\r\n"
            )
            response = bytearray()
            while True:
                chunk = sock.recv(65536)
                if not chunk:
                    break
                response.extend(chunk)
        return response.decode("latin-1")


def wait_until(predicate, timeout: float, label: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except Exception:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"timeout waiting for {label}")


def select_sources(records: list[dict]) -> list[dict]:
    return [records[round(index * (len(records) - 1) / 19)] for index in range(20)]


def nearest(points: list[Point], lat: float, lon: float) -> Point | None:
    candidates = [
        (haversine_m(lat, lon, point.latitude, point.longitude), point)
        for point in points
    ]
    candidates = [item for item in candidates if item[0] <= 100_000]
    return min(candidates, key=lambda item: item[0])[1] if candidates else None


def find_cluster(records: list[dict], count: int, min_m: float, max_m: float) -> list[dict]:
    for anchor in records:
        nearby = []
        for record in records:
            if record is anchor:
                continue
            distance = haversine_m(
                float(anchor["latitude"]),
                float(anchor["longitude"]),
                float(record["latitude"]),
                float(record["longitude"]),
            )
            if min_m <= distance <= max_m:
                nearby.append((distance, record))
        if len(nearby) >= count - 1:
            nearby.sort(key=lambda item: item[0])
            return [anchor] + [item[1] for item in nearby[: count - 1]]
    raise RuntimeError(f"no {count}-station cluster in {min_m}-{max_m}m")


def find_far_pair(records: list[dict], minimum_m: float) -> list[dict]:
    for left_index, left in enumerate(records):
        for right in records[left_index + 1 :]:
            distance = haversine_m(
                float(left["latitude"]),
                float(left["longitude"]),
                float(right["latitude"]),
                float(right["longitude"]),
            )
            if minimum_m <= distance <= 80_000:
                return [left, right]
    raise RuntimeError(f"no pair farther than {minimum_m}m")


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_jsonl(path: Path, values: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(value, ensure_ascii=False) + "\n" for value in values),
        encoding="utf-8",
    )


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
    output = Path(__file__).parent / "runs" / datetime.now().strftime("%Y%m%d-%H%M%S")
    output.mkdir(parents=True, exist_ok=True)

    harness = Harness(args)
    results: list[dict] = []
    cases: list[dict] = []
    evidence_logs: dict[str, list[dict]] = {}
    try:
        harness.start()
        records = harness.selected_records

        def add(name, expected, actual, evidence):
            passed = expected == actual
            cases.append(
                {
                    "name": name,
                    "expected": expected,
                    "actual": actual,
                    "pass": passed,
                    "evidence": evidence,
                }
            )

        static_points = harness.start_sources(records)
        static_ok = 0
        static_log: list[dict] = []
        for point in static_points:
            static_log.append(
                {
                    "t": 0,
                    "event": "case_session",
                    "mountpoint": point.mountpoint,
                    "station_id": point.station_id,
                }
            )
            seen = collect_client(
                harness.port,
                lambda _t, p=point: (p.latitude, p.longitude, 0.9),
                3.0,
                static_log,
                {point.station_id: point.mountpoint},
            )
            expected = nearest(static_points, point.latitude, point.longitude)
            if expected and seen and seen[-1][1] == expected.station_id:
                static_ok += 1
        add("L2-01-static-20", 20, static_ok, "station_id matched")
        evidence_logs["L2-01-static-20"] = static_log
        harness.stop_sources()

        boundary = [records[0]]
        boundary_points = harness.start_sources(boundary)
        boundary_point = boundary_points[0]
        lat99, lon99 = offset(boundary_point.latitude, boundary_point.longitude, 99_000, 0)
        boundary_log: list[dict] = [{"t": 0, "event": "case_phase", "phase": "99km"}]
        seen99 = collect_client(
            harness.port, lambda _t: (lat99, lon99, 0.9), 3.0, boundary_log
        )
        boundary_log.append({"t": 0, "event": "case_phase", "phase": "101km"})
        lat101, lon101 = offset(boundary_point.latitude, boundary_point.longitude, 101_000, 0)
        seen101 = collect_client(
            harness.port, lambda _t: (lat101, lon101, 0.9), 11.0, boundary_log
        )
        add(
            "L2-02-boundary",
            {"within_99km": boundary_point.station_id, "beyond_101km": []},
            {
                "within_99km": seen99[-1][1] if seen99 else None,
                "beyond_101km": [item[1] for item in seen101],
            },
            "99km receives; 101km receives no station",
        )
        evidence_logs["L2-02-boundary"] = boundary_log
        harness.stop_sources()

        cluster = find_cluster(harness.all_records, 3, 300, 5_000)
        cluster_points = harness.start_sources(cluster)
        a, b, c = cluster_points

        hysteresis_log: list[dict] = []
        seen = collect_client(
            harness.port,
            lambda _t: (a.latitude, a.longitude + 0.000003 * math.sin(_t * 6), 0.9),
            8.0,
            hysteresis_log,
            {point.station_id: point.mountpoint for point in cluster_points},
        )
        add("L2-03-hysteresis", [a.station_id], [item[1] for item in seen], "no flap")
        evidence_logs["L2-03-hysteresis"] = hysteresis_log

        distance_ab = haversine_m(a.latitude, a.longitude, b.latitude, b.longitude)
        move_duration = max(2.0, min(6.0, distance_ab / 700.0))

        def ab_plan(t: float):
            if t < 2:
                return a.latitude, a.longitude, 0.9
            if t < 2 + move_duration:
                return (*interpolate(
                    (a.latitude, a.longitude),
                    (b.latitude, b.longitude),
                    (t - 2) / move_duration,
                ), 0.9)
            return b.latitude, b.longitude, 0.9

        one_switch_log: list[dict] = []
        seen = collect_client(
            harness.port,
            ab_plan,
            2 + move_duration + 9,
            one_switch_log,
            {point.station_id: point.mountpoint for point in cluster_points},
        )
        add(
            "L2-04-one-switch-fast-query",
            True,
            len(seen) == 2
            and [item[1] for item in seen] == [a.station_id, b.station_id]
            and seen[1][0] >= 3.0,
            {
                "stations": [item[1] for item in seen],
                "switch_times_s": [round(item[0], 3) for item in seen],
                "move_duration_s": move_duration,
            },
        )
        evidence_logs["L2-04-one-switch"] = one_switch_log

        distance_bc = haversine_m(b.latitude, b.longitude, c.latitude, c.longitude)
        move_duration_bc = max(2.5, distance_bc / 700.0)

        def abc_plan(t: float):
            leg = 2 + move_duration
            if t < 2:
                return a.latitude, a.longitude, 0.9
            if t < leg:
                return (*interpolate(
                    (a.latitude, a.longitude),
                    (b.latitude, b.longitude),
                    (t - 2) / move_duration,
                ), 0.9)
            if t < leg + 9:
                return b.latitude, b.longitude, 0.9
            if t < leg + 9 + move_duration_bc:
                return (*interpolate(
                    (b.latitude, b.longitude),
                    (c.latitude, c.longitude),
                    (t - leg - 9) / move_duration_bc,
                ), 0.9)
            return c.latitude, c.longitude, 0.9

        two_switches_log: list[dict] = []
        seen = collect_client(
            harness.port,
            abc_plan,
            2 + move_duration + 9 + move_duration_bc + 9,
            two_switches_log,
            {point.station_id: point.mountpoint for point in cluster_points},
        )
        add(
            "L2-05-two-switches-fast-query",
            True,
            [item[1] for item in seen]
            == [a.station_id, b.station_id, c.station_id]
            and seen[1][0] >= 3.0
            and seen[2][0] >= seen[1][0] + 3.0,
            {
                "stations": [item[1] for item in seen],
                "switch_times_s": [round(item[0], 3) for item in seen],
                "monotonic": True,
            },
        )
        evidence_logs["L2-05-two-switches"] = two_switches_log
        harness.stop_sources()

        far = find_far_pair(harness.all_records, 50_000)
        far_points = harness.start_sources(far)
        first, second = far_points
        before = harness.metrics.refresh().copy()
        jump_log: list[dict] = []
        seen = collect_client(
            harness.port,
            lambda t: (
                first.latitude,
                first.longitude,
                0.9,
            )
            if t < 2
            else (second.latitude, second.longitude, 0.9),
            6.0,
            jump_log,
            {point.station_id: point.mountpoint for point in far_points},
        )
        after = harness.metrics.refresh()
        add(
            "L2-06-jump-rejected",
            {"stations": [first.station_id], "jump_delta_positive": True},
            {
                "stations": [item[1] for item in seen],
                "jump_delta_positive": after.get(
                    'rcaster_near_events_total{event="gga_jump_rejected"}', 0
                )
                > before.get('rcaster_near_events_total{event="gga_jump_rejected"}', 0),
            },
            "default 1000m/s jump guard",
        )
        evidence_logs["L2-06-jump-rejected"] = jump_log

        before = harness.metrics.refresh().copy()
        sock, buffer = open_near(harness.port, first.latitude, first.longitude)
        for _ in range(12):
            sock.sendall(gga_sentence(first.latitude, first.longitude).encode("ascii"))
            time.sleep(0.02)
        time.sleep(1.0)
        sock.close()
        after = harness.metrics.refresh()
        add(
            "L2-07-rate-limit",
            True,
            after.get('rcaster_near_events_total{event="gga_rate_limited"}', 0)
            > before.get('rcaster_near_events_total{event="gga_rate_limited"}', 0),
            "12 GGA within one second",
        )
        evidence_logs["L2-07-rate-limit"] = [
            {"t": 0, "event": "burst", "count": 12},
            {
                "t": 1,
                "event": "metric_delta",
                "gga_rate_limited": after.get(
                    'rcaster_near_events_total{event="gga_rate_limited"}', 0
                )
                - before.get(
                    'rcaster_near_events_total{event="gga_rate_limited"}', 0
                ),
            },
        ]

        hdop_log: list[dict] = []
        seen = collect_client(
            harness.port,
            lambda t: (first.latitude, first.longitude, 0.9)
            if t < 2
            else (second.latitude, second.longitude, 2.6),
            7.0,
            hdop_log,
            {point.station_id: point.mountpoint for point in far_points},
        )
        add(
            "L2-08-hdop-invalid",
            [first.station_id],
            [item[1] for item in seen],
            "hdop 2.6 rejected",
        )
        evidence_logs["L2-08-hdop-invalid"] = hdop_log
        harness.stop_sources()

        ttl_points = harness.start_sources([records[0], records[1]])
        ttl_source = harness.sources[0]
        before_ttl = harness.metrics.refresh().copy()
        ttl_log: list[dict] = [{"t": 0, "event": "case_phase", "phase": "fresh"}]
        seen_before = collect_client(
            harness.port,
            lambda _t: (ttl_points[0].latitude, ttl_points[0].longitude, 0.9),
            2.0,
            ttl_log,
        )
        ttl_source.pause_stream()
        time.sleep(4)
        ttl_log.append({"t": 2, "event": "case_phase", "phase": "stale_after_ttl"})
        seen_after = collect_client(
            harness.port,
            lambda _t: (ttl_points[0].latitude, ttl_points[0].longitude, 0.9),
            4.0,
            ttl_log,
        )
        add(
            "L2-09-position-ttl",
            {
                "before": [ttl_points[0].station_id],
                "after_stale_ttl": [],
            },
            {
                "before": [item[1] for item in seen_before],
                "after_stale_ttl": [item[1] for item in seen_after],
            },
            "source paused for >3s",
        )
        evidence_logs["L2-09-position-ttl"] = ttl_log
        harness.stop_sources()

        drop_records = find_cluster(harness.all_records, 2, 300, 20_000)
        drop_points = harness.start_sources(drop_records)
        drop_a, drop_b = drop_points
        timer = threading.Timer(2.0, harness.sources[0].close)
        timer.start()
        drop_log: list[dict] = []
        drop_seen = collect_client(
            harness.port,
            lambda _t: (drop_a.latitude, drop_a.longitude, 0.9),
            11.0,
            drop_log,
        )
        timer.join()
        closed_event = next(
            (item for item in drop_log if item.get("event") == "closed"), None
        )
        add(
            "L2-10a-active-disconnect-failover",
            drop_b.station_id,
            drop_seen[-1][1] if drop_seen else None,
            {
                "stations": [item[1] for item in drop_seen],
                "closed": closed_event,
                "sources": [p.station_id for p in drop_points],
            },
        )
        add(
            "L2-10b-active-disconnect-keeps-client",
            True,
            closed_event is None,
            {"closed": closed_event},
        )
        evidence_logs["L2-10-active-disconnect-failover"] = drop_log
        harness.stop_sources()

        close_point = harness.start_sources([drop_records[0]])[0]
        close_timer = threading.Timer(2.0, harness.sources[0].close)
        close_timer.start()
        close_log: list[dict] = []
        collect_client(
            harness.port,
            lambda _t: (close_point.latitude, close_point.longitude, 0.9),
            14.0,
            close_log,
        )
        close_timer.join()
        no_alternative_closed = next(
            (item for item in close_log if item.get("event") == "closed"), None
        )
        add(
            "L2-10c-active-disconnect-no-alternative-close",
            True,
            no_alternative_closed is not None
            and float(no_alternative_closed["t"]) <= 12.5,
            no_alternative_closed or {"event": "connection remained open"},
        )
        evidence_logs["L2-10-active-disconnect-no-alternative"] = close_log
        harness.stop_sources()

        concurrent_points = harness.start_sources(records)
        before = harness.metrics.refresh().copy()

        def run_one(point: Point):
            worker_log: list[dict] = []
            seen = collect_client(
                harness.port,
                lambda _t, p=point: (p.latitude, p.longitude, 0.9),
                4.0,
                worker_log,
                {point.station_id: point.mountpoint},
            )
            expected = nearest(concurrent_points, point.latitude, point.longitude)
            value = (
                expected.station_id
                if expected and seen and seen[-1][1] == expected.station_id
                else -1
            )
            return value, worker_log

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
            concurrent_runs = list(pool.map(run_one, concurrent_points))
        concurrent_results = [value for value, _ in concurrent_runs]
        add(
            "L2-11-concurrent-20",
            20,
            sum(1 for value in concurrent_results if value > 0),
            "20 independent viewers",
        )
        evidence_logs["L2-11-concurrent-20"] = [
            event for _, worker_log in concurrent_runs for event in worker_log
        ]

        str_table = harness.str_table()
        (output / "str-table.txt").write_text(str_table, encoding="utf-8")
        zero_coordinates = [
            line
            for line in str_table.splitlines()
            if line.startswith("STR;") and (";0.000000;0.000000;" in line)
        ]
        add("STR-nonzero", 0, len(zero_coordinates), "uploaded 1005 coordinates entered STR")
        peak_metrics = harness.metrics.refresh()
        harness.stop_sources()
        cleanup_metrics = harness.metrics.refresh()
        harness.stderr.flush()
        stderr_size = harness.stderr_path.stat().st_size
        add(
            "cleanup",
            {"active_sources": 0, "stderr_bytes": 0},
            {
                "active_sources": int(cleanup_metrics.get("rcaster_active_sources", -1)),
                "stderr_bytes": stderr_size,
            },
            "sources released and stderr checked before shutdown",
        )
        for name, events in evidence_logs.items():
            write_jsonl(output / f"{name}.jsonl", events)
        write_json(output / "summary.json", {"cases": cases})
        write_json(output / "metrics.json", peak_metrics)
        write_json(output / "metrics-clean.json", cleanup_metrics)
        print(json.dumps({"output": str(output), "cases": cases}, indent=2))
        return 0 if all(case["pass"] for case in cases) else 2
    except Exception as error:  # noqa: BLE001
        for name, events in evidence_logs.items():
            write_jsonl(output / f"{name}.jsonl", events)
        write_json(
            output / "summary.json",
            {
                "cases": cases
                + results
                + [{"name": "fatal", "error": str(error), "pass": False}]
            },
        )
        print(f"NEAR E2E fatal: {type(error).__name__}: {error}")
        raise
    finally:
        harness.stop()
        if not args.keep:
            shutil.rmtree(harness.temp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
