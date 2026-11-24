#!/usr/bin/env python3
"""Fetch CRTK.NET's public STR table once and store a reproducible JSON snapshot."""

from __future__ import annotations

import argparse
import json
import socket
from datetime import datetime, timezone
from pathlib import Path


def fetch(host: str, port: int, timeout: float) -> tuple[bytes, dict[str, str]]:
    request = (
        f"GET / HTTP/1.0\r\n"
        f"Host: {host}:{port}\r\n"
        "User-Agent: RCaster-NEAR-e2e/1.0\r\n"
        "Accept: text/plain\r\n"
        "Connection: close\r\n\r\n"
    ).encode("ascii")
    with socket.create_connection((host, port), timeout=timeout) as conn:
        conn.settimeout(timeout)
        conn.sendall(request)
        response = bytearray()
        while True:
            chunk = conn.recv(65536)
            if not chunk:
                break
            response.extend(chunk)
    head, separator, body = bytes(response).partition(b"\r\n\r\n")
    if not separator:
        raise RuntimeError("CRTK.NET response has no HTTP header terminator")
    headers: dict[str, str] = {}
    for line in head.decode("latin-1").split("\r\n")[1:]:
        if ":" in line:
            name, value = line.split(":", 1)
            headers[name.strip().lower()] = value.strip()
    return body, headers


def parse(body: bytes) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for raw in body.splitlines():
        fields = raw.decode("utf-8", "replace").rstrip("\r").split(";")
        if len(fields) < 11 or fields[0] != "STR" or fields[3] != "RTCM3":
            continue
        try:
            latitude = float(fields[9])
            longitude = float(fields[10])
        except ValueError:
            continue
        if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
            continue
        records.append(
            {
                "mountpoint": fields[1],
                "identifier": fields[2],
                "format": fields[3],
                "latitude": latitude,
                "longitude": longitude,
                "country": fields[8],
                "network": fields[7],
                "raw": raw.decode("utf-8", "replace"),
            }
        )
    records.sort(key=lambda item: str(item["mountpoint"]))
    return records


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="crtk.net")
    parser.add_argument("--port", type=int, default=2101)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).parent / "data" / "crtk-str-20261125.json",
    )
    args = parser.parse_args()

    body, headers = fetch(args.host, args.port, args.timeout)
    records = parse(body)
    if len(records) < 100:
        raise RuntimeError(f"unexpectedly small CRTK RTCM3 table: {len(records)}")
    fetched_at = datetime.now(timezone.utc).isoformat()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(
            {
                "source": f"http://{args.host}:{args.port}/",
                "fetched_at": fetched_at,
                "http_headers": headers,
                "body_bytes": len(body),
                "total_rtcm3": len(records),
                "records": records,
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"{args.output}: {len(records)} RTCM3 records, fetched {fetched_at}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
