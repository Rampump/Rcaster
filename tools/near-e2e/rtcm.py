#!/usr/bin/env python3
"""Minimal RTCM3 helpers for the offline NEAR end-to-end harness."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class Frame:
    raw: bytes
    message_number: int


def crc24q(data: bytes, initial: int = 0) -> int:
    crc = initial & 0xFFFFFF
    for byte in data:
        crc ^= byte << 16
        for _ in range(8):
            crc <<= 1
            if crc & 0x1000000:
                crc ^= 0x1864CFB
    return crc & 0xFFFFFF


def frames(data: bytes) -> list[Frame]:
    result: list[Frame] = []
    offset = 0
    while offset + 6 <= len(data):
        if data[offset] != 0xD3:
            offset += 1
            continue
        length = ((data[offset + 1] & 0x03) << 8) | data[offset + 2]
        end = offset + 3 + length + 3
        if end > len(data):
            break
        raw = data[offset:end]
        payload = raw[3:-3]
        if payload and crc24q(raw[:-3]) == int.from_bytes(raw[-3:], "big"):
            message_number = int.from_bytes(payload[:2], "big") >> 4
            result.append(Frame(raw=raw, message_number=message_number))
            offset = end
        else:
            offset += 1
    return result


def first_epoch(data: bytes) -> list[Frame]:
    parsed = frames(data)
    first = next(
        (index for index, frame in enumerate(parsed) if frame.message_number == 1005),
        None,
    )
    if first is None:
        raise ValueError("capture contains no RTCM3 1005 frame")
    epoch = [parsed[first]]
    for frame in parsed[first + 1 :]:
        if frame.message_number == 1005:
            break
        epoch.append(frame)
    return epoch


def station_id(frame: Frame) -> int | None:
    if frame.message_number != 1005:
        return None
    payload = frame.raw[3:-3]
    return _bits(payload, 12, 12)


def geodetic_to_ecef(latitude_deg: float, longitude_deg: float, height_m: float = 0.0) -> tuple[float, float, float]:
    a = 6378137.0
    f = 1.0 / 298.257223563
    e2 = f * (2.0 - f)
    latitude = math.radians(latitude_deg)
    longitude = math.radians(longitude_deg)
    sin_lat = math.sin(latitude)
    cos_lat = math.cos(latitude)
    n = a / math.sqrt(1.0 - e2 * sin_lat * sin_lat)
    x = (n + height_m) * cos_lat * math.cos(longitude)
    y = (n + height_m) * cos_lat * math.sin(longitude)
    z = (n * (1.0 - e2) + height_m) * sin_lat
    return x, y, z


def rewrite_1005(frame: Frame, new_station_id: int, latitude_deg: float, longitude_deg: float) -> bytes:
    if frame.message_number != 1005:
        raise ValueError("only RTCM3 1005 can be rewritten")
    x, y, z = geodetic_to_ecef(latitude_deg, longitude_deg)
    payload = bytearray(frame.raw[3:-3])
    _set_bits(payload, 12, 12, new_station_id)
    _set_bits(payload, 31, 38, int(round(x * 10_000)) & ((1 << 38) - 1))
    _set_bits(payload, 69, 38, int(round(y * 10_000)) & ((1 << 38) - 1))
    _set_bits(payload, 107, 38, int(round(z * 10_000)) & ((1 << 38) - 1))
    head = bytes((0xD3, (len(payload) >> 8) & 0x03, len(payload) & 0xFF)) + payload
    return head + crc24q(head).to_bytes(3, "big")


def payload_for_station(
    epoch: list[Frame],
    station: int,
    latitude_deg: float,
    longitude_deg: float,
) -> bytes:
    output = bytearray()
    for frame in epoch:
        if frame.message_number == 1005:
            output.extend(
                rewrite_1005(frame, station, latitude_deg, longitude_deg)
            )
        else:
            output.extend(frame.raw)
    return bytes(output)


def _bits(payload: bytes, start: int, length: int) -> int:
    value = int.from_bytes(payload, "big")
    shift = len(payload) * 8 - start - length
    return (value >> shift) & ((1 << length) - 1)


def _set_bits(payload: bytearray, start: int, length: int, value: int) -> None:
    total = len(payload) * 8
    shift = total - start - length
    mask = ((1 << length) - 1) << shift
    whole = int.from_bytes(payload, "big")
    whole = (whole & ~mask) | ((value << shift) & mask)
    payload[:] = whole.to_bytes(len(payload), "big")
