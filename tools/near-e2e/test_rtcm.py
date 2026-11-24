#!/usr/bin/env python3

import math
import unittest

import rtcm


def synthetic_1005() -> rtcm.Frame:
    payload = bytearray(19)
    rtcm._set_bits(payload, 0, 12, 1005)
    head = bytes((0xD3, (len(payload) >> 8) & 0x03, len(payload) & 0xFF)) + payload
    raw = head + rtcm.crc24q(head).to_bytes(3, "big")
    return rtcm.frames(raw)[0]


class Rtcm1005Tests(unittest.TestCase):
    def test_rewrite_uses_0_1_mm_ecef_units_and_station_id(self):
        frame = synthetic_1005()
        latitude = 47.538
        longitude = 22.774
        rewritten = rtcm.frames(rtcm.rewrite_1005(frame, 577, latitude, longitude))[0]
        self.assertEqual(rtcm.station_id(rewritten), 577)

        payload = rewritten.raw[3:-3]

        def signed(start: int, length: int) -> int:
            value = rtcm._bits(payload, start, length)
            if value & (1 << (length - 1)):
                value -= 1 << length
            return value

        x = signed(31, 38) / 10_000.0
        y = signed(69, 38) / 10_000.0
        z = signed(107, 38) / 10_000.0
        p = math.hypot(x, y)
        decoded_latitude = math.atan2(z, p * (1.0 - 6.69437999014e-3))
        for _ in range(5):
            n = 6_378_137.0 / math.sqrt(
                1.0 - 6.69437999014e-3 * math.sin(decoded_latitude) ** 2
            )
            height = p / math.cos(decoded_latitude) - n
            decoded_latitude = math.atan2(
                z,
                p
                * (
                    1.0
                    - 6.69437999014e-3
                    * n
                    / (n + height)
                ),
            )
        self.assertAlmostEqual(math.degrees(decoded_latitude), latitude, places=6)
        self.assertAlmostEqual(math.degrees(math.atan2(y, x)), longitude, places=6)


if __name__ == "__main__":
    unittest.main()
