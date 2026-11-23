# NTRIP Compatibility Matrix

This matrix records only observations backed by a specific test run. A client
or receiver that has not been exercised is marked `NOT VERIFIED`, even when the
protocol implementation is expected to work.

## Validated Paths

| Client or source | Transport | Test | Result |
|---|---|---|---|
| Real RTK base module on `COM3` (USB-SERIAL CH340) | Serial 460800 baud to RCaster NTRIP2/TCP | 15-second source upload and independent subscriber | PASS |
| RTKLIB `str2str` 2.4.2 p13 | NTRIP2 source and subscriber | Synthetic RTCM3 1005 round trip | PASS |
| Raw NTRIP2 source/subscriber | TCP | Real module stream through standalone Caster | PASS |
| u-center 25.03 NTRIP Client | u-center NTRIP client to RCaster | Real C63 source, sourcetable selection, live subscription | PASS |
| BKG BNC 2.13.7 | BNC NTRIP2 client to RCaster | Real C63 source, raw capture, decoded RTCM logging | PASS |
| Lefebure NTRIP Client 2017.07.27 | Lefebure NTRIP client to RCaster | Real C63 source, connected and received-byte growth | PASS |
| RTKLIB Windows `strsvr` 2.5.0 | strsvr NTRIP client to RCaster | Manual GUI setup, real C63 source, live byte growth | PASS |

The real module run used RCaster release SHA-256
`4d35a6f5a7f0196627de109fcca8021628077284f46c345462e4733f17f45154`.
The USB descriptor identifies the serial bridge as `USB-SERIAL CH340`; it does
not expose a receiver model. The module did not emit message 1033 during this
run, so its receiver/firmware identity could not be recorded from RTCM metadata.

Observed real-module result:

- Serial format: RTCM3 at 460800 baud.
- Duration: 15 seconds.
- Source bytes: 15,650.
- Subscriber bytes: 15,138.
- Source-valid RTCM3 frames: 128.
- Subscriber-valid RTCM3 frames: 123.
- Message types on both paths: 1005, 1074, 1084, 1094, 1114, 1124, 1134.
- NTRIP2 source response: `HTTP/1.1 200 OK`.
- NTRIP2 subscriber response: `HTTP/1.1 200 OK`.

u-center 25.03 was connected through its NTRIP Client to the same standalone
Caster. It loaded the sourcetable, selected `C63`, and reported
`NTRIP client: 127.0.0.1:2101`. In a three-second socket sample, the
u-center connection's received-byte counter increased from 29,873 to 32,624
while the real source connection increased from 160,459 to 163,210 bytes.
u-center requires a receiver serial output; an idle `COM1` was selected so the
test did not write corrections back into the real receiver's `COM3` stream.

The official installers were downloaded and their hashes recorded:

| Client | File | SHA-256 | Digital signature |
|---|---|---|---|
| BKG BNC 2.13.7 | `BNC-2.13.7-Windows.msi` | `5A22015247D015692FB538D47B55854969CFC3B2540E4B008BA8846934F9B536` | Not signed |
| Lefebure NTRIP Client | `NTRIPClient.exe` | `577149DF7BFFC16390F10B809B37C565E76260606F910124E498203B28F72ECF` | Not signed |

Both were executed only after explicit user confirmation.

BNC 2.13.7 ran in its documented `--conf <file> --nw` mode. Its log reported
continuous C63 reception of RTCM3 1005, 1074, 1084, 1094, 1114, 1124, and
1134. The final raw output was 41,550 bytes and 287 CRC-valid RTCM3 frames;
the raw capture contained 1005, 1074, 1094, 1114, 1124, and 1134, while the
decoded log also reported 1084.

Lefebure NTRIP Client 2017.07.27 loaded `C63` from the RCaster sourcetable.
After Connect, its log reported `NTRIP Client is Connected, Waiting for Data`
and then `NTRIP Client is receiving data`. The received-byte display grew
from 341 to 1,200 and then 6,360 bytes.

## Not Verified

| Client or source | Status | Reason |
|---|---|---|
| Additional receiver models | NOT VERIFIED | Only the local CH340-connected RTK base module was available |

Commercial or unavailable clients must remain marked unverified. Do not claim
support based only on the shared NTRIP2/TLS paths exercised by RCaster, RTKLIB,
and the local module.
