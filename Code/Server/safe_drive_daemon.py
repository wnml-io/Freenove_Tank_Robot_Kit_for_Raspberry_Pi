#!/usr/bin/env python3
"""Safe driving daemon for Freenove tank.

Goals (MVP):
- Deadman-by-default + watchdog: if no recent DRIVE command, stop.
- E-stop latch: ESTOP forces stop until cleared.
- Single-writer lease: one active lease token at a time, with TTL.
- Clamp + basic ramp: clamp [-4095..4095], ramp toward target.
- Clean stop on exceptions + SIGTERM.

Protocol (newline-delimited, replies are single-line):
- PING
- STATUS
- LEASE_ACQUIRE <ttl_ms>
- LEASE_RELEASE <token>
- DRIVE <token> <left> <right>
- STOP <token>
- ESTOP
- CLEAR_ESTOP <token>

Notes:
- Uses a Unix domain socket (default /tmp/freenove_tank.sock)
- Intended to be run as a systemd user service.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import secrets
import signal
import time
from dataclasses import dataclass

from motor import tankMotor


MAX_DUTY = 4095


def clamp(v: int, lo: int = -MAX_DUTY, hi: int = MAX_DUTY) -> int:
    return max(lo, min(hi, int(v)))


@dataclass
class Lease:
    token: str
    expires_at: float

    def valid(self, now: float) -> bool:
        return now < self.expires_at


class SafeDrive:
    def __init__(
        self,
        watchdog_ms: int = 250,
        tick_hz: int = 50,
        ramp_per_tick: int = 260,
        lease_default_ms: int = 10_000,
    ):
        self.motor = tankMotor()

        self.watchdog_s = watchdog_ms / 1000.0
        self.tick_s = 1.0 / tick_hz
        self.ramp_per_tick = max(1, int(ramp_per_tick))
        self.lease_default_s = lease_default_ms / 1000.0

        self.estop = False
        self.lease: Lease | None = None

        self.target_l = 0
        self.target_r = 0
        self.actual_l = 0
        self.actual_r = 0
        self.last_drive_cmd_at = 0.0

        self._stop_event = asyncio.Event()

    def _stop_motors_now(self):
        self.target_l = 0
        self.target_r = 0
        self.actual_l = 0
        self.actual_r = 0
        try:
            self.motor.setMotorModel(0, 0)
        except Exception:
            pass

    def _check_lease(self, token: str, now: float) -> tuple[bool, str]:
        if self.lease is None:
            return False, "ERR no_lease"
        if not self.lease.valid(now):
            self.lease = None
            return False, "ERR lease_expired"
        if token != self.lease.token:
            return False, "ERR bad_lease"
        return True, "OK"

    async def run(self):
        try:
            await self._loop()
        finally:
            self._stop_motors_now()
            self.motor.close()

    async def _loop(self):
        while not self._stop_event.is_set():
            now = time.monotonic()

            # Expire lease
            if self.lease is not None and not self.lease.valid(now):
                self.lease = None

            # Watchdog: deadman stop if no recent drive cmd
            if not self.estop:
                if self.last_drive_cmd_at and (now - self.last_drive_cmd_at) > self.watchdog_s:
                    self.target_l = 0
                    self.target_r = 0

            # E-stop latch: always force target=0
            if self.estop:
                self.target_l = 0
                self.target_r = 0

            # Ramp actual toward target
            dl = self.target_l - self.actual_l
            dr = self.target_r - self.actual_r
            if dl:
                step = clamp(dl, -self.ramp_per_tick, self.ramp_per_tick)
                self.actual_l += step
            if dr:
                step = clamp(dr, -self.ramp_per_tick, self.ramp_per_tick)
                self.actual_r += step

            # Apply
            try:
                self.motor.setMotorModel(self.actual_l, self.actual_r)
            except Exception:
                # Fail safe
                self._stop_motors_now()
                raise

            await asyncio.sleep(self.tick_s)

    def stop(self):
        self._stop_event.set()

    async def handle_line(self, line: str) -> str:
        line = line.strip()
        if not line:
            return "ERR empty"

        parts = line.split()
        cmd = parts[0].upper()
        now = time.monotonic()

        if cmd == "PING":
            return "OK"

        if cmd == "STATUS":
            lease_ok = self.lease is not None and self.lease.valid(now)
            lease_exp = self.lease.expires_at if self.lease else 0.0
            return (
                "OK "
                f"estop={int(self.estop)} "
                f"lease={int(lease_ok)} "
                f"lease_exp_s={lease_exp:.3f} "
                f"target=({self.target_l},{self.target_r}) "
                f"actual=({self.actual_l},{self.actual_r}) "
                f"watchdog_ms={int(self.watchdog_s*1000)}"
            )

        if cmd == "LEASE_ACQUIRE":
            ttl_ms = int(parts[1]) if len(parts) > 1 else int(self.lease_default_s * 1000)
            ttl_s = max(0.5, min(60.0, ttl_ms / 1000.0))

            # If an unexpired lease exists, deny.
            if self.lease is not None and self.lease.valid(now):
                return "ERR lease_busy"

            token = secrets.token_urlsafe(16)
            self.lease = Lease(token=token, expires_at=now + ttl_s)
            return f"OK {token} {int(ttl_s*1000)}"

        if cmd == "LEASE_RELEASE":
            if len(parts) < 2:
                return "ERR usage"
            token = parts[1]
            ok, msg = self._check_lease(token, now)
            if not ok:
                return msg
            self.lease = None
            # Stop when releasing
            self._stop_motors_now()
            return "OK"

        if cmd == "ESTOP":
            self.estop = True
            self._stop_motors_now()
            return "OK"

        if cmd == "CLEAR_ESTOP":
            if len(parts) < 2:
                return "ERR usage"
            token = parts[1]
            ok, msg = self._check_lease(token, now)
            if not ok:
                return msg
            self.estop = False
            self.last_drive_cmd_at = 0.0
            self._stop_motors_now()
            return "OK"

        if cmd in ("DRIVE", "STOP"):
            if len(parts) < 2:
                return "ERR usage"
            token = parts[1]
            ok, msg = self._check_lease(token, now)
            if not ok:
                return msg
            if self.estop:
                return "ERR estop"

            if cmd == "STOP":
                self.target_l = 0
                self.target_r = 0
                self.last_drive_cmd_at = now
                return "OK"

            # DRIVE
            if len(parts) < 4:
                return "ERR usage"
            left = clamp(int(parts[2]))
            right = clamp(int(parts[3]))
            self.target_l = left
            self.target_r = right
            self.last_drive_cmd_at = now
            # keep lease alive when driving
            if self.lease is not None:
                self.lease.expires_at = max(self.lease.expires_at, now + self.lease_default_s)
            return "OK"

        return "ERR unknown"


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, sd: SafeDrive):
    try:
        while True:
            data = await reader.readline()
            if not data:
                break
            try:
                resp = await sd.handle_line(data.decode("utf-8", errors="replace"))
            except Exception as e:
                resp = f"ERR exception {type(e).__name__}"
            writer.write((resp + "\n").encode("utf-8"))
            await writer.drain()
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def amain():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sock", default="/tmp/freenove_tank.sock")
    ap.add_argument("--watchdog-ms", type=int, default=250)
    ap.add_argument("--tick-hz", type=int, default=50)
    ap.add_argument("--ramp-per-tick", type=int, default=260)
    ap.add_argument("--lease-default-ms", type=int, default=10_000)
    args = ap.parse_args()

    # Ensure old socket removed
    try:
        os.unlink(args.sock)
    except FileNotFoundError:
        pass

    sd = SafeDrive(
        watchdog_ms=args.watchdog_ms,
        tick_hz=args.tick_hz,
        ramp_per_tick=args.ramp_per_tick,
        lease_default_ms=args.lease_default_ms,
    )

    loop = asyncio.get_running_loop()

    def _sig(*_):
        sd.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _sig)
        except NotImplementedError:
            signal.signal(sig, lambda *_: sd.stop())

    server = await asyncio.start_unix_server(lambda r, w: handle_client(r, w, sd), path=args.sock)

    # Restrict socket perms: user rw only
    try:
        os.chmod(args.sock, 0o600)
    except Exception:
        pass

    async with server:
        await asyncio.gather(server.serve_forever(), sd.run())


if __name__ == "__main__":
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        pass
