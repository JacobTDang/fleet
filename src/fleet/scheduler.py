"""Scheduling primitives: jittered next-run times and per-domain politeness.

Jitter exists so hundreds of watchers never align on the same instant; the
DomainGate exists because the real scale limit is being a polite client —
same-domain requests are serialized and spaced by a minimum gap, while
unrelated domains proceed concurrently.
"""

import asyncio
import time
from contextlib import asynccontextmanager

JITTER = 0.10


def next_run(interval_seconds, *, now, rng):
    return now + interval_seconds * rng.uniform(1 - JITTER, 1 + JITTER)


class DomainGate:
    def __init__(self, min_gap, *, clock=time.monotonic, sleep=asyncio.sleep):
        self._min_gap = min_gap
        self._clock = clock
        self._sleep = sleep
        self._locks = {}
        self._last_done = {}

    @asynccontextmanager
    async def slot(self, domain):
        lock = self._locks.setdefault(domain, asyncio.Lock())
        async with lock:
            last = self._last_done.get(domain)
            if last is not None:
                wait = last + self._min_gap - self._clock()
                if wait > 0:
                    await self._sleep(wait)
            try:
                yield
            finally:
                # gap is measured from when the previous request *finished*
                self._last_done[domain] = self._clock()
