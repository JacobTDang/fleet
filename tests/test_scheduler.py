import asyncio
import random

from fleet.scheduler import DomainGate, next_run


def test_next_run_stays_within_10pct_jitter_bounds():
    rng = random.Random(42)
    now = 1_000_000.0
    samples = [next_run(300, now=now, rng=rng) for _ in range(200)]
    assert all(now + 270 <= s <= now + 330 for s in samples)
    assert min(samples) != max(samples)  # it actually jitters


def test_next_run_is_deterministic_with_seeded_rng():
    a = next_run(60, now=0.0, rng=random.Random(7))
    b = next_run(60, now=0.0, rng=random.Random(7))
    assert a == b


class FakeTime:
    """Deterministic clock+sleep so gap logic is tested without wall-clock flake."""

    def __init__(self):
        self.t = 0.0
        self.sleeps = []

    def clock(self):
        return self.t

    async def sleep(self, d):
        self.sleeps.append(round(d, 6))
        self.t += d


async def test_min_gap_enforced_between_same_domain_requests():
    ft = FakeTime()
    gate = DomainGate(min_gap=2.0, clock=ft.clock, sleep=ft.sleep)
    async with gate.slot("a.com"):
        pass
    async with gate.slot("a.com"):
        pass
    assert ft.sleeps == [2.0]  # first request immediate, second waited the gap


async def test_no_gap_between_different_domains():
    ft = FakeTime()
    gate = DomainGate(min_gap=2.0, clock=ft.clock, sleep=ft.sleep)
    async with gate.slot("a.com"):
        pass
    async with gate.slot("b.com"):
        pass
    assert ft.sleeps == []


async def test_gap_measured_from_end_of_previous_request():
    ft = FakeTime()
    gate = DomainGate(min_gap=2.0, clock=ft.clock, sleep=ft.sleep)
    async with gate.slot("a.com"):
        ft.t += 5.0  # a slow request
    ft.t += 1.5  # only 1.5s idle since it finished
    async with gate.slot("a.com"):
        pass
    assert ft.sleeps == [0.5]  # tops up to the 2.0 gap, not a fresh 2.0


async def test_same_domain_never_overlaps():
    gate = DomainGate(min_gap=0.0)
    events = []

    async def task(name):
        async with gate.slot("same.com"):
            events.append(("enter", name))
            await asyncio.sleep(0.01)
            events.append(("exit", name))

    await asyncio.gather(task("x"), task("y"))
    assert events[0][0] == "enter" and events[1][0] == "exit"  # no interleaving
    assert events[2][0] == "enter" and events[3][0] == "exit"


async def test_different_domains_run_concurrently():
    gate = DomainGate(min_gap=10.0)  # a big gap must not couple unrelated domains
    running = set()
    saw_overlap = False

    async def task(domain):
        nonlocal saw_overlap
        async with gate.slot(domain):
            running.add(domain)
            await asyncio.sleep(0.02)
            if len(running) == 2:
                saw_overlap = True
            running.discard(domain)

    await asyncio.gather(task("a.com"), task("b.com"))
    assert saw_overlap
