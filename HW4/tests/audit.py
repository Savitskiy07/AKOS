#!/usr/bin/env python3
"""Additional requirement-focused review of the delivered programs."""
import argparse
import random
import signal
import socket
import struct
import unittest

import integration as base
from integration import Harness, eventually, fields, verify_history, verify_observer, verify_snapshot


def complete(peer, pictures, already_requested=False):
    for picture in range(1, pictures + 1):
        if picture != 1 or not already_requested:
            peer.send(f"VIEW {picture}\n")
        peer.until(f"DONE {picture}")
    peer.send("LEAVE\n")
    assert peer.line() == "BYE"


class Audit(unittest.TestCase):
    def harness(self, suffix="", **options):
        h = Harness(self._testMethodName + suffix, **options)
        self.addCleanup(h.cleanup)
        return h

    def test_01_ten_observers_and_40_reconnections(self):
        h = self.harness(visitors=61, capacity=8, pictures=6, caps=[1, 2, 3, 2, 1, 4], duration=5)
        observers = {f"observer-{i}": h.observer(f"observer-{i}") for i in range(10)}
        eventually(lambda: all("SNAPSHOT_END" in h.text(name) for name in observers))
        job = h.visitors_async(range(1, 61))
        eventually(lambda: "type=VIEW_START" in h.text())
        for i in range(40):
            peer = h.peer("HELLO OBSERVER\n")
            snapshot = peer.until("SNAPSHOT_END")
            verify_snapshot(h, snapshot)
            (h.directory / f"snapshot-{i}.log").write_text("\n".join(snapshot) + "\n")
            # Include abrupt TCP resets, not just an orderly EOF.
            if i % 2:
                peer.socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            peer.close()
            if i < 5:
                name = f"observer-{i}"
                observers.pop(name).terminate()
                replacement = f"reconnected-{i}"
                observers[replacement] = h.observer(replacement)
        h.join(job)
        h.join(h.visitors_async([61]))
        h.finish()
        for name, process in observers.items():
            self.assertEqual(process.wait(timeout=10), 0)
            verify_observer(h, name)
        verify_history(h)

    def test_02_snapshot_contains_mixed_states(self):
        h = self.harness(visitors=5, capacity=3, pictures=3, duration=500)
        first = h.peer("HELLO VISITOR 1\n")
        first.until("ENTERED")
        complete(first, 3)
        second = h.peer("HELLO VISITOR 2\n")
        second.until("ENTERED")
        second.send("VIEW 1\n")
        second.until("START 1")
        third = h.peer("HELLO VISITOR 3\n")
        third.until("ENTERED")
        third.send("VIEW 1\n")
        third.until("WAIT_PICTURE 1")
        fourth = h.peer("HELLO VISITOR 4\n")
        fourth.until("ENTERED")
        fifth = h.peer("HELLO VISITOR 5\n")
        fifth.until("WAIT_ENTRY")
        observer = h.peer("HELLO OBSERVER\n")
        snapshot = observer.until("SNAPSHOT_END")
        (h.directory / "mixed-snapshot.log").write_text("\n".join(snapshot) + "\n")
        states = {fields(line)["id"]: fields(line)["state"] for line in snapshot if line.startswith("VISITOR ")}
        self.assertEqual(states, {"1": "FINISHED", "2": "VIEWING", "3": "PICTURE_QUEUE", "4": "INSIDE", "5": "ENTRY_QUEUE"})
        verify_snapshot(h, snapshot)
        observer.close()
        complete(second, 3, already_requested=True)
        complete(third, 3, already_requested=True)
        complete(fourth, 3)
        fifth.until("ENTERED")
        complete(fifth, 3)
        h.finish()
        verify_history(h)

    def test_03_300_simultaneous_c_visitors(self):
        h = self.harness(visitors=300, capacity=300, pictures=1, caps=[300], duration=1500)
        observer = h.observer("observer")
        eventually(lambda: "SNAPSHOT_END" in h.text("observer"))
        processes = []
        h.server.send_signal(signal.SIGSTOP)
        try:
            for visitor in range(1, 301):
                processes.append(h.spawn("gallery_visitor", f"visitor-{visitor}", [
                    "--host", h.client_host, "--port", str(h.port), "--id", str(visitor)]))
        finally:
            h.server.send_signal(signal.SIGCONT)
        for process in processes:
            self.assertEqual(process.wait(timeout=20), 0)
        h.finish()
        self.assertEqual(observer.wait(timeout=5), 0)
        verify_history(h)
        verify_observer(h, "observer")
        peak = max(int(fields(line)["inside"]) for line in h.text().splitlines() if line.startswith("EVENT "))
        self.assertEqual(peak, 300)
        (h.directory / "concurrency.txt").write_text(f"peak_inside={peak}; independent_C_processes=300\n")

    def test_04_handshake_timeout_does_not_stop_visitors(self):
        h = self.harness(visitors=2, pictures=1)
        silent = h.peer("HEL")
        h.join(h.visitors_async([1]))
        self.assertEqual(silent.line(), "")
        h.join(h.visitors_async([2]))
        h.finish()
        verify_history(h)

    def test_05_disconnect_in_picture_queue(self):
        h = self.harness(visitors=4, capacity=3, pictures=1, duration=300)
        first = h.peer("HELLO VISITOR 1\n")
        first.until("ENTERED")
        first.send("VIEW 1\n")
        first.until("START 1")
        second = h.peer("HELLO VISITOR 2\n")
        second.until("ENTERED")
        second.send("VIEW 1\n")
        second.until("WAIT_PICTURE 1")
        third = h.peer("HELLO VISITOR 3\n")
        third.until("ENTERED")
        third.send("VIEW 1\n")
        third.until("WAIT_PICTURE 1")
        second.close()
        eventually(lambda: "type=ABORT visitor=2" in h.text())
        complete(first, 1, already_requested=True)
        complete(third, 1, already_requested=True)
        h.join(h.visitors_async([4]))
        h.finish(expected=2)
        verify_history(h, complete=False)
        self.assertIn("finished=3 aborted=1 remaining=0 status=incomplete", h.text())

    def test_06_wildcard_bind_and_different_ipv4_address(self):
        h = self.harness(visitors=10, bind_host="0.0.0.0", client_host="127.0.0.2")
        observer = h.observer("observer")
        eventually(lambda: "SNAPSHOT_END" in h.text("observer"))
        h.join(h.visitors_async())
        h.finish()
        self.assertEqual(observer.wait(timeout=5), 0)
        verify_history(h)
        verify_observer(h, "observer")

    def test_07_ipv6(self):
        h = self.harness(visitors=10, bind_host="::1", client_host="::1")
        observer = h.observer("observer")
        eventually(lambda: "SNAPSHOT_END" in h.text("observer"))
        h.join(h.visitors_async())
        h.finish()
        self.assertEqual(observer.wait(timeout=5), 0)
        verify_history(h)
        verify_observer(h, "observer")

    def test_08_24_parameter_combinations(self):
        rng = random.Random(20261125)
        for case in range(24):
            pictures = rng.randint(1, 12)
            h = self.harness(suffix=f"-{case}", visitors=rng.randint(1, 24), capacity=rng.randint(1, 15),
                             pictures=pictures, caps=[rng.randint(1, 7) for _ in range(pictures)],
                             strategy="sequential" if case % 2 else "random", duration=case % 3,
                             seed=[0, 1, 4294967295, rng.randrange(2**32)][case % 4])
            h.join(h.visitors_async())
            h.finish()
            verify_history(h)
            h.cleanup()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin", default="build")
    parser.add_argument("tests", nargs="*")
    args = parser.parse_args()
    base.BIN = (base.ROOT / args.bin).resolve()
    base.RESULTS = base.ROOT / "test-results" / ("audit-sanitize" if "sanitize" in args.bin else "audit-release")
    base.RESULTS.mkdir(parents=True, exist_ok=True)
    unittest.main(argv=[__file__, *args.tests], verbosity=2)
