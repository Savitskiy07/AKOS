#!/usr/bin/env python3
"""Black-box tests of real TCP processes, including an independent event model."""
import argparse
import concurrent.futures
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import threading
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
BIN = ROOT / "build"
RESULTS = ROOT / "test-results" / "release"


def fields(line):
    return dict(re.findall(r"(\w+)=([^\s]+)", line.split(" | ")[0]))


def eventually(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.01)
    raise AssertionError("Timed out waiting for condition")


class Peer:
    def __init__(self, port, receive_buffer=None, host="127.0.0.1"):
        self.socket = socket.socket(socket.AF_INET6 if ":" in host else socket.AF_INET)
        if receive_buffer:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, receive_buffer)
        self.socket.settimeout(10)
        self.socket.connect((host, port))
        self.stream = self.socket.makefile("rb")

    def send(self, data):
        self.socket.sendall(data.encode() if isinstance(data, str) else data)

    def line(self):
        return self.stream.readline().decode().rstrip("\n")

    def until(self, prefix):
        lines = []
        for _ in range(200000):
            line = self.line()
            if not line:
                raise AssertionError(f"EOF before {prefix}")
            lines.append(line)
            if line.startswith(prefix):
                return lines
        raise AssertionError("Too many lines")

    def close(self):
        self.stream.close()
        self.socket.close()


class Harness:
    def __init__(self, name, visitors=6, capacity=2, pictures=3, caps=None, strategy="random", duration=2,
                 bind_host="127.0.0.1", client_host="127.0.0.1", seed=16):
        self.directory = RESULTS / name
        self.directory.mkdir(parents=True, exist_ok=True)
        self.total, self.capacity, self.pictures = visitors, capacity, pictures
        self.client_host = client_host
        self.caps = caps or [1] * pictures
        with socket.socket(socket.AF_INET6 if ":" in bind_host else socket.AF_INET) as probe:
            probe.bind((bind_host, 0))
            self.port = probe.getsockname()[1]
        self.processes, self.handles, self.peers, self.jobs = [], [], [], []
        self.server = self.spawn("gallery_server", "server", [
            "--host", bind_host, "--port", str(self.port),
            "--visitors", str(visitors), "--capacity", str(capacity),
            "--pictures", str(pictures), "--picture-capacities", ",".join(map(str, self.caps)),
            "--strategy", strategy, "--seed", str(seed), "--view-min-ms", str(duration),
            "--view-max-ms", str(duration)])
        eventually(lambda: "READY " in self.text("server"))

    def spawn(self, executable, name, args):
        out = open(self.directory / (name + ".log"), "wb")
        err = open(self.directory / (name + ".err"), "wb")
        self.handles.extend([out, err])
        process = subprocess.Popen([str(BIN / executable), *args], stdout=out, stderr=err)
        self.processes.append(process)
        return process

    def text(self, name="server", error=False):
        return (self.directory / (name + (".err" if error else ".log"))).read_text()

    def peer(self, role, receive_buffer=None):
        peer = Peer(self.port, receive_buffer, self.client_host)
        self.peers.append(peer)
        peer.send(role)
        return peer

    def observer(self, name):
        return self.spawn("gallery_observer", name, ["--host", self.client_host, "--port", str(self.port)])

    def visitors_async(self, ids=None, workers=32):
        ids = list(ids if ids is not None else range(1, self.total + 1))
        results, errors = [], []

        def one(visitor):
            p = self.spawn("gallery_visitor", f"visitor-{visitor}", [
                "--host", self.client_host, "--port", str(self.port), "--id", str(visitor)])
            code = p.wait(timeout=60)
            if code:
                raise AssertionError(f"Visitor {visitor}: exit {code}; {self.text(f'visitor-{visitor}', True)}")
            return visitor

        def work():
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                    results.extend(pool.map(one, ids))
            except BaseException as error:
                errors.append(error)

        thread = threading.Thread(target=work)
        thread.start()
        job = (thread, results, errors, len(ids))
        self.jobs.append(job)
        return job

    @staticmethod
    def join(job):
        thread, results, errors, expected = job
        thread.join(timeout=90)
        assert not thread.is_alive(), "Visitor launcher timed out"
        if errors:
            raise errors[0]
        assert len(results) == expected

    def finish(self, expected=0):
        code = self.server.wait(timeout=20)
        assert code == expected, f"Server exit {code}; {self.text(error=True)}"
        for path in self.directory.glob("*.err"):
            text = path.read_text()
            assert "Sanitizer" not in text and "runtime error:" not in text, text

    def cleanup(self):
        for peer in self.peers:
            peer.close()
        # Stop the server first so any blocked clients wake up; then reap all.
        if self.server.poll() is None:
            self.server.terminate()
        for process in self.processes:
            if process.poll() is None:
                process.terminate()
        for thread, _, _, _ in self.jobs:
            thread.join(timeout=5)
        for process in self.processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for handle in self.handles:
            handle.close()


def verify_history(h, complete=True):
    """Reconstruct occupancy and visits independently of server counters/asserts."""
    states, chosen, seen = {}, {}, {}
    active = [set() for _ in range(h.pictures)]
    entries, waiting = [], [[] for _ in range(h.pictures)]
    seq = 0
    for line in h.text().splitlines():
        if not line.startswith("EVENT "):
            continue
        f = fields(line)
        assert int(f["seq"]) == seq + 1
        seq += 1
        event, visitor, picture = f["type"], int(f["visitor"]), int(f["picture"]) - 1
        if event == "ARRIVAL":
            assert visitor not in states
            states[visitor], seen[visitor] = "entry", set()
            entries.append(visitor)
        elif event == "ENTER":
            assert states[visitor] == "entry" and entries.pop(0) == visitor
            states[visitor] = "inside"
        elif event == "CHOOSE":
            assert states[visitor] == "inside" and picture not in seen[visitor]
            assert 0 <= picture < h.pictures
            states[visitor], chosen[visitor] = "waiting", picture
            waiting[picture].append(visitor)
        elif event == "VIEW_START":
            assert states[visitor] == "waiting" and chosen[visitor] == picture
            assert waiting[picture].pop(0) == visitor, "Picture queue is not FIFO"
            assert all(visitor not in group for group in active)
            active[picture].add(visitor)
            states[visitor] = "viewing"
        elif event == "VIEW_END":
            assert states[visitor] == "viewing" and chosen[visitor] == picture
            assert visitor in active[picture] and picture not in seen[visitor]
            active[picture].remove(visitor)
            seen[visitor].add(picture)
            states[visitor] = "inside"
        elif event == "EXIT":
            assert states[visitor] == "inside" and len(seen[visitor]) == h.pictures
            states[visitor] = "finished"
        elif event == "ABORT":
            if states[visitor] == "entry":
                entries.remove(visitor)
            if states[visitor] == "waiting":
                waiting[chosen[visitor]].remove(visitor)
            for group in active:
                group.discard(visitor)
            states[visitor] = "aborted"
        inside = sum(state in ("inside", "waiting", "viewing") for state in states.values())
        queued = sum(state == "entry" for state in states.values())
        assert 0 <= inside <= h.capacity and inside == int(f["inside"])
        assert queued == int(f["queued"])
        assert sum(state == "finished" for state in states.values()) == int(f["finished"])
        assert sum(state == "aborted" for state in states.values()) == int(f["aborted"])
        for p, group in enumerate(active):
            assert len(group) <= h.caps[p]
        if picture >= 0:
            assert len(active[picture]) == int(f["viewers"])
        if visitor:
            assert sum(1 << p for p in seen[visitor]) == int(f["seen"], 16)
    if complete:
        assert len(states) == h.total and all(state == "finished" for state in states.values())
        assert all(not group for group in active)
        assert f"finished={h.total} aborted=0 remaining=0 status=complete" in h.text()


def verify_snapshot(h, lines):
    begin = next(i for i, line in enumerate(lines) if line.startswith("SNAPSHOT_BEGIN "))
    end = next(i for i, line in enumerate(lines) if line.startswith("SNAPSHOT_END "))
    header = fields(lines[begin])
    sequence = int(header["seq"])
    assert fields(lines[end])["seq"] == header["seq"]
    assert all(not line.startswith("EVENT ") for line in lines[begin:end])
    snapshots = [fields(line) for line in lines[begin:end] if line.startswith("VISITOR ")]
    pictures = [fields(line) for line in lines[begin:end] if line.startswith("PICTURE ")]
    assert len(pictures) == h.pictures
    assert len(snapshots) == int(header["arrived"])
    assert sum(v["state"] in ("INSIDE", "VIEWING", "PICTURE_QUEUE") for v in snapshots) == int(header["inside"])
    for p in pictures:
        assert sum(v["state"] == "VIEWING" and v["picture"] == p["id"] for v in snapshots) == int(p["used"])
        assert int(p["capacity"]) == h.caps[int(p["id"]) - 1]
    # Rebuild the exact state at the advertised sequence, not only occupancy.
    expected_visitors = {}
    for event_line in h.text().splitlines():
        if not event_line.startswith("EVENT "):
            continue
        f = fields(event_line)
        if int(f["seq"]) > sequence:
            break
        visitor = f["visitor"]
        if f["type"] == "ARRIVAL":
            expected_visitors[visitor] = {"id": visitor, "state": "ENTRY_QUEUE", "picture": "0", "seen": "0000000000000000"}
        if visitor == "0":
            continue
        v = expected_visitors[visitor]
        if f["type"] == "ENTER":
            v["state"] = "INSIDE"
        elif f["type"] == "CHOOSE":
            v.update(state="PICTURE_QUEUE", picture=f["picture"])
        elif f["type"] == "VIEW_START":
            v["state"] = "VIEWING"
        elif f["type"] == "VIEW_END":
            v.update(state="INSIDE", picture="0", seen=f["seen"])
        elif f["type"] in ("EXIT", "ABORT"):
            v.update(state="FINISHED" if f["type"] == "EXIT" else "ABORTED", picture="0")
    assert {v["id"]: v for v in snapshots} == expected_visitors, "Snapshot differs from actual history"
    assert sum(v["state"] == "ENTRY_QUEUE" for v in snapshots) == int(header["queued"])
    assert sum(v["state"] == "FINISHED" for v in snapshots) == int(header["finished"])
    assert sum(v["state"] == "ABORTED" for v in snapshots) == int(header["aborted"])
    return sequence, end


def verify_observer(h, name):
    lines = h.text(name).splitlines()
    sequence, end = verify_snapshot(h, lines)
    events = [line for line in lines[end + 1:] if line.startswith("EVENT ")]
    expected = [line for line in h.text().splitlines()
                if line.startswith("EVENT ") and int(fields(line)["seq"]) > sequence]
    assert events == expected, "Observer missed, reordered or duplicated an event"
    assert lines[-1] == "STOP reason=complete"


class Integration(unittest.TestCase):
    def harness(self, **options):
        h = Harness(self._testMethodName, **options)
        self.addCleanup(h.cleanup)
        return h

    def test_01_sequential_single_capacity(self):
        h = self.harness(visitors=12, capacity=1, pictures=4, strategy="sequential")
        h.join(h.visitors_async())
        h.finish()
        verify_history(h)
        self.assertIn("type=WAIT_ENTRY", h.text())

    def test_02_random_100_visitors_three_observers_and_reconnect(self):
        h = self.harness(visitors=100, capacity=9, pictures=7, caps=[1, 2, 3, 1, 2, 2, 1], duration=4)
        observer1, observer2 = h.observer("observer1"), h.observer("observer2")
        eventually(lambda: "SNAPSHOT_END" in h.text("observer1") and "SNAPSHOT_END" in h.text("observer2"))
        job = h.visitors_async()
        eventually(lambda: "type=VIEW_START" in h.text())
        observer2.terminate()
        observer2.wait(timeout=5)
        observer3 = h.observer("observer_reconnected")
        h.join(job)
        h.finish()
        self.assertEqual(observer1.wait(timeout=5), 0)
        self.assertEqual(observer3.wait(timeout=5), 0)
        verify_history(h)
        verify_observer(h, "observer1")
        verify_observer(h, "observer_reconnected")
        header = next(line for line in h.text("observer_reconnected").splitlines() if line.startswith("SNAPSHOT_BEGIN"))
        self.assertGreater(int(fields(header)["arrived"]), 0)
        self.assertIn("type=WAIT_PICTURE", h.text())

    def test_03_300_visitors(self):
        h = self.harness(visitors=300, capacity=20, pictures=8, caps=[2, 3, 1, 4, 2, 1, 3, 2], duration=0)
        h.join(h.visitors_async(workers=40))
        h.finish()
        verify_history(h)

    def test_04_all_64_picture_bits(self):
        h = self.harness(visitors=4, capacity=4, pictures=64, duration=0)
        h.join(h.visitors_async())
        h.finish()
        verify_history(h)
        self.assertIn("seen=ffffffffffffffff", h.text())

    def test_05_empty_day(self):
        h = self.harness(visitors=0)
        h.finish()
        verify_history(h)

    def test_06_fragmented_and_coalesced_commands(self):
        h = self.harness(visitors=1, pictures=2, duration=0)
        p = h.peer("HEL")
        p.send("LO VIS")
        p.send("ITOR 1\n")
        p.until("ENTERED")
        p.send("V")
        p.send("IEW 1\n")
        p.until("DONE 1")
        p.send("VIEW 2\n")
        p.until("DONE 2")
        p.send("LEAVE\nIGNORED_AFTER_LEAVE\n")
        self.assertEqual(p.line(), "BYE")
        h.finish()
        verify_history(h)

    def test_07_duplicate_id_and_bad_handshakes(self):
        h = self.harness(visitors=2, pictures=1, duration=0)
        first = h.peer("HELLO VISITOR 1\n")
        first.until("ENTERED")
        for greeting in ["HELLO VISITOR 1\n", "HELLO VISITOR 0\n", "HELLO VISITOR 3\n",
                         "HELLO VISITOR -1\n", "HELLO VISITOR 1 extra\n", "x" * 2000 + "\n", b"HELLO\x00OBSERVER\n"]:
            bad = h.peer(greeting)
            self.assertTrue(bad.line().startswith("ERROR "))
        first.send("VIEW 1\n")
        first.until("DONE 1")
        first.send("LEAVE\n")
        self.assertEqual(first.line(), "BYE")
        h.join(h.visitors_async([2]))
        h.finish()
        verify_history(h)

    def test_08_repeat_picture_and_early_exit_rejected(self):
        h = self.harness(visitors=3, pictures=2, duration=0)
        first = h.peer("HELLO VISITOR 1\n")
        first.until("ENTERED")
        first.send("VIEW 1\n")
        first.until("DONE 1")
        first.send("VIEW 1\n")
        self.assertIn("picture_already_seen", first.line())
        second = h.peer("HELLO VISITOR 2\n")
        second.until("ENTERED")
        second.send("LEAVE\n")
        self.assertIn("all_pictures", second.line())
        h.join(h.visitors_async([3]))
        h.finish(expected=2)
        verify_history(h, complete=False)
        self.assertIn("finished=1 aborted=2 remaining=0 status=incomplete", h.text())

    def test_09_disconnected_visitor_releases_places(self):
        h = self.harness(visitors=3, capacity=1, pictures=1, duration=200)
        first = h.peer("HELLO VISITOR 1\n")
        first.until("ENTERED")
        first.send("VIEW 1\n")
        first.until("START ")
        second = h.peer("HELLO VISITOR 2\n")
        second.until("WAIT_ENTRY")
        second.close()
        eventually(lambda: "type=ABORT visitor=2" in h.text())
        job = h.visitors_async([3])
        first.close()
        h.join(job)
        h.finish(expected=2)
        verify_history(h, complete=False)
        self.assertIn("finished=1 aborted=2", h.text())

    def test_10_slow_observer_cannot_stop_gallery(self):
        h = self.harness(visitors=100, capacity=25, pictures=32, caps=[3] * 32, duration=0)
        slow = h.peer("HELLO OBSERVER\n", receive_buffer=1024)
        slow.until("SNAPSHOT_END")
        good = h.observer("good_observer")
        eventually(lambda: "SNAPSHOT_END" in h.text("good_observer"))
        h.join(h.visitors_async())
        h.finish()
        self.assertEqual(good.wait(timeout=10), 0)
        self.assertIn("reason=slow_consumer", h.text(error=True))
        verify_history(h)
        verify_observer(h, "good_observer")

    def test_11_signal_during_view(self):
        h = self.harness(visitors=2, capacity=1, pictures=2, duration=5000)
        observer = h.observer("observer")
        eventually(lambda: "SNAPSHOT_END" in h.text("observer"))
        visitor = h.peer("HELLO VISITOR 1\n")
        visitor.until("ENTERED")
        visitor.send("VIEW 1\n")
        visitor.until("START ")
        h.server.send_signal(signal.SIGINT)
        self.assertEqual(visitor.line(), "STOP reason=interrupt")
        h.finish(expected=130)
        self.assertEqual(observer.wait(timeout=5), 130)
        self.assertIn("remaining=2 status=interrupted", h.text())

    def test_12_invalid_arguments(self):
        cases = [[], ["--host", "127.0.0.1", "--port", "0"],
                 ["--capacity", "0"], ["--visitors", "-1"], ["--pictures", "65"],
                 ["--pictures", "2", "--picture-capacities", "1,,2"],
                 ["--pictures", "2", "--picture-capacities", "1"],
                 ["--pictures", "2", "--picture-capacities", "1,0"],
                 ["--view-min-ms", "10", "--view-max-ms", "1"],
                 ["--seed", "4294967296"], ["--strategy", "invalid"], ["--unknown"]]
        for args in cases:
            base = [] if not args or args[0] == "--host" else ["--host", "127.0.0.1", "--port", "5001"]
            result = subprocess.run([str(BIN / "gallery_server"), *base, *args], capture_output=True, timeout=5)
            self.assertEqual(result.returncode, 1, args)
            self.assertNotIn(b"Sanitizer", result.stderr)

    def test_13_observer_is_read_only(self):
        h = self.harness(visitors=2, pictures=1)
        observer = h.peer("HELLO OBSERVER\nVIEW 1\n")
        observer.until("SNAPSHOT_END")
        self.assertEqual(observer.line(), "ERROR observer_is_read_only")
        h.join(h.visitors_async())
        h.finish()
        verify_history(h)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin", default="build")
    parser.add_argument("tests", nargs="*")
    args = parser.parse_args()
    BIN = (ROOT / args.bin).resolve()
    RESULTS = ROOT / "test-results" / ("sanitize" if "sanitize" in args.bin else "release")
    RESULTS.mkdir(parents=True, exist_ok=True)
    unittest.main(argv=[__file__, *args.tests], verbosity=2)
