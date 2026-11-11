"""Real ensemble and process suspension experiment, no simulated ZooKeeper."""
import argparse
import csv
import json
import logging
from pathlib import Path
import re
import socket
import subprocess
import sys
import time

import psutil
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError

from skill_counts import ROOT, ROLE, TITLE_PATTERN
from zk_coordinator import HOSTS, connect, digest

LOGS = ROOT / "logs" / "zookeeper"


def docker(*args, input=None):
    command = ["docker", "compose", *args]
    result = subprocess.run(command, cwd=ROOT, input=input, capture_output=True,
                            encoding="utf-8", errors="replace", timeout=120)
    with (LOGS / "commands.txt").open("a", encoding="utf-8") as log:
        log.write(f"\n$ {' '.join(command)}\n{result.stdout}{result.stderr}\nexit={result.returncode}\n")
    if result.returncode:
        raise RuntimeError(result.stderr)
    return result.stdout


def wait_for(predicate, timeout=40):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.1)
    raise TimeoutError("Condition not reached")


def server_status():
    result = {}
    for i in (1, 2, 3):
        try:
            with socket.create_connection(("127.0.0.1", 2180 + i), timeout=2) as sock:
                sock.sendall(b"srvr")
                result[f"zk{i}"] = sock.recv(16384).decode()
        except OSError as error:
            result[f"zk{i}"] = str(error)
    return result


def ensemble():
    docker("up", "-d")
    client = connect()
    evidence = {}
    try:
        def all_roles():
            roles = server_status()
            return roles if sum("Mode:" in s for s in roles.values()) == 3 else None
        evidence["initial"] = wait_for(all_roles)
        followers = [name for name, status in evidence["initial"].items() if "follower" in status]
        assert len(followers) == 2
        path = "/hw4-quorum-" + str(time.time_ns())
        client.create(path, b"three alive")
        docker("stop", followers[0])
        client.set(path, b"two alive")
        evidence["one_stopped"] = server_status()
        evidence["write_with_two"] = client.get(path)[0].decode()
        docker("stop", followers[1])
        time.sleep(3)
        evidence["two_stopped"] = server_status()
        # Even an existing connected client cannot commit a write without quorum.
        pending = client.set_async(path, b"must not commit")
        try:
            pending.get(timeout=3)
            raise AssertionError("Write unexpectedly succeeded without quorum")
        except Exception as error:
            if isinstance(error, AssertionError):
                raise
            evidence["no_quorum_write_error"] = type(error).__name__
        probe = KazooClient(hosts=HOSTS, timeout=4)
        try:
            probe.start(timeout=3)
            raise AssertionError("Client unexpectedly connected without quorum")
        except KazooTimeoutError as error:
            evidence["no_quorum_connect_error"] = type(error).__name__
        finally:
            probe.stop()
            probe.close()
    finally:
        docker("start", "zk1", "zk2", "zk3")
        client.stop()
        client.close()
    client = connect()
    try:
        evidence["restored"] = wait_for(all_roles)
        client.set(path, b"restored quorum")
        evidence["write_after_restore"] = client.get(path)[0].decode()
        client.delete(path)
        ephemeral = "/hw4-ephemeral-" + str(time.time_ns())
        with (LOGS / "zkCli.txt").open("w", encoding="utf-8") as output:
            cli = subprocess.Popen(["docker", "compose", "exec", "-T", "zk2", "zkCli.sh",
                                    "-server", "zk1:2181", "-timeout", "10000"], cwd=ROOT,
                                   stdin=subprocess.PIPE, stdout=output, stderr=subprocess.STDOUT,
                                   text=True, encoding="utf-8")
            try:
                cli.stdin.write(f"create -e {ephemeral} session-owned\nget -s {ephemeral}\n")
                cli.stdin.flush()
                stat = wait_for(lambda: client.exists(ephemeral))
                assert stat.ephemeralOwner != 0
                evidence["ephemeral_before_close"] = dict(path=ephemeral, owner=stat.ephemeralOwner)
                cli.stdin.write("quit\n")
                cli.stdin.flush()
                cli.wait(timeout=20)
                wait_for(lambda: client.exists(ephemeral) is None)
                evidence["ephemeral_after_close"] = None
            finally:
                if cli.poll() is None:
                    cli.terminate()
                    cli.wait(timeout=10)
        for name in ("zk1", "zk2", "zk3"):
            docker("exec", "-T", name, "bash", "-c", "cat /conf/zoo.cfg; cat /data/myid; java -version")
        docker("images")
    finally:
        client.stop()
        client.close()
        (LOGS / "ensemble.json").write_text(json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8")


def prepare_input():
    path = ROOT / "checkpoints" / "zk-input.json"
    csv.field_size_limit(2**31 - 1)
    pattern = re.compile(TITLE_PATTERN, re.IGNORECASE)
    with (ROOT / "data" / "vacancies.csv").open(encoding="utf-8-sig", newline="") as stream:
        fields = [row["key_skills"] for row in csv.DictReader(stream)
                  if row["role"] == ROLE and pattern.search(row["name"])]
    assert fields, "No matching vacancies"
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps(fields, ensure_ascii=False), encoding="utf-8")
    return dict(matched=len(fields), input_sha256=digest(path))


def events(run, name):
    path = LOGS / run / f"{name}.log"
    result = []
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if "INFO {" in line:
                result.append(json.loads(line.split("INFO ", 1)[1]))
    return result


def experiment(fencing):
    run = f"fencing-{fencing}-{time.time_ns()}"
    directory = LOGS / run
    directory.mkdir(parents=True)
    processes, outputs = {}, []
    suspended = False
    try:
        for name in ("A", "B"):
            output = (directory / f"{name}-console.txt").open("w", encoding="utf-8")
            outputs.append(output)
            processes[name] = subprocess.Popen([
                sys.executable, "-X", "utf8", str(ROOT / "src" / "zk_coordinator.py"),
                "--run", run, "--name", name, "--fencing", str(fencing)],
                stdout=output, stderr=subprocess.STDOUT, cwd=ROOT)
            if name == "A":
                wait_for(lambda: any(e["event"] == "CHECKPOINT" and e["offset"] >= 600
                                     for e in events(run, "A")))
        wait_for(lambda: any(e["event"] == "WAITING" for e in events(run, "B")))
        leader = psutil.Process(processes["A"].pid)
        leader.suspend()
        suspended = True
        pause = time.monotonic()
        pause_wall = time.time()
        print(f"{run}: suspended leader A PID={leader.pid}", flush=True)
        became_leader = wait_for(lambda: next((e for e in events(run, "B") if e["event"] == "LEADER"), None))
        wait_for(lambda: any(e["event"] == "PUBLISHED" for e in events(run, "B")))
        # Keep the process paused for at least 40 real seconds, including takeover.
        while time.monotonic() - pause < 40:
            time.sleep(0.2)
        leader.resume()
        suspended = False
        for process in processes.values():
            assert process.wait(timeout=60) == 0, "Coordinator failed; see console log"
        a, b = events(run, "A"), events(run, "B")
        client = connect()
        try:
            pointer, stat = client.get(f"/skills-hw4/{run}/result")
            pointer = json.loads(pointer)
        finally:
            client.stop()
            client.close()
        published = [e for e in a + b if e["event"] == "PUBLISHED"]
        restored = next(e for e in b if e["event"] == "RESTORED")
        assert restored["offset"] > 0
        assert all(any(e["event"] == "MATCH_HW3" for e in log) for log in (a, b))
        assert len(published) == (1 if fencing else 2)
        assert stat.version == (1 if fencing else 2)
        assert pointer["coordinator"] == ("B" if fencing else "A")
        assert not fencing or any(e["event"] == "REJECTED" and e["error"] == "BadVersionError" for e in a)
        summary = dict(run=run, fencing=fencing, pause_monotonic=pause, pause_time=pause_wall,
                       failover_seconds=became_leader["monotonic"] - pause,
                       backup_restored_offset=restored["offset"], publications=len(published),
                       pointer_version=stat.version, pointer=pointer, both_match_hw3=True)
        (directory / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(summary, ensure_ascii=False), flush=True)
        return summary
    finally:
        if suspended:
            psutil.Process(processes["A"].pid).resume()
        for process in processes.values():
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=10)
        for output in outputs:
            output.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-ensemble", action="store_true")
    args = parser.parse_args()
    LOGS.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.WARNING)
    try:
        if not args.skip_ensemble:
            ensemble()
        source = prepare_input()
        summaries = [experiment(0), experiment(1)]
        (LOGS / "verification.json").write_text(json.dumps(dict(source=source, runs=summaries),
                                                          ensure_ascii=False, indent=2), encoding="utf-8")
    finally:
        docker("logs", "--no-color")


if __name__ == "__main__":
    main()
