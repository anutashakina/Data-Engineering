"""Checkpointed HW3 calculation; deliberately continue CPU work after session loss.

This fault-injection behaviour demonstrates why the result receiver needs fencing.
Large immutable artifacts live on the shared filesystem, only pointers live in ZK.
"""
import argparse
from collections import Counter
import hashlib
import json
import logging
import os
from pathlib import Path
import time

from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError, NodeExistsError, KazooException

from skill_counts import ROOT, map_chunk, save_counts

HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def commit(transaction):
    results = transaction.commit()
    for result in results:
        if isinstance(result, Exception):
            raise result
    return results


def connect(hosts=HOSTS):
    client = KazooClient(hosts=hosts, timeout=10.0)
    client.start(timeout=20)
    return client


def receive_result(hosts, base, epoch_version, pointer, fencing):
    """Receiver atomically checks the epoch and changes the published pointer.

    A fresh connection models an independently available receiving service; it
    cannot trust the writer's old session or a client-side is_leader check.
    """
    receiver = connect(hosts)
    try:
        transaction = receiver.transaction()
        if fencing:
            transaction.check(base + "/epoch", version=epoch_version)
        transaction.set_data(base + "/result", encoded(pointer))
        commit(transaction)
        return receiver.get(base + "/result")[1].version
    finally:
        receiver.stop()
        receiver.close()


def run(args):
    log_dir = ROOT / "logs" / "zookeeper" / args.run
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.FileHandler(log_dir / f"{args.name}.log", encoding="utf-8"),
                                  logging.StreamHandler()])
    logging.getLogger("kazoo").setLevel(logging.WARNING)

    def event(kind, **values):
        values.update(event=kind, coordinator=args.name, monotonic=time.monotonic(),
                      time=time.time(), pid=os.getpid())
        logging.info(json.dumps(values, ensure_ascii=False))

    fields = json.loads(args.input.read_text(encoding="utf-8"))
    input_hash = digest(args.input)
    base = "/skills-hw4/" + args.run
    client = connect(args.hosts)
    client.add_listener(lambda state: event("SESSION", state=state))
    lock = client.Lock(base + "/election", args.name)
    try:
        client.ensure_path(base)
        for node, value in (("epoch", b"0"), ("checkpoint", b"{}"), ("result", b"{}")):
            try:
                client.create(base + "/" + node, value)
            except NodeExistsError:
                pass
        event("WAITING", requested_session_timeout=10,
              negotiated_session_timeout_ms=client._session_timeout)
        lock.acquire(timeout=120)
        # CAS prevents a stale acquisition from issuing an already used epoch.
        previous, stat = client.get(base + "/epoch")
        epoch = int(previous) + 1
        tx = client.transaction()
        tx.check(lock.path + "/" + lock.node, version=0)
        tx.set_data(base + "/epoch", str(epoch).encode(), version=stat.version)
        commit(tx)
        epoch_version = stat.version + 1
        event("LEADER", epoch=epoch, epoch_version=epoch_version)
        checkpoint = json.loads(client.get(base + "/checkpoint")[0])
        counts, offset = Counter(), 0
        if checkpoint:
            path = ROOT / checkpoint["file"]
            if digest(path) != checkpoint["sha256"]:
                raise ValueError("Checkpoint checksum mismatch")
            saved = json.loads(path.read_text(encoding="utf-8"))
            if saved["input_sha256"] != input_hash:
                raise ValueError("Checkpoint belongs to another input")
            counts = Counter({tuple(key): count for key, count in saved["counts"]})
            offset = saved["offset"]
        event("RESTORED", offset=offset, total=len(fields))
        checkpoint_enabled = True
        while offset < len(fields):
            end = min(offset + args.chunk_size, len(fields))
            counts.update(map_chunk(fields[offset:end]))
            offset = end
            if checkpoint_enabled:
                directory = ROOT / "checkpoints" / "zookeeper" / args.run
                directory.mkdir(parents=True, exist_ok=True)
                path = directory / f"{args.name}-{epoch}-{offset}.json"
                path.write_bytes(encoded(dict(offset=offset, input_sha256=input_hash,
                                              counts=list(counts.items()))))
                pointer = dict(file=path.relative_to(ROOT).as_posix(), sha256=digest(path),
                               offset=offset, epoch=epoch)
                try:
                    tx = client.transaction()
                    tx.check(base + "/epoch", version=epoch_version)
                    tx.set_data(base + "/checkpoint", encoded(pointer))
                    commit(tx)
                    event("CHECKPOINT", offset=offset, epoch=epoch)
                except KazooException as error:
                    checkpoint_enabled = False
                    event("STALE_CHECKPOINT_REJECTED", epoch=epoch, error=type(error).__name__)
            time.sleep(args.delay)
        directory = ROOT / "results" / "zookeeper" / args.run / args.name
        directory.mkdir(parents=True, exist_ok=True)
        save_counts(counts, directory)
        hashes = {f"skills_{size}.csv": digest(directory / f"skills_{size}.csv")
                  for size in (1, 2, 3)}
        expected = {name: digest(ROOT / "results" / "rustfs" / name) for name in hashes}
        if hashes != expected:
            raise ValueError("Results differ from HW3")
        event("MATCH_HW3", hashes=hashes, matched=len(fields))
        pointer = dict(coordinator=args.name, epoch=epoch,
                       directory=directory.relative_to(ROOT).as_posix(), hashes=hashes)
        try:
            version = receive_result(args.hosts, base, epoch_version, pointer, args.fencing)
            event("PUBLISHED", message="записал результат", version=version, epoch=epoch)
        except BadVersionError:
            event("REJECTED", error="BadVersionError", epoch=epoch)
    finally:
        # A lost session may already have removed this process's lock node.
        if client.connected:
            lock.release()
        client.stop()
        client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--run", required=True)
    parser.add_argument("--input", type=Path, default=ROOT / "checkpoints" / "zk-input.json")
    parser.add_argument("--hosts", default=HOSTS)
    parser.add_argument("--chunk-size", type=int, default=200)
    parser.add_argument("--delay", type=float, default=0.5)
    parser.add_argument("--fencing", type=int, choices=(0, 1), default=int(os.getenv("FENCING", "0")))
    options = parser.parse_args()
    if options.chunk_size < 1 or options.delay < 0:
        parser.error("chunk-size > 0 and delay >= 0 required")
    run(options)
