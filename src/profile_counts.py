import argparse
import csv
import re
import subprocess
import sys
from pathlib import Path
from time import perf_counter, sleep

import psutil

from skill_counts import ROOT, TITLE_PATTERN, duckdb_counts, map_s3, s3_client, s3_snapshot


def run_worker(args):
    client = s3_client(args.endpoint, args.profile)
    object_ref = {"Bucket": args.bucket, "Key": args.key}
    if args.method == "mapreduce":
        counts, matched, etag = map_s3(client, object_ref,
                                       re.compile(TITLE_PATTERN, re.IGNORECASE),
                                       args.chunk_size, args.workers)
        if not matched or etag != args.etag:
            raise RuntimeError("Выборка пуста или объект изменился во время замеров.")
    else:
        with s3_snapshot(client, object_ref, args.etag, args.output.parent) as snapshot:
            counts = duckdb_counts(snapshot, TITLE_PATTERN)
    print(f"{args.method}: {len(counts)} уникальных стеков, {sum(counts.values())} вхождений")


def resident_bytes(process):
    try:
        processes = [process, *process.children(recursive=True)]
    except psutil.NoSuchProcess:
        return 0
    total = 0
    for child in processes:
        try:
            total += child.memory_info().rss
        except psutil.NoSuchProcess:
            continue
    return total


def measure(command):
    started = perf_counter()
    with subprocess.Popen(command) as worker:
        process = psutil.Process(worker.pid)
        peak = 0
        while worker.poll() is None:
            peak = max(peak, resident_bytes(process))
            sleep(0.025)
        if worker.returncode:
            raise subprocess.CalledProcessError(worker.returncode, command)
    return round(perf_counter() - started, 3), round(peak / 1024**2, 2)


def main():
    parser = argparse.ArgumentParser(description="Три или более замера частот навыков из RustFS.")
    parser.add_argument("--bucket", default="hh-lake")
    parser.add_argument("--key", required=True)
    parser.add_argument("--profile")
    parser.add_argument("--endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--chunk-size", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--output", type=Path, default=ROOT / "results" / "profiling.csv")
    parser.add_argument("--method", choices=("mapreduce", "duckdb"), help=argparse.SUPPRESS)
    parser.add_argument("--etag", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.runs < 3 or args.chunk_size < 1 or args.workers < 1:
        parser.error("Нужно минимум три запуска, размер чанка и число мапперов положительные.")
    if args.method:
        run_worker(args)
        return

    args.output.parent.mkdir(parents=True, exist_ok=True)
    client = s3_client(args.endpoint, args.profile)
    etag = client.head_object(Bucket=args.bucket, Key=args.key)["ETag"]
    command = [sys.executable, "-X", "utf8", __file__, *sys.argv[1:], "--etag", etag]
    with args.output.open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(["attempt", "method", "seconds", "peak_tree_rss_mib"])
        for attempt in range(1, args.runs + 1):
            for method in ("mapreduce", "duckdb"):
                seconds, peak = measure([*command, "--method", method])
                writer.writerow([attempt, method, seconds, peak])
                output.flush()
                print(f"{attempt}: {method}, {seconds} с, {peak} МиБ", flush=True)


if __name__ == "__main__":
    main()
