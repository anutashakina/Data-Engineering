import csv
import hashlib
import json
import os
import re
import sys
from collections import Counter
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory

from skill_counts import TITLE_PATTERN, map_reduce, map_s3, s3_client

INPUT = Path("/input/vacancies.csv")
REFERENCE = Path(__file__).with_name("reference.csv")
PATTERN = re.compile(TITLE_PATTERN, re.IGNORECASE)
BUCKET = "hh-lake"
PARTS = 4
FIELDS = ("role", "name", "key_skills")


def count(stream):
    counts, matched = map_reduce(stream, PATTERN, 1000, 1)
    return Counter({pair: n for pair, n in counts.items() if len(pair) == 2}), matched


def verify(counts, matched, reference):
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    with reference.open(encoding="utf-8", newline="") as f:
        expected = [(tuple(json.loads(r["skills"])), int(r["vacancies"]))
                    for r in csv.DictReader(f)]
    if ordered != expected or matched != 3676:
        raise ValueError(f"DZ3 mismatch: matched={matched}, pairs={len(ordered)}")
    print(json.dumps(dict(matched=matched, unique_pairs=len(ordered),
                          occurrences=sum(counts.values()), top10=ordered[:10]),
                     ensure_ascii=False))


def split_csv(source, directory):
    csv.field_size_limit(2**31 - 1)
    paths = [directory / f"part-{i}.csv" for i in range(PARTS)]
    with ExitStack() as stack:
        writers = []
        for path in paths:
            f = stack.enter_context(path.open("w", encoding="utf-8", newline=""))
            writer = csv.DictWriter(f, FIELDS, extrasaction="ignore")
            writer.writeheader()
            writers.append(writer)
        f = stack.enter_context(source.open(encoding="utf-8-sig", newline=""))
        reader = csv.DictReader(f, strict=True)
        if not set(FIELDS).issubset(reader.fieldnames or ()):
            raise ValueError("Missing CSV columns")
        for i, row in enumerate(reader):
            if None in row or None in row.values():
                raise ValueError(f"Malformed record near line {reader.line_num}")
            writers[i % PARTS].writerow(row)
    return paths


def main(mode):
    if mode == "single":
        with INPUT.open(encoding="utf-8-sig", newline="") as f:
            pairs, matched = count(f)
        verify(pairs, matched, REFERENCE)
        for path in ("/sys/fs/cgroup/memory.peak",
                     "/sys/fs/cgroup/memory/memory.max_usage_in_bytes"):
            if Path(path).exists():
                print("peak_bytes=" + Path(path).read_text().strip())
                return
        raise RuntimeError("No cgroup peak counter on this node")

    client = s3_client("http://rustfs:9000", None)
    prefix = "lab5/" + os.environ["RUN_ID"]
    if mode == "seed":
        if BUCKET not in [b["Name"] for b in client.list_buckets()["Buckets"]]:
            client.create_bucket(Bucket=BUCKET)
        with INPUT.open("rb") as f:
            digest = hashlib.file_digest(f, "sha256").hexdigest()
        if digest != "303db922b541e4440906f5a7e8be32fb55e8808f950ffddb8c272f1548212a65":
            raise ValueError("Input differs from DZ3")
        client.upload_file(str(INPUT), BUCKET, f"{prefix}/vacancies.csv")
        with TemporaryDirectory() as tmp:
            source = Path(tmp) / "vacancies.csv"
            client.download_file(BUCKET, f"{prefix}/vacancies.csv", str(source))
            with source.open("rb") as f:
                if hashlib.file_digest(f, "sha256").hexdigest() != digest:
                    raise ValueError("S3 snapshot differs from input")
            for path in split_csv(source, Path(tmp)):
                client.upload_file(str(path), BUCKET, f"{prefix}/{path.name}")
        print(f"s3://{BUCKET}/{prefix}/vacancies.csv sha256={digest} parts={PARTS}")
    elif mode == "map":
        i = int(os.environ["JOB_COMPLETION_INDEX"])
        key = f"{prefix}/part-{i}.csv"
        raw, matched, etag = map_s3(client, dict(Bucket=BUCKET, Key=key), PATTERN, 1000, 1)
        rows = [(pair, n) for pair, n in raw.items() if len(pair) == 2]
        body = json.dumps(dict(matched=matched, rows=rows)).encode()
        client.put_object(Bucket=BUCKET, Key=f"{prefix}/map-{i}.json", Body=body)
        print(f"index={i} key={key} etag={etag} matched={matched} pairs={len(rows)}")
    elif mode == "reduce":
        counts = Counter()
        matched = 0
        for i in range(PARTS):
            response = client.get_object(Bucket=BUCKET, Key=f"{prefix}/map-{i}.json")
            with response["Body"] as body:
                part = json.load(body)
            matched += part["matched"]
            counts.update({tuple(pair): n for pair, n in part["rows"]})
        verify(counts, matched, REFERENCE)
    else:
        raise ValueError(mode)


if __name__ == "__main__":
    main(sys.argv[1])
