import argparse
import csv
import io
import json
import re
import shutil
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from contextlib import closing, contextmanager
from itertools import batched, combinations, islice
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]
SIZES = (1, 2, 3)
ROLE = "Программист, разработчик"
# Match C++ (including Cyrillic С++), C/C++, and standalone C in the title.
# Exclude lookalikes such as C#, C&B, and C-level.
TITLE_PATTERN = (
    r"(?:^|[^0-9a-zа-яё_])"
    r"(?:[cс]\+\+(?:$|[^0-9a-zа-яё_+#])|c(?:$|[ /,():;]))"
)


class S3Stream(io.RawIOBase):
    def __init__(self, body):
        super().__init__()
        self.body = body

    def readable(self):
        return True

    def readinto(self, buffer):
        data = self.body.read(len(buffer))
        buffer[:len(data)] = data
        return len(data)


def map_chunk(skill_fields):
    counts = Counter()
    for field in skill_fields:
        skills = normalize_skills(field)
        for size in SIZES:
            counts.update(combinations(skills, size))
    return counts


def normalize_skills(field):
    return sorted({skill.strip(" \t\r\n") for skill in field.split(";")} - {""})


def map_reduce(stream, title_pattern, chunk_size, workers):
    csv.field_size_limit(2**31 - 1)
    reader = csv.DictReader(stream, strict=True)
    if not {"name", "role", "key_skills"}.issubset(reader.fieldnames or ()):
        raise ValueError("CSV должен содержать колонки name, role и key_skills.")

    def matching_skills():
        for row in reader:
            if None in row or None in row.values():
                raise ValueError(f"Неверное число полей около строки {reader.line_num}")
            if row["role"] == ROLE and title_pattern.search(row["name"]):
                yield row["key_skills"]

    chunks = batched(matching_skills(), chunk_size)
    counts = Counter()
    matched = 0
    if workers == 1:
        for chunk in chunks:
            counts.update(map_chunk(chunk))
            matched += len(chunk)
    else:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            # Keep the number of chunks in flight bounded on large CSV files.
            while batch := list(islice(chunks, workers)):
                for chunk, partial in zip(batch, pool.map(map_chunk, batch), strict=True):
                    counts.update(partial)
                    matched += len(chunk)
    return counts, matched


def duckdb_counts(csv_path, title_pattern):
    import duckdb

    sql = Path(__file__).with_suffix(".sql").read_text(encoding="utf-8")
    with duckdb.connect() as connection:
        rows = connection.execute(sql, [str(csv_path), ROLE, title_pattern])
        counts = Counter()
        while batch := rows.fetchmany(4096):
            for _, skills, frequency in batch:
                counts[tuple(sorted(skills))] = frequency
    return counts


def s3_client(endpoint, profile):
    import boto3
    from botocore.config import Config

    session = boto3.Session(profile_name=profile)
    return session.client("s3", endpoint_url=endpoint,
                          config=Config(s3={"addressing_style": "path"}))


def map_s3(client, object_ref, title_pattern, chunk_size, workers):
    response = client.get_object(**object_ref)
    with closing(response["Body"]) as body:
        with io.TextIOWrapper(io.BufferedReader(S3Stream(body)),
                              encoding="utf-8-sig", newline="") as stream:
            counts, matched = map_reduce(stream, title_pattern, chunk_size, workers)
    return counts, matched, response["ETag"]


@contextmanager
def s3_snapshot(client, object_ref, etag, output_dir):
    with TemporaryDirectory(dir=output_dir, prefix=".s3-snapshot-") as directory:
        snapshot = Path(directory) / "vacancies.csv"
        response = client.get_object(**object_ref, IfMatch=etag)
        with closing(response["Body"]) as body, snapshot.open("wb") as output:
            shutil.copyfileobj(body, output, length=1024 * 1024)
        yield snapshot


@contextmanager
def inputs(args, title_pattern):
    if args.local_csv:
        with args.local_csv.open(encoding="utf-8-sig", newline="") as stream:
            counts, matched = map_reduce(stream, title_pattern, args.chunk_size, args.workers)
        yield counts, matched, args.local_csv
        return

    client = s3_client(args.endpoint, args.profile)
    object_ref = {"Bucket": args.bucket, "Key": args.key}
    counts, matched, etag = map_s3(client, object_ref, title_pattern,
                                  args.chunk_size, args.workers)
    # Download the exact object revision for the independent SQL scan.
    with s3_snapshot(client, object_ref, etag, args.output) as snapshot:
        yield counts, matched, snapshot


def save_counts(counts, directory):
    for size in SIZES:
        rows = ((stack, count) for stack, count in counts.items() if len(stack) == size)
        path = directory / f"skills_{size}.csv"
        with path.open("w", encoding="utf-8", newline="") as output:
            writer = csv.writer(output)
            writer.writerow(["skills", "vacancies"])
            for stack, count in sorted(rows, key=lambda row: (-row[1], row[0])):
                writer.writerow([json.dumps(stack, ensure_ascii=False), count])


def main():
    parser = argparse.ArgumentParser(description="Частоты навыков через MapReduce и DuckDB.")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--local-csv", type=Path)
    source.add_argument("--key", help="Ключ объекта CSV в RustFS")
    parser.add_argument("--bucket", default="hh-lake")
    parser.add_argument("--endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--profile", help="Имя локального профиля AWS с ключами RustFS")
    parser.add_argument("--title-pattern", default=TITLE_PATTERN,
                        help="Условие варианта по name, регулярное выражение Python и RE2")
    parser.add_argument("--chunk-size", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--output", type=Path, default=ROOT / "results")
    args = parser.parse_args()
    if args.chunk_size < 1 or args.workers < 1:
        parser.error("--chunk-size и --workers должны быть положительными")
    try:
        title_pattern = re.compile(args.title_pattern, re.IGNORECASE)
    except re.error as error:
        parser.error(str(error))

    args.output.mkdir(parents=True, exist_ok=True)
    with inputs(args, title_pattern) as (map_counts, matched, csv_path):
        if not matched:
            raise SystemExit("Условию варианта не соответствует ни одна вакансия.")
        sql_counts = duckdb_counts(csv_path, args.title_pattern)

    if map_counts != sql_counts:
        sample = list(islice(
            ((stack, map_counts[stack], sql_counts[stack])
             for stack in map_counts.keys() | sql_counts.keys()
             if map_counts[stack] != sql_counts[stack]),
            5,
        ))
        raise SystemExit(f"Расхождения (стек, MapReduce, DuckDB): {sample}")

    save_counts(map_counts, args.output)
    print(f"Вакансий варианта: {matched}")
    for size in SIZES:
        frequencies = [count for stack, count in map_counts.items() if len(stack) == size]
        print(f"Размер {size}: уникальных {len(frequencies)}, вхождений {sum(frequencies)}")
    print("Все частоты MapReduce и DuckDB совпали.")


if __name__ == "__main__":
    main()
