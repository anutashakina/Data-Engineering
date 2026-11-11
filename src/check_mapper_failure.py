import argparse
import hashlib
import json
import os
import subprocess
import sys
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from pathlib import Path
from shutil import copyfile
from tempfile import TemporaryDirectory

import skill_counts


def crash_mapper(fields, marker):
    counts = skill_counts.map_chunk(fields)
    try:
        with Path(marker).open("x") as stream:
            stream.write(str(os.getpid()))
    except FileExistsError:
        return counts
    print(f"FAULT INJECTION: mapper PID {os.getpid()} exits with code 86", flush=True)
    os._exit(86)


class FailurePool(ProcessPoolExecutor):
    def map(self, function, *iterables, **kwargs):
        return super().map(partial(crash_mapper, marker=os.environ["MAPPER_FAILURE_MARKER"]),
                           *iterables, **kwargs)


def hashes(directory):
    return {
        f"skills_{size}.csv": hashlib.sha256(
            (directory / f"skills_{size}.csv").read_bytes()
        ).hexdigest()
        for size in skill_counts.SIZES
    }


def run(command, path, environment):
    completed = subprocess.run(command, env=environment, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, encoding="utf-8")
    path.write_text(completed.stdout, encoding="utf-8")
    print(completed.stdout, end="")
    return completed


def main():
    parser = argparse.ArgumentParser(description="Реальное завершение одного маппера и повторный запуск.")
    parser.add_argument("--profile", default="rustfs")
    parser.add_argument("--key", default="it_vacancies_2024.csv")
    parser.add_argument("--bucket", default="hh-lake")
    parser.add_argument("--endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--output", type=Path, default=skill_counts.ROOT / "results" / "rustfs")
    parser.add_argument("--inject", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.inject:
        skill_counts.ProcessPoolExecutor = FailurePool
        sys.argv = [
            "skill_counts.py", "--profile", args.profile, "--key", args.key,
            "--bucket", args.bucket, "--endpoint", args.endpoint,
            "--output", str(args.output),
        ]
        skill_counts.main()
        return

    expected = hashes(args.output)
    environment = os.environ.copy()
    with TemporaryDirectory(dir=args.output, prefix=".failure-check-") as directory:
        work = Path(directory)
        for filename in expected:
            copyfile(args.output / filename, work / filename)
        environment["MAPPER_FAILURE_MARKER"] = str(work / "failed_mapper.pid")
        command = [
            sys.executable, "-X", "utf8", __file__, "--profile", args.profile,
            "--key", args.key, "--bucket", args.bucket, "--endpoint", args.endpoint,
            "--output", str(work),
        ]
        failed = run([*command, "--inject"], args.output / "mapper_failure_log.txt", environment)
        injected = (work / "failed_mapper.pid").exists()
        unchanged = hashes(work) == expected
        if not (injected and failed.returncode != 0
                and "BrokenProcessPool" in failed.stdout and unchanged):
            raise RuntimeError("Проверка поведения при отказе не пройдена.")

        retry = run([
            sys.executable, "-X", "utf8", str(Path(skill_counts.__file__)),
            *command[4:],
        ], args.output / "mapper_retry_log.txt", environment)
        recovered = retry.returncode == 0 and hashes(work) == expected
        if not recovered:
            raise RuntimeError("Повторный запуск не восстановил ожидаемые результаты.")
        result = {
            "source": f"s3://{args.bucket}/{args.key}",
            "injected_mapper_pid": int((work / "failed_mapper.pid").read_text()),
            "failed_run_exit_code": failed.returncode,
            "error": "BrokenProcessPool",
            "previous_results_unchanged": unchanged,
            "retry_exit_code": retry.returncode,
            "retry_matches_previous_results_sha256": recovered,
            "automatic_retry": False,
            "result_sha256": expected,
        }
        (args.output / "mapper_failure_check.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
