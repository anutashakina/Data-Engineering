import argparse
import csv
import json
import math
import re
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path
from statistics import median

from skill_counts import ROLE, ROOT, TITLE_PATTERN, map_chunk, normalize_skills


def net_ruble_salary(row):
    if row["currency"] not in {"RUR", "RUB"} or row["gross"].lower() != "false":
        return None
    bounds = []
    for field in ("salary_from", "salary_to"):
        value = row[field].strip()
        if value:
            amount = float(value)
            if not math.isfinite(amount) or amount <= 0:
                return None
            bounds.append(amount)
    if not bounds or (len(bounds) == 2 and bounds[0] > bounds[1]):
        return None
    return sum(bounds) / len(bounds)


def read_variant(path, pattern):
    csv.field_size_limit(2**31 - 1)
    with path.open(encoding="utf-8-sig", newline="") as stream:
        for row in csv.DictReader(stream, strict=True):
            if row["role"] == ROLE and pattern.search(row["name"]):
                yield row["area"], row["key_skills"], net_ruble_salary(row)


def write_table(path, columns, rows):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(columns)
        writer.writerows(rows)


def salary_comparisons(vacancies, counts, minimum):
    salaries = defaultdict(list)
    for _, field, salary in vacancies:
        if salary is None:
            continue
        skills = normalize_skills(field)
        for size in (1, 2, 3):
            for stack in combinations(skills, size):
                salaries[stack].append(salary)

    medians = {stack: median(values) for stack, values in salaries.items()
               if len(values) >= minimum}
    changes = defaultdict(list)
    for stack, stack_median in medians.items():
        size = len(stack)
        if size == 1:
            continue
        # Use the most common contained single skill or pair as the baseline.
        baseline = min(combinations(stack, size - 1),
                       key=lambda candidate: (-counts[candidate], candidate))
        baseline_median = medians[baseline]
        difference = stack_median - baseline_median
        if difference:
            changes[size, "higher" if difference > 0 else "lower"].append(
                (difference, stack, baseline, stack_median, baseline_median)
            )

    for (size, direction), records in sorted(changes.items()):
        for delta, stack, baseline, stack_median, base_median in sorted(
            records, key=lambda record: (-abs(record[0]), record[1])
        )[:10]:
            yield (size, direction, json.dumps(stack, ensure_ascii=False),
                   json.dumps(baseline, ensure_ascii=False), len(salaries[stack]),
                   len(salaries[baseline]), stack_median, base_median, delta)


def analyze(csv_path, pattern, directory, minimum):
    vacancies = list(read_variant(csv_path, pattern))
    if not vacancies:
        raise ValueError("Условию варианта не соответствует ни одна вакансия.")
    counts = map_chunk(field for _, field, _ in vacancies)
    write_table(directory / "salary_comparisons.csv", (
        "size", "direction", "skills", "baseline", "salary_count", "baseline_salary_count",
        "median_net_rub", "baseline_median_net_rub", "difference_net_rub",
    ), salary_comparisons(vacancies, counts, minimum))

    region_counts = Counter(region for region, _, _ in vacancies if region)
    regions = sorted(region_counts, key=lambda region: (-region_counts[region], region))[:3]
    region_summary = []
    region_skills = []
    for region in regions:
        fields = [field for area, field, _ in vacancies if area == region]
        salaries = [salary for area, _, salary in vacancies
                    if area == region and salary is not None]
        region_summary.append((region, len(fields), sum(bool(normalize_skills(f)) for f in fields),
                               len(salaries), median(salaries) if salaries else None))
        frequencies = map_chunk(fields)
        for size in (1, 2, 3):
            top = sorted(
                ((stack, count) for stack, count in frequencies.items() if len(stack) == size),
                key=lambda row: (-row[1], row[0]),
            )[:10]
            for stack, count in top:
                region_skills.append((region, size, json.dumps(stack, ensure_ascii=False),
                                      count, round(100 * count / len(fields), 2)))

    write_table(directory / "region_summary.csv",
                ("region", "vacancies", "with_skills", "salary_count", "median_net_rub"),
                region_summary)
    write_table(directory / "region_skills.csv",
                ("region", "size", "skills", "vacancies", "share_pct"), region_skills)


def main():
    parser = argparse.ArgumentParser(description="Сравнение зарплат и регионов для варианта C/C++.")
    parser.add_argument("csv", type=Path)
    parser.add_argument("--output", type=Path, default=ROOT / "results")
    parser.add_argument("--title-pattern", default=TITLE_PATTERN)
    parser.add_argument("--min-salaries", type=int, default=10)
    args = parser.parse_args()
    if args.min_salaries < 1:
        parser.error("--min-salaries должен быть положительным")
    args.output.mkdir(parents=True, exist_ok=True)
    analyze(args.csv, re.compile(args.title_pattern, re.IGNORECASE),
            args.output, args.min_salaries)
    print(f"Таблицы зарплат и регионов сохранены в {args.output}")


if __name__ == "__main__":
    main()
