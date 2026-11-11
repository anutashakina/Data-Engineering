import csv
from pathlib import Path


DATASET = Path(__file__).resolve().parent / "data" / "vacancies.csv"
PREVIEW_COLUMNS = (
    "name",
    "role",
    "key_skills",
    "salary_from",
    "salary_to",
    "currency",
    "area",
)


def main():
    csv.field_size_limit(2**31 - 1)
    vacancy_count = 0
    with_skills = 0
    preview = []

    with DATASET.open(encoding="utf-8-sig", newline="") as dataset:
        reader = csv.reader(dataset)
        columns = next(reader, [])
        missing_columns = [column for column in PREVIEW_COLUMNS if column not in columns]
        if missing_columns:
            raise SystemExit(f"В CSV отсутствуют колонки: {', '.join(missing_columns)}")

        positions = [columns.index(column) for column in PREVIEW_COLUMNS]
        skills_position = columns.index("key_skills")
        for row in reader:
            if not row:
                continue
            if len(row) != len(columns):
                raise SystemExit(f"Неверное число полей около строки {reader.line_num}")
            vacancy_count += 1
            with_skills += bool(row[skills_position].strip())
            if len(preview) < 3:
                preview.append([row[position] for position in positions])

    print("Количество вакансий:", vacancy_count)
    print("\nНазвания колонок:")
    print(", ".join(columns))
    print("\nПервые 3 вакансии:")
    for vacancy in preview:
        print(dict(zip(PREVIEW_COLUMNS, vacancy)))
    print("\nКоличество вакансий с навыками:", with_skills)


if __name__ == "__main__":
    main()
