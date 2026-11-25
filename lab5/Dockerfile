FROM python:3.13-slim AS worker
WORKDIR /app
RUN pip install --no-cache-dir boto3==1.44.0
COPY src/skill_counts.py lab5/pairs.py ./
COPY results/rustfs/skills_2.csv reference.csv
ENTRYPOINT ["python", "-u", "pairs.py"]

FROM worker AS input
COPY data/vacancies.csv /input/vacancies.csv
