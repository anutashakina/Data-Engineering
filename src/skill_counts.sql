WITH vacancies AS MATERIALIZED (
    SELECT list_sort(list_distinct(list_filter(
        list_transform(string_split(coalesce(key_skills, ''), ';'),
            lambda skill: trim(skill, ' ' || chr(9) || chr(10) || chr(13))),
        lambda skill: skill <> ''
    ))) AS skills
    FROM read_csv(?, header = true, delim = ',', quote = '"', escape = '"',
                  all_varchar = true)
    WHERE role = ? AND regexp_matches(name, ?, 'i')
),
combinations AS (
    SELECT 1 AS size, [a.skill] AS stack
    FROM vacancies, unnest(skills) AS a(skill)
    UNION ALL
    SELECT 2 AS size, [skills[i], skills[j]] AS stack
    FROM vacancies, range(1, len(skills) + 1) AS a(i),
         range(i + 1, len(skills) + 1) AS b(j)
    UNION ALL
    SELECT 3 AS size, [skills[i], skills[j], skills[k]] AS stack
    FROM vacancies, range(1, len(skills) + 1) AS a(i),
         range(i + 1, len(skills) + 1) AS b(j),
         range(j + 1, len(skills) + 1) AS c(k)
)
SELECT size, stack, count(*) AS frequency
FROM combinations
GROUP BY size, stack
