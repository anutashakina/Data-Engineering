# Отказоустойчивость средствами Apache ZooKeeper
## Материалы

| Материал | Расположение |
| --- | --- |
| Отчёт со скриншотами (PDF) | [Отказоустойчивость средствами Apache ZooKeeper.pdf](<Отказоустойчивость средствами Apache ZooKeeper.pdf>) |
| Конфигурация ансамбля | [compose.yaml](compose.yaml) |
| Координатор и сценарий проверки | [zk_coordinator.py](src/zk_coordinator.py), [check_zookeeper.py](src/check_zookeeper.py) |
| Журналы A и B обоих запусков | [Без защиты](logs/zookeeper/fencing-0-1794357177778734600/): [A.log](logs/zookeeper/fencing-0-1794357177778734600/A.log), [B.log](logs/zookeeper/fencing-0-1794357177778734600/B.log); [с защитой](logs/zookeeper/fencing-1-1794357228831722700/): [A.log](logs/zookeeper/fencing-1-1794357228831722700/A.log), [B.log](logs/zookeeper/fencing-1-1794357228831722700/B.log) |
| Серверные журналы и выводы команд | [commands.txt](logs/zookeeper/commands.txt) |
| Проверки кворума и эфемерного узла | [ensemble.json](logs/zookeeper/ensemble.json), [zkCli.txt](logs/zookeeper/zkCli.txt), [final-zkCli.txt](logs/zookeeper/final-zkCli.txt) |
| Измерения и итоговые указатели | [verification.json](logs/zookeeper/verification.json), [summary.json без защиты](logs/zookeeper/fencing-0-1794357177778734600/summary.json), [summary.json с защитой](logs/zookeeper/fencing-1-1794357228831722700/summary.json) |
| Результаты расчётов | [results/zookeeper/](results/zookeeper/) |
| Эталон ДЗ № 3 | [Результаты](results/rustfs/), [контрольные числа](results/rustfs/mapper_retry_log.txt), [SHA-256](results/rustfs/mapper_failure_check.json) |

## Результаты эксперимента

| Показатель | Без защиты | С защитой |
| --- | --- | --- |
| Смена лидера | 11,428 с | 10,463 с |
| Восстановление запасного | С 600-й из 3676 вакансий | С 600-й из 3676 вакансий |
| Успешных публикаций | 2 | 1 |
| Версия итогового указателя | 2 | 1 |
| Попытка старого лидера | Принята | Отклонена с `BadVersionError` |
| Совпадение CSV с ДЗ № 3 | Да | Да |
