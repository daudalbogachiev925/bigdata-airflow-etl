# bigdata-airflow-etl

# Big Data: Airflow ETL

ETL-пайплайн с оркестрацией через Apache Airflow.

## Задача
Ежедневный ETL: выгрузка данных из API → трансформация → загрузка в PostgreSQL → отчёт.

## Стек
- Python 3
- Apache Airflow
- PostgreSQL
- pandas
- requests

## Задания
1. Настроить Airflow через Docker
2. Создать DAG с 4 задачами
3. Добавить retry и логирование
4. Настроить расписание @daily
5. Добавить уведомление при ошибке
