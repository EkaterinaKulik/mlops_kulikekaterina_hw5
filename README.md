# Домашнее задание 5

## Описание проекта

Проект реализует ETL/ELT-пайплайн фрод-аналитики с использованием Apache Airflow и PostgreSQL.

## Архитектура решения

```bash
.
├── docker-compose.yml
├── requirements.txt
├── README.md
├── dags/
│   └── fraud_marts_dag.py
├── sql/
│   ├── 00_create_schemas.sql
│   ├── 01_mart_daily_state_metrics.sql
│   ├── 02_mart_fraud_by_category.sql
│   ├── 03_mart_fraud_by_state.sql
│   ├── 04_mart_customer_risk_profile.sql
│   ├── 05_mart_hourly_fraud_pattern.sql
│   └── 06_mart_merchant_analytics.sql
├── initdb/
│   └── 00_create_raw_table.sql
├── data/
│   └── train.csv
├── logs/
│   └── .gitkeep
```

## Быстрый старт
### Требования

- Docker 20.10+
- Docker Compose 1.29+ или Compose V2
- 2 ГБ свободного места
- CPU-инференс, GPU не требуется

### Запуск сервиса

1. Скачайте файл test.csv из соревнования https://www.kaggle.com/competitions/teta-ml-1-2025 и разместите в директории ./data/train.csv.
2. Запустите Docker-окружение
```bash
docker compose up -d
```
3. Откройте Airflow UI: http://localhost:8080 (Логин: admin, Пароль: admin)
4. В интрфейсе Airflow откройте DAG fraud_marts_postgres, нажмите Trigger DAG и дождитесь успешного выполнения всех тасков
5. После выполнения DAG в схеме mart будут созданы следующие таблицы: mart_daily_state_metrics, mart_fraud_by_category, mart_fraud_by_state, mart_customer_risk_profile, mart_hourly_fraud_pattern, mart_merchant_analytics
