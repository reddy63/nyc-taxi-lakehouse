# ┌──────────────────────────────────────────────────────────────────┐
# │  NYC Taxi Data Lakehouse — Makefile                           │
# │  make <target>  to run any step                               │
# └──────────────────────────────────────────────────────────────────┘

.PHONY: help setup env-check build up down restart logs \\
        download upload \\
        bronze silver gold \\
        dbt-run dbt-test dbt-docs \\
        pipeline vacuum \\
        test lint clean

# ── defaults ───────────────────────────────────────────────────────────────
YEAR       ?= 2024
MONTHS     ?= 01
TAXI_TYPE  ?= yellow
SPARK      ?= spark-submit

include .env
export

# ── help ──────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  NYC Taxi Data Lakehouse — available targets"
	@echo ""
	@echo "  Infrastructure"
	@echo "    setup        Install Python dependencies"
	@echo "    build        docker compose build (Spark + Airflow images)"
	@echo "    up           Start all services"
	@echo "    down         Stop all services"
	@echo "    restart      down + up"
	@echo "    logs         Tail all container logs"
	@echo ""
	@echo "  Data"
	@echo "    download     Download TLC Parquet files  (YEAR=2024 MONTHS=01,02)"
	@echo "    upload       Upload to MinIO landing zone"
	@echo ""
	@echo "  Pipeline layers"
	@echo "    bronze       Run Bronze ingestion job"
	@echo "    silver       Run Silver transform job"
	@echo "    gold         Run Gold aggregation job"
	@echo "    pipeline     Run all three layers end-to-end"
	@echo "    vacuum       Vacuum all Delta tables"
	@echo ""
	@echo "  dbt"
	@echo "    dbt-run      dbt run (analytics marts)"
	@echo "    dbt-test     dbt test"
	@echo "    dbt-docs     dbt docs generate + serve"
	@echo ""
	@echo "  Quality"
	@echo "    test         pytest (all unit tests)"
	@echo "    lint         ruff + mypy"
	@echo "    clean        Remove temp files"
	@echo ""

# ── infrastructure ──────────────────────────────────────────────────────────
setup:
	cp .env.example .env 2>/dev/null || true
	pip install -r requirements.txt
	# Airflow containers run as AIRFLOW_UID (50000) but these dirs get bind-mounted from the
	# host, so they must be group/other-writable or Airflow can't write its logs (fails to
	# even start). This matches Airflow's own quick-start guidance for docker-compose setups.
	mkdir -p logs/airflow config
	chmod -R 777 logs config

	@echo "Dependencies installed."

build:
	docker compose build --no-cache

up:
	docker compose up -d
	@echo ""
	@echo "  Services    URL"
	@echo "  MinIO       http://localhost:9001  (minioadmin / minioadmin)"
	@echo "  Spark UI    http://localhost:8083"
	@echo "  Airflow     http://localhost:8082  (admin / admin)"
	@echo ""

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f

# ── data download + upload ────────────────────────────────────────────────────
download:
	bash scripts/download_data.sh $(YEAR) $(MONTHS)

upload:
	python scripts/upload_to_minio.py --year $(YEAR) --month $(MONTHS)

# ── Spark layers ───────────────────────────────────────────────────────────────
bronze:
	docker compose exec spark-master \\
	  spark-submit \\
	    --master $(SPARK_MASTER) \\
	    --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \\
	    /opt/spark/jobs/bronze/ingest_bronze.py \\
	    --year $(YEAR) --months $(MONTHS)

silver:
	docker compose exec spark-master \\
	  spark-submit \\
	    --master $(SPARK_MASTER) \\
	    --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \\
	    /opt/spark/jobs/silver/transform_silver.py \\
	    --year $(YEAR) --months $(MONTHS)

gold:
	docker compose exec spark-master \\
	  spark-submit \\
	    --master $(SPARK_MASTER) \\
	    --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \\
	    /opt/spark/jobs/gold/aggregate_gold.py \\
	    --year $(YEAR) --months $(MONTHS)

pipeline: bronze silver gold dbt-run dbt-test
	@echo "Full pipeline complete (Bronze -> Silver -> Gold -> dbt)"

vacuum:
	docker compose exec spark-master \\
	  spark-submit /opt/spark/jobs/utils/vacuum_all.py

# ── dbt ─────────────────────────────────────────────────────────────────────
dbt-run:
	cd dbt && dbt run --profiles-dir . --target dev

dbt-test:
	cd dbt && dbt test --profiles-dir . --target dev

dbt-docs:
	cd dbt && dbt docs generate --profiles-dir . --target dev
	cd dbt && dbt docs serve --port 8085

# ── quality ──────────────────────────────────────────────────────────────────
test:
	pytest tests/ -v --tb=short

lint:
	ruff check spark/ dags/ scripts/ tests/

clean:
	find . -type f -name '*.pyc' -delete
	find . -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true
	rm -rf dbt/target dbt/dbt_packages
	@echo "Clean done."
