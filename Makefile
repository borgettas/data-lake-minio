build:
	docker build -f airflow/Dockerfile -t brewery-pipeline:latest .

up-db:
	docker compose up airflow-init

up:
	docker compose up -d