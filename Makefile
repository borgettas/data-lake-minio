build:
	docker build -f airflow/Dockerfile -t brewery-pipeline:latest .

up-db:
	docker compose up airflow-init

up:
	docker compose up -d

it:
	docker exec -it desafio-bees-airflow-apiserver-1 /bin/bash