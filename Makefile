up:
	docker compose up -d

down:
	docker compose down

test:
	python3 -m pytest tests/ -q

spark:
	docker compose --profile spark up -d spark

producer:
	python3 src/producer/producer.py
