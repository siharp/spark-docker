build:
	docker compose build

build-nc:
	docker compose build --no-cache

build-progress:
	docker compose build --no-cache --progress=plain

down:
	docker compose down
clean:
	docker compose down --volumes --remove-orphans

run:
	docker compose up

run-scaled:
	make down && docker compose up --scale spark-worker=3

run-d:
	docker compose up -d

stop:
	docker compose stop

dev:
	docker exec -it da-spark-master bash
