test:
	docker context use desktop-linux
	docker-compose up --no-deps --build

test-clean:
	docker context use desktop-linux
	docker-compose up

first-run:
	docker context use prod
	docker-compose up -d

deploy:
	docker context use prod
	docker-compose up -d --no-deps --build dagster_user_code
	docker-compose up -d --no-deps --build dagster_daemon
	docker-compose up -d --no-deps --build dagit

rm:
	docker context use prod
	docker-compose down