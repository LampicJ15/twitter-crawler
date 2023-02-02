run-docker:
	docker-compose -f docker/docker-compose.yaml up -d && docker-compose -f docker/docker-compose.yaml logs -f

build-docker:
	docker-compose -f docker/docker-compose.yaml build

stop-docker:
	docker-compose -f docker/docker-compose.yaml down

restart-docker:
	docker-compose -f docker/docker-compose.yaml restart && docker-compose -f docker/docker-compose.yaml logs -f