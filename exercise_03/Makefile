.DEFAULT_GOAL := help

help: ## Prints help message.
	@ grep -h -E '^[a-zA-Z0-9_-].+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[1m%-30s\033[0m %s\n", $$1, $$2}'

USER := admin
PASSWORD := admin
DB := postgres
PORT := 15432

tests: setup .tests clean ## Runs the validation tests.

setup: ## Setup required env dependencies.
	@ echo "Provision environment"
	@ docker build --label ex3 -t ex3 .
	@ docker run --rm -d --name ex3 \
 		--net ex3 \
 		-p $(PORT):5432 \
 		-e POSTGRES_USER=$(USER) \
 		-e POSTGRES_PASSWORD=$(PASSWORD) \
 		-e POSTGRES_DB=$(DB) \
 		-t ex3
	@ sleep 3 # simple way to make sure db is ready

.tests:
	@ echo "Run tests"
	@ cat solution.sql tests.sql > .tests.sql
	@ docker run --rm --name ex3runner \
 		--net ex3 \
 		-e PGPASSWORD=$(PASSWORD) \
		--entrypoint="psql" \
		-t ex3 -h ex3 -U ${USER} -d ${DB} -c "$$(cat .tests.sql)"
	@ rm .tests.sql

clean: ## Stop docker container.
	@ docker rm -f ex3 > /dev/null 2>&1
	@ docker rmi ex3 > /dev/null 2>&1
