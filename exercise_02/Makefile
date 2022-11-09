.DEFAULT_GOAL := help

help: ## Prints help message.
	@ grep -h -E '^[a-zA-Z0-9_-].+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[1m%-30s\033[0m %s\n", $$1, $$2}'

LABEL := ex2
USER := admin
PASSWORD := admin
PORT := 25432

tests: setup .tests clean ## Runs the validation tests.

setup: ## Setup required env dependencies.
	@ echo "Provision environment"
	@ docker compose up -d

.tests:
	@ echo "Run tests"
	@ cat tests_setup.sql solution.sql tests.sql > .tests.sql
	@ docker compose run --rm --name $(LABEL)runner \
 		-e PGPASSWORD=$(PASSWORD) \
		--entrypoint="psql" \
		db -h localhost -p ${PORT} -U ${USER} -c "$$(cat .tests.sql)"
	@ rm .tests.sql

clean: ## Stop docker containers.
	@ echo "Clean environment"
	@ docker compose down
