.DEFAULT_GOAL := help

help: ## Prints help message.
	@ grep -h -E '^[a-zA-Z0-9_-].+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[1m%-30s\033[0m %s\n", $$1, $$2}'

tests: setup .tests clean ## Runs the validation tests.

setup: ## Setup required env dependencies.
	@ echo "Provision environment"
	@ docker compose up --remove-orphans -d
	@ docker exec ex2db /bin/bash -c 'until psql -U admin -c "\q"; do >&2 echo "db is getting ready, waiting"; sleep 1; done'
	@ cat tests_setup.sql solution.sql tests.sql > .tests.sql

.tests:
	@ echo "Run tests"
	@ docker exec ex2db psql -U admin -c "$$(cat .tests.sql)"

clean: ## Stop docker containers.
	@ echo "Clean environment"
	@ rm .tests.sql
	@ rm -r fixtures
	@ docker compose down
