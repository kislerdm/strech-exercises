SHELL = /bin/sh

.DEFAULT_GOAL := help

help: ## Prints help message.
	@ grep -h -E '^[a-zA-Z0-9_-].+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[1m%-30s\033[0m %s\n", $$1, $$2}'

tests: setup .tests clean ## Runs the validation tests.

BASE := $(PWD)/.dev

setup: ## Setup local env.	@ echo "Provision environment"
	@ cd $(BASE) && docker build --label ex3 -t ex3 .
	@ docker run --rm -d --name ex3 \
 		-e POSTGRES_USER=admin \
 		-e POSTGRES_PASSWORD=admin \
 		-t ex3
	@ docker exec ex3 /bin/bash -c 'until psql -U admin -c "\q"; do >&2 echo "db is getting ready, waiting"; sleep 1; done'

.tests:
	@ echo "Run tests"
	@ docker exec ex3 psql -U admin -c "$$(cat solution.sql $(BASE)/tests.sql)"

clean: ## Clean local env.
	@ echo "Clean environment"
	@ docker rm -f ex3 > /dev/null 2>&1
	@ docker rmi ex3 > /dev/null 2>&1
