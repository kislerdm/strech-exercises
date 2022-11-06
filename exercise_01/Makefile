.DEFAULT_GOAL := help

help: ## Prints help message.
	@ grep -h -E '^[a-zA-Z0-9_-].+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[1m%-30s\033[0m %s\n", $$1, $$2}'

setup.env: ## Setup required env dependencies.
	@ docker compose -f docker-compose.yaml up -d --force-recreate --quiet-pull --remove-orphans

SOLUTION := solution_0

tests: ## Runs unit tests.
	@ docker compose -f docker-compose.yaml \
 		run \
			-v $(PWD):/src \
			-v $(PWD)/fixtures/tests:/data \
			-e BASE_DIR=/data \
 		exec pytest --cov=main -vv solution_0/

lint: ## Apply code linter.
	@ cd $(SOLUTION) && make lint

PATH_DIR := $(PWD)/fixtures

run: ## Runs the application.
	@ docker run \
 		-v $(PATH_DIR):/data \
 		-v $(PWD)/$(SOLUTION)/main.py:/main.py \
  		python:3.9.15-slim-buster python /main.py
