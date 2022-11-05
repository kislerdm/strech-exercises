.DEFAULT_GOAL := help

help: ## Prints help message.
	@ grep -h -E '^[a-zA-Z0-9_-].+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[1m%-30s\033[0m %s\n", $$1, $$2}'

setup.env:
	@ docker build -t jointask:test -f Dockerfile_dev .

SOLUTION := solution_0

tests: setup.env ## Runs unit tests.
	@ cd $(SOLUTION) && make tests

lint: setup.env
	@ cd $(SOLUTION) && make lint

PATH_DIR := $(PWD)/fixtures

run: ## Runs the application.
	@ docker run \
 		-v $(PATH_DIR):/data \
 		-v $(PWD)/$(SOLUTION)/main.py:/main.py \
  		python:3.9.15-slim-buster python /main.py
