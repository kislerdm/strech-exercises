.DEFAULT_GOAL := help

help: ## Prints help message.
	@ grep -h -E '^[a-zA-Z0-9_-].+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[1m%-30s\033[0m %s\n", $$1, $$2}'

setup.env:
	@ docker build -t jointask:test -f Dockerfile_dev .

SOLUTION := solution_0

tests: setup.env ## Runs unit tests.
	@ cd $(SOLUTION) && make tests

PATH_USERS := $(PWD)/fixtures/users.csv
PATH_TRANSACTIONS := $(PWD)/fixtures/transactions.csv

run: ## Runs the application.
	@ docker run \
 		-v $(PATH_USERS):/data/users.csv \
 		-v $(PATH_TRANSACTIONS):/data/transactions.csv \
 		-v $(PWD)/main.py:/main.py \
  		python:3.9.15-slim-buster python /main.py
