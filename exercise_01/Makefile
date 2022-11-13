SHELL = /bin/sh

.DEFAULT_GOAL := help

help: ## Prints help message.
	@ grep -h -E '^[a-zA-Z0-9_-].+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[1m%-30s\033[0m %s\n", $$1, $$2}'

BASE := $(PWD)/.dev
BASE_DIR := $(BASE)/fixtures

setup: ## Setup local env.
	@ echo "Provision environment"
	@ docker run --rm --name ex1generate \
		-w /app \
		-v $(BASE):/app \
		-v $(BASE_DIR):/fixtures \
		-e BASE_DIR=/fixtures \
	    -e NUM_USERS=1000 \
	  	python:3.9.15-slim-buster python3 generate_data.py

	@ cd $(BASE) && docker build --label ex1 -t ex1 -f Dockerfile_db .
	@ docker run -d --rm --name ex1 \
		-v $(BASE_DIR):/fixtures \
		-e POSTGRES_USER=admin \
		-e POSTGRES_PASSWORD=admin \
		-e BASE_DIR=/fixtures \
		-t ex1
	@ docker exec ex1 /bin/bash -c 'until psql -U admin -c "\q"; do >&2 echo "db is getting ready, waiting"; sleep 1; done'

	@ cd $(BASE) && docker build --label ex1dev -t ex1dev -f Dockerfile_dev .

tests: ## Runs unit and data validation tests.
	@ docker run --rm --entrypoint="pytest" \
		-v $(PWD)/solution:/src \
		-v $(BASE_DIR):/fixtures \
		-e BASE_DIR=/fixtures \
	  ex1dev --cov=main -vs .

run: ## Runs the application.
	@ docker run --rm \
		--memory=512m \
		--cpus=.5 \
		-v $(PWD)/solution/main.py:/main.py \
		-v $(BASE_DIR):/fixtures \
		-e BASE_DIR=/fixtures \
	  python:3.9.15-slim-buster python3 /main.py

profiling: ## Runs application profiling.
	@ docker run --rm \
		--memory=512m \
    	--cpus=.5 \
		-v $(PWD)/solution/main.py:/main.py \
		-v $(BASE_DIR):/fixtures \
		-e BASE_DIR=/fixtures \
	  python:3.9.15-slim-buster python3 -m cProfile -s tottime /main.py

linting: ## Apply code linter.
	@ docker run --rm \
		-w /src \
		-v $(PWD)/solution:/src \
		-v $(BASE)/tox.ini:/src/tox.ini \
	  ex1dev -c "isort . && flake8 . && mypy . && black -l 120 ."

clean: ## Clean local env.
	@ echo "Clean environment"
	@ if [ -d $(BASE_DIR) ]; then rm -r $(BASE_DIR); fi
	@ if [ -d ./solution/tox.ini ]; then rm ./solution/tox.ini; fi
	@ if [ -d ./solution/.coverage ]; then rm ./solution/.coverage; fi
	@ if [ -d ./solution/.mypy_cache ]; then rm -r ./solution/.mypy_cache; fi
	@ if [ -d ./solution/.pytest_cache ]; then rm -r ./solution/.pytest_cache; fi
	@ docker stop ex1 > /dev/null 2>&1
	@ docker rmi -f ex1 > /dev/null 2>&1
	@ docker rmi -f ex1dev > /dev/null 2>&1

go.setup: ## Runs unit of the go app, and builds the binary if the tests are OK.
	@ docker run --rm \
		-w /src/alternative \
		-v $(BASE):/src \
		-v $(BASE_DIR):/fixtures \
		-e BASE_DIR=/fixtures \
	  golang:1.19.3 /bin/sh -c 'go mod tidy && go test . && go build -o ../.dev/goapp .'

go.run:
	@ docker run --rm \
		--memory=512m \
		--cpus=.5 \
		-w /src \
		-v $(BASE):/src \
		-v $(BASE_DIR):/fixtures \
		-e BASE_DIR=/fixtures \
		golang:1.19.3 ./goapp
