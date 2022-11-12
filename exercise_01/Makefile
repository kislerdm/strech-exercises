.DEFAULT_GOAL := help

help: ## Prints help message.
	@ grep -h -E '^[a-zA-Z0-9_-].+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[1m%-30s\033[0m %s\n", $$1, $$2}'

setup: ## Setup required env dependencies.
	@ echo "Provision environment"
	@ docker run --rm --name ex1generate \
		-w /app \
		-v $(PWD):/app \
		-v $(PWD)/fixtures:/fixtures \
		-e BASE_DIR=/fixtures \
	    -e NUM_USERS=1000 \
	  	python:3.9.15-slim-buster python3 generate_data.py

	@ docker build --label ex1 -t ex1 -f Dockerfile_db .
	@ docker run -d --rm --name ex1 \
		-v $(PWD)/fixtures:/fixtures \
		-e POSTGRES_USER=admin \
		-e POSTGRES_PASSWORD=admin \
		-e BASE_DIR=/fixtures \
		-t ex1
	@ docker exec ex1 /bin/bash -c 'until psql -U admin -c "\q"; do >&2 echo "db is getting ready, waiting"; sleep 1; done'

	@ docker build --label ex1dev -t ex1dev -f Dockerfile_dev .

clean: ## Stop docker containers.
	@ echo "Clean environment"
	@ rm -r fixtures
	@ docker stop ex1 > /dev/null 2>&1
	@ docker rmi -f ex1 > /dev/null 2>&1
	@ docker rmi -f ex1dev > /dev/null 2>&1

tests: ## Runs unit and data validation tests.
	@ docker run --rm --entrypoint="pytest" \
		-v $(PWD)/solution:/src \
		-v $(PWD)/fixtures:/fixtures \
		-e BASE_DIR=/fixtures \
	  ex1dev --cov=main -vs .

run: ## Runs the application.
	@ docker run --rm \
		--memory=512m \
		--cpus=.5 \
		-v $(PWD)/solution/main.py:/main.py \
		-v $(PWD)/fixtures:/fixtures \
		-e BASE_DIR=/fixtures \
	  python:3.9.15-slim-buster python3 /main.py

profiling: ## Runs application profiling.
	@ docker run --rm \
		--memory=512m \
    	--cpus=.5 \
		-v $(PWD)/solution/main.py:/main.py \
		-v $(PWD)/fixtures:/fixtures \
		-e BASE_DIR=/fixtures \
	  python:3.9.15-slim-buster python3 -m cProfile -s tottime /main.py

linting: ## Apply code linter.
	@ docker run --rm \
		-w /src \
		-v $(PWD)/solution:/src \
		-v $(PWD)/tox.ini:/src/tox.ini \
	  ex1dev -c "isort . && flake8 . && mypy . && black -l 120 ."
