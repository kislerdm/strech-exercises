.DEFAULT_GOAL := help

help: ## Prints help message.
	@ grep -h -E '^[a-zA-Z0-9_-].+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[1m%-30s\033[0m %s\n", $$1, $$2}'

setup.env: ## Setup required env dependencies.
	@ docker compose up -d --build --quiet-pull --remove-orphans

tests: ## Runs unit and data validation tests.
	@ docker compose run --entrypoint="pytest" \
		-v $(PWD)/solution:/src \
		-v $(PWD)/fixtures/tests:/data \
		-e BASE_DIR=/data \
	  dev --cov=main .

run: ## Runs the application.
	@ docker compose run --entrypoint="python" \
		-v $(PWD)/fixtures/tests:/data \
		-e BASE_DIR=/data \
	  exec /main.py

linting: ## Apply code linter.
	@ docker compose run \
 		--entrypoint="isort . && flake8 . && mypy . && black -l 120 ." \
		-v $(PWD)/solution:/src \
		-v $(PWD)/tox.ini:/src/tox.ini \
	  dev
