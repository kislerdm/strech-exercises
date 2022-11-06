.DEFAULT_GOAL := help

help: ## Prints help message.
	@ grep -h -E '^[a-zA-Z0-9_-].+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[1m%-30s\033[0m %s\n", $$1, $$2}'

setup.env: ## Setup required env dependencies.
	@ docker compose -f docker-compose.yaml up \
 		-d --build --quiet-pull --remove-orphans

tests: ## Runs unit and data validation tests.
	@ docker compose -f docker-compose.yaml run \
 			--entrypoint="pytest" \
			-v $(PWD)/solution:/src \
			-v $(PWD)/fixtures/tests:/data \
			-e BASE_DIR=/data \
			dev --cov=main .

lint: ## Apply code linter.
	@ docker compose -f docker-compose.yaml \
		run \
			-v $(PWD)/solution:/app \
			-w /app \
			dev ls
#			dev flake8 . && mypy . && black -l 120 .

run: ## Runs the application.
	@ docker compose -f docker-compose.yaml \
     		run \
    			-v $(PWD)/fixtures/tests:/data \
    			-e BASE_DIR=/data \
     		exec python /main.py
