.DEFAULT_GOAL := help

help: ## Prints help message.
	@ grep -h -E '^[a-zA-Z0-9_-].+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[1m%-30s\033[0m %s\n", $$1, $$2}'

tests: setup .tests clean ## Runs the validation tests.

setup: ## Setup required env dependencies.
	@ echo "Provision environment"
	@ docker run --rm --name ex2generate \
		-w /app \
		-v $(PWD):/app \
		-v $(PWD)/fixtures:/fixtures \
		-e BASE_DIR=/fixtures \
        -e NUM_USERS=10 \
        golang:1.19.3 \
        /bin/bash -c 'python3 generate_data.py && go run main.go'

	@ docker build --label ex2 -t ex2 .
	@ docker run -d --rm --name ex2 \
		-v $(PWD)/fixtures:/fixtures \
		-e POSTGRES_USER=admin \
		-e POSTGRES_PASSWORD=admin \
		-e BASE_DIR=/fixtures \
		-t ex2
	@ docker exec ex2 /bin/bash -c 'until psql -U admin -c "\q"; do >&2 echo "db is getting ready, waiting"; sleep 1; done'

.tests:
	@ echo "Run tests"
	@ cat tests_setup.sql solution.sql tests.sql > .tests.sql
	@ docker exec ex2 psql -U admin -c "$$(cat .tests.sql)"

clean: ## Stop docker containers.
	@ echo "Clean environment"
	@ rm .tests.sql
	@ rm -r fixtures
	@ docker stop ex2 > /dev/null 2>&1
	@ docker rmi ex2 > /dev/null 2>&1
