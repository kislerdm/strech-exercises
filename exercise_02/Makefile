# Copyright 2022 Dmitry Kisler <admin@dkisler.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

SHELL = /bin/sh

.DEFAULT_GOAL := help

help: ## Prints help message.
	@ grep -h -E '^[a-zA-Z0-9_-].+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[1m%-30s\033[0m %s\n", $$1, $$2}'

tests: setup .tests clean ## Runs the validation tests.

BASE := $(PWD)/.dev

setup: ## Setup local env.
	@ echo "Provision environment"
	@ docker run --rm --name ex2generate \
		-w /app \
		-v $(BASE):/app \
		-v $(BASE)/fixtures:/fixtures \
		-e BASE_DIR=/fixtures \
        -e NUM_USERS=10 \
        golang:1.19.3 \
        /bin/bash -c 'python3 generate_data.py && go run main.go'

	@ cd $(BASE) && docker build --label ex2 -t ex2 .
	@ docker run -d --rm --name ex2 \
		-v $(BASE)/fixtures:/fixtures \
		-e POSTGRES_USER=admin \
		-e POSTGRES_PASSWORD=admin \
		-e BASE_DIR=/fixtures \
		-t ex2
	@ docker exec ex2 /bin/bash -c 'until psql -U admin -c "\q"; do >&2 echo "db is getting ready, waiting"; sleep 1; done'

.tests:
	@ echo "Run tests"
	@ echo "CREATE TABLE IF NOT EXISTS got AS " > $(BASE)/.tests.sql
	@ cat solution.sql $(BASE)/tests.sql >> $(BASE)/.tests.sql
	@ docker exec ex2 psql -U admin -c "$$(cat $(BASE)/.tests.sql)"

clean: ## Clean local env.
	@ echo "Clean environment"
	@ if [ -f $(BASE)/.tests.sql ]; then rm $(BASE)/.tests.sql; fi
	@ if [ -d $(BASE)/fixtures ]; then rm -r $(BASE)/fixtures; fi
	@ docker stop ex2 > /dev/null 2>&1
	@ docker rmi ex2 > /dev/null 2>&1

tests.ci: tests
