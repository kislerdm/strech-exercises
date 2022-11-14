#! /bin/bash

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

set -o errexit

readonly REQUIRED_ENV_VARS=(
  "POSTGRES_USER"
  "POSTGRES_DB"
  "BASE_DIR"
)

check_env_vars_set() {
  for required_env_var in "${REQUIRED_ENV_VARS[@]}"; do
    if [[ -z "${!required_env_var}" ]]; then
      echo "Error: environment variable '${required_env_var}' not set.
      Make sure you have the following environment variables set:
      ${REQUIRED_ENV_VARS[*]}
      Aborting."
      exit 1
    fi
  done
}

init_user_and_db() {
  psql -v ON_ERROR_STOP=1 -U ${POSTGRES_USER} -d ${POSTGRES_DB} <<-EOSQL
CREATE EXTENSION "uuid-ossp";

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id          UUID,
    date                    DATE,
    user_id                 UUID,
    is_blocked              BOOL,
    transaction_amount      INTEGER,
    transaction_category_id INTEGER
)
;

COPY transactions
FROM '${BASE_DIR}/transactions.csv'
DELIMITER ','
CSV HEADER
;

CREATE TABLE IF NOT EXISTS users (
    user_id   UUID,
    is_active BOOLEAN
)
;

COPY users
FROM '${BASE_DIR}/users.csv'
DELIMITER ','
CSV HEADER
;

CREATE TABLE IF NOT EXISTS result (
    transaction_category_id   INTEGER,
    sum_amount                INTEGER,
    num_users                 INTEGER
)
;

INSERT INTO result
SELECT t.transaction_category_id,
       SUM(t.transaction_amount) AS sum_amount,
       COUNT(DISTINCT t.user_id) AS num_users
FROM transactions t
JOIN users u USING (user_id)
WHERE NOT t.is_blocked
  AND u.is_active
GROUP BY t.transaction_category_id
ORDER BY sum_amount DESC
;

COPY result
TO '${BASE_DIR}/result.csv'
DELIMITER ','
CSV HEADER
;

EOSQL
}

main() {
  check_env_vars_set
  init_user_and_db
}

main "$@"
