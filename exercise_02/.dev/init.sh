#! /bin/bash

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

CREATE TABLE IF NOT EXISTS want (
    transaction_id UUID,
    user_id        UUID,
    date           DATE,
    total          INTEGER
)
;

COPY want
FROM '${BASE_DIR}/want.csv'
DELIMITER ','
CSV HEADER
;

CREATE OR REPLACE FUNCTION test(name TEXT, pass BOOL) RETURNS VOID AS
\$\$
BEGIN
    IF NOT pass THEN
        RAISE EXCEPTION 'TEST: %. <FAIL>', name;
    ELSE
        RAISE INFO 'TEST: %. <PASS>', name;
    END IF;
END
\$\$ LANGUAGE plpgsql
;

EOSQL
}

main() {
  check_env_vars_set
  init_user_and_db
}

main "$@"
