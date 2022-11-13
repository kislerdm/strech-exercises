#! /bin/bash

set -o errexit

readonly REQUIRED_ENV_VARS=(
  "POSTGRES_USER"
  "POSTGRES_DB"
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
CREATE TABLE IF NOT EXISTS dim_dep_agreement (
     sk             SERIAL2,
     agrmnt_id      INT,
     actual_from_dt DATE,
     actual_to_dt   DATE,
     client_id      INT,
     product_id     INT,
     interest_rate  NUMERIC (2,1)
)
;

INSERT INTO dim_dep_agreement
VALUES
      (1, 101, '2015-01-01', '2015-02-20', 20, 305, 3.5)
    , (2, 101, '2015-02-21', '2015-05-17', 20, 345, 4)
    , (3, 101, '2015-05-18', '2015-07-05', 20, 345, 4)
    , (4, 101, '2015-07-06', '2015-08-22', 20, 539, 6)
    , (5, 101, '2015-08-23', '9999-12-31', 20, 345, 4)
    , (6, 102, '2016-01-01', '2016-06-30', 25, 333, 3.7)
    , (7, 102, '2016-07-01', '2016-07-25', 25, 333, 3.7)
    , (8, 102, '2016-07-26', '2016-09-15', 25, 333, 3.7)
    , (9, 102, '2016-09-16', '9999-12-31', 25, 560, 5.9)
    , (10, 103, '2011-05-22', '9999-12-31', 30, 560, 2)
;

CREATE TABLE IF NOT EXISTS dim_dep_agreement_compacted_want (
    sk             SERIAL2,
    agrmnt_id      INT,
    actual_from_dt DATE,
    actual_to_dt   DATE,
    client_id      INT,
    product_id     INT,
    interest_rate  NUMERIC (2,1)
)
;

INSERT INTO dim_dep_agreement_compacted_want
VALUES
      (1, 101, '2015-01-01', '2015-02-20', 20, 305, 3.5)
    , (2, 101, '2015-02-21', '2015-07-05', 20, 345, 4)
    , (3, 101, '2015-07-06', '2015-08-22', 20, 539, 6)
    , (4, 101, '2015-08-23', '9999-12-31', 20, 345, 4)
    , (5, 102, '2016-01-01', '2016-09-15', 25, 333, 3.7)
    , (6, 102, '2016-09-16', '9999-12-31', 25, 560, 5.9)
    , (7, 103, '2011-05-22', '9999-12-31', 30, 560, 2)
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
