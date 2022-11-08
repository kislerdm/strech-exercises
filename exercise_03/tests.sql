-- GIVEN: then input table dim_dep_agreement; defined and filled below:
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
VALUES (1, 101, '2015-01-01', '2015-02-20', 20, 305, 3.5),
       (2, 101, '2015-02-21', '2015-05-17', 20, 345, 4),
       (3, 101, '2015-05-18', '2015-07-05', 20, 345, 4),
       (4, 101, '2015-07-06', '2015-08-22', 20, 539, 6),
       (5, 101, '2015-08-23', '9999-12-31', 20, 345, 4),
       (6, 102, '2016-01-01', '2016-06-30', 25, 333, 3.7),
       (7, 102, '2016-07-01', '2016-07-25', 25, 333, 3.7),
       (8, 102, '2016-07-26', '2016-09-15', 25, 333, 3.7),
       (9, 102, '2016-09-16', '9999-12-31', 25, 560, 5.9),
       (10, 103, '2011-05-22', '9999-12-31', 30, 560, 2)
;

-- WHEN: running to create and fill dim_dep_agreement_compacted:
CREATE TABLE IF NOT EXISTS dim_dep_agreement_compacted AS
WITH
    deduplication_ranges AS (
        SELECT agrmnt_id
             , actual_from_dt
             , actual_to_dt
             , client_id
             , product_id
             , interest_rate
             , COALESCE(
                 LAG((client_id, product_id, interest_rate), 1) OVER () != (client_id, product_id, interest_rate),
                 TRUE
               ) AS range_l
             , COALESCE(
                 LAG((client_id, product_id, interest_rate), -1) OVER () != (client_id, product_id, interest_rate),
                 TRUE
               ) AS range_r
        FROM dim_dep_agreement
        ORDER BY
            agrmnt_id
          , actual_from_dt
          , actual_to_dt
    )
        ,
    deduplication_trivial AS (
        SELECT agrmnt_id, actual_from_dt, actual_to_dt, client_id, product_id, interest_rate
        FROM deduplication_ranges
        WHERE range_l AND range_r
    )
        ,
    duplication_ranges_left AS (
        SELECT agrmnt_id, actual_from_dt, client_id, product_id, interest_rate
        FROM deduplication_ranges
        WHERE range_l AND NOT range_r
    )
        ,
    duplication_ranges_right AS (
        SELECT agrmnt_id, actual_to_dt, client_id, product_id, interest_rate
        FROM deduplication_ranges
        WHERE range_r AND NOT range_l
    )
        ,
    deduplication_result AS (
        SELECT duplication_ranges_left.agrmnt_id
             , duplication_ranges_left.actual_from_dt
             , duplication_ranges_right.actual_to_dt
             , duplication_ranges_left.client_id
             , duplication_ranges_left.product_id
             , duplication_ranges_left.interest_rate
        FROM duplication_ranges_left
        JOIN duplication_ranges_right USING (agrmnt_id, client_id, product_id, interest_rate)
        UNION
        SELECT *
        FROM deduplication_trivial
        ORDER BY
            agrmnt_id
          , actual_from_dt
          , actual_to_dt
    )

SELECT row_number() OVER () AS sk
     , deduplication_result.*
FROM deduplication_result
;


-- THEN: it is expected that the dim_dep_agreement_compacted content
-- is identical to the one of the table dim_dep_agreement_want; defined and filled below:
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
$$
BEGIN
    IF NOT pass THEN
        RAISE EXCEPTION 'TEST: %. <FAIL>', name;
    ELSE
        RAISE INFO 'TEST: %. <PASS>', name;
    END IF;
END
$$ LANGUAGE plpgsql
;

SELECT test('rows count must match', COUNT(got.sk) = COUNT(want.sk))
FROM dim_dep_agreement_compacted AS got
   , dim_dep_agreement_compacted_want AS want
;

SELECT test('business attributes and intervals must match', COUNT(want.sk) = (SELECT COUNT(1) FROM dim_dep_agreement_compacted_want))
FROM dim_dep_agreement_compacted_want AS want
FULL JOIN dim_dep_agreement_compacted AS got USING (agrmnt_id, actual_from_dt, actual_to_dt, client_id, product_id, interest_rate)
;
