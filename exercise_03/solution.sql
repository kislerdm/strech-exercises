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
    ),

    deduplication_trivial AS (
        SELECT agrmnt_id, actual_from_dt, actual_to_dt, client_id, product_id, interest_rate
        FROM deduplication_ranges
        WHERE range_l AND range_r
    ),

    duplication_ranges_left AS (
        SELECT agrmnt_id, actual_from_dt, client_id, product_id, interest_rate
        FROM deduplication_ranges
        WHERE range_l AND NOT range_r
    ),

    duplication_ranges_right AS (
        SELECT agrmnt_id, actual_to_dt, client_id, product_id, interest_rate
        FROM deduplication_ranges
        WHERE range_r AND NOT range_l
    ),

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
