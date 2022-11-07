-- slowest, but straightforward:
SELECT a.transaction_id
     , a.user_id
     , a.date
     , COALESCE(COUNT(DISTINCT b.transaction_id), 0) AS total
FROM transactions      AS a
LEFT JOIN transactions AS b ON b.user_id = a.user_id
    AND b.date < a.date
    AND b.date >= a.date - 7
GROUP BY 1, 2, 3
ORDER BY 2, 3
;

-- GroupAggregate  (cost=421742.90..436832.25 rows=100000 width=44) (actual time=3430.284..4522.810 rows=100000 loops=1)
-- "  Group Key: a.user_id, a.date, a.transaction_id"
--   ->  Sort  (cost=421742.90..424560.77 rows=1127148 width=52) (actual time=3430.262..3854.149 rows=1090418 loops=1)
-- "        Sort Key: a.user_id, a.date, a.transaction_id"
--         Sort Method: external merge  Disk: 66016kB
--         ->  Hash Left Join  (cost=3967.00..231384.60 rows=1127148 width=52) (actual time=41.550..2083.095 rows=1090418 loops=1)
--               Hash Cond: (a.user_id = b.user_id)
--               Join Filter: ((b.date >= (a.date - 7)) AND (b.date <= (a.date - 1)))
--               Rows Removed by Join Filter: 9014672
--               ->  Seq Scan on transactions a  (cost=0.00..1935.00 rows=100000 width=36) (actual time=0.041..59.053 rows=100000 loops=1)
--               ->  Hash  (cost=1935.00..1935.00 rows=100000 width=36) (actual time=40.930..40.930 rows=100000 loops=1)
--                     Buckets: 65536  Batches: 4  Memory Usage: 2188kB
--                     ->  Seq Scan on transactions b  (cost=0.00..1935.00 rows=100000 width=36) (actual time=0.003..15.785 rows=100000 loops=1)
-- Planning time: 0.441 ms
-- Execution time: 4545.436 ms


-- fast and flexible:

WITH
    transactions_daily AS (
        SELECT user_id
             , date
             , COUNT(DISTINCT transaction_id) AS cnt_daily
        FROM transactions
        GROUP BY 1, 2
    )

SELECT orig.transaction_id
     , a.user_id
     , a.date
     , COALESCE(SUM(b.cnt_daily), 0) AS total
FROM transactions            AS orig
JOIN transactions_daily      AS a ON (a.user_id, a.date) = (orig.user_id, orig.date)
LEFT JOIN transactions_daily AS b ON b.user_id = a.user_id
    AND b.date < a.date
    AND b.date >= a.date - 7
GROUP BY 1, 2, 3
ORDER BY 2, 3
;

-- GroupAggregate  (cost=32365.64..33060.09 rows=27778 width=68) (actual time=3057.981..3240.512 rows=100000 loops=1)
-- "  Group Key: a.user_id, a.date, orig.transaction_id"
--   CTE transactions_daily
--     ->  GroupAggregate  (cost=12976.82..14076.82 rows=10000 width=28) (actual time=83.202..413.281 rows=50916 loops=1)
-- "          Group Key: transactions.user_id, transactions.date"
--           ->  Sort  (cost=12976.82..13226.82 rows=100000 width=36) (actual time=83.182..144.831 rows=100000 loops=1)
-- "                Sort Key: transactions.user_id, transactions.date"
--                 Sort Method: external merge  Disk: 4488kB
--                 ->  Seq Scan on transactions  (cost=0.00..1935.00 rows=100000 width=36) (actual time=0.023..12.702 rows=100000 loops=1)
--   ->  Sort  (cost=18288.82..18358.27 rows=27778 width=44) (actual time=3057.965..3093.731 rows=448516 loops=1)
-- "        Sort Key: a.user_id, a.date, orig.transaction_id"
--         Sort Method: external sort  Disk: 25376kB
--         ->  Merge Left Join  (cost=10588.58..16238.58 rows=27778 width=44) (actual time=674.948..2532.734 rows=448516 loops=1)
--               Merge Cond: (a.user_id = b.user_id)
--               Join Filter: ((b.date < a.date) AND (b.date >= (a.date - 7)))
--               Rows Removed by Join Filter: 4672418
--               ->  Sort  (cost=9724.19..9736.69 rows=5000 width=36) (actual time=653.383..664.232 rows=100000 loops=1)
--                     Sort Key: a.user_id
--                     Sort Method: external merge  Disk: 4488kB
--                     ->  Hash Join  (cost=4217.00..9417.00 rows=5000 width=36) (actual time=149.470..623.834 rows=100000 loops=1)
--                           Hash Cond: ((a.user_id = orig.user_id) AND (a.date = orig.date))
--                           ->  CTE Scan on transactions_daily a  (cost=0.00..200.00 rows=10000 width=20) (actual time=83.205..424.473 rows=50916 loops=1)
--                           ->  Hash  (cost=1935.00..1935.00 rows=100000 width=36) (actual time=66.093..66.093 rows=100000 loops=1)
--                                 Buckets: 65536  Batches: 2  Memory Usage: 3839kB
--                                 ->  Seq Scan on transactions orig  (cost=0.00..1935.00 rows=100000 width=36) (actual time=0.010..8.604 rows=100000 loops=1)
--               ->  Sort  (cost=864.39..889.39 rows=10000 width=28) (actual time=21.547..747.962 rows=5115484 loops=1)
--                     Sort Key: b.user_id
--                     Sort Method: external sort  Disk: 2096kB
--                     ->  CTE Scan on transactions_daily b  (cost=0.00..200.00 rows=10000 width=28) (actual time=0.014..3.907 rows=50916 loops=1)
-- Planning time: 0.409 ms
-- Execution time: 3260.573 ms
