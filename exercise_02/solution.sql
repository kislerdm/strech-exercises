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
