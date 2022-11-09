SELECT test('query results must match the reference', SUM(COALESCE(want.total - got.total, 0)) = 0)
FROM want
LEFT JOIN got USING (transaction_id, user_id, date)
;
