SELECT test('business attributes and intervals must match', COUNT(1) = 0)
FROM dim_dep_agreement_compacted_want AS want
FULL JOIN dim_dep_agreement_compacted AS got USING (agrmnt_id, actual_from_dt, actual_to_dt, client_id, product_id, interest_rate)
WHERE got.sk IS NULL OR want.sk is NULL
;
