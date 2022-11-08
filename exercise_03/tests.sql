SELECT test('rows count must match', COUNT(got.sk) = COUNT(want.sk))
FROM dim_dep_agreement_compacted AS got
   , dim_dep_agreement_compacted_want AS want
;

SELECT test('business attributes and intervals must match', COUNT(want.sk) = (SELECT COUNT(1) FROM dim_dep_agreement_compacted_want))
FROM dim_dep_agreement_compacted_want AS want
FULL JOIN dim_dep_agreement_compacted AS got USING (agrmnt_id, actual_from_dt, actual_to_dt, client_id, product_id, interest_rate)
;
