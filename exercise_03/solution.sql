/*

create table if not exists dim_dep_agreement (
     sk             int,
     agrmnt_id      int,
     actual_from_dt date,
     actual_to_dt   date,
     client_id      int,
     product_id     int,
     interest_rate  float
)
;

insert into dim_dep_agreement values
(1, 101, '2015-01-01', '2015-02-20', 20, 305, 3.5),
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

create table if not exists dim_dep_agreement_compacted (
    sk series8,
    agrmnt_id      int,
    actual_from_dt date,
    actual_to_dt   date,
    client_id      int,
    product_id     int,
    interest_rate  float
)
;
*/
WITH
    rnk as (
        select agrmnt_id, actual_from_dt, actual_to_dt, client_id, product_id, interest_rate
        , lag((client_id, product_id, interest_rate), 1) over (partition by agrmnt_id)
       from dim_dep_agreement as a
    )
    ,
    limits as (
        select *
             , min(rk) over (partition by agrmnt_id, client_id, product_id, interest_rate) as rk_min
             , max(rk) over (partition by agrmnt_id, client_id, product_id, interest_rate) as rk_max
        from rnk
    )
select a.agrmnt_id
     , a.actual_from_dt
     , b.actual_to_dt
     , a.client_id
     , a.product_id
     , a.interest_rate
from limits as a
join limits as b using (agrmnt_id, client_id, product_id, interest_rate)
where a.rk = a.rk_min
and b.rk = b.rk_max
;

