-- Every order-affecting message as one table, in the shape the replay expects.
-- kind: 0 add (A/F), 1 execute (E), 2 execute at price (C), 3 cancel (X), 4 delete (D), 5 replace (U).
-- side: 0 buy, 1 sell for adds, -1 otherwise.
select seq, ts_ns, 0 as kind, locate, ref, cast(0 as bigint) as new_ref,
       case when side = 'S' then 1 else 0 end as side, shares, price from add_order
union all
select seq, ts_ns, case when type = 'E' then 1 else 2 end, locate, ref, 0, -1, shares,
       cast(coalesce(price, 0) as bigint) from execution
union all
select seq, ts_ns, 3, locate, ref, 0, -1, cancelled, cast(0 as bigint) from cancel
union all
select seq, ts_ns, 4, locate, ref, 0, -1, cast(0 as bigint), cast(0 as bigint) from `delete`
union all
select seq, ts_ns, 5, locate, ref, new_ref, -1, shares, price from replace
