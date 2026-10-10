-- How long do displayed orders live? Censoring-aware.
--
-- An order ends at the first of: a delete, a replace of that order, or the moment executions and
-- cancels have used up all of its shares. A replace ends the old order and creates a new one, and
-- each order id is treated as its own order. Orders created by a replace are included.
--
-- The sample stops at a fixed time, so an order added just before the stop has had no chance to
-- end. Counting only orders that ended inside the window would over-represent short lives. To avoid
-- that, the cohort is every order added at or before :cohort_end_ns, which the caller sets to the end
-- of the sample minus the follow-up period (here 10 minutes). Every cohort order was therefore
-- observable for the full follow-up, so any life shorter than that is fully observed.
with orders as (
    select ref, ts_ns as added_ns, shares as start_shares from add_order
    union all
    select new_ref as ref, ts_ns as added_ns, shares as start_shares from replace
),
reductions as (
    select ref, seq, ts_ns, shares as dec from execution
    union all
    select ref, seq, ts_ns, cancelled as dec from cancel
),
running as (
    select ref, ts_ns,
           sum(dec) over (partition by ref order by seq rows between unbounded preceding and current row) as used
    from reductions
),
used_up as (
    select r.ref, min(r.ts_ns) as used_up_ns
    from running r join orders o on o.ref = r.ref
    where r.used >= o.start_shares
    group by r.ref
),
ends as (
    select o.ref, o.added_ns,
           d.ts_ns as deleted_ns, p.ts_ns as replaced_ns, u.used_up_ns
    from orders o
    left join `delete` d on d.ref = o.ref
    left join replace  p on p.ref = o.ref
    left join used_up  u on u.ref = o.ref
    where o.added_ns <= :cohort_end_ns
),
classified as (
    select added_ns,
           least(deleted_ns, replaced_ns, used_up_ns) as end_ns,
           case when least(deleted_ns, replaced_ns, used_up_ns) is null then null
                when least(deleted_ns, replaced_ns, used_up_ns) = deleted_ns  then 'deleted'
                when least(deleted_ns, replaced_ns, used_up_ns) = replaced_ns then 'replaced'
                else 'executed or cancelled in full' end as how_it_ended
    from ends
)
select coalesce(how_it_ended, 'still resting after follow-up') as how_it_ended,
       case when end_ns is null                       then '8 survived the follow-up'
            when end_ns - added_ns <        1000000   then '1 under 1 ms'
            when end_ns - added_ns <       10000000   then '2 1-10 ms'
            when end_ns - added_ns <      100000000   then '3 10-100 ms'
            when end_ns - added_ns <     1000000000   then '4 100 ms-1 s'
            when end_ns - added_ns <    10000000000   then '5 1-10 s'
            when end_ns - added_ns <    60000000000   then '6 10-60 s'
            else                                           '7 1 min to follow-up' end as lifetime,
       count(*) as orders
from classified
group by 1, 2
order by 2, 1
