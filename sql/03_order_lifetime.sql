-- How long do displayed orders live? Lifetime of each order that is later deleted, bucketed.
-- Orders replaced (cancel/replace), executed, or still resting are counted separately by fate.
with fate as (
    select a.ref, a.ts_ns as added_ns, a.shares,
           d.ts_ns as deleted_ns,
           r.ts_ns as replaced_ns,
           e.first_exec_ns
    from add_order a
    left join `delete` d on d.ref = a.ref
    left join replace  r on r.ref = a.ref
    left join (select ref, min(ts_ns) as first_exec_ns from execution group by ref) e on e.ref = a.ref
)
select case when deleted_ns is not null  then 'deleted'
            when replaced_ns is not null then 'replaced'
            when first_exec_ns is not null then 'executed at least once, not deleted or replaced'
            else 'still resting at end of sample' end                               as fate,
       case when deleted_ns is null then null
            when deleted_ns - added_ns <        1000000 then '1 under 1 ms'
            when deleted_ns - added_ns <       10000000 then '2 1-10 ms'
            when deleted_ns - added_ns <      100000000 then '3 10-100 ms'
            when deleted_ns - added_ns <     1000000000 then '4 100 ms-1 s'
            when deleted_ns - added_ns <    60000000000 then '5 1-60 s'
            else '6 over 1 min' end                                                  as lifetime_bucket,
       count(*)                                                                      as orders
from fate
group by 1, 2
order by 1, 2
