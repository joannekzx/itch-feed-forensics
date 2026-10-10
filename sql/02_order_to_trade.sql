-- Order-to-trade ratio per symbol: how many order-entry messages per share-executing message.
-- Only symbols with at least :min_adds added orders and at least one execution.
with adds as (
    select locate, count(*) as adds from add_order group by locate
),
dels as (select locate, count(*) as deletes from `delete` group by locate),
reps as (select locate, count(*) as replaces from replace group by locate),
cans as (select locate, count(*) as cancels from cancel group by locate),
exs  as (select locate, count(*) as executions, sum(shares) as shares_executed from execution group by locate)
select d.stock, a.adds,
       coalesce(x.deletes, 0)  as deletes,
       coalesce(r.replaces, 0) as replaces,
       coalesce(c.cancels, 0)  as cancels,
       e.executions, e.shares_executed,
       round((a.adds + coalesce(x.deletes, 0) + coalesce(r.replaces, 0) + coalesce(c.cancels, 0)) / e.executions, 1)
           as messages_per_execution
from adds a
join directory d on d.locate = a.locate
join exs e       on e.locate = a.locate
left join dels x on x.locate = a.locate
left join reps r on r.locate = a.locate
left join cans c on c.locate = a.locate
where a.adds >= :min_adds
order by messages_per_execution desc
