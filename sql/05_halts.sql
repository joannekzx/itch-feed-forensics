-- Trading halts versus executions. A stock should not execute while it is halted (state H) or paused (P).
-- Each trading-action (H) message sets a stock's state until its next one; the state before the first
-- message is unknown and left out. Counts executions and trades by the state they happened in.
with actions as (
    select locate, ts_ns as from_ns, trading_state as state,
           lead(ts_ns) over (partition by locate order by seq) as to_ns
    from (select * from trading_action) a
),
fills as (
    select locate, ts_ns, 'order executed (E/C)' as what from execution
    union all select locate, ts_ns, 'non-displayed trade (P)' from trade
    union all select locate, ts_ns, 'cross trade (Q)' from `cross` where shares > 0
)
select a.state, f.what, count(*) as events, count(distinct f.locate) as stocks
from fills f
join actions a on a.locate = f.locate and f.ts_ns >= a.from_ns and (a.to_ns is null or f.ts_ns < a.to_ns)
group by a.state, f.what
order by a.state, f.what
