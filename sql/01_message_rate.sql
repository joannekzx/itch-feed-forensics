-- Messages per wall-clock second, with the session phase each second belongs to.
-- ts_ns is nanoseconds since midnight; 34200 s = 09:30:00, the regular-hours open.
select cast(ts_ns / 1000000000 as bigint)                         as second_of_day,
       case when ts_ns < 14400000000000 then 'before 04:00'
            when ts_ns < 34200000000000 then 'pre-market 04:00-09:30'
            else 'regular hours' end                               as phase,
       count(*)                                                    as messages
from messages
group by 1, 2
order by 1
