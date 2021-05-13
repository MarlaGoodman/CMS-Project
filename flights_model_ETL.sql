/*This is written in PostgreSQL*/
select *,
       sum(delay_bool) over (partition by  tailnum order by time_hour asc rows between unbounded preceding and current row) count_delay_per_tail_number
       from (

select *,
--        count(*) over ( partition by case tailnum when null then '123456' else tailnum end order by time_hour ) as count_delay from (
      1 as delay_bool from (
select
       dep_delay,
       month,
--        day,
--        dep_time,
--        sched_dep_time,
--        arr_time,
--        sched_arr_time,
--        arr_delay,
--        a.carrier,
--        flight,
       tailnum,
--        a.origin,
--        dest,
--        air_time,
--        distance,
       hour,
--        minute,
       cast(time_hour as timestamp) time_hour,
       count_of_flight_by_day_carrier_origin,
       new_snow_bool,
       rain_bool
from (
select *,cast(month || '/' || day || '/' || '2017' as date) date_of_flight from katz_2020.flights) a
inner join
(select cast(month || '/' || day || '/' || '2017' as date) date_of_flight, carrier, origin, sum(1) count_of_flight_by_day_carrier_origin from katz_2020.flights group by 1,2,3 ) b
on a.date_of_flight = b.date_of_flight
and a.carrier= b.carrier
and a.origin = b.origin
inner join (select cast("Date" as date) , new_snow_bool, rain_bool
from katz_2020.weather_noaa) c
on a.date_of_flight = c."Date") d
where dep_delay > 0 or dep_delay is null

union all

select *, 0 from (
select
       dep_delay,
       month,
--        day,
--        dep_time,
--        sched_dep_time,
--        arr_time,
--        sched_arr_time,
--        arr_delay,
--        a.carrier,
--        flight,
       tailnum,
--        a.origin,
--        dest,
--        air_time,
--        distance,
       hour,
--        minute,
       cast(time_hour as timestamp) time_hour,
       count_of_flight_by_day_carrier_origin,
       new_snow_bool,
       rain_bool
from (
select *,cast(month || '/' || day || '/' || '2017' as date) date_of_flight from katz_2020.flights) a
inner join
(select cast(month || '/' || day || '/' || '2017' as date) date_of_flight, carrier, origin, sum(1) count_of_flight_by_day_carrier_origin from katz_2020.flights group by 1,2,3 ) b
on a.date_of_flight = b.date_of_flight
and a.carrier= b.carrier
and a.origin = b.origin
inner join (select cast("Date" as date) , new_snow_bool, rain_bool
from katz_2020.weather_noaa) c
on a.date_of_flight = c."Date") d
where dep_delay <=0)e
-- where tailnum = 'N102AA'

;