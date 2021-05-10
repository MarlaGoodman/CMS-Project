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
--        tailnum,
--        a.origin,
--        dest,
--        air_time,
--        distance,
       hour,
--        minute,
--        time_hour,
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
on a.date_of_flight = c."Date"
;


