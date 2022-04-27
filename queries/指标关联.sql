drop table temp_db.temp_bus_od_trip_all_

-- drop table temp_db.temp_bus_od_trip_all_info

-- select * from temp_db.temp_bus_od_trip_all_info


-- with temp as (
-- select origin_station_name, destination_station_name, origin_hour, t.route_list, cc,
-- cc/sum(cc) over(partition by origin_station_name, destination_station_name, origin_hour) ppp from 
-- temp_db.temp_bus_od_trip_all_info t
-- )
-- select * from temp --order by cnt desc
-- where 
-- origin_station_name = '黄庄站' and destination_station_name = '同和站'  	
-- and 
-- origin_hour = '2022-03-07#17'
-- order by cc desc


-- select * from temp_db.temp_bus_od_trip_stats_hourly


create table temp_db.temp_bus_od_trip_all_info as 
with bus_arr_info_hourly as (
    select
        cur_station_id,
        -- cur_station_name,
        -- route_name_run,
        route_id_run,
        count(*) hourly_cnt,
        direction,
        hour(arrival_time) hour_,
        pdate -- from_unixtime(unix_timestamp(arrival_time), 'yyyy-MM-dd HH') _hour -- from_unixtime(unix_timestamp(arrival_time), 'HH') _hour
    from
        ods_db.ods_apts_bus_ad
    where
        pdate between '${from_date}'
        and '${to_date}'
        and from_unixtime(unix_timestamp(arrival_time), 'yyyy-MM-dd HH') is not null
    group by
        -- route_name_run,
        route_id_run,
        cur_station_id,
        -- cur_station_name,
        direction,
        hour(arrival_time),
        pdate
),
bus_od_trip_stats_hourly as (
    select
        route_list,
        split_part(origin_hour, '#', 2) hour_,
        split_part(split_part(route_list, '\\|', 1), '#', 1) route_id,
        split_part(split_part(route_list, '\\|', 1), '#', 2) route_dir,
        -- split(origin_hour,'#')[1] hour_,
        -- split(split(route_list, '\\|')[0], '#')[0] route_id, 
        -- split(split(route_list, '\\|')[0], '#')[1] route_dir,
        origin_station_name,
        origin_station_id,
        destination_station_name,
        destination_station_id,
        transfer_time,
        vehicle_flag_list,
        cc,
        avg_time,
        origin_date,
        full_price,
        origin_hour,
        via_stations
    from
        (
            select
                *
            from
                temp_db.temp_bus_od_trip_stats_hourly
        ) t
    where
        transfer_time = 0
),
stats_join_busarr as (
    select
        a.*,
        b.hourly_cnt
    from
        bus_od_trip_stats_hourly a
        join bus_arr_info_hourly b on a.route_id = b.route_id_run
        and a.route_dir = b.direction
        and a.origin_station_id = b.cur_station_id
        and a.origin_date = b.pdate
        and cast(a.hour_ as int) = b.hour_
), -- select * from stats_join_busarr a 
final as (
select
    a.*,
    b.normal_cnt,
    b.normal_ratio,
    b.eld_cnt,
    b.eld_ratio,
    b.total_cnt
from
    stats_join_busarr a -- where origin_station_name=  '龙津中路站' and destination_station_name = '龙津东路站'
    join temp_db.temp_bus_user_portion_202203 b on a.origin_station_name = b.origin_station_name
    and a.destination_station_name = b.destination_station_name
    and a.origin_date = b.origin_date
    and cast(split_part(a.origin_hour, '#', 2) as int) = b.origin_hour
    -- and a.cc > 10
order by
    b.eld_ratio desc
),
final2 as (
select
    t.*,
    row_number() over(
        partition by origin_station_name,
        destination_station_name,
        origin_date,
        hour_
        order by
            cc desc
    ) cnt_rnk
from
    final t
),
-- with 
final3 as (
select * from
final2
where cnt_rnk <= 5
)
-- select * from final3
select 
t.*,
cc/sum(cc) over(partition by origin_station_name, destination_station_name, origin_hour) od_ratio,
cc cnt
from 
final3 t

-- select * from temp_db.temp_bus_od_trip_all_info
-- where
-- origin_station_name = '黄庄站' and destination_station_name = '同和站'  	
-- and 
-- origin_hour = '2022-03-07#17'
-- order by cc desc

-------------------------------------------------------------------------------------------------------------------------------------------------------

-- 导出小时客流大于10的OD之公交特征
with temp as (
select sum(cc) total_cnt,  origin_station_name, destination_station_name, origin_hour, concat(origin_station_name, destination_station_name, origin_hour) odt
from temp_db.temp_bus_od_trip_all_info
group by  origin_station_name, destination_station_name, origin_hour
having sum(cc) > 10
),
temp2 as (
select * from temp_db.temp_bus_od_trip_all_info
where concat(origin_station_name, destination_station_name, origin_hour) in 
(select odt from temp)
)
select count(*) from temp2


select count(*), origin_date from temp_db.temp_bus_od_trip_stats_hourly
group by origin_date

