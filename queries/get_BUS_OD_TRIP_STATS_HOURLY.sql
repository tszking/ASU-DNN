-- drop table temp_db.temp_bus_od_trip_stats_daily3

-- select * from temp_db.temp_bus_line_station_up_mod where route_id = '88' order by cast(order_number as int) 

-- select * from temp_db.temp_bus_od_trip_stats_daily3 where origin_station_id is not null and destination_station_id is not null and transfer_time = 1 order by cc desc

-- drop table temp_db.temp_bus_od_trip_stats_hourly

-- select * from temp_db.temp_bus_od_trip_stats_hourly where origin_station_id is not null and destination_station_id is not null order by cc desc


-- select * from temp_db.temp_bus_od_trip_stats_hourly order by cc desc

create table temp_db.temp_bus_od_trip_stats_hourly as
with dwd_yct_od_filtered as (
    select
        logcardid,
        trip_num2,
        origin_route,
        origin_time,
        origin_station_name,
        origin_station_id,
        destination_station_name,
        destination_station_id,
        cast(transfer_type as string) transfer_type,
        cast(vehicle_flag as string) vehicle_flag,
        -- trade_money,
        direction,
        pdate,
        trade_money, 
        via_stations,
        destination_time
    from
        temp_db.temp_dwd_yct_od_202203_07to07
    where
        pdate between '${from_date}'
        and '${to_date}'
        -- and origin_station_name is not null
        -- and origin_station_id is not null
        -- and destination_station_name is not null
        -- and destination_station_id is not null
        and origin_date = destination_date
        and destination_time > origin_time
),
transfer_info as (
    select
        logcardid,
        trip_num2,
        pdate,
        concat_ws('|', sort_array(collect_list(concat(origin_route, '#', direction)))) as route_list,
        concat_ws(
            '|',
            sort_array(collect_list(origin_station_name))
        ) as ori_station_list,
        concat_ws('|', sort_array(collect_list(origin_station_id))) as ori_stationid_list,
        concat_ws(
            '|',
            sort_array(collect_list(destination_station_name))
        ) as dest_station_list,
        concat_ws(
            '|',
            sort_array(collect_list(destination_station_id))
        ) as dest_stationid_list,
        concat_ws('|', sort_array(collect_set(vehicle_flag))) as vehicle_flag_list,
        concat_ws('|', sort_array(collect_set(transfer_type))) as transfer_type_list,
        (count(*) -1) transfer_time,
        sum(trade_money) total_fee,
        sum(via_stations) via_stations
    from
        dwd_yct_od_filtered
    group by
        logcardid,
        trip_num2,
        pdate
),
trip_order_asc as (
    select
        logcardid,
        trip_num2,
        -- origin_time,
        -- from_unixtime(unix_timestamp(concat(pdate, ' ', origin_time)), 'yyyy-MM-dd HH:mm:ss') origin_time,
        unix_timestamp(concat(pdate, ' ', origin_time)) origin_time,
        from_unixtime(
            unix_timestamp(concat(pdate, ' ', origin_time)),
            'yyyy-MM-dd#HH'
        ) origin_hour,
        pdate origin_date,
        origin_station_name,
        origin_station_id,
        row_number() over (
            partition by logcardid,
            trip_num2,
            pdate
            order by
                origin_time asc
        ) ascrnk
    from
        dwd_yct_od_filtered
),
trip_order_desc as (
    select
        logcardid,
        trip_num2,
        -- destination_time,
        -- from_unixtime(unix_timestamp(concat(pdate, ' ', destination_time)), 'yyyy-MM-dd HH:mm:ss') destination_time,
        unix_timestamp(concat(pdate, ' ', destination_time)) destination_time,
        from_unixtime(
            unix_timestamp(concat(pdate, ' ', destination_time)),
            'yyyy-MM-dd#HH'
        ) destination_hour,
        pdate destination_date,
        destination_station_name,
        destination_station_id,
        row_number() over (
            partition by logcardid,
            trip_num2,
            pdate
            order by
                origin_time desc
        ) descrnk
    from
        dwd_yct_od_filtered
),
od_info as (
    select
        a.logcardid,
        a.trip_num2,
        a.origin_station_name,
        a.origin_station_id,
        b.destination_station_name,
        b.destination_station_id,
        a.origin_hour,
        b.destination_hour,
        a.origin_date,
        b.destination_date,
        round(((b.destination_time - a.origin_time) / 60), 3) time_diff,
        a.origin_time,
        b.destination_time
    from
        trip_order_asc a
        join trip_order_desc b on a.logcardid = b.logcardid
        and a.trip_num2 = b.trip_num2
        and a.origin_date = b.destination_date
    where
        ascrnk = 1
        and descrnk = 1
        and b.destination_time > a.origin_time
),
trip_info as (
    select
        a.logcardid,
        a.trip_num2,
        a.origin_station_name,
        a.origin_station_id,
        a.destination_station_name,
        a.destination_station_id,
        a.origin_hour,
        a.destination_hour,
        a.origin_date,
        a.destination_date,
        a.time_diff,
        b.ori_station_list,
        b.ori_stationid_list,
        b.dest_station_list,
        b.dest_stationid_list,
        b.vehicle_flag_list,
        b.transfer_time,
        b.route_list,
        b.total_fee,
        b.via_stations,
        a.origin_time,
        a.destination_time
    from
        od_info a
        left join transfer_info b on a.logcardid = b.logcardid
        and a.trip_num2 = b.trip_num2
        and a.origin_date = b.pdate
),
result as (
    select
        origin_station_name,       -- 起点公交站名
        origin_station_id,         -- 起点公交站ID      
        destination_station_name,  -- 终点公交站名
        destination_station_id,    -- 终点公交站ID
        route_list,                -- 线路组合（sort by name）
        transfer_time,             -- 换乘次数
        vehicle_flag_list,         -- 交通方式组合（sort by name）  0-公交  1-地铁
        count(*) cc,            -- 日总客流
        max(total_fee) full_price,    -- 日平均费用（不准，部分记录费用为空）
        avg(time_diff) avg_time,   -- 平均车内行程时间（分钟）
        origin_date,              -- 起点日期
        origin_hour,
        avg(via_stations) via_stations
    from
        trip_info
    group by
        origin_station_name,
        origin_station_id,
        destination_station_name,
        destination_station_id,
        route_list,
        transfer_time,
        vehicle_flag_list,
        origin_date,
        origin_hour
)
select
    *
from
    result --where origin_station_id = '10007907' and destination_station_id = '10004186'
