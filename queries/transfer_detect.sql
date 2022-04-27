-- drop table temp_db.temp_dwd_yct_od_202203_07to07

create table temp_db.temp_dwd_yct_od_202203_07to07 as 
with bus_arr_info_hourly as (
    select
        cur_station_id,
        -- cur_station_name,
        route_name_run,
        route_id_run,
        count(*) hourly_cnt,
        direction,
        from_unixtime(unix_timestamp(arrival_time), 'yyyy-MM-dd HH') _hour -- from_unixtime(unix_timestamp(arrival_time), 'HH') _hour
    from
        ods_db.ods_apts_bus_ad
    where
        pdate between '${from_date}'
        and '${to_date}'
        and from_unixtime(unix_timestamp(arrival_time), 'yyyy-MM-dd HH') is not null
    group by
        route_name_run,
        -- from_unixtime(unix_timestamp(arrival_time), 'HH'),
        from_unixtime(unix_timestamp(arrival_time), 'yyyy-MM-dd HH'),
        cur_station_id,
        -- cur_station_name,
        route_name_run,
        route_id_run,
        direction
),
bus_line_station_down_descrnk as (
    select
        t.*,
        row_number() over (
            partition by route_id, route_name
            order by
                cast(order_number as int) desc
        ) descrnk
    from
        temp_db.temp_bus_line_station_down_mod t
),
bus_line_station_down_ascrnk as (
    select
        t.*,
        row_number() over (
            partition by route_id, route_name
            order by
                cast(order_number as int) asc
        ) ascrnk
    from
        temp_db.temp_bus_line_station_down_mod t
),
loop_route_down as (
    select
        a.route_name,
        a.route_id
    from
        (
            select
                *
            from
                bus_line_station_down_ascrnk
            where
                ascrnk = 1
        ) a,
        (
            select
                *
            from
                bus_line_station_down_descrnk
            where
                descrnk = 1
        ) b
    where
        a.route_id = b.route_id
        and a.route_name = b.route_name
        and a.station_id = b.station_id
),
bus_line_station_up_ascrnk as (
    select
        t.*,
        row_number() over (
            partition by route_id, route_name
            order by
                cast(order_number as int) asc
        ) ascrnk
    from
        temp_db.temp_bus_line_station_up_mod t
),
bus_line_station_up_descrnk as (
    select
        t.*,
        row_number() over (
            partition by route_id, route_name
            order by
                cast(order_number as int) desc
        ) descrnk
    from
        temp_db.temp_bus_line_station_up_mod t
),
loop_route_up as (
    select
        a.route_name,
        a.route_id
    from
        (
            select
                *
            from
                bus_line_station_up_ascrnk
            where
                ascrnk = 1
        ) a,
        (
            select
                *
            from
                bus_line_station_up_descrnk
            where
                descrnk = 1
        ) b
    where
        a.route_id = b.route_id
        and a.route_name = b.route_name
        and a.station_id = b.station_id
),
loop_route as (
    (
        select
            route_name,
            route_id
        from
            loop_route_down
    )
    union
    (
        select
            route_name,
            route_id
        from
            loop_route_up
    )
),
loop_route_distinct as (
    select
        distinct route_name,
        route_id
    from
        loop_route
),
loop_bus_line_station_up_mod as (
    select
        distinct 
        -- a.route_name,
        a.route_id,
        a.station_name,
        a.station_id,
        a.ascrnk order_number,
        'loop' dir
    from 
        bus_line_station_up_ascrnk a
    where
        a.route_id in (select route_id from loop_route_distinct)
),
loop_bus_line_station_down_mod as (
    select
        distinct
        -- a.route_name,
        a.route_id,
        a.station_name,
        a.station_id,
        a.ascrnk order_number,
        'loop' dir
    from 
        bus_line_station_down_ascrnk a 
    where 
        a.route_id in (select route_id from loop_route_distinct)
),
loop_bus_line_station_mod as (
    (select * from loop_bus_line_station_up_mod)
        union 
    (select * from loop_bus_line_station_down_mod)
),
bus_line_station as (
    select route_id, 
           from_sn, 
           from_sid, 
           min(from_order) from_order, 
           to_sn, 
           to_sid, 
           min(to_order) to_order, 
           dir 
    from (
        select
            distinct 
            -- a.route_name,
            a.route_id,
            a.station_name from_sn,
            a.station_id from_sid,
            a.ascrnk from_order,
            -- a.order_number from_order,
            b.station_name to_sn,
            b.station_id to_sid,
            -- b.order_number to_order,
            b.ascrnk to_order,
            'up' dir
        from
            bus_line_station_up_ascrnk a,
            bus_line_station_up_ascrnk b
        where
            a.route_id = b.route_id
            and cast(a.ascrnk as int) < cast(b.ascrnk as int)
            and a.route_id not in (select route_id from loop_route_distinct)
        ) t
    group by route_id, from_sn, from_sid, to_sn, to_sid, dir
    union
    select route_id, 
           from_sn, 
           from_sid, 
           min(from_order) from_order, 
           to_sn, 
           to_sid, 
           min(to_order) to_order, 
           dir 
    from (
        select
            distinct 
            -- a.route_name,
            a.route_id,
            a.station_name from_sn,
            a.station_id from_sid,
            -- a.order_number from_order,
            a.ascrnk from_order,
            b.station_name to_sn,
            b.station_id to_sid,
            -- b.order_number to_order,
            b.ascrnk to_order,
            'down' dir
        from
            bus_line_station_down_ascrnk a,
            bus_line_station_down_ascrnk b
        where
            a.route_id = b.route_id
            and cast(a.order_number as int) < cast(b.order_number as int)
            and a.route_id not in (select route_id from loop_route_distinct)
        ) t
    group by route_id, from_sn, from_sid, to_sn, to_sid, dir
    union
    select route_id, 
           from_sn, 
           from_sid, 
           min(from_order) from_order, 
           to_sn, 
           to_sid, 
           min(to_order) to_order, 
           dir 
    from (
        select
            distinct
            -- a.route_name,
            a.route_id,
            a.station_name from_sn,
            a.station_id from_sid,
            a.order_number from_order,
            b.station_name to_sn,
            b.station_id to_sid,
            b.order_number to_order,
            'loop' dir
        from
            loop_bus_line_station_mod a,
            loop_bus_line_station_mod b
        where
            a.route_id = b.route_id
            and cast(a.order_number as int) < cast(b.order_number as int)
        ) t
    group by route_id, from_sn, from_sid, to_sn, to_sid, dir
),
yct_pre as (
    select
        logcardid,
        origin_date,
        transfer_num,
        trip_num,
        origin_time,
        destination_time,
        origin_station_name,
        destination_station_name,
        origin_station_id,
        destination_station_id,
        destination_date,
        origin_route,
        card_type,
        direction,
        transfer_type,
        vehicle_flag,
        pdate,
        trade_money
    from
        dw_db.dwd_yct_od t
    where
        pdate between '${from_date}'
        and '${to_date}'
        and origin_station_name is not null
        and destination_station_name is not null
        and origin_date = destination_date
        and destination_time > origin_time
    group by
        logcardid,
        origin_date,
        transfer_num,
        trip_num,
        origin_time,
        destination_time,
        origin_station_name,
        destination_station_name,
        origin_station_id,
        destination_station_id,
        destination_date,
        origin_route,
        card_type,
        direction,
        transfer_type,
        vehicle_flag,
        pdate,
        trade_money
),
yct_od as (
    select
        t.*,
        -- logcardid,
        -- t.origin_date,
        -- t.origin_time,
        -- t.origin_station_id,
        -- ori
        concat_ws(' ', t.origin_date, strleft(origin_time, 2)) origin_timestamp
    from
        yct_pre t
),
yct_od_join_dir as (
    select
        a.*,
        b.dir,
        b.via_stations
    from
        yct_od a
        left join (
            select
                from_sid,
                to_sid,
                route_id,
                group_concat(dir, '/') dir,
                -- dir --环线！！
                avg(cast(to_order as int)- cast(from_order as int)) via_stations  -- 地铁站没有order_number，只要涉及地铁站均为NULL
            from
                bus_line_station
            group by from_sid, to_sid, route_id
        ) b on a.origin_station_id = b.from_sid -- and origin_station_name = b.from_sn
        and a.destination_station_id = b.to_sid -- and destination_station_name = b.to_sn
        and a.origin_route = b.route_id
),
-- select * from yct_od_join_dir where via_stations is null
-- select * from yct_od_join_dir where origin_station_name = '里仁洞站' and destination_station_name = '天安番禺节能科技园站'
final as (
    select
        a.*,
        b.direction arr_dir,
        b.hourly_cnt -- logcardid, origin_date, origin_time, origin_station_id, origin_station_name, origin_route,
        -- destination_date, destination_time, destination_station_id, destination_station_name, destination_route, vehicle_flag, direction, trip_num, transfer_type, transfer_num, 
    from
        yct_od_join_dir a
        left join bus_arr_info_hourly b on a.origin_station_id = b.cur_station_id
        and a.origin_route = b.route_id_run
        and a.origin_timestamp = b._hour
        and cast(a.direction as string) = b.direction
),
temp as (
    select
        logcardid,
        origin_date,
        hourly_cnt,
        nvl(60 / t.hourly_cnt, 60) avg_arr_minutes,
        -- 如果公交到站表中没有记录，则取最大60分钟间隔阈值(换乘地铁全部落入此情况)
        origin_time,
        origin_station_id,
        origin_station_name,
        destination_time,
        destination_station_id,
        destination_station_name,
        origin_route,
        card_type,
        transfer_num,
        trip_num,
        lag(destination_time, 1) over(
            partition by logcardid,
            origin_date
            order by
                origin_time
        ) prev_destination_time,
        lag(origin_route, 1) over(
            partition by logcardid,
            origin_date
            order by
                origin_time
        ) prev_route,
        direction,
        transfer_type,
        vehicle_flag,
        pdate,
        destination_date,
        via_stations,
        trade_money
    from
        final t
),
temp2 as (
    select
        t.*,
        round(
            (
                (
                    unix_timestamp(concat(t.origin_date, ' ', origin_time)) - unix_timestamp(
                        concat(t.origin_date, ' ', prev_destination_time)
                    )
                ) / 60
            ),
            3
        ) time_gap -- round(((origin_time - prev_destination_time) / 60), 3) time_diff
    from
        temp t
),
temp3 as (
    select
        logcardid,
        origin_date,
        hourly_cnt,
        (
            case
                when time_gap is null then 0 -- 第一条记录，肯定为tour第一个trip
                when time_gap > (avg_arr_minutes + 10) then 2 -- 时间间隔超出阈值
                when time_gap <= (avg_arr_minutes + 10)
                and (origin_route = prev_route) then 3 -- 时间间隔未超出阈值，但与上一次trip同一条线路
                else 1 -- 与上一个trip属于同一个tour
            end
        ) as flag,
        -- 本 trip 是否连接上一 tour
        transfer_num,
        trip_num,
        avg_arr_minutes,
        time_gap,
        origin_time,
        destination_time,
        origin_station_id,
        origin_station_name,
        destination_station_id,
        destination_station_name,
        origin_route,
        card_type,
        direction,
        transfer_type,
        vehicle_flag,
        pdate,
        destination_date,
        prev_route,
        via_stations,
        trade_money
    from
        temp2
),
temp4 as (
    select
        lead(flag, 1) over(
            partition by logcardid,
            origin_date
            order by
                origin_time
        ) ishead,
        t.*
    from
        temp3 t
),
temp5 as (
    select
        (
            case
                when t.flag = 0 then 'H'
                when t.flag = 2 then 'H'
                when t.flag = 3 then 'H'
                when (t.flag = 1 & t.ishead = 1) then 'M'
                else 'T'
            end
        ) as transfer_num2,
        t.*
    from
        temp4 t -- order by logcardid, origin_time
),
trip_num as (
    select
        row_number() over(
            partition by logcardid,
            origin_date
            order by
                origin_time
        ) trip_num,
        origin_time,
        logcardid,
        origin_date,
        'H' transfer_num2
    from
        temp5 t
    where
        transfer_num2 = 'H'
),
trip_num_join as (
    select
        a.*,
        b.trip_num trip_num2
    from
        temp5 a
        left join trip_num b on a.logcardid = b.logcardid
        and a.origin_date = b.origin_date
        and a.transfer_num2 = b.transfer_num2
        and a.origin_time = b.origin_time
),
temp6 as (
    select
        last_value(trip_num2 ignore nulls) over(
            partition by logcardid,
            origin_date
            order by
                origin_time ROWS between UNBOUNDED PRECEDING
                AND current row -- ignore null: 忽略空值  rows between: 限定窗口范围  unbounded preceding: 窗口内无上界  n following: 当前值后移n个值  current row: 当前值
        ) trip_num2_fill,
        t.*
    from
        trip_num_join t --order by logcardid, origin_time
),
temp7 as (
    select
        (
            case
                when trip_num2 is not null then trip_num2
                else trip_num2_fill
            end
        ) trip_num2_full,
        t.*
    from
        temp6 t --order by logcardid, origin_time
) -- 提取可能存在分类差异的出行记录
select
    -- trip_num2_full, trip_num2_fill, trip_num2, logcardid
    trip_num2_full trip_num2,
    transfer_num2,
    logcardid,
    trip_num,
    transfer_num,
    origin_time,
    destination_time,
    origin_station_name,
    origin_station_id,
    destination_station_name,
    destination_station_id,
    origin_route,
    card_type,
    direction,
    transfer_type,
    vehicle_flag,
    pdate,
    origin_date,
    destination_date,
    via_stations,
    trade_money
from
    temp7

/*
where
    logcardid in (
        select
            logcardid
        from
            temp5
        where
            transfer_num = 'M'
    )
order by
    logcardid,
    origin_time 
*/