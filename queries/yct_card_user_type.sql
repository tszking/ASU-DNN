create table bus_user_portion_202203_07to07 as 
with dwd_yct_od_new as (
    select
        case
            when card_type in ('43', '44', '72', '73') then 'old_disable'
            when card_type in ('03', 'qrcode', '01', '06', '63', '56') then 'normal'
            else card_type
        end as user_type,
        hour(t.origin_time) origin_hour,
        t.*
    from
        dw_db.dwd_yct_od t
),
yct_od_od_usertype_portion as (
    select
        user_type,
        origin_date,
        origin_hour,
        origin_station_name,
        destination_station_name,
        count(*) cnt,
        count(*) / (
            sum(count(*)) over(
                partition by origin_date,
                origin_hour,
                origin_station_name,
                destination_station_name
            )
        ) ratio
    from
        dwd_yct_od_new t
    where
        pdate between '${from_date}'
        and '${to_date}'
        and origin_station_name is not null
        and destination_station_name is not null
        and origin_date = destination_date
        and destination_time > origin_time
        and origin_station_id is not null
        and destination_station_id is not null -- and user_type in ('old_disable', 'normal')
    group by
        origin_date,
        origin_hour,
        user_type,
        origin_station_name,
        destination_station_name
)
select
    a.origin_date,
    a.origin_hour,
    a.origin_station_name,
    a.destination_station_name,
    a.cnt as normal_cnt,
    a.ratio as normal_ratio,
    b.cnt as eld_cnt,
    b.ratio as eld_ratio,
    a.cnt + b.cnt as total_cnt
from
    yct_od_od_usertype_portion a,
    yct_od_od_usertype_portion b
where
    a.user_type = 'normal'
    and b.user_type = 'old_disable'
    and a.origin_date = b.origin_date
    and a.origin_hour = b.origin_hour
    and a.origin_station_name = b.origin_station_name
    and a.destination_station_name = b.destination_station_name
    -- and a.origin_station_name = '万胜围站'
    -- and a.destination_station_name = '琶洲石基村（黄埔古港）总站'