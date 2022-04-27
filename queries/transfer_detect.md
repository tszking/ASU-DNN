

1. 出行识别（新公交换乘判断逻辑） （impala） 
脚本：[transfer_detect.sql](./transfer_detect.sql)
生成表：temp_db.temp_dwd_yct_od_202201_22to25

2. 统计公交站点OD的出行方案选择分布 （Hive）
脚本：[get_BUS_OD_TRIP_STATS_HOURLY.sql](./get_BUS_OD_TRIP_STATS_HOURLY.sql)

依赖表

        origin_station_name,        -- 起点公交站名
        origin_station_id,          -- 起点公交站ID      
        destination_station_name,   -- 终点公交站名
        destination_station_id,     -- 终点公交站ID
        route_list,                 -- 线路组合（sort by name）
        transfer_time,              -- 换乘次数
        vehicle_flag_list,          -- 交通方式组合（sort by name）  0-公交  1-地铁
        cc,                         -- 日总客流
        avg(time_diff) avg_time,    -- 平均车内行程时间（分钟）
        origin_date                 -- 起点日期

3. 统计空间OD、小时粒度下的老人羊城通刷卡占比、普通刷卡占比（HIVE）

依赖表


脚本：[yct_card_user_type.sql](./yct_card_user_type.sql)

4. 将步骤2所得表与步骤3所得表进行关联 （impala）

脚本：[指标关联.sql](./指标关联.sql)
