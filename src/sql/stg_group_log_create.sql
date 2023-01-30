drop table if exists NUTSLYC_YANDEX_RU__STAGING.group_log;

create table NUTSLYC_YANDEX_RU__STAGING.group_log
(
    group_id int,
    user_id int,
    user_id_from int NULL,
    event varchar(6),
    event_dt datetime
)
ORDER BY group_id
SEGMENTED BY HASH(group_id) ALL NODES
PARTITION BY event_dt::date
GROUP BY calendar_hierarchy_day(event_dt::date, 3, 2);
;