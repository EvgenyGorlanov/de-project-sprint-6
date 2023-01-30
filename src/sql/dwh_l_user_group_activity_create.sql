drop table if exists NUTSLYC_YANDEX_RU__DWH.l_user_group_activity;

create table NUTSLYC_YANDEX_RU__DWH.l_user_group_activity
(
    hk_l_user_group_activity int PRIMARY KEY,
    hk_user_id int REFERENCES NUTSLYC_YANDEX_RU__DWH.h_users(hk_user_id),
    hk_group_id int REFERENCES NUTSLYC_YANDEX_RU__DWH.h_groups(hk_group_id),
    load_dt datetime,
    load_src VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY HASH(hk_l_user_group_activity) ALL NODES
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


