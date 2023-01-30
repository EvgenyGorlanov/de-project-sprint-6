drop table if exists NUTSLYC_YANDEX_RU__DWH.s_auth_history;

create table NUTSLYC_YANDEX_RU__DWH.s_auth_history
(
	hk_l_user_group_activity int REFERENCES NUTSLYC_YANDEX_RU__DWH.l_user_group_activity(hk_l_user_group_activity),
	user_id_from int NULL,
	event VARCHAR(6),
	event_dt datetime,
    load_dt datetime,
    load_src VARCHAR(20)
)
ORDER BY event_dt
SEGMENTED BY HASH(hk_l_user_group_activity) ALL NODES
PARTITION BY event_dt::date
GROUP BY calendar_hierarchy_day(event_dt::date, 3, 2);