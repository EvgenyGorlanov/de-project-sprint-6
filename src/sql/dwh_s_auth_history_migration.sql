INSERT INTO NUTSLYC_YANDEX_RU__DWH.s_auth_history(hk_l_user_group_activity, user_id_from,event,event_dt,load_dt,load_src)
select 
	luga.hk_l_user_group_activity as hk_l_user_group_activity,
	gl.user_id_from  as user_id_from,
	gl.event as event,
	gl.event_dt as event_dt,
	now() as load_dt,
	's3' as load_src
from NUTSLYC_YANDEX_RU__STAGING.group_log as gl
left join NUTSLYC_YANDEX_RU__DWH.h_groups as hg on gl.group_id = hg.group_id
left join NUTSLYC_YANDEX_RU__DWH.h_users as hu on gl.user_id = hu.user_id
left join NUTSLYC_YANDEX_RU__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id;