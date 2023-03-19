INSERT INTO NUTSLYC_YANDEX_RU__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)
select distinct
        hash(hu.hk_user_id, hg.hk_group_id) as hk_l_user_group_activity,
        hu.hk_user_id as hk_user_id,
        hg.hk_group_id as hk_group_id,
        now() as load_dt,
		's3' as load_src
from NUTSLYC_YANDEX_RU__STAGING.group_log as gl
left join NUTSLYC_YANDEX_RU__DWH.h_users hu ON gl.user_id = hu.user_id 
left join NUTSLYC_YANDEX_RU__DWH.h_groups hg ON gl.group_id = hg.group_id 
; 