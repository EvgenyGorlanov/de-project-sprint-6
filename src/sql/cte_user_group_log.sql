with user_group_log as (
    select 
   		luga.hk_group_id as hk_group_id,
   		hg.registration_dt, 
   		COUNT(DISTINCT luga.hk_user_id) as cnt_added_users
    from NUTSLYC_YANDEX_RU__DWH.s_auth_history sah
    left join NUTSLYC_YANDEX_RU__DWH.l_user_group_activity luga ON luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
    left join NUTSLYC_YANDEX_RU__DWH.h_groups hg ON luga.hk_group_id = hg.hk_group_id 
    WHERE
    	sah.event = 'add'
    GROUP BY luga.hk_group_id, hg.registration_dt
    order by hg.registration_dt ASC 
    limit 10
)
select hk_group_id
            ,cnt_added_users
from user_group_log
order by cnt_added_users
limit 10
; 