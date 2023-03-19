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
),
user_group_messages as (
    select 
   		lgd.hk_group_id as hk_group_id,
   		COUNT(DISTINCT um.hk_user_id) as cnt_users_in_group_with_messages
    from NUTSLYC_YANDEX_RU__DWH.l_user_message um
    left join NUTSLYC_YANDEX_RU__DWH.l_groups_dialogs lgd ON lgd.hk_message_id = um.hk_message_id 
    group by lgd.hk_group_id
)
select 
	ugl.hk_group_id as hk_group_id,
	ugl.cnt_added_users as cnt_added_users,
	ugm.cnt_users_in_group_with_messages as cnt_users_in_group_with_messages,
	(ugm.cnt_users_in_group_with_messages/ugl.cnt_added_users) as group_conversion
from user_group_log ugl
inner join user_group_messages ugm ON ugl.hk_group_id = ugm.hk_group_id
order by ugm.cnt_users_in_group_with_messages/ugl.cnt_added_users desc