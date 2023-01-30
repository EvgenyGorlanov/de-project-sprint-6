with user_group_messages as (
    select 
   		lgd.hk_group_id as hk_group_id,
   		COUNT(DISTINCT um.hk_user_id) as cnt_users_in_group_with_messages
    from NUTSLYC_YANDEX_RU__DWH.l_user_message um
    left join NUTSLYC_YANDEX_RU__DWH.l_groups_dialogs lgd ON lgd.hk_message_id = um.hk_message_id 
    group by lgd.hk_group_id
)
select 	hk_group_id,
        cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10
;