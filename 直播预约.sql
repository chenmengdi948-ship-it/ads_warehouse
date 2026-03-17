select
  report_id,
  anchor_id,
  report_title,
  report_live_start_time,
  report_live_end_time,
  report_create_time,
  related_live_id,
  nickname,
  sum(report_impression_cnt) as report_impression_cnt,
  sum(report_impression_user_num) as report_impression_user_num,
  sum(report_click_cnt) as report_click_cnt,
  sum(report_click_user_num) as report_click_user_num,
  sum(last_subscirbe_user_num) as last_subscirbe_user_num,
  sum(related_live_view_cnt) as related_live_view_cnt,
  sum(related_live_view_user_num) as related_live_view_user_num,
  sum(related_live_view_duration) as related_live_view_duration,
  sum(related_live_view_5s_more_user_num) as related_live_view_5s_more_user_num,
  sum(related_live_engage_cnt) as related_live_engage_cnt,
  sum(related_live_engage_user_num) as related_live_engage_user_num,
  sum(related_live_like_cnt) as related_live_like_cnt,
  sum(related_live_comment_cnt) as related_live_comment_cnt,
  sum(related_live_follow_cnt) as related_live_follow_cnt,
  sum(related_live_share_cnt) as related_live_share_cnt
from
  redcdm.dm_live_report_source_subscribe_behav_backfill_nd_di
where
  dtm = '{{ds_nodash}}'
  group by report_id,
  anchor_id,
  report_title,
  report_live_start_time,
  report_live_end_time,
  report_create_time,
  related_live_id,
  nickname



  -----user_id
  select
  report_id,
  anchor_id,
  user_id,
  carrier_type,
  related_live_id,
  anchor_nickname,
  is_success_subscribe,
  is_fans,
  sum(report_impression_cnt) as report_impression_cnt,
  sum(report_click_cnt) as report_click_cnt,
  sum(related_live_view_cnt) as related_live_view_cnt,
  sum(related_live_view_duration) as related_live_view_duration,
  sum(related_live_like_cnt) as related_live_like_cnt,
  sum(related_live_comment_cnt) as related_live_comment_cnt,
  sum(related_live_follow_cnt) as related_live_follow_cnt,
  sum(related_live_share_cnt) as related_live_share_cnt
from
  redcdm.dm_live_report_source_subscribe_enagge_deal_1d_di
WHERE
  dtm = '{{ ds_nodash }}'
  AND spam_level = 1
  and report_enabled = 1
group by
  report_id,
  carrier_type,
  anchor_id,
  user_id,
  related_live_id,
  anchor_nickname,
  is_success_subscribe,
  is_fans