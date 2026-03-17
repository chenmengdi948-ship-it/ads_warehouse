select
  module,
  product,
  creativity_id,
  t1.user_id,
  t1.report_id,
  t1.anchor_id,
  unit_id,
  launch_type,
  
  page_instance_name,
  note_id,
  huati_id,
  report_impression_cnt,
  report_click_cnt,
  report_success_subscribe_cnt,
  related_live_impression_cnt,
  related_live_click_cnt,
  related_live_view_cnt,
  related_live_view_duration,
  related_live_engage_cnt,
  related_live_like_cnt,
  related_live_comment_cnt,
  related_live_follow_cnt,
  related_live_share_cnt,
  is_fans,
  nickname as anchor_name,
  create_dtm
from
  (
    select
      '品牌' as module,
      dtm,
      report_id,
      anchor_id,
      page_instance_name,
      note_id,
      huati_id,
      brand_subscribe_channel as product,
      creativity_id,
      unit_id,
      launch_type,
      user_id
    from
      redcdm.dwd_ads_brand_cvr_live_report_di --主键预告id和user_id
    where
      dtm >= f_getdate('{{ ds_nodash }}', -31) --为用户预约时间
      AND dtm <= '{{ ds_nodash }}'
      and subscribe_enabled = 1
  ) t1
  join (
    SELECT
      report_id,
      user_id,
      sum(report_impression_cnt) as report_impression_cnt,
      sum(report_click_cnt) as report_click_cnt,
      sum(report_success_subscribe_cnt) as report_success_subscribe_cnt,
      sum(related_live_impression_cnt) as related_live_impression_cnt,
      sum(related_live_click_cnt) as related_live_click_cnt,
      sum(related_live_view_cnt) as related_live_view_cnt,
      sum(related_live_view_duration) as related_live_view_duration,
      sum(related_live_engage_cnt) as related_live_engage_cnt,
      sum(related_live_like_cnt) as related_live_like_cnt,
      sum(related_live_comment_cnt) as related_live_comment_cnt,
      sum(related_live_follow_cnt) as related_live_follow_cnt,
      sum(related_live_share_cnt) as related_live_share_cnt,
      is_fans
    FROM
      redcdm.dm_live_report_source_subscribe_enagge_deal_1d_di
    WHERE
      dtm >= f_getdate('{{ ds_nodash }}', -31)
      AND dtm <= '{{ ds_nodash }}'
      AND spam_level = 1
      and report_enabled = 1
    group by
      report_id,
      user_id,
      is_fans
  ) trd on t1.report_id = trd.report_id
  and t1.user_id = trd.user_id
  join (
    SELECT
      *,
      regexp_replace(to_date(create_time), '-', '') AS create_dtm
    FROM
      redcdm.dwd_liv_live_report_base_df
    WHERE
      dtm = '{{ ds_nodash }}'
      AND to_date(create_time) >= f_getdate('{{ ds }}', -31)
      AND to_date(create_time) <= '{{ ds }}' --AND enabled = 1
  ) t2 on t1.report_id = t2.report_id
  LEFT JOIN --关联主播的维度信息
  (
    SELECT
      anchor_id,
      nickname
    FROM
      redcdm.dim_live_anchor_df
    WHERE
      dtm = '{{ ds_nodash }}'
  ) t3 ON t1.anchor_id = t3.anchor_id