select
 t1.user_id,
  t1.creativity_id,
  unit_id,
  campaign_id,
  advertiser_id,
  brand_account_id,
  carrier_id,
  carrier_type,
  module,
  product,
  report_id,
  report_anchor_id,
  report_anchor_name,
  report_create_time,
  report_status,
  report_source,
  report_live_id,
  marketing_target,
  optimize_target,
  like_cnt,
  fav_cnt,
  cmt_cnt,
  follow_cnt,
  share_cnt,
  imp_cnt,
  click_cnt,
  income_amt,
  cash_income_amt,
  report_imp_cnt,
  report_click_cnt,
  report_subscribe_cnt,
  case when substring(t4.create_time,1,10)<substring(report_create_time,1,10) then 1 else 0 end as is_fans
from
  (
    select
      user_id,
      ads_uuid,
      creativity_id,
      unit_id,
      campaign_id,
      advertiser_id,
      brand_account_id,
      carrier_id,
      module,
      product,
      report_id,
      report_anchor_id,
      report_create_time,
      report_live_start_time,
      report_live_end_time,
      report_live_id,
      live_anchor_id,
      total_amount as income_amt,
      cash_amount as cash_income_amt,
      report_imp_cnt,
      report_click_cnt,
      report_subscribe_cnt,
      carrier_type,
      report_anchor_name,
      marketing_target,
      optimize_target
    from
      redcdm.dws_ads_rtb_live_report_creativity_user_1d_di
    where
      dtm = '{{ds_nodash}}'
      and report_id <> ''
      and report_status=1
  ) t1 --left join 
  --is_fans 
  left join (
    select
      creativity_id,
      user_id,
      like_cnt,
      fav_cnt,
      cmt_cnt,
      follow_cnt,
      share_cnt,
      imp_cnt,
      click_cnt
    from
      redcdm.dm_ads_creativity_user_1d_di
    where
      dtm = '{{ds_nodash}}'
  ) t2 on t1.creativity_id = t2.creativity_id
  and t1.user_id= t2.user_id

  --是否粉丝 
  left join (
    select
      user_id,
      target_user_id,
      1 as is_fans,
      create_time
    from
      reddw.dw_soc_follow_record_day
    where
      dtm = greatest('{{ds_nodash}}', '20230901')
      and coalesce(follow_type, '') != 'fake'
      and coalesce(follow_source, '') != 'fake_user_migrate'
      and spam_disabled = false --去除spam行为
      and coalesce(enabled, true) --未取消
  ) t4 on t4.user_id = t1.user_id
  and t4.target_user_id = t1.report_anchor_id
----------------








--用户去重
select
  creativity_id,
  t1.report_id,
  unit_id,
  campaign_id,
  advertiser_id,
  brand_account_id,
  carrier_id,
  carrier_type,
  module,
  product,
  report_anchor_id,
  report_anchor_name,
  report_create_time,
  report_live_id,
  marketing_target,
  optimize_target,
  related_live_id,
  report_live_start_time,
  report_live_end_time,
  like_cnt,
  fav_cnt,
  cmt_cnt,
  follow_cnt,
  share_cnt,
  imp_cnt,
  click_cnt,
  income_amt,
  cash_income_amt,
  report_imp_cnt,
  report_click_cnt,
  report_subscribe_cnt,
  report_imp_uv,
  report_click_uv,
  report_subscribe_uv,
  dtm
from
  (
    select
      creativity_id,
      unit_id,
      campaign_id,
      advertiser_id,
      brand_account_id,
      carrier_id,
      carrier_type,
      module,
      product,
      report_id,
      report_anchor_id,
      report_anchor_name,
      report_create_time,
      report_live_id,
      marketing_target,
      optimize_target,
      dtm,
      sum(like_cnt) as like_cnt,
      sum(fav_cnt) as fav_cnt,
      sum(cmt_cnt) as cmt_cnt,
      sum(follow_cnt) as follow_cnt,
      sum(share_cnt) as share_cnt,
      sum(imp_cnt) as imp_cnt,
      sum(click_cnt) as click_cnt,
      sum(income_amt) as income_amt,
      sum(cash_income_amt) as cash_income_amt,
      sum(report_imp_cnt) as report_imp_cnt,
      sum(report_click_cnt) as report_click_cnt,
      sum(report_subscribe_cnt) as report_subscribe_cnt,
      count(case when report_imp_cnt>0 then 1 else null end) as report_imp_uv,
      count(case when report_click_cnt>0 then 1 else null end) as report_click_uv,
      count(case when report_subscribe_cnt>0 then 1 else null end) as report_subscribe_uv
    from
      redcdm.dm_ads_industry_live_report_creativity_user_1d_di
    where
      dtm >= f_getdate('{{ ds_nodash }}', -31) --为用户预约时间
      AND dtm <= '{{ ds_nodash }}'
    group by creativity_id,
      unit_id,
      campaign_id,
      advertiser_id,
      brand_account_id,
      carrier_id,
      carrier_type,
      module,
      product,
      report_id,
      report_anchor_id,
      report_anchor_name,
      report_create_time,
      report_live_id,
      marketing_target,
      optimize_target,
      dtm
  ) t1
  left join (
    SELECT
      report_id,
      related_live_id,
      report_live_start_time,
      report_live_end_time,
      regexp_replace(to_date(create_time), '-', '') AS create_dtm
    FROM
      redcdm.dwd_liv_live_report_base_df
    WHERE
      dtm = '{{ ds_nodash }}'
  ) t2 on t1.report_id = t2.report_id