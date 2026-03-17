
insert overwrite table redapp_dev.app_ads_rtb_operation_metric_di  partition(dtm)
select t1.campaign_id,
  brand_account_id,
  marketing_target,
  market_target,
  biz_product_type,
  target_type_list,
  target_type_msg_list,
  optimize_target,
  campaign_placement,
  is_rtb,
  is_today_create,
  income_amt,
  cash_income_amt,
  like_cnt,
  fav_cnt,
  cmt_cnt,
  follow_cnt,
  share_cnt,
  screenshot_cnt,
  imp_cnt,
  click_cnt,
  account_cash_income_amt,
  group_ids,
  valid_keywords,
  cost_keywords,
  is_today_start,
  t1.conversion_cnt,
  case when t2.is_config_finish is not null then t2.is_config_finish else 0 end  as is_config_finish,
  is_effective,
  build_type,
  product,
  t1.dtm
from 
(
SELECT
  campaign_id,
  brand_account_id,
  marketing_target,
  market_target,
  biz_product_type,
  target_type_list,
  target_type_msg_list,
  optimize_target,
  campaign_placement,
  is_rtb,
  is_today_create,
  income_amt,
  cash_income_amt,
  like_cnt,
  fav_cnt,
  cmt_cnt,
  follow_cnt,
  share_cnt,
  screenshot_cnt,
  imp_cnt,
  click_cnt,
  account_cash_income_amt,
  group_ids,
  valid_keywords,
  cost_keywords,
  is_today_start,
  conversion_cnt,
  is_config_finish,
  is_effective,
  build_type,
  product,
  dtm
FROM
  redapp.app_ads_rtb_operation_metric_di
WHERE
  dtm >='20230101' and dtm<='20240626'
  )t1 
  left join 
  (select campaign_id, 
  conversion_cnt,
  rtb_dtm,
  is_config_finish
from temp.temp_app_ads_rtb_operation_metric_di_20240626_online 
)t2 
on t1.campaign_id = t2.campaign_id and t2.rtb_dtm=t1.dtm


drop table if exists temp.temp_app_ads_rtb_operation_metric_di_{{ds_nodash}}_online;

create table
  temp.temp_app_ads_rtb_operation_metric_di_{{ds_nodash}}_online as
select campaign_id, 
  conversion_cnt,
  rtb_dtm,
  1 as is_config_finish
from 
(select t1.campaign_id, 
  
  rtb_dtm,
  sum(conversion_cnt) as conversion_cnt
  from 
  (select f_getdate(dtm) as date_key,campaign_id,sum(conversion_cnt) as conversion_cnt
  from redcdm.dm_ads_rtb_creativity_1d_di 
  where dtm >=  '20240101' and dtm <=  '{{ds_nodash}}'
  group by campaign_id,f_getdate(dtm)
  )t1 
  join 
  (select
      ca.*,
      from_unixtime(floor(coalesce(ca.create_time,0) / 1000 + 28800), 'yyyy-MM-dd') as create_dt,
      from_unixtime(floor(ca.start_time / 1000 + 28800), 'yyyy-MM-dd') as start_dt,
      from_unixtime(floor(ca.start_time / 1000 + 28800), 'yyyyMMdd') as start_dtm,
      from_unixtime(
        floor(
          greatest(ca.start_time,coalesce(ca.create_time,0)) / 1000 + 28800
        ),
        'yyyyMMdd'
      ) as rtb_dtm,
      from_unixtime(
        floor(
          greatest(ca.start_time,coalesce(ca.create_time,0)) / 1000 + 28800
        ),
        'yyyy-MM-dd'
      ) as rtb_dt,
      from_unixtime(
        floor(
          greatest(ca.expire_time, coalesce(ca.create_time,0)) / 1000 + 28800
        ),
        'yyyy-MM-dd'
      ) as expire_dt
    from
      redcdm.dwd_ads_rtb_campaign_df ca
      -- left join redods.ods_shequ_feed_ads_t_advertiser a on ca.advertiser_id = a.id
      -- and a.dtm = '{{ds_nodash}}'
    where
      ca.dtm =  '{{ds_nodash}}'
      -- and a.state = 1
      -- and a.budget_state = 1
      -- and a.balance_state = 1
      -- and ca.state = 1
      -- and ca.enable = 1
      -- and ca.budget_state = 1
      and from_unixtime(floor(greatest(ca.start_time, coalesce(ca.create_time,0))/ 1000 + 28800), 'yyyyMMdd')>= '20240101' 
      and from_unixtime(floor(greatest(ca.start_time, coalesce(ca.create_time,0)) / 1000 + 28800), 'yyyyMMdd')<= '{{ds_nodash}}' 
) t2
on t1.campaign_id = t2.id
where datediff(date_key,rtb_dt)<4 and datediff(date_key,rtb_dt)>=0 
group by t1.campaign_id, 
  rtb_dtm
  )base 
where conversion_cnt>=10