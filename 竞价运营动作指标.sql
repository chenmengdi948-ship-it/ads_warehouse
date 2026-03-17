drop table if exists temp.temp_app_ads_rtb_operation_metric_di_{{ds_nodash}};
create table temp.temp_app_ads_rtb_operation_metric_di_{{ds_nodash}}
select coalesce(t2.campaign_id,t1.campaign_id) as campaign_id,
coalesce(t2.marketing_target,t1.marketing_target) as marketing_target,
coalesce(t2.biz_product_type,t1.biz_product_type) as biz_product_type,
coalesce(t2.target_type_list,t1.target_type_list) as target_type_list,
coalesce(t2.target_type_msg_list,t1.target_type_msg_list) as target_type_msg_list,
coalesce(t2.optimize_target,t1.optimize_target) as optimize_target,
coalesce(t2.campaign_placement,t1.campaign_placement) as campaign_placement,
coalesce(t2.brand_account_id,t1.brand_account_id) as brand_account_id,
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
  is_today_start,
  coalesce(t2.dtm,t1.dtm) as dtm
from 
(select
  id as campaign_id,
  marketing_target,
  case
    when coalesce(search_flag, -1) > -1
    and coalesce(placement, 0) = 1 then 3 --搜索快投
    when coalesce(placement, 0) = 4 then 2 -- 搜推同投
    when coalesce(placement, 0) <> 4 then 1 -- 子计划已经打上了 搜索追投
    else '-1'
  end as biz_product_type,
  null as target_type_list,
  null as target_type_msg_list,
  optimize_target,
  placement as campaign_placement,
  base.v_seller_id ,
  case
    when create_dt = f_getdate(dtm) then 1
    else 0
  end as is_today_create,
  case
    when start_dt = f_getdate(dtm) then 1
    else 0
  end as is_today_start,
  case
    when rtb_dt <= f_getdate(dtm)
    and expire_dt >= f_getdate(dtm) then 1
    else 0
  end as is_rtb,
  brand_account_id,
  dtm
from
  (
    select
      ca.*,
      from_unixtime(floor(ca.create_time / 1000 + 28800), 'yyyy-MM-dd') as create_dt,
      from_unixtime(floor(ca.start_time / 1000 + 28800), 'yyyy-MM-dd') as start_dt,
      from_unixtime(
        floor(
          greatest(ca.start_time, ca.create_time) / 1000 + 28800
        ),
        'yyyy-MM-dd'
      ) as rtb_dt,
      from_unixtime(
        floor(
          greatest(ca.expire_time, ca.create_time) / 1000 + 28800
        ),
        'yyyy-MM-dd'
      ) as expire_dt
    from
      redods.ods_shequ_feed_ads_t_ads_rtb_campaign ca
      left join redods.ods_shequ_feed_ads_t_advertiser a on ca.advertiser_id = a.id and ca.dtm=a.dtm
      and a.dtm <= '{{ds_nodash}}' and a.dtm >=  '{{ds_3_days_ago_nodash}}'
    where
      ca.dtm <= '{{ds_nodash}}' and ca.dtm >=  '{{ds_3_days_ago_nodash}}'
      and a.state = 1
      and a.budget_state = 1
      and a.balance_state = 1
      and ca.state = 1
      and ca.enable = 1
      and ca.budget_state = 1
  ) base
  left join (
    select
      virtual_seller_id,
      brand_account_id
    from
      redcdm.dim_ads_advertiser_df
    where dtm='{{ds_nodash}}'
  ) adv on adv.virtual_seller_id = base.v_seller_id
  )t1 
full outer join
(select
  campaign_id,
  marketing_target,
  biz_product_type,
  --1:其他 1:搜索追投 2:搜推同投 3:搜索快投
  target_type_list,
  target_type_msg_list,
  optimize_target,
  -- module,
  -- product,
  campaign_placement,
  --1：信息流；2：搜索；4：全站智投；7：视频内流
  t1.brand_account_id,
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
  conversion_cnt,
  t1.dtm
from
(select dtm,
      campaign_id,
      marketing_target,
      biz_product_type,
      --1:其他 1:搜索追投 2:搜推同投 3:搜索快投
      target_type_list,
      target_type_msg_list,
      optimize_target,
      -- module,
      -- product,
      campaign_placement,
      --1：信息流；2：搜索；4：全站智投；7：视频内流
      brand_account_id,
      sum(total_amount) as income_amt,
      sum(cost_amount) as cash_income_amt,
      sum(like_cnt) as like_cnt,
      sum(fav_cnt) as fav_cnt,
      sum(cmt_cnt) as cmt_cnt,
      sum(follow_cnt) as follow_cnt,
      sum(share_cnt) as share_cnt,
      sum(screenshot_cnt) as screenshot_cnt,
      sum(unique_imp_cnt) as imp_cnt,
      sum(unique_click_cnt) as click_cnt,
      concat_ws(',',array_distinct(split(concat_ws(',',collect_list(dmp_group_id)),','))) as group_ids,
      sum(conversion_cnt) as conversion_cnt
    from
    (select creativity_id,
      campaign_id,
      marketing_target,
      biz_product_type,
      --1:其他 1:搜索追投 2:搜推同投 3:搜索快投
      target_type_list,
      target_type_msg_list,
      optimize_target,
      -- module,
      -- product,
      campaign_placement,
      --1：信息流；2：搜索；4：全站智投；7：视频内流
      brand_account_id,
      total_amount,
      cost_amount,
      like_cnt,
      fav_cnt,
      cmt_cnt,
      follow_cnt,
      share_cnt,
      screenshot_cnt,
      unique_imp_cnt,
      unique_click_cnt,
      conversion_cnt,
      dtm
    from  redcdm.dm_ads_rtb_creativity_1d_di
    where
      dtm <= '{{ds_nodash}}' and dtm >=  '{{ds_3_days_ago_nodash}}'
      )t1 
    left join 
    (select creativity_id,dmp_group_id
      from redcdm.dim_ads_creativity_core_df
     where
      dtm = '{{ds_nodash}}'
        )t2 on t1.creativity_id=t2.creativity_id
    group by dtm,
      campaign_id,
      marketing_target,
      biz_product_type,
      --1:其他 1:搜索追投 2:搜推同投 3:搜索快投
      target_type_list,
      target_type_msg_list,
      optimize_target,
      -- module,
      -- product,
      campaign_placement,
      brand_account_id
  ) t1
  left join (
    select dtm,
      brand_account_id,
      sum(cost_amount) as account_cash_income_amt
    from
      redcdm.dm_ads_rtb_creativity_1d_di
    where
     dtm <= '{{ds_nodash}}' and dtm >=  '{{ds_3_days_ago_nodash}}'
    group by
      brand_account_id,
      dtm
  ) t2 on t1.brand_account_id = t2.brand_account_id and t1.dtm=t2.dtm
  )t2
on t1.campaign_id=t2.campaign_id  and t1.dtm=t2.dtm
;
insert overwrite table redapp.app_ads_rtb_operation_metric_di  partition(dtm)
select info.campaign_id,
 brand_account_id,
 marketing_target,
case
  when marketing_target in (3, 8,14,15) then '闭环电商'
  when marketing_target in (13) then '非闭环电商'
  when marketing_target in (2, 5, 9) then '线索'
  when marketing_target not in (3, 8, 2, 5, 9, 13,14,15) then '种草'
  end  as market_target,
 biz_product_type,
 target_type_list,
 target_type_msg_list,
 optimize_target,
 campaign_placement,
 is_rtb,
 is_today_create,income_amt,
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
  info.dtm
from 
temp.temp_app_ads_rtb_operation_metric_di_{{ds_nodash}} info
left join 
(select dtm,campaign_id,concat_ws(',',collect_set(keyword)) as valid_keywords
from reddw.dw_ads_t_rtb_search_keyword_day
where dtm <= '{{ds_nodash}}' and dtm >=  '{{ds_3_days_ago_nodash}}'
group by campaign_id,dtm
)word 
on word.campaign_id=info.campaign_id and word.dtm=info.dtm
left join 
--有消耗竞价词
(select dtm,campaign_id,concat_ws(',',collect_set(bidword)) as cost_keywords
from redcdm.dws_ads_log_search_bidword_creativity_1d_di
where dtm <= '{{ds_nodash}}' and dtm >=  '{{ds_3_days_ago_nodash}}' and cash_cost>0
group by campaign_id,dtm
)word_cost
on word_cost.campaign_id=info.campaign_id and word_cost.dtm=info.dtm
left join

(

select campaign_id, 
conversion_cnt,
start_dtm,
1 as is_config_finish
from 
(select '{{ds}}' as date_key,campaign_id,sum(conversion_cnt) as conversion_cnt
from redcdm.dm_ads_rtb_creativity_1d_di 
where dtm >=  '{{ds_3_days_ago_nodash}}' and dtm <=  '{{ds_nodash}}'
group by campaign_id
)t1 
 join 
(select
      ca.*,
      from_unixtime(floor(ca.create_time / 1000 + 28800), 'yyyy-MM-dd') as create_dt,
      from_unixtime(floor(ca.start_time / 1000 + 28800), 'yyyy-MM-dd') as start_dt,
      from_unixtime(floor(ca.start_time / 1000 + 28800), 'yyyyMMdd') as start_dtm,
      from_unixtime(
        floor(
          greatest(ca.start_time, ca.create_time) / 1000 + 28800
        ),
        'yyyy-MM-dd'
      ) as rtb_dt,
      from_unixtime(
        floor(
          greatest(ca.expire_time, ca.create_time) / 1000 + 28800
        ),
        'yyyy-MM-dd'
      ) as expire_dt
    from
      redods.ods_shequ_feed_ads_t_ads_rtb_campaign ca
      -- left join redods.ods_shequ_feed_ads_t_advertiser a on ca.advertiser_id = a.id
      -- and a.dtm = '{{ds_nodash}}'
    where
      ca.dtm = '{{ds_nodash}}'
      -- and a.state = 1
      -- and a.budget_state = 1
      -- and a.balance_state = 1
      -- and ca.state = 1
      -- and ca.enable = 1
      -- and ca.budget_state = 1
      and from_unixtime(floor(ca.start_time / 1000 + 28800), 'yyyyMMdd')>= '{{ds_3_days_ago_nodash}}' 
      and from_unixtime(floor(ca.start_time / 1000 + 28800), 'yyyyMMdd')<= '{{ds_nodash}}' 
) t2
on t1.campaign_id = t2.id
where datediff(date_key,start_dt)<4 and conversion_cnt>=10
)cov 
on cov.campaign_id = info.campaign_id and cov.start_dtm=info.dtm