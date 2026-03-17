
    -- ************************************************
    -- Author: chenmengdi
    -- CreateTime:2023-12-04T11:22:23+08:00
    -- Update: Task Update Description
    -- ************************************************
    SET "kyuubi.spark.option.--conf spark.sql.crossJoin.enabled=true";
SET "kyuubi.spark.option.--conf spark.redExtension.allowBroadcastNestedLoopJoin=true";
drop table if exists temp.temp_app_ads_rtb_operation_metric_di_{{ds_nodash}};
create table temp.temp_app_ads_rtb_operation_metric_di_{{ds_nodash}}
select coalesce(t2.campaign_id,t1.campaign_id) as campaign_id,
  coalesce(t2.marketing_target,t1.marketing_target) as marketing_target,
  coalesce(t2.biz_product_type,t1.biz_product_type) as biz_product_type,
  coalesce(t2.target_type_list,t1.target_type_list) as target_type_list,
  case when coalesce(t2.target_type_list,t1.target_type_list)='1001' then '智能定向' 
  when coalesce(t2.target_type_list,t1.target_type_list) is null or coalesce(t2.target_type_list,t1.target_type_list) = '1000' then '通投' else '高级定向' end as target_type_msg_list,
  coalesce(t2.optimize_target,t1.optimize_target) as optimize_target,
  coalesce(t2.campaign_placement,t1.campaign_placement) as campaign_placement,
  coalesce(t2.brand_account_id,t1.brand_account_id) as brand_account_id,
  is_effective,
  case when group_ids = '' then null else group_ids end as group_ids,
  coalesce(t2.dtm,t1.dtm) as dtm,
  coalesce(max(is_rtb),0) as is_rtb,
  coalesce(max(is_today_create),0) as is_today_create,
  sum(income_amt) as income_amt,
  sum(cash_income_amt) as cash_income_amt,
  sum(like_cnt) as like_cnt,
  sum(fav_cnt) as fav_cnt,
  sum(cmt_cnt) as cmt_cnt,
  sum(follow_cnt) as follow_cnt,
  sum(share_cnt) as share_cnt,
  sum(screenshot_cnt) as screenshot_cnt,
  sum(imp_cnt) as imp_cnt,
  sum(click_cnt) as click_cnt,
  max(account_cash_income_amt) as account_cash_income_amt,
  coalesce(max(is_today_start),0) as is_today_start
  
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
    and expire_dt >= f_getdate(dtm) and is_valid=1 then 1
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
      ) as expire_dt,
      case when a.state = 1 
      and  a.budget_state = 1
      and a.balance_state = 1
      and ca.state = 1
      and ca.enable = 1
      and ca.budget_state = 1 then 1 else 0 end as is_valid
    from
      (select   id,
          enable,
          state,
          advertiser_id,
          campaign_name,
          limit_day_budget,
          campaign_day_budget,
          budget_state,
          balance_state,
          create_audit,
          update_audit,
          create_time,
          modify_time,
          v_seller_id,
          uac_budget_state,
          marketing_target,
          origin_campaign_day_budget,
          campaign_smart_switch,
          adv_budget_state,
          build_type,
          mode_type,
          campaign_type,
          start_time,
          expire_time,
          time_period_type,
          time_period,
          time_state,
          placement,
          sub_placement,
          optimize_target,
          promotion_target,
          bidding_strategy,
          bid_type,
          pacing_mode,
          constraint_type,
          search_flag,
          day_dtm as dtm
      from 
      (select *
      from  redods.ods_shequ_feed_ads_t_ads_rtb_campaign ca
      where dtm =  '{{ds_nodash}}'
      )ca
      left join 
      (
        select
          day_dtm,
          dt
        from
          redcdm.dim_ads_date_df
        where
          dtm = 'all'
          and day_dtm between '{{ds_30_days_ago_nodash}}' and '{{ds_nodash}}'
      ) dt on 1 = 1
      )ca
      left join redods.ods_shequ_feed_ads_t_advertiser a on ca.advertiser_id = a.id and ca.dtm=a.dtm
      and a.dtm <= '{{ds_nodash}}' and a.dtm >=  '{{ds_30_days_ago_nodash}}'
    
      -- and a.state = 1
      -- and a.budget_state = 1
      -- and a.balance_state = 1
      -- and ca.state = 1
      -- and ca.enable = 1
      -- and ca.budget_state = 1
  ) base
  left join (
    select
      virtual_seller_id,
      brand_account_id
    from
      redcdm.dim_ads_advertiser_df
    where dtm=max_dtm('redcdm.dim_ads_advertiser_df')
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
  is_effective,
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
      is_effective,
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
      concat_ws(',',array_distinct(split(concat_ws(',',collect_list(case when total_amount>0 then dmp_group_id else null end)),','))) as group_ids,
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
      is_effective,
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
      dtm <= '{{ds_nodash}}' and dtm >=  '{{ds_30_days_ago_nodash}}'
      )t1 
    left join 
    (select creativity_id,dmp_group_id
      from redcdm.dim_ads_creativity_core_df
     where
      dtm = max_dtm('redcdm.dim_ads_advertiser_df')
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
      brand_account_id,
      is_effective
  ) t1
  left join (
    select dtm,
      brand_account_id,
      sum(cost_amount) as account_cash_income_amt
    from
      redcdm.dm_ads_rtb_creativity_1d_di
    where
     dtm <= '{{ds_nodash}}' and dtm >=  '{{ds_30_days_ago_nodash}}'
    group by
      brand_account_id,
      dtm
  ) t2 on t1.brand_account_id = t2.brand_account_id and t1.dtm=t2.dtm
  )t2
on t1.campaign_id=t2.campaign_id  and t1.dtm=t2.dtm
group by 1,2,3,4,5,6,7,8,9,10,11
;

insert overwrite table redapp.app_ads_rtb_operation_metric_di  partition(dtm)
select coalesce(info.campaign_id,cov.campaign_id) as campaign_id,
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
  '' as valid_keywords,
  cost_keywords,
  is_today_start,
  conversion_cnt,
  coalesce(is_config_finish,0) as is_config_finish,
  coalesce(is_effective,0) as is_effective,
  coalesce(info.dtm, cov.start_dtm) as dtm
from 
temp.temp_app_ads_rtb_operation_metric_di_{{ds_nodash}} info
-- left join 
-- (select dtm,campaign_id,concat_ws(',',collect_set(keyword)) as valid_keywords
-- from reddw.dw_ads_t_rtb_search_keyword_day
-- where dtm <= '{{ds_nodash}}' and dtm >=  '{{ds_3_days_ago_nodash}}'
-- group by campaign_id,dtm
-- )word 
-- on word.campaign_id=info.campaign_id and word.dtm=info.dtm
left join 
--有消耗竞价词
(select dtm,campaign_id,concat_ws(',',collect_set(bidword)) as cost_keywords
from redcdm.dws_ads_log_search_bidword_creativity_1d_di
where dtm <= '{{ds_nodash}}' and dtm >=  '{{ds_30_days_ago_nodash}}' and cost>0
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
(select t1.campaign_id, 
  
  start_dtm,
  sum(conversion_cnt) as conversion_cnt
  from 
  (select f_getdate(dtm) as date_key,campaign_id,sum(conversion_cnt) as conversion_cnt
  from redcdm.dm_ads_rtb_creativity_1d_di 
  where dtm >=  '{{ds_30_days_ago_nodash}}' and dtm <=  '20231129'
  group by campaign_id,f_getdate(dtm)
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
      and from_unixtime(floor(ca.start_time / 1000 + 28800), 'yyyyMMdd')>= '{{ds_30_days_ago_nodash}}' 
      and from_unixtime(floor(ca.start_time / 1000 + 28800), 'yyyyMMdd')<= '{{ds_nodash}}' 
) t2
on t1.campaign_id = t2.id
where datediff(date_key,start_dt)<4 and datediff(date_key,start_dt)>=0 
group by t1.campaign_id, 
  start_dtm
  )base 
where conversion_cnt>=10
)cov 
on cov.campaign_id = info.campaign_id and cov.start_dtm=info.dtm

