insert overwrite table redcdm_dev.dm_ads_pub_product_account_detail_td_df  partition(dtm = '{{ ds_nodash }}') 
select date_key,
  brand_account_id,
  module,
  product,
  marketing_target,
  optimize_target,
  market_target_type,
  is_marketing_product,
  sum(imp_cnt) as imp_cnt,
  sum(click_cnt) as click_cnt,
  sum(like_cnt) as like_cnt,
  sum(fav_cnt) as fav_cnt ,
  sum(cmt_cnt) as cmt_cnt,
  sum(follow_cnt) as follow_cnt,
  sum(share_cnt) as share_cnt,
  sum(screenshot_cnt) as screenshot_cnt,
  sum(image_save_cnt) as image_save_cnt,
  sum(cash_income_amt) as cash_income_amt,
  sum(income_amt) as income_amt,
  sum(open_sale_num) as open_sale_num,
  sum(direct_cash_income_amt) as direct_cash_income_amt,
 sum(direct_income_amt) as direct_income_amt,
  sum(channel_cash_income_amt) as  channel_cash_income_amt,
  sum(channel_income_amt) as channel_income_amt,
  sum(campaign_cnt) as campaign_cnt,
  sum(unit_cnt) as unit_cnt,
  sum(brand_campaign_cnt) as brand_campaign_cnt,
  sum(rtb_cost_income_amt) as rtb_cost_income_amt,
  sum(rtb_budget_income_amt) as rtb_budget_income_amt
fROM 
(
select  date_key,
  brand_account_id,
  module,
  product,
  marketing_target,
  optimize_target,
  market_target_type,
  is_marketing_product,
  imp_cnt,
  click_cnt,
  like_cnt,
  fav_cnt,
  cmt_cnt,
  follow_cnt,
  share_cnt,
  screenshot_cnt,
  image_save_cnt,
  0 as cash_income_amt,
  0 as income_amt,
  open_sale_num,
  0 as direct_cash_income_amt,
  0 as direct_income_amt,
  0 as channel_cash_income_amt,
  0 as channel_income_amt,
  campaign_cnt,
  unit_cnt,
  brand_campaign_cnt,
  rtb_cost_income_amt,
  rtb_budget_income_amt
from  redcdm.dm_ads_pub_product_account_detail_td_df 
where dtm='{{ds_nodash}}' and (module<>'效果' or date_key<'2022-01-01' or date_key>'2023-08-02' or coalesce(market_target_type,'整体')='整体') --保留
union all 
  select 
    from_unixtime(unix_timestamp(a.dtm,'yyyyMMdd'),'yyyy-MM-dd') as date_key
    ,a.brand_account_id
    ,'效果' as module
    ,case 
      when a.module = '发现feed' then '信息流' 
      when a.module = '搜索feed' then '搜索' 
      when a.module = '视频内流' then '视频内流'
    end as product
    ,marketing_target
  ,optimize_target
    ,case when a.marketing_target in (3,8,14,15) then '闭环电商'
     when a.marketing_target in (13) then '非闭环电商'
     when a.marketing_target in (2,5,9) then '线索'
     when a.marketing_target not in (3,8,2,5,9,13,14,15) then '种草'
     end as market_target_type
     ,'0' as is_marketing_product
    ,sum(a.imp_num) as imp_cnt
    ,sum(a.click_num) as click_cnt
    ,sum(a.like_num) as like_cnt
    ,sum(a.fav_num) as fav_cnt
    ,sum(a.comment_num) as cmt_cnt
    ,sum(a.follow_num) as follow_cnt
    ,sum(a.share_num) as share_cnt
    ,0 as screenshot_cnt,
  0 as image_save_cnt,
  0 as cash_income_amt,
  0 as income_amt,
  0 as open_sale_num,
  0 as direct_cash_income_amt,
  0 as direct_income_amt,
  0 as channel_cash_income_amt,
  0 as channel_income_amt,
  count(distinct campaign_id) as campaign_cnt,
  count(distinct unit_id) as unit_cnt,
  0 as brand_campaign_cnt,
  0 as rtb_cost_income_amt,
  0 as rtb_budget_income_amt
  from 
    reddw.dw_ads_wide_cpc_creativity_base_day_inc a
  where
    a.dtm >= '20220101' and a.dtm <= '20230802'
    and a.is_effective = 1
  group by 
    1,2,3,4,5,6,7,8
union all --全量消耗

--收入中间层
select
  date_key,
  brand_user_id as brand_account_id,
  module,
  product,
  if(marketing_target_id='',-911,marketing_target_id) as marketing_target,
  if(optimize_target_id='',-911,optimize_target_id)  as optimize_target,
  coalesce(if(marketing_target_type='',null,marketing_target_type), '整体') as market_target_type,
  coalesce(is_marketing_product, '0') as is_marketing_product,
  0 as imp_cnt,
  0 as click_cnt,
  0 as like_cnt,
  0 as fav_cnt,
  0 as cmt_cnt,
  0 as follow_cnt,
  0 as share_cnt,
  0 as screenshot_cnt,
  0 as image_save_cnt,
  sum(cash_income_amt) as cash_income_amt,
  sum(income_amt) income_amt,
  0 as open_sale_num,
  sum(
    case
      when coalesce(sales_system, '') <> '渠道业务部' then cash_income_amt
    end
  ) as direct_cash_cost,
  sum(
    case
      when coalesce(sales_system, '') <> '渠道业务部' then income_amt
    end
  ) as direct_cost,
  sum(
    case
      when coalesce(sales_system, '') = '渠道业务部' then cash_income_amt
    end
  ) as channel_cash_cost,
  sum(
    case
      when coalesce(sales_system, '') = '渠道业务部' then income_amt
    end
  ) as channel_cost,
  0 as campaign_cnt,
  0 as unit_cnt,
  0 as brand_campaign_cnt,
  0 as rtb_cost_income_amt,
  0 as rtb_budget_income_amt
from
  redcdm.dws_ads_advertiser_product_income_detail_df_view a
where
  a.dtm = '{{ds_nodash}}'
  and a.date_key <= '{{ds}}'
group by
  1,2,3,4,5,6,7,8
)t1 
group by  1,2,3,4,5,6,7,8