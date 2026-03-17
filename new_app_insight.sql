SELECT
  a.date_key,
  a.module,
  a.product,
  a.marketing_target,
  a.optimize_target,
  a.market_target as market_target_type,
  a.brand_account_id,
  brand_user_name as brand_account_name,
  company_code,
  company_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  CASE
    WHEN (
      (a.module IN ('效果', '薯条', '品合', '内容加热'))
      OR (a.module IS NULL)
    ) THEN b.cpc_direct_sales_dept3_name
    WHEN (a.module IN ('品牌', 'IP')) THEN b.brand_direct_sales_dept3_name
  END AS direct_sales_dept3_name,
  CASE
    WHEN (a.module IN ('效果', '薯条', '品合', '内容加热'))
      OR (a.module IS NULL)
   THEN b.cpc_direct_sales_dept4_name
    WHEN  (a.module IN ('品牌', 'IP')) THEN b.brand_direct_sales_dept4_name
  END AS direct_sales_dept4_name,
  CASE
    WHEN (
      (a.module IN ('效果', '薯条', '品合', '内容加热'))
      OR (a.module IS NULL)
    ) THEN b.cpc_direct_sales_dept5_name
    WHEN (a.module IN ('品牌', 'IP')) THEN b.brand_direct_sales_dept5_name
  END AS direct_sales_dept5_name,
  CASE
    WHEN (
      (a.module IN ('效果', '薯条', '品合', '内容加热'))
      OR (a.module IS NULL)
    ) THEN b.cpc_direct_sales_dept6_name
    WHEN (a.module IN ('品牌', 'IP')) THEN b.brand_direct_sales_dept6_name
  END AS direct_sales_dept6_name,
  brand_tag_code,
  brand_tag_name,
  brand_group_tag_code,
  brand_group_tag_name,
  first_industry_name,
  second_industry_name,
  CASE
    WHEN (a.module IN ('效果', '薯条', '品合', '内容加热')) THEN b.cpc_direct_sales_name
    WHEN (a.module IN ('品牌', 'IP')) THEN b.brand_direct_sales_name
  END AS direct_sales_name,
  cpc_operator_name,
  account.seller_id,
  case when account.seller_id is not null then 1 else 0 end as is_seller_user,
  shop_name,
  main_category_name,
  industry,
  first_category_name,
  second_category_name,
  third_category_name,
  live_dgmv,
  k_live_dgmv,
  s_live_dgmv,
  other_live_dgmv,
  dgmv,
  ads_live_dgmv,
  ads_sx_live_dgmv,
  ads_dgmv,
  cash_income_amt,
  income_amt
FROM 
(select
       date_key,
      module,
      case
        when module in ('品合' '内容加热') then '品合'
        when module = '薯条' then '薯条'
        when product='火焰话题'  then '品牌其他'
        when product='信息流' then '竞价-信息流'
        when product='搜索' then '竞价-搜索'
        when product='视频内流' then '竞价-视频内流'
        else product
      end as product,
      brand_account_id,
      coalesce(market_target_type,'整体') as market_target,
      coalesce(marketing_target,-911) as marketing_target,
      coalesce(optimize_target,-911) as optimize_target,
      sum(cash_income_amt) as cash_income_amt,
      sum(income_amt) as income_amt,
      0 as live_dgmv,
      0 as k_live_dgmv,
      0 as s_live_dgmv,
      0 as other_live_dgmv,
      0 as dgmv,
      0 as ads_live_dgmv,
      0 as ads_sx_live_dgmv,
      0 as ads_dgmv
    from
      redcdm.dm_ads_pub_product_account_detail_td_df a
    where
      a.dtm = '{{ds_nodash}}'
      and module in ('效果') 
      and marketing_target in (3, 8,13,14,15)
      and (income_amt>0 or cash_income_amt>0)
    group by
      1,2,3,4,5,6,7
      union all 
      select 
    date_key,
    '整体' as module,
    '整体' as product,
    seller_user_id as brand_account_id,
    '整体' as market_target,
    -911 as marketing_target,
    -911 as optimize_target,
    0 as cash_income_amt,
    0 as income_amt,
    sum(live_dgmv) as live_dgmv,
    sum(k_live_dgmv) as k_live_dgmv,
    sum(s_live_dgmv) as s_live_dgmv,
    sum(other_live_dgmv) as other_live_dgmv,
    sum(dgmv) as dgmv,
    sum(ads_live_dgmv) as ads_live_dgmv,
    sum(ads_sx_live_dgmv) as ads_sx_live_dgmv,
    sum(ads_dgmv) as ads_dgmv
  from
     redapp.app_ads_insight_account_dgmv_df a
  where
   dtm = '{{ds_nodash}}'
  group by
    1,2, 3,4, 5,6
      )a

LEFT JOIN (
    SELECT
      brand_account_id,
      brand_user_name,
      company_code,
      company_name,
      
      track_group_id,
      track_group_name,
     
      cpc_direct_sales_code,
      cpc_direct_sales_name,
      cpc_direct_sales_dept1_code,
      cpc_direct_sales_dept1_name,
      cpc_direct_sales_dept2_code,
      cpc_direct_sales_dept2_name,
      cpc_direct_sales_dept3_code,
      cpc_direct_sales_dept3_name,
      cpc_direct_sales_dept4_code,
      cpc_direct_sales_dept4_name,
      cpc_direct_sales_dept5_code,
      cpc_direct_sales_dept5_name,
      cpc_direct_sales_dept6_code,
      cpc_direct_sales_dept6_name,
      brand_direct_sales_code,
      brand_direct_sales_name,
      brand_direct_sales_dept1_code,
      brand_direct_sales_dept1_name,
      brand_direct_sales_dept2_code,
      brand_direct_sales_dept2_name,
      brand_direct_sales_dept3_code,
      brand_direct_sales_dept3_name,
      brand_direct_sales_dept4_code,
      brand_direct_sales_dept4_name,
      brand_direct_sales_dept5_code,
      brand_direct_sales_dept5_name,
      brand_direct_sales_dept6_code,
      brand_direct_sales_dept6_name,
      brand_tag_code,
      brand_tag_name,
      brand_group_tag_code,
      brand_group_tag_name,
      first_industry_name,
      second_industry_name,
      track_detail_name,
      track_industry_name,
      cpc_operator_code,
      cpc_operator_name
    FROM
      redcdm.dim_ads_industry_account_df
    WHERE
      dtm = '{{ds_nodash}}'
  ) b ON a.brand_account_id = b.brand_account_id
   left join 
   (
    select distinct 
      user_id as seller_user_id,
      seller_id,
      relation_type
    from
      redcdm.dim_pro_soc_user_relation_df
    WHERE
      dtm = '{{ds_nodash}}'
      and seller_id <> 'UNKNOWN'
      and is_valid = 1
      and relation_type in (1)
  ) account on a.brand_account_id = account.seller_user_id
  left join (
    select
      seller_id,
      shopname as shop_name,
      user_id,
      user_name,
      main_category_name,
      industry,
      first_category_name,
      second_category_name,
      third_category_name
    from
      reddw.dw_trd_seller_base_metrics_day
    where
      dtm = '{{ds_nodash}}'
  ) t2 on account.seller_id = t2.seller_id
  ;
  
---未上线，单独建表
with base as 
(select 
  coalesce(a.date_key_1,b.date_key) as date_key
  ,coalesce(a.module_1,b.module) as module
  ,coalesce(a.product_1,b.product) as product
  ,coalesce(a.brand_account_id_1,b.brand_account_id) as brand_account_id
  ,sum(imp_cnt) as imp_cnt
  ,sum(click_cnt) as click_cnt
  ,sum(case when coalesce(a.date_key_1,b.date_key)<='2023-08-20' then b.like_cnt else a.like_cnt end) as like_cnt
  ,sum(case when coalesce(a.date_key_1,b.date_key)<='2023-08-20' then b.fav_cnt else a.fav_cnt end) as fav_cnt
  ,sum(case when coalesce(a.date_key_1,b.date_key)<='2023-08-20' then b.cmt_cnt else a.cmt_cnt end) as cmt_cnt
  ,sum(case when coalesce(a.date_key_1,b.date_key)<='2023-08-20' then b.share_cnt else a.share_cnt end) as share_cnt
  ,sum(case when coalesce(a.date_key_1,b.date_key)<='2023-08-20' then b.follow_cnt else a.follow_cnt end) as follow_cnt
  ,sum(cash_cost) as cash_cost
  ,sum(cost) as cost
  ,sum(mkt_ecm_cost) as mkt_ecm_cost
  ,sum(mkt_leads_cost) as mkt_leads_cost
  ,sum(mkt_zc_cost) as mkt_zc_cost
  ,sum(open_sale_num) as open_sale_num
  ,sum(direct_cash_cost) as direct_cash_cost
  ,sum(direct_cost) as direct_cost
  ,sum(channel_cash_cost) as channel_cash_cost
  ,sum(channel_cost) as channel_cost
  ,sum(campaign_cnt) as campaign_cnt
  ,sum(unit_cnt) as unit_cnt
  ,sum(brand_campaign_cnt) as brand_campaign_cnt
  ,sum(cpc_cost_budget_rate) as cpc_cost_budget_rate
  ,sum(cpc_budget) as cpc_budget
  ,sum(mkt_ecm_cash_cost) as mkt_ecm_cash_cost
  ,sum(mkt_leads_cash_cost) as mkt_leads_cash_cost
  ,sum(mkt_zc_cash_cost) as mkt_zc_cash_cost
  ,sum(mkt_ecm_direct_cost) as mkt_ecm_direct_cost
  ,sum(mkt_leads_direct_cost) as mkt_leads_direct_cost
  ,sum(mkt_zc_direct_cost) as mkt_zc_direct_cost
  ,sum(mkt_ecm_direct_cash_cost) as mkt_ecm_direct_cash_cost
  ,sum(mkt_leads_direct_cash_cost) as mkt_leads_direct_cash_cost
  ,sum(mkt_zc_direct_cash_cost) as mkt_zc_direct_cash_cost
  ,sum(mkt_ecm_channel_cost) as mkt_ecm_channel_cost
  ,sum(mkt_leads_channel_cost) as mkt_leads_channel_cost
  ,sum(mkt_zc_channel_cost) as mkt_zc_channel_cost
  ,sum(mkt_ecm_channel_cash_cost) as mkt_ecm_channel_cash_cost
  ,sum(mkt_leads_channel_cash_cost) as mkt_leads_channel_cash_cost
  ,sum(mkt_zc_channel_cash_cost) as mkt_zc_channel_cash_cost
  ,sum(mkt_ecm_unclosed_cost) as mkt_ecm_unclosed_cost
  ,sum(mkt_ecm_unclosed_cash_cost) as mkt_ecm_unclosed_cash_cost
  ,sum(mkt_ecm_unclosed_direct_cost) as mkt_ecm_unclosed_direct_cost
  ,sum(mkt_ecm_unclosed_direct_cash_cost) as mkt_ecm_unclosed_direct_cash_cost
  ,sum(mkt_ecm_unclosed_channel_cost) as mkt_ecm_unclosed_channel_cost
  ,sum(mkt_ecm_unclosed_channel_cash_cost) as mkt_ecm_unclosed_channel_cash_cost
  ,sum(conversion_cnt) as conversion_cnt
  ,sum(leads_cnt) as leads_cnt
  ,sum(msg_num) as msg_num
  ,sum(msg_open_num) as msg_open_num
  ,sum(live_rgmv) as live_rgmv
  ,sum(live_dgmv) as live_dgmv
  ,sum(click_rgmv_7d) as click_rgmv_7d
  ,sum(rtb_cost_income_amt) as rtb_cost_income_amt
  ,sum(rtb_budget_income_amt) as rtb_budget_income_amt
  ,sum(origin_live_dgmv) as origin_live_dgmv
  ,sum(origin_k_live_dgmv) as origin_k_live_dgmv
  ,sum(origin_s_live_dgmv) as origin_s_live_dgmv
  ,sum(origin_other_live_dgmv) as origin_other_live_dgmv
  ,sum(origin_dgmv) as origin_dgmv
  ,sum(ads_live_dgmv) as ads_live_dgmv
  ,sum(ads_sx_live_dgmv) as ads_sx_dgmv
  ,sum(ads_dgmv) as ads_dgmv
  ,coalesce(a.market_target_1,b.market_target) as market_target
  ,coalesce(a.is_marketing_product_1,b.is_marketing_product) as is_marketing_product
  ,coalesce(a.marketing_target_1,b.marketing_target) as marketing_target
  ,coalesce(a.optimize_target_1,b.optimize_target) as optimize_target
from 
(select a.*,
  rtb_cost_income_amt,
  rtb_budget_income_amt,
  live_dgmv as origin_live_dgmv,
  k_live_dgmv as origin_k_live_dgmv,
  s_live_dgmv as origin_s_live_dgmv,
  other_live_dgmv as origin_other_live_dgmv,
  dgmv as origin_dgmv,
  ads_live_dgmv,
  ads_sx_live_dgmv,
  ads_dgmv,
  coalesce(a.date_key,c.date_key) as date_key_1,
  coalesce(a.module,c.module) as module_1,
  coalesce(a.product,c.product) as product_1,
  coalesce(a.brand_account_id,c.brand_account_id) as brand_account_id_1,
  coalesce(a.market_target,c.market_target) as market_target_1,
  coalesce(a.is_marketing_product,c.is_marketing_product) as is_marketing_product_1,
  coalesce(a.marketing_target,c.marketing_target) as marketing_target_1,
  coalesce(a.optimize_target,c.optimize_target) as optimize_target_1
from 
  ( 
    
    select
       date_key
      ,module
      ,case
        when module in ('品合' '内容加热') then '品合'
        when module = '薯条' then '薯条'
        when product='火焰话题'  then '品牌其他'
        when product='信息流' then '竞价-信息流'
        when product='搜索' then '竞价-搜索'
        when product='视频内流' then '竞价-视频内流'
        else product
      end as product
      ,brand_account_id
      ,coalesce(market_target_type,'整体') as market_target
      ,coalesce(is_marketing_product,'0') as is_marketing_product
      ,coalesce(marketing_target,-911) as marketing_target
      ,coalesce(optimize_target,-911) as optimize_target
      ,sum(imp_cnt) as imp_cnt
      ,sum(click_cnt) as click_cnt
      ,sum(like_cnt) as like_cnt
      ,sum(fav_cnt) as fav_cnt
      ,sum(cmt_cnt) as cmt_cnt
      ,sum(share_cnt) as share_cnt
      ,sum(follow_cnt) as follow_cnt
      ,sum(cash_income_amt) as cash_cost
      ,sum(income_amt) as cost
      ,sum(case when market_target_type = '闭环电商' then income_amt end) as mkt_ecm_cost
      ,sum(case when market_target_type = '线索' then income_amt end) as mkt_leads_cost
      ,sum(case when market_target_type = '种草' then income_amt end) as mkt_zc_cost
      ,sum(open_sale_num) as open_sale_num
      ,sum(direct_cash_income_amt  ) as direct_cash_cost
      ,sum(direct_income_amt ) as direct_cost
      ,sum(channel_cash_income_amt ) as channel_cash_cost
      ,sum(channel_income_amt) as channel_cost
      ,sum(campaign_cnt) as campaign_cnt
      ,sum(unit_cnt) as unit_cnt
      ,sum(brand_campaign_cnt) as brand_campaign_cnt
      ,sum(rtb_cost_income_amt) as cpc_cost_budget_rate
      ,sum(rtb_budget_income_amt) as cpc_budget
      ,sum(case when market_target_type = '闭环电商' then cash_income_amt end) as mkt_ecm_cash_cost
      ,sum(case when market_target_type = '线索' then cash_income_amt end) as mkt_leads_cash_cost
      ,sum(case when market_target_type = '种草' then cash_income_amt end) as mkt_zc_cash_cost
      ,sum(case when  market_target_type = '闭环电商' then direct_income_amt end) as mkt_ecm_direct_cost
      ,sum(case when  market_target_type = '线索' then direct_income_amt end) as mkt_leads_direct_cost
      ,sum(case when  market_target_type = '种草' then direct_income_amt end) as mkt_zc_direct_cost
      ,sum(case when  market_target_type = '闭环电商' then direct_cash_income_amt end) as mkt_ecm_direct_cash_cost
      ,sum(case when  market_target_type = '线索' then direct_cash_income_amt end) as mkt_leads_direct_cash_cost
      ,sum(case when  market_target_type = '种草' then direct_cash_income_amt end) as mkt_zc_direct_cash_cost
      ,sum(case when  market_target_type = '闭环电商' then channel_income_amt end) as mkt_ecm_channel_cost
      ,sum(case when  market_target_type = '线索' then channel_income_amt end) as mkt_leads_channel_cost
      ,sum(case when  market_target_type = '种草' then channel_income_amt end) as mkt_zc_channel_cost
      ,sum(case when  market_target_type = '闭环电商' then channel_cash_income_amt end) as mkt_ecm_channel_cash_cost
      ,sum(case when  market_target_type = '线索' then channel_cash_income_amt end) as mkt_leads_channel_cash_cost
      ,sum(case when  market_target_type = '种草' then channel_cash_income_amt end) as mkt_zc_channel_cash_cost
       --20230508新增非闭环电商字段
      ,sum(case when market_target_type = '非闭环电商' then income_amt end) as mkt_ecm_unclosed_cost
      ,sum(case when market_target_type = '非闭环电商' then cash_income_amt end) as mkt_ecm_unclosed_cash_cost
      ,sum(case when market_target_type = '非闭环电商' then direct_income_amt end) as mkt_ecm_unclosed_direct_cost
      ,sum(case when market_target_type = '非闭环电商' then direct_cash_income_amt end) as mkt_ecm_unclosed_direct_cash_cost
      ,sum(case when market_target_type = '非闭环电商' then channel_income_amt end) as mkt_ecm_unclosed_channel_cost
      ,sum(case when market_target_type = '非闭环电商' then channel_cash_income_amt end) as mkt_ecm_unclosed_channel_cash_cost
    from
      redcdm.dm_ads_pub_product_account_detail_td_df a
    where
      a.dtm = '{{ds_nodash}}'
      
    group by
      1,2,3,4,5,6,7,8
    
  ) a
  full outer join 
  --20231007增加电商字段
  --20230912增加成本所需字段以及分场域/营销目的预算字段
  (select 
    f_getdate(dtm) as date_key,
    '效果' as module,
    case 
        when module='搜索feed' then '竞价-搜索'
        when module='发现feed' then '竞价-信息流'
        when module='视频内流' then '竞价-视频内流'
        else ''
      end as product,
    brand_account_id,
    case
      when ads_type='闭环电商广告' then '闭环电商'
      when ads_type='非闭环电商广告' then '非闭环电商'
      when ads_type='线索广告' then '线索'
       when ads_type='种草广告' then  '种草'
    end as market_target,
    '0' as is_marketing_product, --营销目的
    -911 as marketing_target,
    -911 as optimize_target,
    sum(cost_special_campaign) as rtb_cost_income_amt,
    sum(campaign_budget) as rtb_budget_income_amt,
    0 as live_dgmv,
    0 as k_live_dgmv,
    0 as s_live_dgmv,
    0 as other_live_dgmv,
    0 as dgmv,
    0 as ads_live_dgmv,
    0 as ads_sx_live_dgmv,
    0 as ads_dgmv
  from
    redapp.app_ads_overall_budget_1d_di a
  where
   dtm >= '20230320'
    and granularity = '广告主x场域x营销目标'
  group by
    1,2, 3,4, 5,6
  union all 
  select 
    date_key,
    '整体' as module,
    '整体' as product,
    seller_user_id as brand_account_id,
    '整体' as market_target,
    '0' as is_marketing_product, --营销目的
    -911 as marketing_target,
    -911 as optimize_target,
    0 as rtb_cost_income_amt,
    0 as rtb_budget_income_amt,
    sum(live_dgmv) as live_dgmv,
    sum(k_live_dgmv) as k_live_dgmv,
    sum(s_live_dgmv) as s_live_dgmv,
    sum(other_live_dgmv) as other_live_dgmv,
    sum(dgmv) as dgmv,
    sum(ads_live_dgmv) as ads_live_dgmv,
    sum(ads_sx_live_dgmv) as ads_sx_live_dgmv,
    sum(ads_dgmv) as ads_dgmv
  from
     redapp.app_ads_insight_account_dgmv_df a
  where
   dtm = '{{ds_nodash}}'
  group by
    1,2, 3,4, 5,6
  )c 
  on a.date_key=c.date_key
  and a.module=c.module
  and a.product=c.product
  and a.brand_account_id=c.brand_account_id
  and a.market_target=c.market_target
  and a.is_marketing_product=c.is_marketing_product
  and a.marketing_target=c.marketing_target
  and a.optimize_target=c.optimize_target
)a 
--20230912增加成本所需字段以及分场域/营销目的预算字段
full outer join 
(select date_key,
  module,
  case when product='信息流' then '竞价-信息流'
    when product='搜索' then '竞价-搜索'
    when product='视频内流' then '竞价-视频内流' else null end as product,
  brand_account_id,
  market_target,
  '0' as is_marketing_product,
  coalesce(cast(marketing_target as bigint),-911) as marketing_target,
  coalesce(cast(optimize_target as bigint),-911) as optimize_target,
  sum(case when optimize_target not in ("0") then conversion_cnt else 0 end) as conversion_cnt,
  sum(leads_cnt) as leads_cnt,
  sum(msg_num) as msg_num,
  sum(msg_open_num) as msg_open_num,
  sum(like_cnt) as like_cnt,
  sum(fav_cnt) as fav_cnt,
  sum(cmt_cnt) as cmt_cnt,
  sum(share_cnt) as share_cnt,
  sum(follow_cnt) as follow_cnt,
  sum(case when marketing_target = '8' then coalesce(live_rgmv,live_dgmv,0)/100.0 when marketing_target =3 then coalesce(click_rgmv_7d,0) else 0 end) as live_rgmv,
  sum(live_dgmv) as live_dgmv,
  sum(click_rgmv_7d) as click_rgmv_7d
from redcdm.dm_ads_industry_product_advertiser_td_df
where dtm = '{{ds_nodash}}' 
and (coalesce(conversion_cnt,0)+coalesce(leads_cnt,0)+coalesce(msg_num,0)+coalesce(msg_open_num,0)
+coalesce(like_cnt,0)+coalesce(fav_cnt,0)+coalesce(cmt_cnt,0)+coalesce(share_cnt,0)
+coalesce(follow_cnt,0)+coalesce(live_rgmv,0)+coalesce(live_dgmv,0)+coalesce(click_rgmv_7d,0)>0)
and module in ('效果')
group by 1,2,3,4,5,6,7,8
)b 
on a.date_key=b.date_key
and a.module=b.module
and a.product=b.product
and a.brand_account_id=b.brand_account_id
and a.market_target=b.market_target
and a.is_marketing_product=b.is_marketing_product
and a.marketing_target=b.marketing_target
and a.optimize_target=b.optimize_target
group by
   coalesce(a.date_key_1,b.date_key)
  ,coalesce(a.module_1,b.module)
  ,coalesce(a.product_1,b.product)
  ,coalesce(a.brand_account_id_1,b.brand_account_id)
  ,coalesce(a.market_target_1,b.market_target)
  ,coalesce(a.is_marketing_product_1,b.is_marketing_product)
  ,coalesce(a.marketing_target_1,b.marketing_target)
  ,coalesce(a.optimize_target_1,b.optimize_target)
)
insert overwrite table redapp_dev.app_ads_insight_industry_product_account_td_df partition(dtm = '{{ ds_nodash }}')
SELECT
  coalesce(a.date_key,f_getdate(c.dtm)) AS date_key,
  a.module,
  a.product,
  coalesce(a.brand_account_id,c.brand_account_id) AS brand_account_id,
  coalesce(b.brand_user_name,t2.user_name) as brand_user_name,
  b.company_code,
  b.company_name,
  b.group_code,
  b.group_name,
  b.trade_type_first_name,
  b.trade_type_second_name,
  b.track_group_id,
  b.track_group_name,
  b.track_industry_name,
  b.track_detail_name,
  CASE
    WHEN (b.active_level = '其他客户') THEN '持续沉睡客户'
    ELSE b.active_level
  END AS active_level,
  CASE
    WHEN (
      (a.module IN ('效果', '薯条', '品合', '内容加热'))
      OR (a.module IS NULL)
    ) THEN b.cpc_direct_sales_dept1_name
    WHEN (a.module IN ('品牌', 'IP')) THEN b.brand_direct_sales_dept1_name
  END AS direct_sales_dept1_name,
  CASE
    WHEN (
      (a.module IN ('效果', '品合', '内容加热'))
      OR (a.module IS NULL)
    ) THEN coalesce(b.cpc_direct_sales_dept2_name,b.cpc_operator_dept2_name,if(b.company_name is null,'创作者商业化部','未挂接'))
    WHEN a.module in ('薯条') then coalesce(b.cpc_direct_sales_dept2_name,b.cpc_operator_dept2_name,'创作者商业化部')
    WHEN (a.module IN ('品牌', 'IP')) THEN b.brand_direct_sales_dept2_name
  END AS direct_sales_dept2_name,
  CASE
    WHEN (
      (a.module IN ('效果', '薯条', '品合', '内容加热'))
      OR (a.module IS NULL)
    ) THEN b.cpc_direct_sales_dept3_name
    WHEN (a.module IN ('品牌', 'IP')) THEN b.brand_direct_sales_dept3_name
  END AS direct_sales_dept3_name,
  CASE
    WHEN (a.module IN ('效果', '薯条', '品合', '内容加热'))
      OR (a.module IS NULL)
   THEN b.cpc_direct_sales_dept4_name
    WHEN  (a.module IN ('品牌', 'IP')) THEN b.brand_direct_sales_dept4_name
  END AS direct_sales_dept4_name,
  CASE
    WHEN (
      (a.module IN ('效果', '薯条', '品合', '内容加热'))
      OR (a.module IS NULL)
    ) THEN b.cpc_direct_sales_dept5_name
    WHEN (a.module IN ('品牌', 'IP')) THEN b.brand_direct_sales_dept5_name
  END AS direct_sales_dept5_name,
  CASE
    WHEN (
      (a.module IN ('效果', '薯条', '品合', '内容加热'))
      OR (a.module IS NULL)
    ) THEN b.cpc_direct_sales_dept6_name
    WHEN (a.module IN ('品牌', 'IP')) THEN b.brand_direct_sales_dept6_name
  END AS direct_sales_dept6_name,
  b.brand_tag_code,
  b.brand_tag_name,
  b.brand_group_tag_code,
  b.brand_group_tag_name,
  b.cpc_direct_sales_code,
  b.cpc_direct_sales_name,
  b.cpc_direct_sales_dept1_code,
  b.cpc_direct_sales_dept1_name,
  b.cpc_direct_sales_dept2_code,
  b.cpc_direct_sales_dept2_name,
  b.cpc_direct_sales_dept3_code,
  b.cpc_direct_sales_dept3_name,
  b.cpc_direct_sales_dept4_code,
  b.cpc_direct_sales_dept4_name,
  b.cpc_direct_sales_dept5_code,
  b.cpc_direct_sales_dept5_name,
  b.cpc_direct_sales_dept6_code,
  b.cpc_direct_sales_dept6_name,
  b.brand_direct_sales_code,
  b.brand_direct_sales_name,
  b.brand_direct_sales_dept1_code,
  b.brand_direct_sales_dept1_name,
  b.brand_direct_sales_dept2_code,
  b.brand_direct_sales_dept2_name,
  b.brand_direct_sales_dept3_code,
  b.brand_direct_sales_dept3_name,
  b.brand_direct_sales_dept4_code,
  b.brand_direct_sales_dept4_name,
  b.brand_direct_sales_dept5_code,
  b.brand_direct_sales_dept5_name,
  b.brand_direct_sales_dept6_code,
  b.brand_direct_sales_dept6_name,
  b.chips_direct_operator_code,
  b.chips_direct_operator_name,
  b.chips_direct_operator_dept1_code,
  b.chips_direct_operator_dept1_name,
  b.chips_direct_operator_dept2_code,
  b.chips_direct_operator_dept2_name,
  b.chips_direct_operator_dept3_code,
  b.chips_direct_operator_dept3_name,
  b.chips_direct_operator_dept4_code,
  b.chips_direct_operator_dept4_name,
  b.chips_direct_operator_dept5_code,
  b.chips_direct_operator_dept5_name,
  b.chips_direct_operator_dept6_code,
  b.chips_direct_operator_dept6_name,
  b.bcoo_direct_sales_code,
  b.bcoo_direct_sales_name,
  b.bcoo_direct_sales_dept1_code,
  b.bcoo_direct_sales_dept1_name,
  b.bcoo_direct_sales_dept2_code,
  b.bcoo_direct_sales_dept2_name,
  b.bcoo_direct_sales_dept3_code,
  b.bcoo_direct_sales_dept3_name,
  b.bcoo_direct_sales_dept4_code,
  b.bcoo_direct_sales_dept4_name,
  b.bcoo_direct_sales_dept5_code,
  b.bcoo_direct_sales_dept5_name,
  b.bcoo_direct_sales_dept6_code,
  b.bcoo_direct_sales_dept6_name,
  a.imp_cnt,
  a.click_cnt,
  a.like_cnt,
  a.fav_cnt,
  a.cmt_cnt,
  a.share_cnt,
  a.follow_cnt,
  a.cash_cost,
  a.cost,
  a.mkt_ecm_cash_cost as mkt_ecm_cost, --注意！！为兼容后端逻辑，现金消耗替换了运营消耗
  a.mkt_leads_cash_cost asmkt_leads_cost,
  a.mkt_zc_cash_cost asmkt_zc_cost,
  a.open_sale_num,
  a.direct_cash_cost,
  a.direct_cost,
  a.channel_cash_cost,
  a.channel_cost,
  a.campaign_cnt,
  a.unit_cnt,
  case when rk = 1 then a.brand_campaign_cnt else 0 end as brand_campaign_cnt,
  a.cpc_cost_budget_rate,
  a.cpc_budget,
  b.first_industry_name,
  b.second_industry_name,
  a.market_target,
  b.cpc_operator_code,
  b.cpc_operator_name,
  b.cpc_operator_dept1_code,
  b.cpc_operator_dept1_name,
  b.cpc_operator_dept2_code,
  b.cpc_operator_dept2_name,
  b.cpc_operator_dept3_code,
  b.cpc_operator_dept3_name,
  b.cpc_operator_dept4_code,
  CASE
    WHEN (b.cpc_operator_dept4_name LIKE '%美妆%') THEN '美妆洗护行业'
    WHEN (b.cpc_operator_dept4_name = '奢品行业部门') THEN '奢品行业'
    ELSE b.cpc_operator_dept4_name
  END AS cpc_operator_dept4_name,
  b.cpc_operator_dept5_code,
  CASE
    WHEN (b.cpc_operator_dept4_name LIKE '%美妆%') THEN b.cpc_operator_dept4_name
    ELSE b.cpc_operator_dept5_name
  END AS cpc_operator_dept5_name,
  b.cpc_operator_dept6_code,
  b.cpc_operator_dept6_name,
  CASE
    WHEN (a.module IN ('效果', '薯条', '品合', '内容加热')) THEN b.cpc_direct_sales_name
    WHEN (a.module IN ('品牌', 'IP')) THEN b.brand_direct_sales_name
  END AS direct_sales_name,
  c.ads_last_cost_days,
  c.bimonthly_last_brand_active_level,
  c.brand_active_level,
  c.ads_last_cost_date,
  substr(b.brand_account_create_time, 1, 10) AS brand_account_create_date,
  a.is_marketing_product,
  b.ads_first_cost_date,
  a.conversion_cnt,
  a.leads_cnt,
  a.msg_num,
  a.msg_open_num,
  a.live_rgmv,
  a.live_dgmv,
  a.click_rgmv_7d,
  a.rtb_cost_income_amt,
  a.rtb_budget_income_amt,
  a.marketing_target,
  a.optimize_target,
  account.seller_id,
  shop_name,
  main_category_name,
  industry as main_industry,
  first_category_name,
  second_category_name,
  third_category_name,
  origin_live_dgmv,
  origin_k_live_dgmv,
  origin_s_live_dgmv,
  
  origin_dgmv,
  ads_live_dgmv,
  ads_sx_dgmv,
  ads_dgmv
FROM
  (
    select
      date_key,
      module,
      product,
      brand_account_id,
      imp_cnt,
      click_cnt,
      like_cnt,
      fav_cnt,
      cmt_cnt,
      share_cnt,
      follow_cnt,
      cash_cost,
      cost,
      mkt_ecm_cost,
      mkt_leads_cost,
      mkt_zc_cost,
      open_sale_num,
      direct_cash_cost,
      direct_cost,
      channel_cash_cost,
      channel_cost,
      campaign_cnt,
      unit_cnt,
      brand_campaign_cnt,
      cpc_cost_budget_rate,
      cpc_budget,
      mkt_ecm_cash_cost,
      mkt_leads_cash_cost,
      mkt_zc_cash_cost,
      mkt_ecm_direct_cost,
      mkt_leads_direct_cost,
      mkt_zc_direct_cost,
      mkt_ecm_direct_cash_cost,
      mkt_leads_direct_cash_cost,
      mkt_zc_direct_cash_cost,
      mkt_ecm_channel_cost,
      mkt_leads_channel_cost,
      mkt_zc_channel_cost,
      mkt_ecm_channel_cash_cost,
      mkt_leads_channel_cash_cost,
      mkt_zc_channel_cash_cost,
      mkt_ecm_unclosed_cost,
      mkt_ecm_unclosed_cash_cost,
      mkt_ecm_unclosed_direct_cost,
      mkt_ecm_unclosed_direct_cash_cost,
      mkt_ecm_unclosed_channel_cost,
      mkt_ecm_unclosed_channel_cash_cost,
      market_target,
      is_marketing_product,
      conversion_cnt,
      leads_cnt,
      msg_num,
      msg_open_num,
      live_rgmv,
      live_dgmv,
      click_rgmv_7d,
      rtb_cost_income_amt,
      rtb_budget_income_amt,
      marketing_target,
      optimize_target,
      origin_live_dgmv,
      origin_k_live_dgmv,
      origin_s_live_dgmv,
      origin_other_live_dgmv,
      origin_dgmv,
      ads_live_dgmv,
      ads_sx_dgmv,
      ads_dgmv,
      row_number() over (partition by brand_account_id,date_key,module order by product) as rk
    from
      base
  ) a
  full outer JOIN (
    SELECT
      brand_account_id,
      dtm,
      case when dtm <= '20230603' and brand_active_level in ('双月新客(潜客激活)','双月沉睡激活老客','双月持续活跃老客','双月沉睡风险客户') then 0 
      when dtm <= '20230603' then null else ads_last_cost_days end as ads_last_cost_days,--历史分区不可用,0603后可用
      bimonthly_last_brand_active_level,
      brand_active_level,
      ads_last_cost_date--历史分区不可用
    FROM
      redcdm.dim_ads_industry_account_df
    where
      dtm >= '20220101'
      and dtm <= '{{ds_nodash}}'
    group by brand_account_id,
      dtm,
      ads_last_cost_days,
      bimonthly_last_brand_active_level,
      brand_active_level,
      ads_last_cost_date
  ) c ON a.brand_account_id = c.brand_account_id
  and a.date_key = f_getdate(c.dtm)
  LEFT JOIN (
    SELECT
      brand_account_id,
      brand_user_name,
      company_code,
      company_name,
      group_code,
      group_name,
      trade_type_first_name,
      trade_type_second_name,
      track_group_id,
      track_group_name,
      active_level,
      cpc_direct_sales_code,
      cpc_direct_sales_name,
      cpc_direct_sales_dept1_code,
      cpc_direct_sales_dept1_name,
      cpc_direct_sales_dept2_code,
      cpc_direct_sales_dept2_name,
      cpc_direct_sales_dept3_code,
      cpc_direct_sales_dept3_name,
      cpc_direct_sales_dept4_code,
      cpc_direct_sales_dept4_name,
      cpc_direct_sales_dept5_code,
      cpc_direct_sales_dept5_name,
      cpc_direct_sales_dept6_code,
      cpc_direct_sales_dept6_name,
      brand_direct_sales_code,
      brand_direct_sales_name,
      brand_direct_sales_dept1_code,
      brand_direct_sales_dept1_name,
      brand_direct_sales_dept2_code,
      brand_direct_sales_dept2_name,
      brand_direct_sales_dept3_code,
      brand_direct_sales_dept3_name,
      brand_direct_sales_dept4_code,
      brand_direct_sales_dept4_name,
      brand_direct_sales_dept5_code,
      brand_direct_sales_dept5_name,
      brand_direct_sales_dept6_code,
      brand_direct_sales_dept6_name,
      chips_direct_operator_code,
      chips_direct_operator_name,
      chips_direct_operator_dept1_code,
      chips_direct_operator_dept1_name,
      chips_direct_operator_dept2_code,
      chips_direct_operator_dept2_name,
      chips_direct_operator_dept3_code,
      chips_direct_operator_dept3_name,
      chips_direct_operator_dept4_code,
      chips_direct_operator_dept4_name,
      chips_direct_operator_dept5_code,
      chips_direct_operator_dept5_name,
      chips_direct_operator_dept6_code,
      chips_direct_operator_dept6_name,
      bcoo_direct_sales_code,
      bcoo_direct_sales_name,
      bcoo_direct_sales_dept1_code,
      bcoo_direct_sales_dept1_name,
      bcoo_direct_sales_dept2_code,
      bcoo_direct_sales_dept2_name,
      bcoo_direct_sales_dept3_code,
      bcoo_direct_sales_dept3_name,
      bcoo_direct_sales_dept4_code,
      bcoo_direct_sales_dept4_name,
      bcoo_direct_sales_dept5_code,
      bcoo_direct_sales_dept5_name,
      bcoo_direct_sales_dept6_code,
      bcoo_direct_sales_dept6_name,
      dtm,
      brand_tag_code,
      brand_tag_name,
      brand_group_tag_code,
      brand_group_tag_name,
      first_industry_name,
      second_industry_name,
      track_detail_name,
      track_industry_name,
      cpc_operator_code,
      cpc_operator_name,
      cpc_operator_dept1_code,
      cpc_operator_dept1_name,
      cpc_operator_dept2_code,
      cpc_operator_dept2_name,
      cpc_operator_dept3_code,
      cpc_operator_dept3_name,
      cpc_operator_dept4_code,
      cpc_operator_dept4_name,
      cpc_operator_dept5_code,
      cpc_operator_dept5_name,
      cpc_operator_dept6_code,
      cpc_operator_dept6_name,
      brand_active_level,
      ads_last_cost_days,
      bimonthly_last_brand_active_level,
      ads_last_cost_date,
      brand_account_create_time,
      ads_first_cost_date
    FROM
      redcdm.dim_ads_industry_account_df
    WHERE
      dtm = '{{ds_nodash}}'
  ) b ON coalesce(a.brand_account_id,c.brand_account_id) = b.brand_account_id
   left join 
   (
    select distinct 
      user_id as seller_user_id,
      seller_id,
      relation_type
    from
      redcdm.dim_pro_soc_user_relation_df
    WHERE
      dtm = '{{ds_nodash}}'
      and seller_id <> 'UNKNOWN'
      and is_valid = 1
      and relation_type in (1)
  ) account on coalesce(a.brand_account_id,c.brand_account_id) = account.seller_user_id
  left join (
    select
      seller_id,
      shopname as shop_name,
      user_id,
      user_name,
      main_category_name,
      industry,
      first_category_name,
      second_category_name,
      third_category_name
    from
      reddw.dw_trd_seller_base_metrics_day
    where
      dtm = '{{ds_nodash}}'
  ) t2 on account.seller_id = t2.seller_id
  ;