drop table if exists temp.temp_app_ads_industry_advertiser_agent_operation_di_{{ds_nodash}};
create table temp.temp_app_ads_industry_advertiser_agent_operation_di_{{ds_nodash}} as 
select id,
  opt_time,
  opt_level,
  opt_level_name,
  opt_type,
  opt_type_name,
  base.virtual_seller_id,
  campaign_id,
  campaign_name,
  creativity_id,
  creativity_name,
  keyword_id,
  keyword,
  placement,
  create_time,
  base.advertiser_id,
  base.advertiser_name,
  
  --coalesce(creativity.optimize_target,campaign.optimize_target) as optimize_target,
  marketing_target,
  base.brand_account_id,
  agent_user_id,
  agent_user_name,
  agent_company_code,
  agent_company_name
  rtb_channel_code,
  rtb_channel_name,
  channel_op_code,
  channel_op_name,
  brand_user_name,
  company_code,
  company_name,
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
  track_industry_name,
  track_detail_name,
  brand_group_tag_code,
  brand_group_tag_name
from 
(select
  id,
  opt_time,
  opt_level,
  opt_level_name,
  opt_type,
  opt_type_name,
  virtual_seller_id,
  campaign_id,
  campaign_name,
  creativity_id,
  creativity_name,
  brand_account_id,
  keyword_id,
  keyword,
  placement,
  create_time,
  advertiser_id,
  advertiser_name,
  optimize_target,
  marketing_target
from
 redcdm.dwd_ads_rtb_optlog_df 
where dtm = '{{ds_nodash}}'
  )base 
  left join 
  --广告主
  (select virtual_seller_id,
  brand_user_id,
  agent_user_id,
  agent_user_name,
  agent_company_code,
  agent_company_name
  from reddw.dw_ads_crm_advertiser_day 
  where dtm = '{{ds_nodash}}'
  )adv 
  on adv.virtual_seller_id=base.virtual_seller_id
  left join 
  (select virtual_seller_id,
  rtb_seller_code,
  rtb_seller_name,
  rtb_operator_code,
  rtb_operator_name,
  rtb_channel_code,
  rtb_channel_name,
  channel_op_code,
  channel_op_name
  from reddw.dw_ads_crm_virtual_seller_relation_day 
  where dtm= '{{ds_nodash}}'
  )relation 
  on relation.virtual_seller_id=base.virtual_seller_id
  left join 
  (select  brand_account_id,
  brand_user_name,
  company_code,
  company_name,
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
  track_industry_name,
  track_detail_name,
  brand_group_tag_code,
  brand_group_tag_name
  from redcdm.dim_ads_industry_account_df
  where dtm= '{{ds_nodash}}'
  )account 
  on account.brand_account_id=base.brand_account_id








  -------------------final---------------------
drop table if exists temp.temp_app_ads_industry_advertiser_agent_operation_di_{{ds_nodash}}_01;
create table temp.temp_app_ads_industry_advertiser_agent_operation_di_{{ds_nodash}}_01 as 
select date_key,
    campaign.v_seller_id as virtual_seller_id,
    t1.campaign_id,
    campaign_name,
    brand_account_id,
    placement,
    optimize_target,
    marketing_target,
    campaign.advertiser_id,
    campaign.advertiser_name,
    sum(rgmv) as rgmv,
    sum(income_amt) as income_amt,
    sum(cash_income_amt) as cash_income_amt
  from 
  (select f_getdate(dtm) as date_key,
    campaign_id,
    sum(rgmv) as rgmv,
    0 as income_amt,
    0 as cash_income_amt
  from redst.st_ads_creativity_mix_metrics_biz_day_inc
  where dtm>='20230101' and dtm<='20230731'and coalesce(rgmv,0)>0
  group by  f_getdate(dtm),
    campaign_id
  union all 
  select f_getdate(dtm) as date_key,
    campaign_id,
    sum(coalesce(rgmv,0)+coalesce(mini_rgmv,0)) as rgmv,
     0 as income_amt,
    0 as cash_income_amt
  from redcdm.dws_ads_cvr_creativity_1d_di 
  where dtm>='20230801' and coalesce(rgmv,0)+coalesce(mini_rgmv,0)>0
  group by f_getdate(dtm),
    campaign_id
  union all 
  select f_getdate(dtm) as date_key,
    campaign_id,
    0 as rgmv,
    sum(cost) as income_amt,
    sum(cash_cost) as cash_income_amt
  from reddw.dw_ads_wide_cpc_creativity_base_day_inc 
  where dtm>='20230101' and dtm<='{{ds_nodash}}'
  group by f_getdate(dtm),
    campaign_id
  )t1
  left join (
    select
      campaign_id,
      campaign_name,
      max(advertiser_id) as advertiser_id,
      --brand_account_id,
      min(v_seller_id) as v_seller_id,
      min(advertiser_name) as advertiser_name,
      max(placement) as placement,
      max(optimize_target) as optimize_target,
      max(marketing_target) as marketing_target
    from
      redcdm.dim_ads_campaign_df
    where
      dtm = '{{ds_nodash}}'
      and campaign_id <> 0 --注意这张表两个id重复campaign_id in (5586281,5534656)
    group by campaign_id,campaign_name
  ) campaign on campaign.campaign_id = t1.campaign_id  
  left join  reddw.dw_ads_cpc_advertiser_new_day  account on campaign.v_seller_id=account.v_seller_id and account.dtm='{{ds_nodash}}'
  group by date_key,
    t1.campaign_id,
    campaign_name,
    campaign.advertiser_id,
    brand_account_id,
    campaign.v_seller_id,
    campaign.advertiser_name,
    placement,
    optimize_target,
    marketing_target
;
insert overwrite table redcdm.dm_ads_industry_campaign_agent_operation_df partition(dtm = '{{ ds_nodash }}')
select date_key,
  base.virtual_seller_id,
  campaign_id,
  campaign_name,
  placement,
  base.advertiser_id,
  base.advertiser_name,
  optimize_target,
  marketing_target,
  base.brand_account_id,
  agent_user_id,
  agent_user_name,
  agent_company_code,
  agent_company_name,
  rtb_channel_code,
  rtb_channel_name,
  channel_op_code,
  channel_op_name,
  brand_user_name,
  company_code,
  company_name,
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
  track_industry_name,
  track_detail_name,
  brand_group_tag_code,
  brand_group_tag_name,
  create_campaign_cnt,
  campaign_date_cnt,
  campaign_budget_cnt,
  campaign_period_cnt,
  bid_cnt,
  keyword_match_cnt,
  advertiser_budget_cnt,
  rgmv,
  income_amt,
  cash_income_amt
from 
(select date_key,
  virtual_seller_id,
  campaign_id,
  campaign_name,
  brand_account_id,
  placement,
  advertiser_id,
  advertiser_name,
  optimize_target,
  marketing_target,
  sum(create_campaign_cnt) as create_campaign_cnt,
  sum(campaign_date_cnt) as campaign_date_cnt,
  sum(campaign_budget_cnt) as campaign_budget_cnt,
  sum(campaign_period_cnt) as campaign_period_cnt,
  sum(bid_cnt) as bid_cnt,
  sum(keyword_match_cnt) as keyword_match_cnt,
  sum(advertiser_budget_cnt) as advertiser_budget_cnt,
  sum(rgmv) as rgmv,
  sum(income_amt) as income_amt,
  sum(cash_income_amt) as cash_income_amt
from 
(select
  opt_date as date_key,
  virtual_seller_id,
  campaign_id,
  campaign_name,
  brand_account_id,
  placement,
  advertiser_id,
  advertiser_name,
  optimize_target,
  marketing_target,
  count(case when opt_type=0 and opt_level=1 then 1 else null end) as create_campaign_cnt,
  count(case when opt_type=6 and opt_level=1 then 1 else null end) as campaign_date_cnt,
  count(case when opt_type=17 and opt_level=1 then 1 else null end) as campaign_budget_cnt,
  count(case when opt_type=20 and opt_level=1 then 1 else null end) as campaign_period_cnt,
  count(case when opt_type=9  then 1 else null end) as bid_cnt,
  count(case when opt_type=10 and opt_level=4 then 1 else null end) as keyword_match_cnt,
  count(case when opt_type=22 and opt_level=0 then 1 else null end) as advertiser_budget_cnt,
  0 as rgmv,
  0 as income_amt,
  0 as cash_income_amt
from
 redcdm.dwd_ads_rtb_optlog_df 
where dtm = '{{ds_nodash}}'
group by opt_date,
  virtual_seller_id,
  campaign_id,
  campaign_name,
  brand_account_id,
  placement,
  advertiser_id,
  advertiser_name,
  optimize_target,
  marketing_target
union all 
select date_key,
  virtual_seller_id,
  campaign_id,
  campaign_name,
  brand_account_id,
  placement,
  advertiser_id,
  advertiser_name,
  optimize_target,
  marketing_target,
  0 as create_campaign_cnt,
  0 as campaign_date_cnt,
  0 as campaign_budget_cnt,
  0 as campaign_period_cnt,
  0 as bid_cnt,
  0 as keyword_match_cnt,
  0 as advertiser_budget_cnt,
  rgmv,
  income_amt,
  cash_income_amt
from temp.temp_app_ads_industry_advertiser_agent_operation_di_{{ds_nodash}}_01
)t1 
group by date_key,
  virtual_seller_id,
  campaign_id,
  campaign_name,
  brand_account_id,
  placement,
  advertiser_id,
  advertiser_name,
  optimize_target,
  marketing_target
  )base 
  left join 
  --广告主
  (select virtual_seller_id,
  brand_user_id,
  agent_user_id,
  agent_user_name,
  agent_company_code,
  agent_company_name
  from reddw.dw_ads_crm_advertiser_day 
  where dtm = '{{ds_nodash}}'
  )adv 
  on adv.virtual_seller_id=base.virtual_seller_id
  left join 
  (select virtual_seller_id,
  rtb_seller_code,
  rtb_seller_name,
  rtb_operator_code,
  rtb_operator_name,
  rtb_channel_code,
  rtb_channel_name,
  channel_op_code,
  channel_op_name
  from reddw.dw_ads_crm_virtual_seller_relation_day 
  where dtm= '{{ds_nodash}}'
  )relation 
  on relation.virtual_seller_id=base.virtual_seller_id
  left join 
  (select  brand_account_id,
  brand_user_name,
  company_code,
  company_name,
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
  track_industry_name,
  track_detail_name,
  brand_group_tag_code,
  brand_group_tag_name
  from redcdm.dim_ads_industry_account_df
  where dtm= '{{ds_nodash}}'
  )account 
  on account.brand_account_id=base.brand_account_id