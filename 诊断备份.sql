-- 关联主键
-- select date_key,
--       brand_account_id,
--       advertiser_id,
--       v_seller_id,
--       module,
--       product,
--       marketing_target,
--       market_target
---------非闭环电商-----------
drop table
  if exists temp.temp_app_ads_insight_advertiser_product_diagnosis_info_df_{{ds_nodash}}_01_ecm_01;

create table
  temp.temp_app_ads_insight_advertiser_product_diagnosis_info_df_{{ds_nodash}}_01_ecm_01 as
select date_key,
  brand_account_id,
  advertiser_id,
  v_seller_id,
  agent_user_id,
  module,
  product,
  marketing_target,
  market_target,
  sum(ecm_unclosed_rgmv) as ecm_unclosed_rgmv
from
  (
    select
      f_getdate(dtm) as date_key,
      cast(creativity_id as bigint) as creativity_id,
      brand_account_id,
      module,
      product,
      marketing_target,
      '非闭环电商' as market_target,
      sum(ecm_unclosed_rgmv) as ecm_unclosed_rgmv
    from
      redcdm.dws_ads_cvr_creativity_user_pos_page_detail_1d_di
    where
      dtm between '20230528' and '{{ds_nodash}}'
      and ecm_unclosed_rgmv > 0
    group by
      1,
      2,
      3,
      4,
      5,
      6
  ) t1
  left join (
    select
      creativity_id,
      advertiser_id,
      v_seller_id,
      agent_user_id,
      agent_name
    from
      redcdm.dim_ads_creativity_core_df
    where
      dtm = '{{ds_nodash}}'
      and coalesce(brand_account_id, '') <> ''
    group by
      1,
      2,
      3,
      4,
      5
  ) t2 on t1.creativity_id = t2.creativity_id
group by date_key,
  brand_account_id,
  advertiser_id,
  v_seller_id,
  agent_user_id,
  module,
  product,
  marketing_target,
  market_target;
---------预算---------
drop table
  if exists temp.temp_app_ads_insight_advertiser_product_diagnosis_info_df_{{ds_nodash}}_01_budget_01;

create table
  temp.temp_app_ads_insight_advertiser_product_diagnosis_info_df_{{ds_nodash}}_01_budget_01 as
select
  f_getdate(a.dtm) as date_key,
  '效果' as module,
  case
    when module = '搜索feed' then '搜索'
    when module = '发现feed' then '信息流'
    when module = '视频内流' then '视频内流'
    else ''
  end as product,
  brand_account_id,
  v_seller_id,
  advertiser_id,
  '整体' as market_target,
  -911 as marketing_target,
  --ads_purpose字段就是marketing_type对应中文描述
  0 as rtb_cost_income_amt,
  0 as rtb_budget_income_amt,
  sum(cost_special_campaign) as cost_income_amt,
  sum(min_budget) as budget_income_amt,
  0 as advertiser_budget,
  0 as advertiser_cost,
  0 as cash_balance,
  0 as total_balance
from
  redapp.app_ads_overall_budget_1d_di a
where
  dtm <= '{{ds_nodash}}'
  and dtm >= '20230101'
  and granularity = '分场域'
  and groups = 3
group by
  1,
  2,
  3,
  4,
  5,
  6,
  7
union all
--20230912增加成本所需字段以及分场域/营销目的预算字段
select
  f_getdate(dtm) as date_key,
  '效果' as module,
  case
    when module = '搜索feed' then '搜索'
    when module = '发现feed' then '信息流'
    when module = '视频内流' then '视频内流'
    else ''
  end as product,
  brand_account_id,
  v_seller_id,
  advertiser_id,
  case
    when ads_type = '闭环电商广告' then '闭环电商'
    when ads_type = '非闭环电商广告' then '非闭环电商'
    when ads_type = '线索广告' then '线索'
    when ads_type = '种草广告' then '种草'
  end as market_target,
  -911 as marketing_target,
  sum(cost_special_campaign) as rtb_cost_income_amt,
  sum(campaign_budget) as rtb_budget_income_amt,
  0 as cost_income_amt,
  0 as budget_income_amt,
  0 as advertiser_budget,
  0 as advertiser_cost,
  0 as cash_balance,
  0 as total_balance
from
  redapp.app_ads_overall_budget_1d_di a
where
  dtm >= '20230320' and  dtm <= '{{ds_nodash}}'
  and granularity = '广告主x场域x营销目标'
group by
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8
union all
--广告主预算和余额
select
  f_getdate(a.dtm) as date_key,
  '效果' as module,
  '整体' as product,
  brand_account_id,
  v_seller_id,
  a.advertiser_id,
  '整体' as market_target,
  -911 as marketing_target,
  sum(cost_special_campaign) as rtb_cost_income_amt,
  sum(campaign_budget) as rtb_budget_income_amt,
  0 as cost_income_amt,
  0 as budget_income_amt,
  sum(min_advertiser_budget) as advertiser_budget,
  sum(cost_advertiser) as advertiser_cost,
  sum(cash_balance) as cash_balance,
  sum(total_balance) as total_balance
from
  redapp.app_ads_overall_budget_1d_di a
  left join (
        -- 广告主余额
        select b.dtm,a.advertiser_id,sum(total_balance) as total_balance
        from reddw.dw_ads_cpc_advertiser_day a
        left join (
            select dtm,account_no,cash_balance,total_balance
            from redods.ods_gondar_ad_account_1
            where dtm >= f_getdate('{{ds_nodash}}',-124) and dtm<='{{ds_nodash}}' --看最近4个月
            -- and to_date(from_unixtime(floor(updated_time/1000+28800))) = f_getdate('{{ds}}',1)
            ) as b on a.account_id = b.account_no
        where a.dtm = '{{ds_nodash}}'
        group by 1,2
    ) c on a.advertiser_id = c.advertiser_id and a.dtm=c.dtm
where 
  a.dtm >= '20230101'  and a.dtm <= '{{ds_nodash}}'
  and granularity = '广告主粒度'
group by
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8
;
--------------base_metric-------------
drop table
  if exists temp.temp_app_ads_insight_advertiser_product_diagnosis_info_df_{{ds_nodash}}_01;

create table
  temp.temp_app_ads_insight_advertiser_product_diagnosis_info_df_{{ds_nodash}}_01 as
SELECT
  date_key,
  substring(date_key, 1, 7) as stat_month,
  if(
    month(date_key) & 1 = 1,
    trunc(date_key, 'MM'),
    trunc(add_months(date_key, -1), 'MM')
  ) as stat_bimonthly_month,
  add_months(date_key, 12) as next_date_key,
  if(
    month(add_months(date_key, 12)) & 1 = 1,
    trunc(add_months(date_key, 12), 'MM'),
    trunc(add_months(add_months(date_key, 12), -1), 'MM')
  ) as next_stat_bimonthly_month,
  module,
  product,
  advertiser_id,
  advertiser_name,
  v_seller_id,
  v_seller_name,
  agent_user_id,
  agent_name,
  market_target,
  case when marketing_target='整体' then -911 else marketing_target end as marketing_target,
  
  brand_account_id,
  brand_account_name,
  company_code,
  company_name,
  track_group_name,
  track_industry_name,
  track_detail_name,
  direct_sales_dept1_name,
  direct_sales_dept2_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  planner_name,
  channel_sales_name,
  channel_sales_code,
  channel_operator_name,
  brand_group_tag_code,
  brand_group_tag_name,
  first_industry_name,
  second_industry_name,
  case
    when track_detail_name = '其他'
    and direct_sales_dept4_name = '美妆洗护行业' then '美护'
    when track_detail_name = '其他'
    and direct_sales_dept4_name = '奢品行业' then '奢品'
    when track_detail_name = '其他'
    and direct_sales_dept4_name = '服饰潮流行业' then '服饰潮流'
    when track_detail_name = '其他' then '暂无赛道行业'
    else track_industry_name
  end as process_track_industry_name,
  --赛道行业处理后
  max(cpc_direct_sales_name) as direct_sales_name,
  max(cpc_operator_name) as cpc_operator_name,
  sum(
    case when module in ('品牌') then perf_brand_cash_income_amt else cash_income_amt end
    
  ) as cash_income_amt, --20240510品牌收入口径切换为业绩口径
  sum(
    case when module in ('品牌') then perf_brand_income_amt else income_amt end
    
  ) as income_amt, --20240510品牌收入口径切换为业绩口径
  sum(
    case
      when module in ('效果', '薯条','口碑通') then cash_income_amt
      when module in ('品牌') then perf_brand_cash_income_amt
      else 0
    end
  ) as ads_cash_income_amt,
  sum(
    case
      when module in ('效果') then cash_income_amt
      else 0
    end
  ) as rtb_cash_income_amt,
  sum(
    case
      when module in ('品牌') then perf_brand_cash_income_amt
      else 0
    end
  ) as brand_cash_income_amt,
  sum(
    case
      when module in ('薯条') then cash_income_amt
      else 0
    end
  ) as chips_cash_income_amt,
  sum(
    case
      when module in ('品合') then cash_income_amt
      else 0
    end
  ) as bcoo_cash_income_amt,
  sum(imp_cnt) as imp_cnt,
  sum(click_cnt) as click_cnt,
  
  sum(like_cnt) as like_cnt,
  sum(fav_cnt) as fav_cnt,
  sum(cmt_cnt) as cmt_cnt,
  sum(share_cnt) as share_cnt,
  sum(follow_cnt) as follow_cnt,
  sum(screenshot_cnt) as screenshot_cnt ,
  sum(coalesce(like_cnt,0)+coalesce(fav_cnt,0)+coalesce(cmt_cnt,0)+coalesce(share_cnt,0)+coalesce(follow_cnt,0)) as engage_cnt,
  sum(click_rgmv_7d) as click_rgmv_7d,
  sum(conversion_cnt) as conversion_cnt
FROM
  redcdm.dm_ads_industry_product_advertiser_td_df
WHERE
  dtm = '{{ds_nodash}}'
  and date_key >= '2022-01-01'
group by
  date_key,
  substring(date_key, 1, 7),
  add_months(date_key, 12),
  if(
    month(date_key) & 1 = 1,
    trunc(date_key, 'MM'),
    trunc(add_months(date_key, -1), 'MM')
  ),
  module,
  product,
  advertiser_id,
  advertiser_name,
  v_seller_id,
  v_seller_name,
  agent_user_id,
  agent_name,
  market_target,
  case when marketing_target='整体' then -911 else marketing_target end,
  brand_account_id,
  brand_account_name,
  company_code,
  company_name,
  track_group_name,
  track_industry_name,
  track_detail_name,
  direct_sales_dept1_name,
  direct_sales_dept2_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  planner_name,
  channel_sales_name,
  channel_sales_code,
  channel_operator_name,
  brand_group_tag_code,
  brand_group_tag_name,
  first_industry_name,
  second_industry_name,
  case
    when track_detail_name = '其他'
    and direct_sales_dept4_name = '美妆洗护行业' then '美护'
    when track_detail_name = '其他'
    and direct_sales_dept4_name = '奢品行业' then '奢品'
    when track_detail_name = '其他'
    and direct_sales_dept4_name = '服饰潮流行业' then '服饰潮流'
    when track_detail_name = '其他' then '暂无赛道行业'
    else track_industry_name
  end;

------当日指标-----------
drop table
  if exists temp.temp_app_ads_insight_advertiser_product_diagnosis_info_df_{{ds_nodash}}_01_all_01;

create table
  temp.temp_app_ads_insight_advertiser_product_diagnosis_info_df_{{ds_nodash}}_01_all_01 as
select date_key,
  brand_account_id,
  advertiser_id,
  v_seller_id,
  module,
  product,
  marketing_target,
  market_target,
  cash_income_amt,
  income_amt,
  ads_cash_income_amt,
  rtb_cash_income_amt,
  brand_cash_income_amt,
  chips_cash_income_amt,
  bcoo_cash_income_amt,
  imp_cnt,
  click_cnt,
  like_cnt,
  fav_cnt,
  cmt_cnt,
  share_cnt,
  follow_cnt,
  screenshot_cnt,
  engage_cnt,
  click_rgmv_7d,
  conversion_cnt,
  ecm_unclosed_rgmv,
  rtb_cost_income_amt,
  rtb_budget_income_amt,
  cost_income_amt,
  budget_income_amt,
  advertiser_budget,
  advertiser_cost,
  cash_balance,
  total_balance,
  case when date_key = '{{ds}}'  then 1 else 0 end as is_day,
  case when date_key = f_getdate('{{ds}}', -1)  then 1 else 0 end as is_ystd,
  case when substring(date_key,1,4)=substring('{{ds}}',1,4) and date_key<='{{ds}}' then 1 else 0 end as is_year,
  case when substring(date_key,1,4)=substring(add_months('{{ds}}',-12),1,4) and date_key<=add_months('{{ds}}', -12) then 1 else 0 end as is_last_year,
  case when date_key<='{{ ds }}' and date_key>=if(month('{{ds}}') & 1 = 1,trunc('{{ds}}','MM'),trunc(add_months('{{ds}}',-1),'MM')) then 1 else 0 end as is_bimonth,
  case when date_key<=add_months('{{ds}}',-12) and date_key>=if(month(add_months('{{ds}}',-12)) & 1 = 1,trunc(add_months('{{ds}}',-12),'MM'),trunc(add_months(add_months('{{ds}}',-12),-1),'MM')) then 1 else 0 end as is_last_bimonth,
  case when  date_key<=add_months('{{ds}}',-2) and date_key>=if(month(add_months('{{ds}}',-2)) & 1 = 1,trunc(add_months('{{ds}}',-2),'MM'),trunc(add_months(add_months('{{ds}}',-2),-1),'MM')) then 1 else 0 end as is_before_bimonth,
  case when date_key<add_months(if(month('{{ds}}') & 1 = 1,trunc('{{ds}}','MM'),trunc(add_months('{{ds}}',-1),'MM')) ,-12) and date_key>=add_months(if(month(add_months('{{ds}}',-2)) & 1 = 1,trunc(add_months('{{ds}}',-2),'MM'),trunc(add_months(add_months('{{ds}}',-2),-1),'MM')),-12)  then 1 else 0 end as is_before_last_all_bimonth,
  case when  date_key< if(month('{{ds}}') & 1 = 1,trunc('{{ds}}','MM'),trunc(add_months('{{ds}}',-1),'MM')) and date_key>=if(month(add_months('{{ds}}',-2)) & 1 = 1,trunc(add_months('{{ds}}',-2),'MM'),trunc(add_months(add_months('{{ds}}',-2),-1),'MM')) then 1 else 0 end as is_before_all_bimonth
from
(select  coalesce(t1.date_key, t3.date_key) as date_key,
  coalesce(t1.brand_account_id, t3.brand_account_id) as brand_account_id,
  coalesce(t1.advertiser_id, t3.advertiser_id) as advertiser_id,
  coalesce(t1.v_seller_id, t3.v_seller_id) as v_seller_id,
  --coalesce(t1.agent_user_id, t3.agent_user_id) as agent_user_id,
  coalesce(t1.module, t3.module) as module,
  coalesce(t1.product, t3.product) as product,
  coalesce(t1.marketing_target, t3.marketing_target) as marketing_target,
  coalesce(t1.market_target, t3.market_target) as market_target,
  cash_income_amt,
  income_amt,
  ads_cash_income_amt,
  rtb_cash_income_amt,
  brand_cash_income_amt,
  chips_cash_income_amt,
  bcoo_cash_income_amt,
  imp_cnt,
  click_cnt,
  like_cnt,
  fav_cnt,
  cmt_cnt,
  share_cnt,
  follow_cnt,
  screenshot_cnt,
  engage_cnt,
  click_rgmv_7d,
  conversion_cnt,
  ecm_unclosed_rgmv,
  rtb_cost_income_amt,
  rtb_budget_income_amt,
  cost_income_amt,
  budget_income_amt,
  advertiser_budget,
  advertiser_cost,
  cash_balance,
  total_balance
from
  (
    select coalesce(t1.date_key, t2.date_key) as date_key,
      coalesce(t1.brand_account_id, t2.brand_account_id) as brand_account_id,
      coalesce(t1.advertiser_id, t2.advertiser_id) as advertiser_id,
      coalesce(t1.v_seller_id, t2.v_seller_id) as v_seller_id,
      --coalesce(t1.agent_user_id, t2.agent_user_id) as agent_user_id,
      coalesce(t1.module, t2.module) as module,
      coalesce(t1.product, t2.product) as product,
      coalesce(t1.marketing_target, t2.marketing_target) as marketing_target,
      coalesce(t1.market_target, t2.market_target) as market_target,
      cash_income_amt,
      income_amt,
      ads_cash_income_amt,
      rtb_cash_income_amt,
      brand_cash_income_amt,
      chips_cash_income_amt,
      bcoo_cash_income_amt,
      imp_cnt,
      click_cnt,
      like_cnt,
      fav_cnt,
      cmt_cnt,
      share_cnt,
      follow_cnt,
      screenshot_cnt,
      engage_cnt,
      click_rgmv_7d,
      conversion_cnt,
      ecm_unclosed_rgmv
    from
      temp.temp_app_ads_insight_advertiser_product_diagnosis_info_df_{{ds_nodash}}_01 t1
      full outer join temp.temp_app_ads_insight_advertiser_product_diagnosis_info_df_{{ds_nodash}}_01_ecm_01 t2 on t1.date_key = t2.date_key
      and t1.brand_account_id = t2.brand_account_id
      and t1.advertiser_id = t2.advertiser_id
      and t1.v_seller_id = t2.v_seller_id
      --and t1.agent_user_id = t2.agent_user_id
      and t1.module = t2.module
      and t1.product = t2.product
      and t1.marketing_target = t2.marketing_target
      and t1.market_target = t2.market_target
  ) t1
  full outer join temp.temp_app_ads_insight_advertiser_product_diagnosis_info_df_{{ds_nodash}}_01_budget_01 t3 on t1.date_key = t3.date_key
  and t1.brand_account_id = t3.brand_account_id
  and t1.advertiser_id = t3.advertiser_id
  and t1.v_seller_id = t3.v_seller_id
  --and t1.agent_user_id = t3.agent_user_id
  and t1.module = t3.module
  and t1.product = t3.product
  and t1.marketing_target = t3.marketing_target
  and t1.market_target = t3.market_target
  )
  ;
drop table
  if exists temp.temp_app_ads_insight_advertiser_product_diagnosis_info_df_{{ds_nodash}}_01_detail_01;

create table
  temp.temp_app_ads_insight_advertiser_product_diagnosis_info_df_{{ds_nodash}}_01_detail_01 as
select brand_account_id_1 as brand_account_id,
  advertiser_id_1 as advertiser_id,
  v_seller_id_1 as v_seller_id,
  virtual_seller_name,
  agent_user_id,
  agent_user_name,
  module_1 as module,
  product_1 as product,
  marketing_target_1 as marketing_target,
  market_target_1 as market_target,
  brand_user_name,
  company_code,
  company_name,
  track_group_name,
  
  CASE
    WHEN (
      (t1.module IN ('效果', '薯条', '品合', '内容加热','口碑通'))
      OR (t1.module IS NULL)
    ) THEN  t3.cpc_direct_sales_name
    WHEN (t1.module IN ('品牌', 'IP')) THEN t3.brand_direct_sales_name
  END AS direct_sales_name,
  CASE
    WHEN (
      (t1.module IN ('效果', '薯条', '品合', '内容加热','口碑通'))
      OR (t1.module IS NULL)
    ) THEN t3.cpc_direct_sales_dept1_name
    WHEN (t1.module IN ('品牌', 'IP')) THEN t3.brand_direct_sales_dept1_name
  END AS direct_sales_dept1_name,
  CASE
    WHEN (
      (t1.module IN ('效果', '品合', '内容加热','口碑通'))
      OR (t1.module IS NULL)
    ) THEN coalesce(t3.cpc_direct_sales_dept2_name,t3.cpc_operator_dept2_name,if(t3.company_name is null,'创作者商业化部','未挂接'))
    WHEN t1.module in ('薯条') then coalesce(t3.cpc_direct_sales_dept2_name,t3.cpc_operator_dept2_name,'创作者商业化部')
    WHEN (t1.module IN ('品牌', 'IP')) THEN t3.brand_direct_sales_dept2_name
  END AS direct_sales_dept2_name,
  CASE
    WHEN (
      (t1.module IN ('效果', '薯条', '品合', '内容加热','口碑通'))
      OR (t1.module IS NULL)
    ) THEN t3.cpc_direct_sales_dept3_name
    WHEN (t1.module IN ('品牌', 'IP')) THEN t3.brand_direct_sales_dept3_name
  END AS direct_sales_dept3_name,
  CASE
    WHEN (t1.module IN ('效果', '薯条', '品合', '内容加热','口碑通'))
      OR (t1.module IS NULL)
   THEN t3.cpc_direct_sales_dept4_name
    WHEN  (t1.module IN ('品牌', 'IP')) THEN t3.brand_direct_sales_dept4_name
  END AS direct_sales_dept4_name,
  CASE
    WHEN (
      (t1.module IN ('效果', '薯条', '品合', '内容加热','口碑通'))
      OR (t1.module IS NULL)
    ) THEN t3.cpc_direct_sales_dept5_name
    WHEN (t1.module IN ('品牌', 'IP')) THEN t3.brand_direct_sales_dept5_name
  END AS direct_sales_dept5_name,
  CASE
    WHEN (
      (t1.module IN ('效果', '薯条', '品合', '内容加热','口碑通'))
      OR (t1.module IS NULL)
    ) THEN t3.cpc_direct_sales_dept6_name
    WHEN (t1.module IN ('品牌', 'IP')) THEN t3.brand_direct_sales_dept6_name
  END AS direct_sales_dept6_name,
  brand_group_tag_code,
  brand_group_tag_name,
  cpc_operator_code,
  cpc_operator_name,
  first_industry_name,
  second_industry_name,
  track_industry_name,
  track_detail_name,
  brand_id,
  brand_name,
  planner_name,
----redbi使用赛道逻辑
case when track_detail_name='其他' and if(module='品牌',brand_direct_sales_dept4_name,cpc_direct_sales_dept4_name)='美妆洗护行业' then '美护' 
when track_detail_name='其他'  and if(module='品牌',brand_direct_sales_dept4_name,cpc_direct_sales_dept4_name)='奢品行业' then '奢品' 
when track_detail_name='其他'  and if(module='品牌',brand_direct_sales_dept4_name,cpc_direct_sales_dept4_name)='服饰潮流行业' then '服饰潮流' 
when track_detail_name='其他' then '暂无赛道行业' else track_industry_name end as process_track_industry_name,
case when track_detail_name='其他' then '暂无一级赛道' else track_group_name end as process_track_group_name,
case when track_detail_name='其他' then '暂无二级赛道' ELSE split(track_group_name,'-')[2] end as process_track_second_name,
case when track_detail_name='其他' then '暂无三级赛道' ELSE split(track_group_name,'-')[3] end as process_track_third_name,
 
channel_sales_name,
  channel_sales_code,
  channel_operator_name,
  ecm_closed_dgmv_2m,
  cps_note_num_2m,
  s_live_dgmv_2m,
  is_sx_ti,
  taolian_cash_income_amt_2m,
  bcoo_cash_income_amt_2m,
  track_taolian_cash_income_amt_2m,
  track_bcoo_cash_income_amt_2m,
  --ecm_closed_dgmv_2m,
  ecm_closed_dgmv_cash_income_amt_2m,
  track_ecm_closed_dgmv_2m,
  track_ecm_closed_dgmv_cash_income_amt_2m,

    cash_income_amt,
  income_amt,
  imp_cnt,
  click_cnt,
  like_cnt,
  fav_cnt,
  cmt_cnt,
  share_cnt,
  follow_cnt,
  screenshot_cnt,
  engage_cnt,
  click_rgmv_7d,
  conversion_cnt,
  ecm_unclosed_rgmv,
  rtb_cost_income_amt,
  rtb_budget_income_amt,
  cost_income_amt,
  budget_income_amt,
  advertiser_budget,
  advertiser_cost,
  cash_balance,
  total_balance,
  --cash_balance_days,redbi配置
  -------------前一日--------------
  ystd_cash_income_amt_1d,
  ystd_income_amt_1d,
  ystd_imp_cnt_1d,
  ystd_click_cnt_1d,
  ystd_like_cnt_1d,
  ystd_fav_cnt_1d,
  ystd_cmt_cnt_1d,
  ystd_share_cnt_1d,
  ystd_follow_cnt_1d,
  ystd_screenshot_cnt_1d,
  ystd_engage_cnt_1d,
  ystd_click_rgmv_7d_1d,
  ystd_conversion_cnt_1d,
  ystd_ecm_unclosed_rgmv_1d,
  ystd_rtb_cost_income_amt_1d,
  ystd_rtb_budget_income_amt_1d,
  ystd_cost_income_amt_1d,
  ystd_budget_income_amt_1d,
  ystd_advertiser_budget_1d,
  ystd_advertiser_cost_1d,
  ystd_cash_balance_1d,
  ystd_total_balance_1d,
--------------------本双月----------------
  cash_income_amt_2m,
  income_amt_2m,
  imp_cnt_2m,
  click_cnt_2m,
  like_cnt_2m,
  fav_cnt_2m,
  cmt_cnt_2m,
  share_cnt_2m,
  follow_cnt_2m,
  screenshot_cnt_2m,
  engage_cnt_2m,
  click_rgmv_7d_2m,
  conversion_cnt_2m,
  ecm_unclosed_rgmv_2m,
  rtb_cost_income_amt_2m,
  rtb_budget_income_amt_2m,
  cost_income_amt_2m,
  budget_income_amt_2m,
  advertiser_budget_2m,
  advertiser_cost_2m,
  cash_balance_2m,
  total_balance_2m,
    ------------------去年双月-----------------
   cash_income_amt_last_2m,
  income_amt_last_2m,
  imp_cnt_last_2m,
  click_cnt_last_2m,
  like_cnt_last_2m,
  fav_cnt_last_2m,
  cmt_cnt_last_2m,
  share_cnt_last_2m,
  follow_cnt_last_2m,
  screenshot_cnt_last_2m,
  engage_cnt_last_2m,
  click_rgmv_7d_last_2m,
  conversion_cnt_last_2m,
  ecm_unclosed_rgmv_last_2m,
  rtb_cost_income_amt_last_2m,
  rtb_budget_income_amt_last_2m,
  cost_income_amt_last_2m,
  budget_income_amt_last_2m,
  advertiser_budget_last_2m,
  advertiser_cost_last_2m,
  cash_balance_last_2m,
  total_balance_last_2m,
    -------------上双月--------------------
    cash_income_amt_before_2m,
  income_amt_before_2m,
  imp_cnt_before_2m,
  click_cnt_before_2m,
  like_cnt_before_2m,
  fav_cnt_before_2m,
  cmt_cnt_before_2m,
  share_cnt_before_2m,
  follow_cnt_before_2m,
  screenshot_cnt_before_2m,
  engage_cnt_before_2m,
  click_rgmv_7d_before_2m,
  conversion_cnt_before_2m,
  ecm_unclosed_rgmv_before_2m,
  rtb_cost_income_amt_before_2m,
  rtb_budget_income_amt_before_2m,
  cost_income_amt_before_2m,
  budget_income_amt_before_2m,
  advertiser_budget_before_2m,
  advertiser_cost_before_2m,
  cash_balance_before_2m,
  total_balance_before_2m,
  cash_income_amt_1y,
  cash_income_amt_before_1y,
  rtb_all_cash_income_amt_before_2m,
  rtb_alll_cash_income_amt_before_last_2m
from 
(select t1.*,
  coalesce(t1.brand_account_id, bimonth.brand_account_id) as brand_account_id_1,
  coalesce(t1.advertiser_id, bimonth.advertiser_id) as advertiser_id_1,
  coalesce(t1.v_seller_id, bimonth.v_seller_id) as v_seller_id_1,
  --coalesce(t1.agent_user_id, t2.agent_user_id) as agent_user_id,
  coalesce(t1.module, bimonth.module) as module_1,
  coalesce(t1.product, bimonth.product) as product_1,
  coalesce(t1.marketing_target, bimonth.marketing_target) as marketing_target_1,
  coalesce(t1.market_target, bimonth.market_target) as market_target_1,
  ecm_closed_dgmv_2m,
  cps_note_num_2m,
  s_live_dgmv_2m,
  is_sx_ti,
  taolian_cash_income_amt_2m,
  bcoo_cash_income_amt_2m,
  track_taolian_cash_income_amt_2m,
  track_bcoo_cash_income_amt_2m,
  ecm_closed_dgmv_2m,
  ecm_closed_dgmv_cash_income_amt_2m,
  track_ecm_closed_dgmv_2m,
  track_ecm_closed_dgmv_cash_income_amt_2m
from 
(
select 
  brand_account_id,
  advertiser_id,
  v_seller_id,
  module,
  product,
  marketing_target,
  market_target,
  sum(case when date_key = '{{ds}}' then cash_income_amt else 0 end) as cash_income_amt,
  sum(case when date_key = '{{ds}}' then income_amt else 0 end) as income_amt,
  sum(case when date_key = '{{ds}}' then imp_cnt else 0 end) as imp_cnt,
  sum(case when date_key = '{{ds}}' then click_cnt else 0 end) as click_cnt,
  sum(case when date_key = '{{ds}}' then like_cnt else 0 end) as like_cnt,
  sum(case when date_key = '{{ds}}' then fav_cnt else 0 end) as fav_cnt,
  sum(case when date_key = '{{ds}}' then cmt_cnt else 0 end) as cmt_cnt,
  sum(case when date_key = '{{ds}}' then share_cnt else 0 end) as share_cnt,
  sum(case when date_key = '{{ds}}' then follow_cnt else 0 end) as follow_cnt,
  sum(case when date_key = '{{ds}}' then screenshot_cnt else 0 end) as screenshot_cnt,
  sum(case when date_key = '{{ds}}' then engage_cnt else 0 end) as engage_cnt,
  sum(case when date_key = '{{ds}}' then click_rgmv_7d else 0 end) as click_rgmv_7d,
  sum(case when date_key = '{{ds}}' then conversion_cnt else 0 end) as conversion_cnt,
  sum(case when date_key = '{{ds}}' then ecm_unclosed_rgmv else 0 end) as ecm_unclosed_rgmv,
  sum(case when date_key = '{{ds}}' then rtb_cost_income_amt else 0 end) as rtb_cost_income_amt,
  sum(case when date_key = '{{ds}}' then rtb_budget_income_amt else 0 end) as rtb_budget_income_amt,
  sum(case when date_key = '{{ds}}' then cost_income_amt else 0 end) as cost_income_amt,
  sum(case when date_key = '{{ds}}' then budget_income_amt else 0 end) as budget_income_amt,
  sum(case when date_key = '{{ds}}' then advertiser_budget else 0 end) as advertiser_budget,
  sum(case when date_key = '{{ds}}' then advertiser_cost else 0 end) as advertiser_cost,
  sum(case when date_key = '{{ds}}' then cash_balance else 0 end) as cash_balance,
  sum(case when date_key = '{{ds}}' then total_balance else 0 end) as total_balance,
  -------------前一日--------------
  sum(case when date_key = f_getdate('{{ds}}', -1) then cash_income_amt else 0 end) as ystd_cash_income_amt_1d,
  sum(case when date_key = f_getdate('{{ds}}', -1) then income_amt else 0 end) as ystd_income_amt_1d,
  sum(case when date_key = f_getdate('{{ds}}', -1) then imp_cnt else 0 end) as ystd_imp_cnt_1d,
  sum(case when date_key = f_getdate('{{ds}}', -1) then click_cnt else 0 end) as ystd_click_cnt_1d,
  sum(case when date_key = f_getdate('{{ds}}', -1) then like_cnt else 0 end) as ystd_like_cnt_1d,
  sum(case when date_key = f_getdate('{{ds}}', -1) then fav_cnt else 0 end) as ystd_fav_cnt_1d,
  sum(case when date_key = f_getdate('{{ds}}', -1) then cmt_cnt else 0 end) as ystd_cmt_cnt_1d,
  sum(case when date_key = f_getdate('{{ds}}', -1) then share_cnt else 0 end) as ystd_share_cnt_1d,
  sum(case when date_key = f_getdate('{{ds}}', -1) then follow_cnt else 0 end) as ystd_follow_cnt_1d,
  sum(case when date_key = f_getdate('{{ds}}', -1) then screenshot_cnt else 0 end) as ystd_screenshot_cnt_1d,
  sum(case when date_key = f_getdate('{{ds}}', -1) then engage_cnt else 0 end) as ystd_engage_cnt_1d,
  sum(case when date_key = f_getdate('{{ds}}', -1) then click_rgmv_7d else 0 end) as ystd_click_rgmv_7d_1d,
  sum(case when date_key = f_getdate('{{ds}}', -1) then conversion_cnt else 0 end) as ystd_conversion_cnt_1d,
  sum(case when date_key = f_getdate('{{ds}}', -1) then ecm_unclosed_rgmv else 0 end) as ystd_ecm_unclosed_rgmv_1d,
  sum(case when date_key = f_getdate('{{ds}}', -1) then rtb_cost_income_amt else 0 end) as ystd_rtb_cost_income_amt_1d,
  sum(case when date_key = f_getdate('{{ds}}', -1) then rtb_budget_income_amt else 0 end) as ystd_rtb_budget_income_amt_1d,
  sum(case when date_key = f_getdate('{{ds}}', -1) then cost_income_amt else 0 end) as ystd_cost_income_amt_1d,
  sum(case when date_key = f_getdate('{{ds}}', -1) then budget_income_amt else 0 end) as ystd_budget_income_amt_1d,
  sum(case when date_key = f_getdate('{{ds}}', -1) then advertiser_budget else 0 end) as ystd_advertiser_budget_1d,
  sum(case when date_key = f_getdate('{{ds}}', -1) then advertiser_cost else 0 end) as ystd_advertiser_cost_1d,
  sum(case when date_key = f_getdate('{{ds}}', -1) then cash_balance else 0 end) as ystd_cash_balance_1d,
  sum(case when date_key = f_getdate('{{ds}}', -1) then total_balance else 0 end) as ystd_total_balance_1d,
--------------------本双月----------------
sum(case when is_bimonth=1 then cash_income_amt else 0 end) as cash_income_amt_2m,
  sum(case when is_bimonth=1 then income_amt else 0 end) as income_amt_2m,
  sum(case when is_bimonth=1 then imp_cnt else 0 end) as imp_cnt_2m,
  sum(case when is_bimonth=1 then click_cnt else 0 end) as click_cnt_2m,
  sum(case when is_bimonth=1 then like_cnt else 0 end) as like_cnt_2m,
  sum(case when is_bimonth=1 then fav_cnt else 0 end) as fav_cnt_2m,
  sum(case when is_bimonth=1 then cmt_cnt else 0 end) as cmt_cnt_2m,
  sum(case when is_bimonth=1 then share_cnt else 0 end) as share_cnt_2m,
  sum(case when is_bimonth=1 then follow_cnt else 0 end) as follow_cnt_2m,
  sum(case when is_bimonth=1 then screenshot_cnt else 0 end) as screenshot_cnt_2m,
  sum(case when is_bimonth=1 then engage_cnt else 0 end) as engage_cnt_2m,
  sum(case when is_bimonth=1 then click_rgmv_7d else 0 end) as click_rgmv_7d_2m,
  sum(case when is_bimonth=1 then conversion_cnt else 0 end) as conversion_cnt_2m,
  sum(case when is_bimonth=1 then ecm_unclosed_rgmv else 0 end) as ecm_unclosed_rgmv_2m,
  sum(case when is_bimonth=1 then rtb_cost_income_amt else 0 end) as rtb_cost_income_amt_2m,
  sum(case when is_bimonth=1 then rtb_budget_income_amt else 0 end) as rtb_budget_income_amt_2m,
  sum(case when is_bimonth=1 then cost_income_amt else 0 end) as cost_income_amt_2m,
  sum(case when is_bimonth=1 then budget_income_amt else 0 end) as budget_income_amt_2m,
  sum(case when is_bimonth=1 then advertiser_budget else 0 end) as advertiser_budget_2m,
  sum(case when is_bimonth=1 then advertiser_cost else 0 end) as advertiser_cost_2m,
  sum(case when is_bimonth=1 then cash_balance else 0 end) as cash_balance_2m,
  sum(case when is_bimonth=1 then total_balance else 0 end) as total_balance_2m,
  ------------------去年双月-----------------
  sum(case when is_last_bimonth=1 then cash_income_amt else 0 end) as cash_income_amt_last_2m,
  sum(case when is_last_bimonth=1 then income_amt else 0 end) as income_amt_last_2m,
  sum(case when is_last_bimonth=1 then imp_cnt else 0 end) as imp_cnt_last_2m,
  sum(case when is_last_bimonth=1 then click_cnt else 0 end) as click_cnt_last_2m,
  sum(case when is_last_bimonth=1 then like_cnt else 0 end) as like_cnt_last_2m,
  sum(case when is_last_bimonth=1 then fav_cnt else 0 end) as fav_cnt_last_2m,
  sum(case when is_last_bimonth=1 then cmt_cnt else 0 end) as cmt_cnt_last_2m,
  sum(case when is_last_bimonth=1 then share_cnt else 0 end) as share_cnt_last_2m,
  sum(case when is_last_bimonth=1 then follow_cnt else 0 end) as follow_cnt_last_2m,
  sum(case when is_last_bimonth=1 then screenshot_cnt else 0 end) as screenshot_cnt_last_2m,
  sum(case when is_last_bimonth=1 then engage_cnt else 0 end) as engage_cnt_last_2m,
  sum(case when is_last_bimonth=1 then click_rgmv_7d else 0 end) as click_rgmv_7d_last_2m,
  sum(case when is_last_bimonth=1 then conversion_cnt else 0 end) as conversion_cnt_last_2m,
  sum(case when is_last_bimonth=1 then ecm_unclosed_rgmv else 0 end) as ecm_unclosed_rgmv_last_2m,
  sum(case when is_last_bimonth=1 then rtb_cost_income_amt else 0 end) as rtb_cost_income_amt_last_2m,
  sum(case when is_last_bimonth=1 then rtb_budget_income_amt else 0 end) as rtb_budget_income_amt_last_2m,
  sum(case when is_last_bimonth=1 then cost_income_amt else 0 end) as cost_income_amt_last_2m,
  sum(case when is_last_bimonth=1 then budget_income_amt else 0 end) as budget_income_amt_last_2m,
  sum(case when is_last_bimonth=1 then advertiser_budget else 0 end) as advertiser_budget_last_2m,
  sum(case when is_last_bimonth=1 then advertiser_cost else 0 end) as advertiser_cost_last_2m,
  sum(case when is_last_bimonth=1 then cash_balance else 0 end) as cash_balance_last_2m,
  sum(case when is_last_bimonth=1 then total_balance else 0 end) as total_balance_last_2m,
  -------------上双月--------------------
  sum(case when is_before_bimonth=1 then cash_income_amt else 0 end) as cash_income_amt_before_2m,
  sum(case when is_before_bimonth=1 then income_amt else 0 end) as income_amt_before_2m,
  sum(case when is_before_bimonth=1 then imp_cnt else 0 end) as imp_cnt_before_2m,
  sum(case when is_before_bimonth=1 then click_cnt else 0 end) as click_cnt_before_2m,
  sum(case when is_before_bimonth=1 then like_cnt else 0 end) as like_cnt_before_2m,
  sum(case when is_before_bimonth=1 then fav_cnt else 0 end) as fav_cnt_before_2m,
  sum(case when is_before_bimonth=1 then cmt_cnt else 0 end) as cmt_cnt_before_2m,
  sum(case when is_before_bimonth=1 then share_cnt else 0 end) as share_cnt_before_2m,
  sum(case when is_before_bimonth=1 then follow_cnt else 0 end) as follow_cnt_before_2m,
  sum(case when is_before_bimonth=1 then screenshot_cnt else 0 end) as screenshot_cnt_before_2m,
  sum(case when is_before_bimonth=1 then engage_cnt else 0 end) as engage_cnt_before_2m,
  sum(case when is_before_bimonth=1 then click_rgmv_7d else 0 end) as click_rgmv_7d_before_2m,
  sum(case when is_before_bimonth=1 then conversion_cnt else 0 end) as conversion_cnt_before_2m,
  sum(case when is_before_bimonth=1 then ecm_unclosed_rgmv else 0 end) as ecm_unclosed_rgmv_before_2m,
  sum(case when is_before_bimonth=1 then rtb_cost_income_amt else 0 end) as rtb_cost_income_amt_before_2m,
  sum(case when is_before_bimonth=1 then rtb_budget_income_amt else 0 end) as rtb_budget_income_amt_before_2m,
  sum(case when is_before_bimonth=1 then cost_income_amt else 0 end) as cost_income_amt_before_2m,
  sum(case when is_before_bimonth=1 then budget_income_amt else 0 end) as budget_income_amt_before_2m,
  sum(case when is_before_bimonth=1 then advertiser_budget else 0 end) as advertiser_budget_before_2m,
  sum(case when is_before_bimonth=1 then advertiser_cost else 0 end) as advertiser_cost_before_2m,
  sum(case when is_before_bimonth=1 then cash_balance else 0 end) as cash_balance_before_2m,
  sum(case when is_before_bimonth=1 then total_balance else 0 end) as total_balance_before_2m,
  sum(case when is_year=1 then cash_income_amt else 0 end) as cash_income_amt_1y,
  sum(case when is_last_year=1 then cash_income_amt else 0 end) as cash_income_amt_before_1y,
  sum(case when is_before_all_bimonth=1 then cash_income_amt else 0 end) as rtb_all_cash_income_amt_before_2m,
  sum(case when is_before_last_all_bimonth=1 then cash_income_amt else 0 end) as rtb_alll_cash_income_amt_before_last_2m
from temp.temp_app_ads_insight_advertiser_product_diagnosis_info_df_{{ds_nodash}}_01_all_01 
group by  brand_account_id,
    advertiser_id,
    v_seller_id,
    module,
    product,
    marketing_target,
    market_target
)t1 
full outer join
(select brand_account_id,
  0 as advertiser_id,
  '' as v_seller_id,
  '整体' as module,
  '整体'  as product,
  -911 as marketing_target,
  '整体' as market_target,
  ecm_closed_dgmv_2m,
  cps_note_num_2m,
  s_live_dgmv_2m,
  is_sx_ti,
  taolian_cash_income_amt_2m,
  bcoo_cash_income_amt_2m,
  track_taolian_cash_income_amt_2m,
  track_bcoo_cash_income_amt_2m,
  ecm_closed_dgmv_2m,
  ecm_closed_dgmv_cash_income_amt_2m,
  track_ecm_closed_dgmv_2m,
  track_ecm_closed_dgmv_cash_income_amt_2m
from redapp.app_ads_insight_account_diagnosis_info_df
where dtm='{{ds_nodash}}' and account_type =1
)bimonth 
on t1.brand_account_id =bimonth.brand_account_id
  and t1.advertiser_id = bimonth.advertiser_id
  and t1.v_seller_id = bimonth.v_seller_id
  and t1.module = bimonth.module
  and t1.product = bimonth.product
  and t1.marketing_target = bimonth.marketing_target
  and t1.market_target = bimonth.market_target
)t1
left join 
--子账户维表 
(select  virtual_seller_id,
  virtual_seller_name,
  agent_user_id,
  agent_user_name,
  channel_sales_name,
  channel_sales_code,
  channel_operator_name
from redcdm.dim_ads_advertiser_df 
where dtm='{{ds_nodash}}'
)t2 
on t1.v_seller_id_1=t2.virtual_seller_id
left join 
--企业号维表
(select  brand_account_id,
  brand_user_name,
  company_code,
  company_name,
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
  brand_group_tag_code,
  brand_group_tag_name,
  cpc_operator_code,
  cpc_operator_name,
  first_industry_name,
  second_industry_name,
  track_industry_name,
  track_detail_name,
  brand_id,
  brand_name,
  planner_name,
  cpc_operator_dept2_name
from redcdm.dim_ads_industry_account_df
where dtm='{{ds_nodash}}'
)t3 
on t3.brand_account_id= t1.brand_account_id_1
;



insert overwrite table redapp.app_ads_insight_advertiser_product_diagnosis_info_df  partition(dtm = '{{ds_nodash}}')
select brand_account_id,
  advertiser_id,
  case when v_seller_id='' then null else v_seller_id end as v_seller_id,
  virtual_seller_name,
  agent_user_id,
  agent_user_name,
  module,
  product,
  marketing_target,
  market_target,
  brand_user_name,
  company_code,
  company_name,
  track_group_name,
  direct_sales_name,
  direct_sales_dept1_name,
  direct_sales_dept2_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_group_tag_code,
  brand_group_tag_name,
  cpc_operator_code,
  cpc_operator_name,
  first_industry_name,
  second_industry_name,
  track_industry_name,
  track_detail_name,
  brand_id,
  brand_name,
  planner_name,
  concat_ws(',',cpc_operator_name,direct_sales_name,channel_sales_name,channel_operator_name,planner_name) as staff_name,
----redbi使用赛道逻辑
  t1.process_track_industry_name, 
  t1.process_track_group_name,
  t1.process_track_second_name, 
  t1.process_track_third_name,
 channel_sales_name,
  channel_sales_code,
  channel_operator_name,

  ecm_closed_dgmv_2m,
  cps_note_num_2m,
  s_live_dgmv_2m,
  is_sx_ti,
  taolian_cash_income_amt_2m,
  bcoo_cash_income_amt_2m,
  track_taolian_cash_income_amt_2m,
  track_bcoo_cash_income_amt_2m,
  --ecm_closed_dgmv_2m,
  ecm_closed_dgmv_cash_income_amt_2m,
  track_ecm_closed_dgmv_2m,
  track_ecm_closed_dgmv_cash_income_amt_2m,

  cash_income_amt,
  income_amt,
  imp_cnt,
  click_cnt,
  like_cnt,
  fav_cnt,
  cmt_cnt,
  share_cnt,
  follow_cnt,
  screenshot_cnt,
  engage_cnt,
  click_rgmv_7d,
  conversion_cnt,
  ecm_unclosed_rgmv,
  rtb_cost_income_amt,
  rtb_budget_income_amt,
  cost_income_amt,
  budget_income_amt,
  advertiser_budget,
  advertiser_cost,
  cash_balance,
  --cash_balance_days,redbi配置
  -------------前一日--------------
  ystd_cash_income_amt_1d,
  ystd_income_amt_1d,
  ystd_imp_cnt_1d,
  ystd_click_cnt_1d,
  ystd_like_cnt_1d,
  ystd_fav_cnt_1d,
  ystd_cmt_cnt_1d,
  ystd_share_cnt_1d,
  ystd_follow_cnt_1d,
  ystd_screenshot_cnt_1d,
  ystd_engage_cnt_1d,
  ystd_click_rgmv_7d_1d,
  ystd_conversion_cnt_1d,
  ystd_ecm_unclosed_rgmv_1d,
  ystd_rtb_cost_income_amt_1d,
  ystd_rtb_budget_income_amt_1d,
  ystd_cost_income_amt_1d,
  ystd_budget_income_amt_1d,
  ystd_advertiser_budget_1d,
  ystd_advertiser_cost_1d,
  ystd_cash_balance_1d,
--------------------本双月----------------
  cash_income_amt_2m,
  income_amt_2m,
  imp_cnt_2m,
  click_cnt_2m,
  like_cnt_2m,
  fav_cnt_2m,
  cmt_cnt_2m,
  share_cnt_2m,
  follow_cnt_2m,
  screenshot_cnt_2m,
  engage_cnt_2m,
  click_rgmv_7d_2m,
  conversion_cnt_2m,
  ecm_unclosed_rgmv_2m,
  rtb_cost_income_amt_2m,
  rtb_budget_income_amt_2m,
  cost_income_amt_2m,
  budget_income_amt_2m,
  advertiser_budget_2m,
  advertiser_cost_2m,
  cash_balance_2m,
    ------------------去年双月-----------------
   cash_income_amt_last_2m,
  income_amt_last_2m,
  imp_cnt_last_2m,
  click_cnt_last_2m,
  like_cnt_last_2m,
  fav_cnt_last_2m,
  cmt_cnt_last_2m,
  share_cnt_last_2m,
  follow_cnt_last_2m,
  screenshot_cnt_last_2m,
  engage_cnt_last_2m,
  click_rgmv_7d_last_2m,
  conversion_cnt_last_2m,
  ecm_unclosed_rgmv_last_2m,
  rtb_cost_income_amt_last_2m,
  rtb_budget_income_amt_last_2m,
  cost_income_amt_last_2m,
  budget_income_amt_last_2m,
  advertiser_budget_last_2m,
  advertiser_cost_last_2m,
  cash_balance_last_2m,
    -------------上双月--------------------
    cash_income_amt_before_2m,
  income_amt_before_2m,
  imp_cnt_before_2m,
  click_cnt_before_2m,
  like_cnt_before_2m,
  fav_cnt_before_2m,
  cmt_cnt_before_2m,
  share_cnt_before_2m,
  follow_cnt_before_2m,
  screenshot_cnt_before_2m,
  engage_cnt_before_2m,
  click_rgmv_7d_before_2m,
  conversion_cnt_before_2m,
  ecm_unclosed_rgmv_before_2m,
  rtb_cost_income_amt_before_2m,
  rtb_budget_income_amt_before_2m,
  cost_income_amt_before_2m,
  budget_income_amt_before_2m,
  advertiser_budget_before_2m,
  advertiser_cost_before_2m,
  cash_balance_before_2m,
  track_group_cost_income_amt,
  track_group_budget_income_amt,
  track_group_advertiser_cost_income_amt,
  track_group_advertiser_budget_income_amt,
  track_group_rtb_cost_income_amt,
  track_group_rtb_budget_income_amt,
  track_cash_income_amt_before_2m,
  track_cash_income_amt_2m,
  track_cash_income_amt_last_2m,
  track_cost_income_amt,
  track_budget_income_amt,
  track_advertiser_cost_income_amt,
  track_advertiser_budget_income_amt,
  track_rtb_cost_income_amt,
  track_rtb_budget_income_amt,
  cash_income_amt_1y,
  cash_income_amt_before_1y,
  total_balance,
  ystd_total_balance_1d,
  total_balance_2m,
  total_balance_last_2m,
  total_balance_before_2m,
  rtb_all_cash_income_amt_before_2m,
  rtb_alll_cash_income_amt_before_last_2m
from 
temp.temp_app_ads_insight_advertiser_product_diagnosis_info_df_{{ds_nodash}}_01_detail_01 t1 
left join 
(
select process_track_industry_name,
  sum(cost_income_amt) as track_cost_income_amt,
  sum(budget_income_amt) as track_budget_income_amt,
  sum(advertiser_cost) as track_advertiser_cost_income_amt,
  sum(advertiser_budget) as track_advertiser_budget_income_amt,
  sum(rtb_cost_income_amt) as track_rtb_cost_income_amt,
  sum(rtb_budget_income_amt) as track_rtb_budget_income_amt

from 
  temp.temp_app_ads_insight_advertiser_product_diagnosis_info_df_{{ds_nodash}}_01_detail_01
group by process_track_industry_name 
)t2 
on t1.process_track_industry_name=t2.process_track_industry_name
left join 
(select process_track_group_name ,
  sum(cost_income_amt) as track_group_cost_income_amt,
  sum(budget_income_amt) as track_group_budget_income_amt,
  sum(advertiser_cost) as track_group_advertiser_cost_income_amt,
  sum(advertiser_budget) as track_group_advertiser_budget_income_amt,
  sum(rtb_cost_income_amt) as track_group_rtb_cost_income_amt,
  sum(rtb_budget_income_amt) as track_group_rtb_budget_income_amt,
  sum(cash_income_amt_before_2m) as track_cash_income_amt_before_2m,
  sum(cash_income_amt_2m) as track_cash_income_amt_2m,
  sum(cash_income_amt_last_2m) as track_cash_income_amt_last_2m
from temp.temp_app_ads_insight_advertiser_product_diagnosis_info_df_{{ds_nodash}}_01_detail_01
group by process_track_group_name
)t3 
on t3.process_track_group_name=t1.process_track_group_name 
