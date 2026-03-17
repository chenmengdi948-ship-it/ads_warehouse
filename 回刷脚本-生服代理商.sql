
    -- ************************************************
    -- Author: chenmengdi
    -- CreateTime:2024-07-04T16:30:11+08:00
    -- Update: Task Update Description
    -- ************************************************
drop table if exists temp.temp_dm_ads_industry_account_agent_detail_{{ds_nodash}};
create table temp.temp_dm_ads_industry_account_agent_detail_{{ds_nodash}}
SELECT
  f_getdate(a.dtm) as date_key,
  module,
  product,
  a.brand_account_id,
  v_seller_id,
  b.market_target,
  optimize_target,
  marketing_target,
  note_ads_type as ads_note_type,
  promotion_target,
  '0' as is_marketing_product,
  '-911' as agent_user_id,
  sum(unique_imp_cnt) as imp_cnt,
  sum(unique_click_cnt) as click_cnt,
  sum(leads_cnt) as leads_cnt,
  sum(msg_num) as msg_num,
  sum(msg_user_num) as msg_user_num,
  sum(msg_open_num) as msg_open_num,
  sum(like_cnt) as like_cnt,
  sum(fav_cnt) as fav_cnt,
  sum(cmt_cnt) as cmt_cnt,
  sum(share_cnt) as share_cnt,
  sum(follow_cnt) as follow_cnt,
  sum(screenshot_cnt) as screenshot_cnt,
  sum(image_save_cnt) as image_save_cnt,
  sum(live_rgmv) as live_rgmv,
  sum(live_dgmv) as live_dgmv,
  sum(conversion_cnt) as conversion_cnt,
  -- sum(cash_income_amt) as cash_income_amt,
  -- sum(income_amt) as income_amt,
  sum(coalesce(purchase_rgmv,0)+coalesce(mini_purchase_rgmv,0)) as click_rgmv_7d,
  sum(coalesce(purchase_order_num,0)+coalesce(mini_purchase_order_num,0)) as purchase_order_num,
  sum(click_purchase_order_pv_7d) as click_purchase_order_pv_7d,
  sum(live_order_num) as live_order_num,
  sum(ecm_unclosed_purchase_rgmv) as ecm_unclosed_purchase_rgmv,
  sum(leads_submit_cnt) as leads_submit_cnt,
  sum(msg_driving_open_num) as msg_driving_open_num,
  sum(case when optimize_target = 50 then conversion_cnt else 0 end) as msg_leads_num,
  sum(coalesce(deal_order_num,0)+coalesce(mini_deal_order_num,0)) as deal_order_num,
  sum(coalesce(rgmv,0)+coalesce(mini_rgmv,0)) as deal_rgmv_7d
FROM
  redcdm.dm_ads_rtb_creativity_1d_di a 
  left join 
  (select
   
    dim_value,
    dim_value_name ,
    value_type as market_target
  FROM
    redcdm.dim_ads_industry_dimension_code_df
  WHERE
    dtm = 'all' and dimension_code = 'marketing_target'
  )b --枚举值维表
  on a.marketing_target = b.dim_value
  left join redcdm.dim_ads_social_note_base_info_df c 
  on c.dtm='{{ds_nodash}}' and c.note_id =a.ads_material_id
WHERE
  a.dtm >= '20230101' and a.dtm<='{{ds_nodash}}' and (is_effective=1 or total_amount>0)
group by f_getdate(a.dtm),
  module,
  product,
  a.brand_account_id,
  v_seller_id,
  b.market_target,
  optimize_target,
  marketing_target,
  promotion_target,
  note_ads_type 
union all 
SELECT
  date_key,
  module,
  product,
  brand_account_id,
  '-911' as v_seller_id,
  market_target,
  -911 as optimize_target,
  -911 as marketing_target,
  '非竞价' as ads_note_type,
  -911 as promotion_target,
  '0' as is_marketing_product,
  '-911' as agent_user_id,
  sum(imp_cnt) as imp_cnt,
  sum(click_cnt) as click_cnt,
  0 as leads_cnt,
  sum(msg_num) as msg_num,
  0 as msg_user_num,
  sum(msg_open_num) as msg_open_num,
  sum(like_cnt) as like_cnt,
  sum(fav_cnt) as fav_cnt,
  sum(cmt_cnt) as cmt_cnt,
  sum(share_cnt) as share_cnt,
  sum(follow_cnt) as follow_cnt,
  0 as screenshot_cnt,
  0 as image_save_cnt,
  0 as live_rgmv,
  0 as live_dgmv,
  sum(conversion_cnt) as conversion_cnt,
  -- sum(cash_income_amt) as cash_income_amt,
  -- sum(income_amt) as income_amt,
  0 as click_rgmv_7d,
  0 as purchase_order_num,
  0 as click_purchase_order_pv_7d,
  0 as live_order_num,
  0 as ecm_unclosed_purchase_rgmv,
  0 as leads_submit_cnt,
  0 as msg_driving_open_num,
  0 as msg_leads_num,
  0 as deal_order_num,
  0 as deal_order_rgmv
FROM
  redapp.app_ads_insight_product_account_df
WHERE
  dtm = max_dtm('redapp.app_ads_insight_product_account_df') and date_key<='{{ds}}' and module<>'效果'
group by date_key,
  module,
  product,
  brand_account_id,
  --v_seller_id,
  market_target,
  optimize_target,
  marketing_target
;

insert overwrite table redapp.app_ads_industry_product_account_agent_detail_td_df  partition(dtm = '{{ ds_nodash }}') 
--各种广告类型展点转化指标
select t1.date_key,
  t1.brand_account_id,
  module,
  product,
  marketing_target,
  optimize_target,
  case when module in ('效果') then coalesce(market_target,'种草') else '整体' end as market_target,
  coalesce(is_marketing_product,'0') as is_marketing_product,
  t1.virtual_seller_id,
  t1.agent_user_id,
  coalesce(ads_note_type,'其他') as ads_note_type,
  promotion_target,
  case when first_ads_cost_date<'2019-01-01' or first_ads_cost_date>'{{ds}}' then null else first_ads_cost_date end as first_ads_cost_date,
  case when company_first_ads_cost_date<'2019-01-01' or company_first_ads_cost_date>'{{ds}}' then null else company_first_ads_cost_date end as company_first_ads_cost_date,
  case when first_recharge_date<'2019-01-01' or first_recharge_date>'{{ds}}' then null else first_recharge_date end as first_recharge_date,
  account.brand_user_name as brand_account_name,
  account.company_code,
  account.company_name,
  track_group_name,
  cpc_direct_sales_code,
  cpc_direct_sales_name,
  cpc_direct_sales_dept1_name,
  cpc_direct_sales_dept2_name,
  cpc_direct_sales_dept3_name,
  cpc_direct_sales_dept4_name,
  cpc_direct_sales_dept5_name,
  cpc_direct_sales_dept6_name,
  brand_tag_name,
  cpc_operator_name,
  cpc_operator_dept1_name,
  cpc_operator_dept2_name,
  cpc_operator_dept3_name,
  cpc_operator_dept4_name,
  cpc_operator_dept5_name,
  cpc_operator_dept6_name,
  track_industry_name,
  track_detail_name,
  adv.channel_sales_code,
  adv.channel_sales_name,
  adv.channel_operator_code,
  adv.channel_operator_name,
  adv.virtual_seller_name,
  case when module ='效果' then rtb_seller_code else cpc_direct_sales_code end as rtb_seller_code, --效果已删除的挂接关系未统计
  case when module ='效果' then rtb_seller_name else cpc_direct_sales_name end as rtb_seller_name,
  case when module ='效果' then v_seller_dept1_name else cpc_direct_sales_dept1_name end as v_seller_dept1_name,
  case when module ='效果' then v_seller_dept2_name else cpc_direct_sales_dept2_name end as v_seller_dept2_name,
  case when module ='效果' then v_seller_dept3_name else cpc_direct_sales_dept3_name end as v_seller_dept3_name,
  case when module ='效果' then v_seller_dept4_name else cpc_direct_sales_dept4_name end as v_seller_dept4_name,
  case when module ='效果' then v_seller_dept5_name else cpc_direct_sales_dept5_name end as v_seller_dept5_name,
  case when module ='效果' then v_seller_dept6_name else cpc_direct_sales_dept6_name end as v_seller_dept6_name,
  rtb_advertiser_id,
  avg_qtd_cost as dim_avg_qtd_cost,
    avg_ytd_cost as dim_avg_ytd_cost,
    case when first_pass_time<'2019-01-01' or first_pass_time>'{{ds}}' then null else first_pass_time end as first_pass_time,
    ag.agent_user_name,
    agent_type,
  imp_cnt,
  click_cnt,
  leads_cnt,
  msg_num,
  msg_user_num,
  msg_open_num,
  like_cnt,
  fav_cnt,
  cmt_cnt,
  share_cnt,
  follow_cnt,
  screenshot_cnt,
  image_save_cnt,
  live_rgmv,
  live_dgmv,
  conversion_cnt,
  click_rgmv_7d,
  purchase_order_num,
  click_purchase_order_pv_7d,
  live_order_num,
  ecm_unclosed_purchase_rgmv,
  leads_submit_cnt,
  msg_driving_open_num,
  msg_leads_num,
  deal_order_num,
  deal_rgmv_7d,
  cash_income_amt,
  income_amt,
  cpc_direct_sales_dept1_code,
  cpc_direct_sales_dept2_code,
  cpc_direct_sales_dept3_code,
  cpc_direct_sales_dept4_code,
  cpc_direct_sales_dept5_code,
  cpc_direct_sales_dept6_code,
  cpc_operator_code,
  cpc_operator_dept1_code,
  cpc_operator_dept2_code,
  cpc_operator_dept3_code,
  cpc_operator_dept4_code,
  cpc_operator_dept5_code,
  cpc_operator_dept6_code,
  ce.direct_sales_first_dept_name,
  ce.direct_sales_second_dept_name,
  ce.direct_sales_third_dept_name
from 
(select date_key,
    brand_account_id,
    module,
    case  when module in ('薯条','口碑通','品合') then module when product='火焰话题' then '品牌其他' else product end as product,
    marketing_target,
    optimize_target,
    market_target,
    is_marketing_product,
    virtual_seller_id,
    agent_user_id,
    ads_note_type,
    promotion_target,
    sum(imp_cnt) as imp_cnt,
    sum(click_cnt) as click_cnt,
    sum(leads_cnt) as leads_cnt,
    sum(msg_num) as msg_num,
    sum(msg_user_num) as msg_user_num,
    sum(msg_open_num) as msg_open_num,
    sum(like_cnt) as like_cnt,
    sum(fav_cnt) as fav_cnt,
    sum(cmt_cnt) as cmt_cnt,
    sum(share_cnt) as share_cnt,
    sum(follow_cnt) as follow_cnt,
    sum(screenshot_cnt) as screenshot_cnt,
    sum(image_save_cnt) as image_save_cnt,
    sum(live_rgmv) as live_rgmv,
    sum(live_dgmv) as live_dgmv,
    sum(conversion_cnt) as conversion_cnt,
    sum(click_rgmv_7d) as click_rgmv_7d,
    sum(purchase_order_num) as purchase_order_num,
    sum(click_purchase_order_pv_7d) as click_purchase_order_pv_7d,
    sum(live_order_num) as live_order_num,
    sum(ecm_unclosed_purchase_rgmv) as ecm_unclosed_purchase_rgmv,
    sum(leads_submit_cnt) as leads_submit_cnt,
    sum(msg_driving_open_num) as msg_driving_open_num,
    sum(msg_leads_num) as msg_leads_num,
    sum(deal_order_num) as deal_order_num,
    sum(deal_rgmv_7d) as deal_rgmv_7d,
    sum(cash_income_amt) as cash_income_amt,
    sum(income_amt) as income_amt
from 
    (SELECT
    t1.date_key,
    t1.brand_account_id,
    module,
    product,
    marketing_target,
    optimize_target,
    market_target,
    is_marketing_product,
    v_seller_id as virtual_seller_id,
    coalesce(t2.agent_user_id,t1.agent_user_id) as agent_user_id,
    ads_note_type,
    promotion_target,
    imp_cnt,
    click_cnt,
    leads_cnt,
    msg_num,
    msg_user_num,
    msg_open_num,
    like_cnt,
    fav_cnt,
    cmt_cnt,
    share_cnt,
    follow_cnt,
    screenshot_cnt,
    image_save_cnt,
    live_rgmv,
    live_dgmv,
    conversion_cnt,
    click_rgmv_7d,
    purchase_order_num,
    click_purchase_order_pv_7d,
    live_order_num,
    ecm_unclosed_purchase_rgmv,
    leads_submit_cnt,
    msg_driving_open_num,
    msg_leads_num,
    deal_order_num,
    deal_rgmv_7d,
    0 as cash_income_amt,
    0 as income_amt
    from  temp.temp_dm_ads_industry_account_agent_detail_{{ds_nodash}} t1
    left join redcdm.dim_ads_advertiser_df t2 
    on t1.v_seller_id=t2.virtual_seller_id and t2.dtm='{{ds_nodash}}'
    union all 
    SELECT
        date_key,
        brand_account_id,
        module,
        product,
        marketing_target,
        optimize_target,
        market_target,
        is_marketing_product,
        virtual_seller_id,
        agent_user_id,
        ads_note_type,
        promotion_target,
        0 as imp_cnt,
        0 as click_cnt,
        0 as leads_cnt,
        0 as msg_num,
        0 as msg_user_num,
        0 as msg_open_num,
        0 as like_cnt,
        0 as fav_cnt,
        0 as cmt_cnt,
        0 as share_cnt,
        0 as follow_cnt,
        0 as screenshot_cnt,
        0 as image_save_cnt,
        0 as live_rgmv,
        0 as live_dgmv,
        0 as conversion_cnt,
        0 as click_rgmv_7d,
        0 as purchase_order_num,
        0 as click_purchase_order_pv_7d,
        0 as live_order_num,
        0 as ecm_unclosed_purchase_rgmv,
        0 as leads_submit_cnt,
        0 as msg_driving_open_num,
        0 as msg_leads_num,
        0 as deal_order_num,
        0 as deal_rgmv_7d,
        cash_income_amt,
        income_amt
    FROM
    redapp.app_ads_industry_product_account_agent_cost_detail_td_df
    WHERE
    dtm = '{{ds_nodash}}' 
    )base
group by date_key,
    brand_account_id,
    module,
    case  when module in ('薯条','口碑通','品合') then module when product='火焰话题' then '品牌其他' else product end,
    marketing_target,
    optimize_target,
    market_target,
    is_marketing_product,
    virtual_seller_id,
    agent_user_id,
    ads_note_type,
    promotion_target
)t1
left join 
--首次广告投放时间
(select brand_account_id,
  min(first_ads_cost_date) as first_ads_cost_date,
  min(company_first_ads_cost_date) as company_first_ads_cost_date
from redapp.app_ads_industry_product_account_agent_cost_detail_td_df
where  dtm = '{{ds_nodash}}'
group by brand_account_id
)cost_type2
on cost_type2.brand_account_id=t1.brand_account_id 
-- left join 
-- --coalesce(company_code,brand_account_id)首次投放时间
-- (select company_id,
--  min(company_first_ads_cost_date) as company_first_ads_cost_date
-- from redapp.app_ads_industry_product_account_agent_cost_detail_td_df
-- where  dtm = '{{ds_nodash}}'
-- group by company_id
-- )cost_type 
-- on cost_type.company_id=t1.company_id
left join 
--首次充值时间 
(SELECT
  user_id,
  min(first_recharge_date) as first_recharge_date
FROM
  redapp.app_ads_industry_account_apply_recharge_df
WHERE
  dtm = max_dtm('redapp.app_ads_industry_account_apply_recharge_df') and first_recharge_date<='{{ds}}'
group by 1
)recharge 
on recharge.user_id=t1.brand_account_id
left join 
--企业号维度
(SELECT
  brand_account_id,
  brand_user_name,
  company_code,
  company_name,
  track_group_name,
  cpc_direct_sales_code,
  cpc_direct_sales_name,
  cpc_direct_sales_dept1_name,
  cpc_direct_sales_dept2_name,
  cpc_direct_sales_dept3_name,
  cpc_direct_sales_dept4_name,
  cpc_direct_sales_dept5_name,
  cpc_direct_sales_dept6_name,
  cpc_direct_sales_dept1_code,
  cpc_direct_sales_dept2_code,
  cpc_direct_sales_dept3_code,
  cpc_direct_sales_dept4_code,
  cpc_direct_sales_dept5_code,
  cpc_direct_sales_dept6_code,
  brand_tag_name,
  cpc_operator_name,
  cpc_operator_code,
  cpc_operator_dept1_name,
  cpc_operator_dept2_name,
  cpc_operator_dept3_name,
  cpc_operator_dept4_name,
  cpc_operator_dept5_name,
  cpc_operator_dept6_name,
  cpc_operator_dept1_code,
  cpc_operator_dept2_code,
  cpc_operator_dept3_code,
  cpc_operator_dept4_code,
  cpc_operator_dept5_code,
  cpc_operator_dept6_code,
  track_industry_name,
  track_detail_name,
  case when cpc_direct_sales_dept2_name<>'行业团队' then cpc_direct_sales_dept2_code 
  when cpc_direct_sales_dept2_name='行业团队' and cpc_direct_sales_dept3_name<>'生活服务行业' then concat_ws('-',coalesce(cpc_direct_sales_dept3_code,''),coalesce(cpc_direct_sales_dept4_code,''))
  when cpc_direct_sales_dept3_name='生活服务行业' and coalesce(cpc_direct_sales_dept5_code,'')<>'fb6affc249da4bb3a141d58362a6f451' then concat_ws('-',coalesce(cpc_direct_sales_dept3_code,''),coalesce(cpc_direct_sales_dept4_code,''),coalesce(cpc_direct_sales_dept5_code,''))
  else concat_ws('-',coalesce(cpc_direct_sales_dept3_code,''),coalesce(cpc_direct_sales_dept4_code,''),coalesce(cpc_direct_sales_dept5_code,''),coalesce(cpc_direct_sales_dept6_code,'')) end as code
from redcdm.dim_ads_industry_account_df
WHERE
  dtm = '{{ds_nodash}}'
)account 
on account.brand_account_id = t1.brand_account_id
left join 
--子账户维度
(SELECT
  virtual_seller_id,
  virtual_seller_name,
  rtb_advertiser_id,
  channel_sales_code,
  channel_sales_name,
  channel_operator_code,
  channel_operator_name,
  rtb_seller_code,
  rtb_seller_name,
  v_seller_dept1_name,
  v_seller_dept2_name,
  v_seller_dept3_name,
  v_seller_dept4_name,
  v_seller_dept5_name,
  v_seller_dept6_name
FROM
  redcdm.dim_ads_advertiser_df
WHERE
  dtm = '{{ds_nodash}}' and state=1
  )adv
on adv.virtual_seller_id=t1.virtual_seller_id
left join 
--分层
(select date_key,brand_account_id,
  max(dim_ads_cash_cost_qtd)/max(dim_days_qtd) as avg_qtd_cost,
  max(dim_ads_cash_cost_ytd)/max(dim_days_ytd) as avg_ytd_cost
from redapp.app_ads_industry_product_account_agent_cost_detail_td_df
where  dtm = '{{ds_nodash}}'
group by date_key,brand_account_id
)cost_type 
on cost_type.brand_account_id=t1.brand_account_id and cost_type.date_key=t1.date_key
left join 
--开户日期
(
select 
  user_id,
  min(substring(audit_time,1,10)) as first_pass_time
from redods.ods_uranus_ba_ads_industry_qualification_apply_df
where dtm =max_dtm('redods.ods_uranus_ba_ads_industry_qualification_apply_df') and substring(audit_time,1,10)<'{{ds}}'
and apply_status = 'auditPass'
group by 1
)t3 
on t3.user_id = t1.brand_account_id  
left join 
--代理商
(select agent_user_id, agent_user_name
from reddim.dim_ads_crm_agent_day
where dtm='{{ds_nodash}}' 

)ag 
on ag.agent_user_id = t1.agent_user_id
left join 
--代理商
(select agent_user_id, max(agent_type) as agent_type --防止主键重复
from reddm.dm_ads_crm_advertiser_income_wide_day 
where dtm='{{ds_nodash}}' and agent_user_id is not null 
group by 1
)agent 
on agent.agent_user_id=t1.agent_user_id
left join 
--策略中台维护赛道
(select 
  direct_sales_first_dept_name,
  direct_sales_second_dept_name,
  direct_sales_third_dept_name,
  concat_ws('-',cpc_direct_sales_dept3_code,cpc_direct_sales_dept4_code) as code,
  is_valid
from ads_data_crm.app_app_ads_sales_org_mapping_df
where dtm=max_dtm('ads_data_crm.app_app_ads_sales_org_mapping_df')
and is_valid=1
and cpc_direct_sales_dept2_name='行业团队'
and match_type=1
group by direct_sales_first_dept_name,
  direct_sales_second_dept_name,
  direct_sales_third_dept_name,
  concat_ws('-',cpc_direct_sales_dept3_code,cpc_direct_sales_dept4_code),
  is_valid
union all
select 
  direct_sales_first_dept_name,
  direct_sales_second_dept_name,
  direct_sales_third_dept_name,
  case when cpc_direct_sales_dept5_code='fb6affc249da4bb3a141d58362a6f451' then concat_ws('-',cpc_direct_sales_dept3_code,cpc_direct_sales_dept4_code,cpc_direct_sales_dept5_code,cpc_direct_sales_dept6_code) 
  else concat_ws('-',cpc_direct_sales_dept3_code,cpc_direct_sales_dept4_code,cpc_direct_sales_dept5_code) end as code, --生活服务行业五部（医美医疗金融行业）需要看到六级做赛道划分
  is_valid
from ads_data_crm.app_app_ads_sales_org_mapping_df
where dtm=max_dtm('ads_data_crm.app_app_ads_sales_org_mapping_df')
and is_valid=1
and cpc_direct_sales_dept2_name='行业团队'
and match_type in (2,3)
group by 
  direct_sales_first_dept_name,
  direct_sales_second_dept_name,
  direct_sales_third_dept_name,
 case when cpc_direct_sales_dept5_code='fb6affc249da4bb3a141d58362a6f451' then concat_ws('-',cpc_direct_sales_dept3_code,cpc_direct_sales_dept4_code,cpc_direct_sales_dept5_code,cpc_direct_sales_dept6_code) 
  else concat_ws('-',cpc_direct_sales_dept3_code,cpc_direct_sales_dept4_code,cpc_direct_sales_dept5_code) end,
  is_valid
union all 
select 
  direct_sales_first_dept_name,
  direct_sales_second_dept_name,
  direct_sales_third_dept_name,
  cpc_direct_sales_dept2_code as code,
  is_valid
from ads_data_crm.app_app_ads_sales_org_mapping_df
where dtm=max_dtm('ads_data_crm.app_app_ads_sales_org_mapping_df')
and cpc_direct_sales_dept2_name<>'行业团队'
and is_valid=1
group by direct_sales_first_dept_name,
  direct_sales_second_dept_name,
  direct_sales_third_dept_name,
  cpc_direct_sales_dept2_code,
  is_valid
)ce 
on ce.code = account.code