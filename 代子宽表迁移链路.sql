
    -- ************************************************
    -- Author: chenmengdi
    -- CreateTime:2024-07-04T16:30:11+08:00
    -- Update: Task Update Description
    -- ************************************************
drop table if exists temp.temp_dm_ads_industry_account_agent_detail_{{ds_nodash}};
create table temp.temp_dm_ads_industry_account_agent_detail_{{ds_nodash}}
select   date_key,
  module,
  product,
  brand_account_id,
  v_seller_id,
  market_target,
  optimize_target,
  marketing_target,
  ads_note_type,
  promotion_target,
  is_marketing_product,
  agent_user_id,
  platform,
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
sum(deal_order_rgmv) as deal_order_rgmv,
sum(cost_income_amt) as cost_income_amt,
sum(budget_income_amt) as budget_income_amt,
sum(cost_creativity_num) as cost_creativity_num,
sum(new_cost_creativity_num) as new_cost_creativity_num,
sum(new_cost_campaign_num) as new_cost_campaign_num,
sum(campaign_num) as campaign_num,
sum(cost_campaign_num) as cost_campaign_num,
sum(create_campaign_num) as create_campaign_num,
sum(create_cost_campaign_num) as create_cost_campaign_num,
sum(effecient_campaign_num) as effecient_campaign_num,
sum(rtb_note_num) as rtb_note_num,
sum(rtb_cost_note_num) as rtb_cost_note_num,
sum(new_cost_note_num) as new_cost_note_num,
sum(launch_finish_campaign_num) as launch_finish_campaign_num,
sum(min_advertiser_budget_income_amt) as min_advertiser_budget_income_amt,
sum(advertiser_cost_income_amt) as advertiser_cost_income_amt,
sum(msg_income_cnt) as msg_income_cnt,
sum(msg_income_user_cnt) as msg_income_user_cnt,
sum(msg_open_user_cnt) as msg_open_user_cnt,
sum(msg_leads_user_cnt) as msg_leads_user_cnt,
sum(valid_leads_num) as valid_leads_num,
sum(msg_first_15s_reply_cnt) as msg_first_15s_reply_cnt,
sum(msg_first_15s_cnt) as msg_first_15s_cnt,
sum(msg_driving_open_wait_duration) as msg_driving_open_wait_duration,
sum(msg_driving_open_30s_cnt) as msg_driving_open_30s_cnt,
sum(msg_driving_open_60s_cnt) as msg_driving_open_60s_cnt,
sum(msg_driving_open_effi_cnt) as msg_driving_open_effi_cnt,
sum(live_reserve_cnt) as live_reserve_cnt,
sum(live_subscribe_user_cnt) as live_subscribe_user_cnt,
sum(live_comment_cnt) as live_comment_cnt,
sum(live_watch_num) as live_watch_num,
sum(live_effective_shutdown_num) as live_effective_shutdown_num,
sum(live_effective_shutdown_distinct_eventid_num) as live_effective_shutdown_distinct_eventid_num,
sum(live_watch_duration) as live_watch_duration,
sum(live_watch_distinct_eventid_num) as live_watch_distinct_eventid_num,
sum(live_follow_num) as live_follow_num,
sum(enter_seller_cnt_7d) as enter_seller_cnt_7d,
sum(goods_view_cnt_7d) as goods_view_cnt_7d,
sum(add_cart_cnt_7d) as add_cart_cnt_7d,
sum(search_component_click_cnt) as search_component_click_cnt,
sum(deal_gmv) as deal_gmv,
sum(cost_campaign_cnt) as cost_campaign_cnt
from 
(select date_key,
module,
product,
brand_account_id,
virtual_seller_id as v_seller_id,
market_target,
optimize_target,
marketing_target,
ads_note_type,
promotion_target,
is_marketing_product,
agent_user_id,
platform,
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
-- sum(cash_income_amt) as cash_income_amt,
-- sum(income_amt) as income_amt,
click_rgmv_7d,
purchase_order_num,
click_purchase_order_pv_7d,
live_order_num,
ecm_unclosed_purchase_rgmv,
leads_submit_cnt,
msg_driving_open_num,
msg_leads_num,
deal_order_num,
deal_order_rgmv,
0 as cost_income_amt,
0 as budget_income_amt,
0 as cost_creativity_num,
0 as new_cost_creativity_num,
0 as new_cost_campaign_num,
0 as campaign_num,
0 as cost_campaign_num,
0 as create_campaign_num,
0 as create_cost_campaign_num,
0 as effecient_campaign_num,
0 as rtb_note_num,
0 as rtb_cost_note_num,
0 as new_cost_note_num,
0 as launch_finish_campaign_num,
0 as min_advertiser_budget_income_amt,
0 as advertiser_cost_income_amt,
0 as msg_income_cnt,
0 as msg_income_user_cnt,
0 as msg_open_user_cnt,
0 as msg_leads_user_cnt,
0 as valid_leads_num,
0 as msg_first_15s_reply_cnt,
0 as msg_first_15s_cnt,
0 as msg_driving_open_wait_duration,
0 as msg_driving_open_30s_cnt,
0 as msg_driving_open_60s_cnt,
0 as msg_driving_open_effi_cnt,
0 as live_reserve_cnt,
0 as live_subscribe_user_cnt,
0 as live_comment_cnt,
0 as live_watch_num,
0 as live_effective_shutdown_num,
0 as live_effective_shutdown_distinct_eventid_num,
0 as live_watch_duration,
0 as live_watch_distinct_eventid_num,
0 as live_follow_num,
0 as enter_seller_cnt_7d,
0 as goods_view_cnt_7d,
0 as add_cart_cnt_7d,
0 as search_component_click_cnt,
0 as deal_gmv,
0 as cost_campaign_cnt
from redapp.app_ads_industry_product_account_agent_detail_td_df 
WHERE
  dtm=f_getdate('{{ds_nodash}}' ,-1) and module in ('效果') --加2023年效果展点消数据
  and date_key<'2024-01-01'
union all 
SELECT
  date_key,
  module,
  product,
  brand_account_id,
  v_seller_id,
  market_target,
  optimize_target,
  marketing_target,
  ads_note_type,
  promotion_target,
  is_marketing_product,
  agent_user_id,
  platform,
  sum(imp_num) as imp_cnt,
  sum(click_num) as click_cnt,
  sum(leads_submit_num) as leads_cnt,
  sum(msg_income_cnt) as msg_num,
  sum(msg_income_user_cnt) as msg_user_num,
  sum(msg_open_cnt) as msg_open_num,
  sum(like_num) as like_cnt,
  sum(fav_num) as fav_cnt,
  sum(cmt_num) as cmt_cnt,
  sum(share_num) as share_cnt,
  sum(follow_num) as follow_cnt,
  0 as screenshot_cnt,
  0 as image_save_cnt,
  sum(live_gmv) as live_rgmv,
  sum(live_gmv) as live_dgmv,
  sum(conversion_cnt) as conversion_cnt,
  -- sum(cash_income_amt) as cash_income_amt,
  -- sum(income_amt) as income_amt,
  sum(purchase_order_gmv_7d) as click_rgmv_7d,
  sum(purchase_order_num_7d) as purchase_order_num,
  sum(purchase_order_num_7d) as click_purchase_order_pv_7d,
  sum(live_order_num) as live_order_num,
  sum(ecm_unclosed_purchase_order_gmv_7d) as ecm_unclosed_purchase_rgmv,
  sum(leads_submit_num) as leads_submit_cnt,
  sum(msg_open_cnt) as msg_driving_open_num,
  sum(msg_leads_num) as msg_leads_num,
  sum(deal_order_num_7d) as deal_order_num,
  sum(deal_order_gmv_7d) as deal_order_rgmv,
  sum(rtb_cost_income_amt) as cost_income_amt,
  sum(rtb_budget_income_amt) as budget_income_amt,
  sum(cost_creativity_num) as cost_creativity_num,
sum(new_cost_creativity_num) as   new_cost_creativity_num,
sum(new_cost_campaign_num) as   new_cost_campaign_num,
sum(campaign_num) as   campaign_num,
sum(cost_campaign_num) as   cost_campaign_num,
sum(create_campaign_num) as   create_campaign_num,
sum(create_cost_campaign_num) as   create_cost_campaign_num,
sum(effecient_campaign_num) as   effecient_campaign_num,
sum(rtb_note_num) as   rtb_note_num,
sum(rtb_cost_note_num) as   rtb_cost_note_num,
sum(new_cost_note_num) as   new_cost_note_num,
sum(launch_finish_campaign_num) as   launch_finish_campaign_num,
sum(min_advertiser_budget_income_amt) as   min_advertiser_budget_income_amt,
sum(advertiser_cost_income_amt) as   advertiser_cost_income_amt,
sum(msg_income_cnt) as   msg_income_cnt,
sum(msg_income_user_cnt) as   msg_income_user_cnt,
sum(msg_open_user_cnt) as   msg_open_user_cnt,
sum(msg_leads_user_cnt) as   msg_leads_user_cnt,
sum(valid_leads_num) as   valid_leads_num,
sum(msg_first_15s_reply_cnt) as   msg_first_15s_reply_cnt,
sum(msg_first_15s_cnt) as   msg_first_15s_cnt,
sum(msg_driving_open_wait_duration) as   msg_driving_open_wait_duration,
sum(msg_driving_open_30s_cnt) as   msg_driving_open_30s_cnt,
sum(msg_driving_open_60s_cnt) as   msg_driving_open_60s_cnt,
sum(msg_driving_open_effi_cnt) as   msg_driving_open_effi_cnt,
sum(live_reserve_cnt) as   live_reserve_cnt,
sum(live_subscribe_user_cnt) as   live_subscribe_user_cnt,
sum(live_comment_cnt) as   live_comment_cnt,
sum(live_watch_num) as   live_watch_num,
sum(live_effective_shutdown_num) as   live_effective_shutdown_num,
sum(live_effective_shutdown_distinct_eventid_num) as   live_effective_shutdown_distinct_eventid_num,
sum(live_watch_duration) as   live_watch_duration,
sum(live_watch_distinct_eventid_num) as   live_watch_distinct_eventid_num,
sum(live_follow_num) as   live_follow_num,
sum(enter_seller_cnt_7d) as   enter_seller_cnt_7d,
sum(goods_view_cnt_7d) as   goods_view_cnt_7d,
sum(add_cart_cnt_7d) as   add_cart_cnt_7d,
sum(search_component_click_cnt) as   search_component_click_cnt,
sum(deal_gmv) as deal_gmv,
sum(cost_campaign_num) as cost_campaign_cnt
FROM
  redcdm.dm_ad_pub_advertiser_product_metrics_detail_df
WHERE
  dtm = '{{ds_nodash}}' 
group by date_key,
  module,
  product,
  brand_account_id,
  v_seller_id,
  market_target,
  optimize_target,
  marketing_target,
  ads_note_type,
  promotion_target,
  is_marketing_product,
  agent_user_id,
  platform
  )base 
group by date_key,
  module,
  product,
  brand_account_id,
  v_seller_id,
  market_target,
  optimize_target,
  marketing_target,
  ads_note_type,
  promotion_target,
  is_marketing_product,
  agent_user_id,
  platform
;

insert overwrite table redapp.app_ads_industry_product_account_agent_detail_td_df  partition(dtm = '{{ ds_nodash }}') 
--各种广告类型展点转化指标
select t1.date_key,
  t1.brand_account_id,
  t1.module,
  t1.product,
  marketing_target,
  optimize_target,
  case when t1.module in ('效果') then coalesce(market_target,'种草') else '整体' end as market_target,
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
  case when t1.module ='效果' then rtb_seller_code else cpc_direct_sales_code end as rtb_seller_code, --效果已删除的挂接关系未统计
  case when t1.module ='效果' then rtb_seller_name else cpc_direct_sales_name end as rtb_seller_name,
  case when t1.module ='效果' then v_seller_dept1_name 
  when t1.module ='薯条' then coalesce(cpc_direct_sales_dept1_name,cpc_operator_dept1_name) else cpc_direct_sales_dept1_name end as v_seller_dept1_name,
  case when t1.module ='效果' then v_seller_dept2_name 
  when t1.module ='薯条' then coalesce(cpc_direct_sales_dept2_name,cpc_operator_dept2_name) else cpc_direct_sales_dept2_name end as v_seller_dept2_name,
  case when t1.module ='效果' then v_seller_dept3_name 
  when t1.module ='薯条' then coalesce(cpc_direct_sales_dept3_name,cpc_operator_dept3_name)
  else cpc_direct_sales_dept3_name end as v_seller_dept3_name,
  case when t1.module ='效果' then v_seller_dept4_name 
  when t1.module ='薯条' then coalesce(cpc_direct_sales_dept4_name,cpc_operator_dept4_name)
  else cpc_direct_sales_dept4_name end as v_seller_dept4_name,
  case when t1.module ='效果' then v_seller_dept5_name 
  when t1.module ='薯条' then coalesce(cpc_direct_sales_dept5_name,cpc_operator_dept5_name)
  else cpc_direct_sales_dept5_name end as v_seller_dept5_name,
  case when t1.module ='效果' then v_seller_dept6_name 
  when t1.module ='薯条' then coalesce(cpc_direct_sales_dept6_name,cpc_operator_dept6_name)
  else cpc_direct_sales_dept6_name end as v_seller_dept6_name,
  rtb_advertiser_id,
  avg_qtd_cost as dim_avg_qtd_cost,
    avg_ytd_cost as dim_avg_ytd_cost,
    case when first_pass_time<'2019-01-01' or first_pass_time>'{{ds}}' then null else first_pass_time end as first_pass_time,
  ag.agent_user_name,
  first_group_name as agent_type,
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
  ce.direct_sales_third_dept_name,
  case when t1.module ='效果' then rtb_seller_dept_name else account.direct_sales_dept_name end as rtb_seller_dept_name,
  case when t1.module ='效果' then rtb_operator_dept_name else account.operator_dept_name end as rtb_operator_dept_name,
  case when t1.module ='效果' then rtb_seller_dept_code else account.direct_sales_dept_code end as rtb_seller_dept_code,
  case when t1.module ='效果' then rtb_operator_dept_code else account.operator_dept_code end as rtb_operator_dept_code,
  sales_system,
  brand_account_level,
  first_industry_name,
  second_industry_name,
  cost_campaign_cnt,--在投计划数
  cost_income_amt,
  budget_income_amt,
  deal_gmv,
  rtb.first_ad_industry_name,
  rtb.second_ad_industry_name,
  case when t1.module ='效果' then rtb_operator_name else cpc_operator_name end as rtb_operator_name,
  spu_cash_income_amt,
  spu_bind_cash_income_amt,
  spu_other_cash_income_amt,
  company_first_ads_rtb_cost_date,
  first_group_name,
  second_group_name,
  third_group_name,
  gp,
  brand_channel_sales_name,
  cost.ads_cash_cost_180d,
  cost.ads_cash_cost_120d,
  primary_channel_code,
  primary_channel_name,
    platform,
  cost_creativity_num,
  new_cost_creativity_num,
  new_cost_campaign_num,
  campaign_num,
  cost_campaign_num,
  create_campaign_num,
  create_cost_campaign_num,
  effecient_campaign_num,
  rtb_note_num,
  rtb_cost_note_num,
  new_cost_note_num,
  launch_finish_campaign_num,
  min_advertiser_budget_income_amt,
  advertiser_cost_income_amt,
  msg_income_cnt,
  msg_income_user_cnt,
  msg_open_user_cnt,
  msg_leads_user_cnt,
  valid_leads_num,
  msg_first_15s_reply_cnt,
  msg_first_15s_cnt,
  msg_driving_open_wait_duration,
  msg_driving_open_30s_cnt,
  msg_driving_open_60s_cnt,
  msg_driving_open_effi_cnt,
  live_reserve_cnt,
  live_subscribe_user_cnt,
  live_comment_cnt,
  live_watch_num,
  live_effective_shutdown_num,
  live_effective_shutdown_distinct_eventid_num,
  live_watch_duration,
  live_watch_distinct_eventid_num,
  live_follow_num,
  enter_seller_cnt_7d,
  goods_view_cnt_7d,
  add_cart_cnt_7d,
  search_component_click_cnt
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
    platform,
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
    sum(income_amt) as income_amt,
    sum(cost_campaign_cnt) as cost_campaign_cnt,
    sum(cost_income_amt) as cost_income_amt,
    sum(budget_income_amt) as budget_income_amt,
    sum(dgmv) as deal_gmv,
    sum(spu_cash_income_amt) as spu_cash_income_amt,
    sum(spu_bind_cash_income_amt) as  spu_bind_cash_income_amt,
    sum(spu_other_cash_income_amt) as spu_other_cash_income_amt,
    sum(cost_creativity_num) as cost_creativity_num,
      sum(new_cost_creativity_num) as   new_cost_creativity_num,
      sum(new_cost_campaign_num) as   new_cost_campaign_num,
      sum(campaign_num) as   campaign_num,
      sum(cost_campaign_num) as   cost_campaign_num,
      sum(create_campaign_num) as   create_campaign_num,
      sum(create_cost_campaign_num) as   create_cost_campaign_num,
      sum(effecient_campaign_num) as   effecient_campaign_num,
      sum(rtb_note_num) as   rtb_note_num,
      sum(rtb_cost_note_num) as   rtb_cost_note_num,
      sum(new_cost_note_num) as   new_cost_note_num,
      sum(launch_finish_campaign_num) as   launch_finish_campaign_num,
      sum(min_advertiser_budget_income_amt) as   min_advertiser_budget_income_amt,
      sum(advertiser_cost_income_amt) as   advertiser_cost_income_amt,
      sum(msg_income_cnt) as   msg_income_cnt,
      sum(msg_income_user_cnt) as   msg_income_user_cnt,
      sum(msg_open_user_cnt) as   msg_open_user_cnt,
      sum(msg_leads_user_cnt) as   msg_leads_user_cnt,
      sum(valid_leads_num) as   valid_leads_num,
      sum(msg_first_15s_reply_cnt) as   msg_first_15s_reply_cnt,
      sum(msg_first_15s_cnt) as   msg_first_15s_cnt,
      sum(msg_driving_open_wait_duration) as   msg_driving_open_wait_duration,
      sum(msg_driving_open_30s_cnt) as   msg_driving_open_30s_cnt,
      sum(msg_driving_open_60s_cnt) as   msg_driving_open_60s_cnt,
      sum(msg_driving_open_effi_cnt) as   msg_driving_open_effi_cnt,
      sum(live_reserve_cnt) as   live_reserve_cnt,
      sum(live_subscribe_user_cnt) as   live_subscribe_user_cnt,
      sum(live_comment_cnt) as   live_comment_cnt,
      sum(live_watch_num) as   live_watch_num,
      sum(live_effective_shutdown_num) as   live_effective_shutdown_num,
      sum(live_effective_shutdown_distinct_eventid_num) as   live_effective_shutdown_distinct_eventid_num,
      sum(live_watch_duration) as   live_watch_duration,
      sum(live_watch_distinct_eventid_num) as   live_watch_distinct_eventid_num,
      sum(live_follow_num) as   live_follow_num,
      sum(enter_seller_cnt_7d) as   enter_seller_cnt_7d,
      sum(goods_view_cnt_7d) as   goods_view_cnt_7d,
      sum(add_cart_cnt_7d) as   add_cart_cnt_7d,
      sum(search_component_click_cnt) as   search_component_click_cnt
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
    platform,
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
    deal_order_rgmv as deal_rgmv_7d,
    0 as cash_income_amt,
    0 as income_amt,
    cost_campaign_cnt as cost_campaign_cnt,
    cost_income_amt,
    budget_income_amt,
    deal_gmv as dgmv,
    0 as spu_cash_income_amt,
    0 as  spu_bind_cash_income_amt,
    0 as spu_other_cash_income_amt,
    cost_creativity_num,
  new_cost_creativity_num,
  new_cost_campaign_num,
  campaign_num,
  cost_campaign_num,
  create_campaign_num,
  create_cost_campaign_num,
  effecient_campaign_num,
  rtb_note_num,
  rtb_cost_note_num,
  new_cost_note_num,
  launch_finish_campaign_num,
  min_advertiser_budget_income_amt,
  advertiser_cost_income_amt,
  msg_income_cnt,
  msg_income_user_cnt,
  msg_open_user_cnt,
  msg_leads_user_cnt,
  valid_leads_num,
  msg_first_15s_reply_cnt,
  msg_first_15s_cnt,
  msg_driving_open_wait_duration,
  msg_driving_open_30s_cnt,
  msg_driving_open_60s_cnt,
  msg_driving_open_effi_cnt,
  live_reserve_cnt,
  live_subscribe_user_cnt,
  live_comment_cnt,
  live_watch_num,
  live_effective_shutdown_num,
  live_effective_shutdown_distinct_eventid_num,
  live_watch_duration,
  live_watch_distinct_eventid_num,
  live_follow_num,
  enter_seller_cnt_7d,
  goods_view_cnt_7d,
  add_cart_cnt_7d,
  search_component_click_cnt
    from  temp.temp_dm_ads_industry_account_agent_detail_{{ds_nodash}} t1
    left join redcdm.dim_ads_advertiser_df t2 
    on t1.v_seller_id=t2.virtual_seller_id and t2.dtm='{{ds_nodash}}'
    full outer
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
        platform,
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
        income_amt,
        cost_campaign_cnt,
        0 as cost_income_amt,
        0 as budget_income_amt,
        0 as dgmv,
        spu_cash_income_amt,
        spu_bind_cash_income_amt,
        spu_other_cash_income_amt,
        0 as cost_creativity_num,
      0 as new_cost_creativity_num,
      0 as new_cost_campaign_num,
      0 as campaign_num,
      0 as cost_campaign_num,
      0 as create_campaign_num,
      0 as create_cost_campaign_num,
      0 as effecient_campaign_num,
      0 as rtb_note_num,
      0 as rtb_cost_note_num,
      0 as new_cost_note_num,
      0 as launch_finish_campaign_num,
      0 as min_advertiser_budget_income_amt,
      0 as advertiser_cost_income_amt,
      0 as msg_income_cnt,
      0 as msg_income_user_cnt,
      0 as msg_open_user_cnt,
      0 as msg_leads_user_cnt,
      0 as valid_leads_num,
      0 as msg_first_15s_reply_cnt,
      0 as msg_first_15s_cnt,
      0 as msg_driving_open_wait_duration,
      0 as msg_driving_open_30s_cnt,
      0 as msg_driving_open_60s_cnt,
      0 as msg_driving_open_effi_cnt,
      0 as live_reserve_cnt,
      0 as live_subscribe_user_cnt,
      0 as live_comment_cnt,
      0 as live_watch_num,
      0 as live_effective_shutdown_num,
      0 as live_effective_shutdown_distinct_eventid_num,
      0 as live_watch_duration,
      0 as live_watch_distinct_eventid_num,
      0 as live_follow_num,
      0 as enter_seller_cnt_7d,
      0 as goods_view_cnt_7d,
      0 as add_cart_cnt_7d,
      0 as search_component_click_cnt
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
    promotion_target,
    platform
)t1
left join 
--首次广告投放时间
(select brand_account_id,
      min(first_ads_cost_date) as first_ads_cost_date,
      min(company_first_ads_cost_date) as company_first_ads_cost_date,
  min(company_first_ads_rtb_cost_date) as company_first_ads_rtb_cost_date
from redapp.app_ads_industry_product_account_agent_cost_detail_td_df
where  dtm = '{{ds_nodash}}'
group by brand_account_id
)cost_type2
on cost_type2.brand_account_id=t1.brand_account_id 
-- left join 
-- --coalesce(company_code,brand_account_id)首次投放时间
-- (select company_id,
--    min(company_first_ads_cost_date) as company_first_ads_cost_date
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
  dtm = '{{ds_nodash}}'
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
  brand_account_level,
  first_industry_name,
  second_industry_name,
  direct_sales_dept_name,
  operator_dept_name,
  direct_sales_dept_code,
  operator_dept_code,
  case when cpc_direct_sales_dept2_name<>'行业团队' then cpc_direct_sales_dept2_code 
  when cpc_direct_sales_dept2_name='行业团队' and cpc_direct_sales_dept3_name<>'生活服务行业' then concat_ws('-',coalesce(cpc_direct_sales_dept3_code,''),coalesce(cpc_direct_sales_dept4_code,''))
  when cpc_direct_sales_dept3_name='生活服务行业' and coalesce(cpc_direct_sales_dept5_code,'') not in ('fb6affc249da4bb3a141d58362a6f451','3cedc77e9eac4c30985fcc830cd2a45b') then concat_ws('-',coalesce(cpc_direct_sales_dept3_code,''),coalesce(cpc_direct_sales_dept4_code,''),coalesce(cpc_direct_sales_dept5_code,''))
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
  (SELECT
  virtual_seller_id,
  rtb_seller_dept_name,
  rtb_operator_dept_name,
  rtb_seller_dept_code,
  rtb_operator_dept_code,
  first_ad_industry_name,
  second_ad_industry_name,
  rtb_operator_code,
  rtb_operator_name
FROM
  ads_data_crm.dim_ads_crm_virtual_seller_id_info_df
WHERE
  dtm = '{{ds_nodash}}' and state=1
  )vseller 
  on vseller.virtual_seller_id=t1.virtual_seller_id
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
where dtm ='{{ds_nodash}}'
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
-- left join 
-- --代理商
-- (select agent_user_id, max(agent_type) as agent_type --防止主键重复
-- from reddm.dm_ads_crm_advertiser_income_wide_day 
-- where dtm='{{ds_nodash}}' and agent_user_id is not null 
-- group by 1
-- )agent 
-- on agent.agent_user_id=t1.agent_user_id
left join 
(select date_key,
  brand_account_id,
  agent_user_id,
  agent_name,
  first_group_name,
  second_group_name,
  third_group_name,
  gp,
  brand_channel_sales_name,
  -- ads_cash_cost_180d,
  -- ads_cash_cost_120d,
  primary_channel_code,
  primary_channel_name
from
  (select date_key,
    brand_account_id,
    agent_user_id,
    agent_name,
    first_group_name,
    second_group_name,
    third_group_name,
    gp,
    brand_channel_sales_name,
    ads_cash_cost_180d,
    ads_cash_cost_120d,
    primary_channel_code,
    primary_channel_name,
    row_number()over(partition by date_key,brand_account_id,agent_user_id order by if(first_group_name ='渠道清退',1,2) desc) as rn--重复值处理排序键
    
  from redapp.app_ads_insight_channel_agent_info_df 
  where dtm=max_dtm('redapp.app_ads_insight_channel_agent_info_df')
  )t
where rn = 1
)agent
on t1.date_key = agent.date_key and t1.brand_account_id=agent.brand_account_id and t1.agent_user_id=agent.agent_user_id
left join 
(select date_key,
    company_code,
    max(ads_cash_cost_180d) as ads_cash_cost_180d,
    max(ads_cash_cost_120d) as ads_cash_cost_120d
  from redapp.app_ads_insight_channel_agent_info_df 
  where dtm=max_dtm('redapp.app_ads_insight_channel_agent_info_df')
  group by 1,2
  )cost 
  on cost.date_key = t1.date_key and cost.company_code=account.company_code
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
and match_type in (2)
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
  concat_ws('-',cpc_direct_sales_dept3_code,cpc_direct_sales_dept4_code,cpc_direct_sales_dept5_code,cpc_direct_sales_dept6_code)  as code, --生活服务行业五部（医美医疗金融行业）需要看到六级做赛道划分
  is_valid
from ads_data_crm.app_app_ads_sales_org_mapping_df
where dtm=max_dtm('ads_data_crm.app_app_ads_sales_org_mapping_df')
and is_valid=1
and cpc_direct_sales_dept2_name='行业团队'
and match_type in (3)
group by 
  direct_sales_first_dept_name,
  direct_sales_second_dept_name,
  direct_sales_third_dept_name,
 concat_ws('-',cpc_direct_sales_dept3_code,cpc_direct_sales_dept4_code,cpc_direct_sales_dept5_code,cpc_direct_sales_dept6_code),
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
left join 
--sales_system 
(select  date_key,coalesce(virtual_seller_id,'') as virtual_seller_id,brand_user_id,module,max(sales_system) as sales_system
from reddm.dm_ads_crm_advertiser_income_wide_day 
where dtm='{{ds_nodash}}'
group by 1,2,3,4
)sys 
on sys.date_key=t1.date_key 
  and coalesce(sys.virtual_seller_id,'')=coalesce(t1.virtual_seller_id,'') 
  and coalesce(sys.brand_user_id,'')=coalesce(t1.brand_account_id,'')  
  and sys.module=t1.module
left join
(SELECT
  brand_account_id,
  first_ad_industry_name,
  second_ad_industry_name
  -- direct_sales_dept_name,
  -- operator_dept_name,
  -- direct_sales_dept_code,
  -- operator_dept_code
FROM
  redapp.app_ads_insight_industry_account_df
WHERE
   dtm = '{{ds_nodash}}' 
  ) rtb on  t1.brand_account_id= rtb.brand_account_id

