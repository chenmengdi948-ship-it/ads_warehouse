
    -- ************************************************
    -- Author: chenmengdi
    -- CreateTime:2023-11-18T17:53:58+08:00
    -- Update: Task Update Description
    -- ************************************************
  -- SET "kyuubi.spark.option.--conf spark.sql.crossJoin.enabled=true";
-- SET "kyuubi.spark.option.--conf spark.redExtension.allowBroadcastNestedLoopJoin=true";
drop table
  if exists temp.dm_ads_rtb_advertiser_metric_hi_01{{ds_nodash}}_{{ts[11: 13]}}_creativity;

create table
  temp.dm_ads_rtb_advertiser_metric_hi_01{{ds_nodash}}_{{ts[11: 13]}}_creativity
select
   un.id as creativity_id,
    un.name as creativity_name,
    un.state,
    un.enable,
    un.advertiser_id,
    un.campaign_id,
    un.unit_id,
    un.material_type,
    un.note_id,
    
    --un.audit_comment,
    un.create_time,
    un.modify_time,
    un.placement,
    un.first_jump_type,
    un.second_jump_type,
    un.balance_state,
    un.budget_state,
    un.time_state,
    un.start_time,
    un.expire_time,
    un.valid,
    --un.latest_audit_time,
    un.hh,
    un.v_seller_id as virtual_seller_id,
    case
      when un.placement = 1 then '信息流'
      when un.placement = 2 then '搜索'
      when un.placement = 4 then '全站智投'
      when un.placement = 7 then '视频内流'
    end as product,
    ca.optimize_target,
    ca.marketing_target,
  
  case when audit_status in (2,9) then 300 when audit_status in (1,4,5,6,7) then 100 else 200 end as audit_status,
  ca.campaign_name
from
  (
  select  id,
    name,
    state,
    enable,
    advertiser_id,
    campaign_id,
    unit_id,
    material_type,
    note_id,
    audit_status,
    audit_comment,
    brand_id_type,
    brand_id,
    create_audit,
    update_audit,
    from_unixtime(floor(create_time / 1000) + 28800) as create_time,
    from_unixtime(floor(modify_time / 1000) + 28800) as modify_time,
    v_seller_id,
    -- campaign_enable,
    -- unit_enable,
    placement,
    first_jump_type,
    second_jump_type,
    balance_state,
    budget_state,
    time_state,
    from_unixtime(floor(start_time / 1000) + 28800) as start_time,
    from_unixtime(floor(expire_time / 1000) + 28800) as expire_time,
    valid,
    latest_audit_time,
    hh
    from
      redods.ods_shequ_feed_ads_t_ads_creativity_hf
    where
      dtm = '{{ds_nodash}}'
      and hh = (
        select
          max(hh)
        from
          redods.ods_shequ_feed_ads_t_ads_creativity_hf
        where
          dtm = '{{ds_nodash}}'
      )
      and placement in (1, 2, 4, 7) -- and state = 1
      -- and enable = 1
      -- and budget_state = 1
  ) un
   join 
  (
    select
      advertiser_id,
      v_seller_id,
      -- enable,
      -- budget_state,
      -- state,
      id,
      -- limit_day_budget,
      -- campaign_day_budget,
      campaign_name,
      --placement,
      -- bidding_strategy,
      -- bid_type,
      optimize_target,
      marketing_target
    from
      redods.ods_shequ_feed_ads_t_ads_rtb_campaign_hf
    where
      dtm = '{{ds_nodash}}'
      and hh = (
        select
          max(hh)
        from
          redods.ods_shequ_feed_ads_t_ads_rtb_campaign_hf
        where
          dtm = '{{ds_nodash}}'
      )
      and placement in (1, 2, 4, 7) -- and state = 1
      -- and enable = 1
      -- and budget_state = 1
  ) ca
    on un.campaign_id = ca.id
  ;


drop table
  if exists temp.advertiser_info_alias{{ds_nodash}}_{{ts[11: 13]}}_creativity;

create table
  temp.advertiser_info_alias{{ds_nodash}}_{{ts[11: 13]}}_creativity -- 账户信息
select
  virtual_seller_id,
  virtual_seller_name,
  t1.brand_account_id,
  brand_user_name,
  advertiser_id,
  brand_tag_code,
  brand_tag_name,
  company_code,
  company_name,
  agent_user_id,
  agent_user_name,
  agent_company_code,
  agent_company_name,
  first_industry_name,
  second_industry_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_group_tag_code,
  brand_group_tag_name,
  direct_sales_name,
  cpc_operator_name,
  case
    when track_detail_name = '其他'
    and direct_sales_dept4_name = '美妆洗护行业' then '美妆'
    when track_detail_name = '其他'
    and direct_sales_dept4_name = '奢品行业' then '奢品'
    when track_detail_name = '其他'
    and direct_sales_dept4_name = '服饰潮流行业' then '服饰潮流'
    when track_detail_name = '其他' then '暂无赛道行业'
    else track_industry_name
  end as process_track_industry_name,
  case
    when track_detail_name = '其他' then '暂无一级赛道'
    else track_group_name
  end as process_track_group_name,
  case
    when track_detail_name = '其他' then '暂无二级赛道'
    ELSE split(track_detail_name, '-') [2]
  end as process_track_second_name,
  case
    when track_detail_name = '其他' then '暂无三级赛道'
    ELSE split(track_detail_name, '-') [3]
  end as process_track_third_name
from
  (
    select
      virtual_seller_id,
      brand_user_id as brand_account_id,
      --brand_user_name,
      brand_virtual_seller_id,
      sub_virtual_seller_name as virtual_seller_name,
      agent_user_id,
      agent_user_name,
      agent_virtual_seller_id,
      agent_company_code,
      agent_company_name,
      rtb_advertiser_id as advertiser_id
    from
      ads_data_crm.dim_ads_crm_virtual_seller_id_info_df t0
    where
      dtm = max_dtm('ads_data_crm.dim_ads_crm_virtual_seller_id_info_df')
      and rtb_advertiser_id <> 0
  ) t1
  left join (
    select
      brand_account_id,
      brand_user_name,
      brand_tag_code,
      brand_tag_name,
      company_code,
      company_name,
      first_industry_name,
      second_industry_name,
      track_group_id,
      track_group_name,
      track_industry_name,
      track_detail_name,
      cpc_direct_sales_dept3_name as direct_sales_dept3_name,
      cpc_direct_sales_dept4_name as direct_sales_dept4_name,
      cpc_direct_sales_dept5_name as direct_sales_dept5_name,
      cpc_direct_sales_dept6_name as direct_sales_dept6_name,
      brand_group_tag_code,
      brand_group_tag_name,
      cpc_direct_sales_name as direct_sales_name,
      cpc_operator_name
    from
      redcdm.dim_ads_industry_account_df
    where
      dtm = max_dtm('redcdm.dim_ads_industry_account_df')
  ) t2 on t1.brand_account_id = t2.brand_account_id;

--20231117新增小时级互动转化
drop table
  if exists temp.advertiser_engage_alias{{ds_nodash}}_{{ts[11: 13]}}_creativity;

create table
  temp.advertiser_engage_alias{{ds_nodash}}_{{ts[11: 13]}}_creativity
select
  creativity_id,
  hh,
  coalesce(sum(ystd_imp_cnt), 0) as ystd_imp_cnt,
  coalesce(sum(ystd_click_cnt), 0) as ystd_click_cnt,
  coalesce(sum(ystd_cost), 0) as ystd_cost,
  coalesce(sum(before_imp_cnt), 0) as before_imp_cnt,
  coalesce(sum(before_click_cnt), 0) as before_click_cnt,
  coalesce(sum(before_cost), 0) as before_cost,
  coalesce(sum(imp_cnt), 0) as imp_cnt,
  coalesce(sum(click_cnt), 0) as click_cnt,
  coalesce(sum(cost), 0) as cost,
  coalesce(sum(like_cnt), 0) as like_cnt,
  coalesce(sum(comment_cnt), 0) as comment_cnt,
  coalesce(sum(share_cnt), 0) as share_cnt,
  coalesce(sum(follow_cnt), 0) as follow_cnt,
  coalesce(sum(collect_cnt), 0) as collect_cnt,
  coalesce(sum(save_cnt), 0) as save_cnt,
  coalesce(sum(screenshot_cnt), 0) as screenshot_cnt,
  coalesce(sum(engage_cnt), 0) as engage_cnt,
  coalesce(sum(add_cart_cnt), 0) as add_cart_cnt,
  coalesce(sum(buy_now_cnt), 0) as buy_now_cnt,
  coalesce(sum(goods_view_cnt), 0) as goods_view_cnt,
  coalesce(sum(seller_view_cnt), 0) as seller_view_cnt,
  coalesce(sum(rgmv), 0) as rgmv,
  coalesce(sum(leads_cnt), 0) as leads_cnt,
  coalesce(sum(valid_leads_cnt), 0) as valid_leads_cnt,
  coalesce(sum(leads_success_cnt), 0) as leads_success_cnt,
  coalesce(sum(leads_success_valid_cnt), 0) as leads_success_valid_cnt,
  coalesce(sum(msg_num), 0) as msg_num,
  coalesce(sum(msg_open_num), 0) as msg_open_num,
  coalesce(sum(msg_driven_open_num), 0) as msg_driven_open_num,
  coalesce(sum(live_24h_click_rgmv), 0) as live_24h_click_rgmv,
  coalesce(sum(live_24h_click_effective_shutdown_num), 0) as live_24h_click_effective_shutdown_num,
  coalesce(sum(all_24h_click_rgmv), 0) as all_24h_click_rgmv,
  coalesce(sum(out_click_goods_view_pv_7d), 0) as out_click_goods_view_pv_7d,
  coalesce(sum(out_click_rgmv_7d), 0) as out_click_rgmv_7d,
  coalesce(sum(total_order_num), 0) as total_order_num,
  coalesce(sum(presale_order_gmv_7d), 0) as presale_order_gmv_7d,
  coalesce(sum(purchase_order_gmv_7d), 0) as purchase_order_gmv_7d,
  coalesce(sum(search_after_read_num), 0) as search_after_read_num,
  coalesce(sum(ystd_like_cnt), 0) as ystd_like_cnt,
  coalesce(sum(ystd_comment_cnt), 0) as ystd_comment_cnt,
  coalesce(sum(ystd_share_cnt), 0) as ystd_share_cnt,
  coalesce(sum(ystd_follow_cnt), 0) as ystd_follow_cnt,
  coalesce(sum(ystd_collect_cnt), 0) as ystd_collect_cnt,
  coalesce(sum(ystd_save_cnt), 0) as ystd_save_cnt,
  coalesce(sum(ystd_screenshot_cnt), 0) as ystd_screenshot_cnt,
  coalesce(sum(ystd_engage_cnt), 0) as ystd_engage_cnt,
  coalesce(sum(ystd_add_cart_cnt), 0) as ystd_add_cart_cnt,
  coalesce(sum(ystd_buy_now_cnt), 0) as ystd_buy_now_cnt,
  coalesce(sum(ystd_goods_view_cnt), 0) as ystd_goods_view_cnt,
  coalesce(sum(ystd_seller_view_cnt), 0) as ystd_seller_view_cnt,
  coalesce(sum(ystd_rgmv), 0) as ystd_rgmv,
  coalesce(sum(ystd_leads_cnt), 0) as ystd_leads_cnt,
  coalesce(sum(ystd_valid_leads_cnt), 0) as ystd_valid_leads_cnt,
  coalesce(sum(ystd_leads_success_cnt), 0) as ystd_leads_success_cnt,
  coalesce(sum(ystd_leads_success_valid_cnt), 0) as ystd_leads_success_valid_cnt,
  coalesce(sum(ystd_msg_num), 0) as ystd_msg_num,
  coalesce(sum(ystd_msg_open_num), 0) as ystd_msg_open_num,
  coalesce(sum(ystd_msg_driven_open_num), 0) as ystd_msg_driven_open_num,
  coalesce(sum(ystd_live_24h_click_rgmv), 0) as ystd_live_24h_click_rgmv,
  coalesce(
    sum(ystd_live_24h_click_effective_shutdown_num),
    0
  ) as ystd_live_24h_click_effective_shutdown_num,
  coalesce(sum(ystd_all_24h_click_rgmv), 0) as ystd_all_24h_click_rgmv,
  coalesce(sum(ystd_out_click_goods_view_pv_7d), 0) as ystd_out_click_goods_view_pv_7d,
  coalesce(sum(ystd_out_click_rgmv_7d), 0) as ystd_out_click_rgmv_7d,
  coalesce(sum(ystd_total_order_num), 0) as ystd_total_order_num,
  coalesce(sum(ystd_presale_order_gmv_7d), 0) as ystd_presale_order_gmv_7d,
  coalesce(sum(ystd_purchase_order_gmv_7d), 0) as ystd_purchase_order_gmv_7d,
  coalesce(sum(ystd_search_after_read_num), 0) as ystd_search_after_read_num,
  coalesce(sum(before_like_cnt), 0) as before_like_cnt,
  coalesce(sum(before_comment_cnt), 0) as before_comment_cnt,
  coalesce(sum(before_share_cnt), 0) as before_share_cnt,
  coalesce(sum(before_follow_cnt), 0) as before_follow_cnt,
  coalesce(sum(before_collect_cnt), 0) as before_collect_cnt,
  coalesce(sum(before_save_cnt), 0) as before_save_cnt,
  coalesce(sum(before_screenshot_cnt), 0) as before_screenshot_cnt,
  coalesce(sum(before_engage_cnt), 0) as before_engage_cnt,
  coalesce(sum(before_add_cart_cnt), 0) as before_add_cart_cnt,
  coalesce(sum(before_buy_now_cnt), 0) as before_buy_now_cnt,
  coalesce(sum(before_goods_view_cnt), 0) as before_goods_view_cnt,
  coalesce(sum(before_seller_view_cnt), 0) as before_seller_view_cnt,
  coalesce(sum(before_rgmv), 0) as before_rgmv,
  coalesce(sum(before_leads_cnt), 0) as before_leads_cnt,
  coalesce(sum(before_valid_leads_cnt), 0) as before_valid_leads_cnt,
  coalesce(sum(before_leads_success_cnt), 0) as before_leads_success_cnt,
  coalesce(sum(before_leads_success_valid_cnt), 0) as before_leads_success_valid_cnt,
  coalesce(sum(before_msg_num), 0) as before_msg_num,
  coalesce(sum(before_msg_open_num), 0) as before_msg_open_num,
  coalesce(sum(before_msg_driven_open_num), 0) as before_msg_driven_open_num,
  coalesce(sum(before_live_24h_click_rgmv), 0) as before_live_24h_click_rgmv,
  coalesce(
    sum(before_live_24h_click_effective_shutdown_num),
    0
  ) as before_live_24h_click_effective_shutdown_num,
  coalesce(sum(before_all_24h_click_rgmv), 0) as before_all_24h_click_rgmv,
  coalesce(sum(before_out_click_goods_view_pv_7d), 0) as before_out_click_goods_view_pv_7d,
  coalesce(sum(before_out_click_rgmv_7d), 0) as before_out_click_rgmv_7d,
  coalesce(sum(before_total_order_num), 0) as before_total_order_num,
  coalesce(sum(before_presale_order_gmv_7d), 0) as before_presale_order_gmv_7d,
  coalesce(sum(before_purchase_order_gmv_7d), 0) as before_purchase_order_gmv_7d,
  coalesce(sum(before_search_after_read_num), 0) as before_search_after_read_num,
  coalesce(sum(before_out_click_rgmv_15d),0) as before_out_click_rgmv_15d,
  coalesce(sum(ystd_out_click_rgmv_15d),0) as ystd_out_click_rgmv_15d,
  coalesce(sum(out_click_rgmv_15d),0) as out_click_rgmv_15d
from
  redcdm.dm_ads_rtb_creativity_product_hi
where
  dtm = '{{ds_nodash}}'
  and hh <= '24'
group by
  creativity_id,
  hh;

insert overwrite table redcdm.dm_ads_rtb_creativity_metric_hf partition (dtm, hh)
select creativity_id,
    creativity_name,
    state,
    enable,
    t0.advertiser_id,
    campaign_id,
    campaign_name,
    unit_id,
    material_type,
    note_id,
    audit_status,
    --audit_comment,
    create_time,
    modify_time,
    placement,
    first_jump_type,
    second_jump_type,
    balance_state,
    budget_state,
    time_state,
    start_time,
    expire_time,
    valid,
   
    t0.virtual_seller_id,
    product,
    optimize_target,
    marketing_target,

 
  case
    when marketing_target in (3, 8, 14, 15) then '闭环电商'
    when marketing_target = 13 then '非闭环电商'
    when marketing_target in (2, 5, 9) then '线索'
    when marketing_target not in (3, 8, 2, 5, 9, 13, 14, 15) then '种草'
  end as market_target_type,

  virtual_seller_name,
  brand_account_id,
  brand_user_name,
  brand_tag_code,
  brand_tag_name,
  company_code,
  company_name,
  agent_user_id,
  agent_user_name,
  agent_company_code,
  agent_company_name,
  first_industry_name,
  second_industry_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_group_tag_code,
  brand_group_tag_name,
  direct_sales_name,
  cpc_operator_name,
  process_track_industry_name,
  process_track_group_name,
  process_track_second_name,
  process_track_third_name,
  title as note_name,
  cost_1h,
  cost_1d,
  before_cost_1d,
 
  imp_cnt_1d,
  click_cnt_1d,
  ystd_imp_cnt_1d,
  ystd_click_cnt_1d,
  before_imp_cnt_1d,
  before_click_cnt_1d,
  rgmv_1d,
  purchase_order_gmv_7d_1d,
  ystd_rgmv_1d,
  ystd_purchase_order_gmv_7d_1d,
  before_rgmv_1d,
  before_purchase_order_gmv_7d_1d,
  out_click_rgmv_7d_1d,
  engage_cnt_1d,
  before_engage_cnt_1d,
  ystd_engage_cnt_1d,
  out_click_rgmv_15d_1d,
  ystd_out_click_rgmv_15d_1d,
  before_out_click_rgmv_15d_1d,
 -- campaign_name,
  ystd_cost_1d,
  total_order_num_1d,
  before_total_order_num_1d,
  ystd_total_order_num_1d,

  seller_view_cnt_1d,
  before_seller_view_cnt_1d,
  ystd_seller_view_cnt_1d,
  '{{ds_nodash}}' as dtm,
  hh
from
  (
    
    select
      '{{ds_nodash}}' as dtm,
      t0.creativity_id,
      t0.creativity_name,
      t0.state,
      t0.enable,


      t0.campaign_name,
      t0.unit_id,
      t0.material_type,
      t0.note_id,
      t0.audit_status,
      --audit_comment,
      t0.create_time,
      t0.modify_time,
      t0.placement,
      t0.first_jump_type,
      t0.second_jump_type,
      t0.balance_state,
      t0.budget_state,
      t0.time_state,
      t0.start_time,
      t0.expire_time,
      t0.valid,

      t0.hh,
      t0.campaign_id,
      t0.virtual_seller_id,
      t0.advertiser_id,
      t0.product,

      t0.optimize_target,
      t0.marketing_target,
     
      
      --0不限1设限
      sum(coalesce(if(t1.hh = t0.hh, cost, 0), 0)) as cost_1h,
      sum(coalesce(if(t1.hh <= t0.hh, cost, 0), 0)) as cost_1d,
      sum(if(t1.hh <= t0.hh,before_cost,0)) as before_cost_1d,
      sum(
              coalesce(if(t1.hh <= t0.hh, ystd_cost, 0), 0)
            ) as ystd_cost_1d,
      --新增互动转化
      sum(if(t1.hh <= t0.hh, imp_cnt, 0)) as imp_cnt_1d,
      sum(if(t1.hh <= t0.hh, click_cnt, 0)) as click_cnt_1d,
      sum(if(t1.hh <= t0.hh,ystd_imp_cnt,0)) as ystd_imp_cnt_1d,
      sum(if(t1.hh <= t0.hh,ystd_click_cnt,0)) as ystd_click_cnt_1d,
      sum(if(t1.hh <= t0.hh,before_imp_cnt,0)) as before_imp_cnt_1d,
      sum(if(t1.hh <= t0.hh,before_click_cnt,0)) as before_click_cnt_1d,
      sum(if(t1.hh <= t0.hh, rgmv, 0)) as rgmv_1d,
      sum(if(t1.hh <= t0.hh, purchase_order_gmv_7d, 0)) as purchase_order_gmv_7d_1d,
      sum(if(t1.hh <= t0.hh,ystd_rgmv,0)) as ystd_rgmv_1d,
      sum(if(t1.hh <= t0.hh,ystd_purchase_order_gmv_7d,0)) as ystd_purchase_order_gmv_7d_1d,
      sum(if(t1.hh <= t0.hh,before_rgmv,0)) as before_rgmv_1d,
      sum(if(t1.hh <= t0.hh,before_purchase_order_gmv_7d,0)) as before_purchase_order_gmv_7d_1d,
      sum(if(t1.hh <= t0.hh, out_click_rgmv_7d, 0)) as out_click_rgmv_7d_1d,
      sum(if(t1.hh <= t0.hh, engage_cnt, 0)) as engage_cnt_1d,
      sum(if(t1.hh <= t0.hh,before_engage_cnt,0)) as before_engage_cnt_1d,
      sum(if(t1.hh <= t0.hh,ystd_engage_cnt,0)) as ystd_engage_cnt_1d,
      sum(if(t1.hh <= t0.hh,out_click_rgmv_15d,0)) as out_click_rgmv_15d_1d,
      sum(if(t1.hh <= t0.hh,before_out_click_rgmv_15d,0)) as before_out_click_rgmv_15d_1d,
      sum(if(t1.hh <= t0.hh,ystd_out_click_rgmv_15d,0)) as ystd_out_click_rgmv_15d_1d,

      sum(if(t1.hh <= t0.hh,total_order_num,0)) as total_order_num_1d,
      sum(if(t1.hh <= t0.hh,before_total_order_num,0)) as before_total_order_num_1d,
      sum(if(t1.hh <= t0.hh,ystd_total_order_num,0)) as ystd_total_order_num_1d,

      sum(if(t1.hh <= t0.hh,seller_view_cnt,0)) as seller_view_cnt_1d,
      sum(if(t1.hh <= t0.hh,before_seller_view_cnt,0)) as before_seller_view_cnt_1d,
      sum(if(t1.hh <= t0.hh,ystd_seller_view_cnt,0)) as ystd_seller_view_cnt_1d
    from
      temp.dm_ads_rtb_advertiser_metric_hi_01{{ds_nodash}}_{{ts[11: 13]}}_creativity t0
      left join temp.advertiser_engage_alias{{ds_nodash}}_{{ts[11: 13]}}_creativity t1 on t0.creativity_id = t1.creativity_id
    group by t0.creativity_id,
      t0.creativity_name,
      t0.state,
      t0.enable,


      t0.campaign_name,
      t0.unit_id,
      t0.material_type,
      t0.note_id,
      t0.audit_status,
      --audit_comment,
      t0.create_time,
      t0.modify_time,
      t0.placement,
      t0.first_jump_type,
      t0.second_jump_type,
      t0.balance_state,
      t0.budget_state,
      t0.time_state,
      t0.start_time,
      t0.expire_time,
      t0.valid,

      t0.hh,
      t0.campaign_id,
      t0.virtual_seller_id,
      t0.advertiser_id,
      t0.product,

      t0.optimize_target,
      t0.marketing_target
  ) t0
  left join temp.advertiser_info_alias{{ds_nodash}}_{{ts[11: 13]}}_creativity t1 on t0.virtual_seller_id = t1.virtual_seller_id
  left join 
  (select discovery_id,title
  from reddw.dw_soc_discovery_delta_7_day 
  where dtm=f_getdate('{{ds_nodash}}',-1)
  )t2 on t0.note_id =t2.discovery_id 
where
  t0.hh <= (select max(hh)  from redcdm.dwd_ads_rtb_campaign_advertiser_budget_hf  where dtm = '{{ds_nodash}}' )