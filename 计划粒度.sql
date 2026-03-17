
--     -- ************************************************
--     -- Author: chenmengdi
--     -- CreateTime:2023-11-18T17:53:58+08:00
--     -- Update: Task Update Description
--     -- ************************************************
--   -- SET "kyuubi.spark.option.--conf spark.sql.crossJoin.enabled=true";
-- -- SET "kyuubi.spark.option.--conf spark.redExtension.allowBroadcastNestedLoopJoin=true";
-- drop table
--   if exists temp.dm_ads_rtb_advertiser_metric_hi_01{{ds_nodash}}_{{ts[11: 13]}}_campaign;

-- create table
--   temp.dm_ads_rtb_advertiser_metric_hi_01{{ds_nodash}}_{{ts[11: 13]}}_campaign
-- select
--   ca.hh,
--   ca.id as campaign_id,
--   v_seller_id as virtual_seller_id,
--   advertiser_id,
--   case
--     when placement = 1 then '信息流'
--     when placement = 2 then '搜索'
--     when placement = 4 then '全站智投'
--     when placement = 7 then '视频内流'
--   end as product,
--   bidding_strategy,
--   bid_type,
--   optimize_target,
--   marketing_target,
--   case
--     when state = 1
--     and enable = 1
--     and budget_state = 1
--     and a.id is not null then 1
--     else 0
--   end as campaign_status,
--   campaign_day_budget as campaign_day_budget,
--   limit_day_budget as limit_day_budget --0不限1设限
-- from
--   (
--     select
--       advertiser_id,
--       v_seller_id,
--       enable,
--       budget_state,
--       state,
--       id,
--       limit_day_budget,
--       campaign_day_budget,
--       placement,
--       bidding_strategy,
--       bid_type,
--       optimize_target,
--       marketing_target,
--       hh
--     from
--       redods.ods_shequ_feed_ads_t_ads_rtb_campaign_hf
--     where
--       dtm = '{{ds_nodash}}'
--       and hh = (
--         select
--           max(hh)
--         from
--           redods.ods_shequ_feed_ads_t_ads_rtb_campaign_hf
--         where
--           dtm = '{{ds_nodash}}'
--       )
--       and placement in (1, 2, 4, 7) -- and state = 1
--       -- and enable = 1
--       -- and budget_state = 1
--   ) ca
--   left join (
--     select
--       id
--     from
--       redods.ods_shequ_feed_ads_t_advertiser_hf
--     where
--       dtm = '{{ds_nodash}}'
--       and hh = (
--         select
--           max(hh)
--         from
--           redods.ods_shequ_feed_ads_t_advertiser_hf
--         where
--           dtm = '{{ds_nodash}}'
--       )
--       and state = 1
--       and budget_state = 1
--       and balance_state = 1
--   ) a on ca.advertiser_id = a.id;

-- drop table
--   if exists temp.dm_ads_rtb_advertiser_metric_hi_03{{ds_nodash}}_{{ts[11: 13]}}_campaign;

-- create table
--   temp.dm_ads_rtb_advertiser_metric_hi_03{{ds_nodash}}_{{ts[11: 13]}}_campaign --计划日预算
-- select
--   tag,
--   campaign_id,
--   advertiser_id,
--   product,
--   market_target,
--   hh,
--   case
--     when hh = '23' then null
--     else substring(
--       from_unixtime(
--         unix_timestamp(concat('{{ds}}', ' ', hh, ':00:00')) + 3600
--       ),
--       12,
--       2
--     )
--   end as after_hh,
--   coalesce(
--     sum(
--       case
--         when dtm = '{{ds_nodash}}' then min_budget
--         else 0
--       end
--     ),
--     0
--   ) as account_day_budget,
--   coalesce(
--     sum(
--       case
--         when dtm = '{{ds_nodash}}' then advertiser_budget
--         else 0
--       end
--     ),
--     0
--   ) as advertiser_budget,
--   coalesce(
--     sum(
--       case
--         when dtm = '{{ds_nodash}}' then ystd_advertiser_budget
--         else 0
--       end
--     ),
--     0
--   ) as ystd_advertiser_budget,
--   coalesce(
--     sum(
--       case
--         when dtm = '{{ds_1_days_ago_nodash}}' then min_budget
--         else 0
--       end
--     ),
--     0
--   ) as ystd_account_day_budget
-- from
--   redcdm.dwd_ads_rtb_campaign_advertiser_budget_hf t0
-- where
--   dtm <= '{{ds_nodash}}'
--   and dtm >= '{{ds_1_days_ago_nodash}}'
--   and hh <= (
--     select
--       max(hh)
--     from
--       redcdm.dwd_ads_rtb_campaign_advertiser_budget_hf
--     where
--       dtm = '{{ds_nodash}}'
--   )
--   and (tag in (4))
-- group by
--   tag,
--   campaign_id,
--   advertiser_id,
--   product,
--   market_target,
--   hh,
--   case
--     when hh = '23' then null
--     else substring(
--       from_unixtime(
--         unix_timestamp(concat('{{ds}}', ' ', hh, ':00:00')) + 3600
--       ),
--       12,
--       2
--     )
--   end;

-- drop table
--   if exists temp.advertiser_info_alias{{ds_nodash}}_{{ts[11: 13]}}_campaign;

-- create table
--   temp.advertiser_info_alias{{ds_nodash}}_{{ts[11: 13]}}_campaign -- 账户信息
-- select
--   virtual_seller_id,
--   virtual_seller_name,
--   t1.brand_account_id,
--   brand_user_name,
--   advertiser_id,
--   brand_tag_code,
--   brand_tag_name,
--   company_code,
--   company_name,
--   agent_user_id,
--   agent_user_name,
--   agent_company_code,
--   agent_company_name,
--   first_industry_name,
--   second_industry_name,
--   track_group_id,
--   track_group_name,
--   track_industry_name,
--   track_detail_name,
--   direct_sales_dept3_name,
--   direct_sales_dept4_name,
--   direct_sales_dept5_name,
--   direct_sales_dept6_name,
--   brand_group_tag_code,
--   brand_group_tag_name,
--   direct_sales_name,
--   cpc_operator_name,
--   case
--     when track_detail_name = '其他'
--     and direct_sales_dept4_name = '美妆洗护行业' then '美妆'
--     when track_detail_name = '其他'
--     and direct_sales_dept4_name = '奢品行业' then '奢品'
--     when track_detail_name = '其他'
--     and direct_sales_dept4_name = '服饰潮流行业' then '服饰潮流'
--     when track_detail_name = '其他' then '暂无赛道行业'
--     else track_industry_name
--   end as process_track_industry_name,
--   case
--     when track_detail_name = '其他' then '暂无一级赛道'
--     else track_group_name
--   end as process_track_group_name,
--   case
--     when track_detail_name = '其他' then '暂无二级赛道'
--     ELSE split(track_detail_name, '-') [2]
--   end as process_track_second_name,
--   case
--     when track_detail_name = '其他' then '暂无三级赛道'
--     ELSE split(track_detail_name, '-') [3]
--   end as process_track_third_name
-- from
--   (
--     select
--       virtual_seller_id,
--       brand_user_id as brand_account_id,
--       --brand_user_name,
--       brand_virtual_seller_id,
--       sub_virtual_seller_name as virtual_seller_name,
--       agent_user_id,
--       agent_user_name,
--       agent_virtual_seller_id,
--       agent_company_code,
--       agent_company_name,
--       rtb_advertiser_id as advertiser_id
--     from
--       reddw.dw_ads_crm_advertiser_day t0
--     where
--       dtm = max_dtm('reddw.dw_ads_crm_advertiser_day')
--       and rtb_advertiser_id <> 0
--   ) t1
--   left join (
--     select
--       brand_account_id,
--       brand_user_name,
--       brand_tag_code,
--       brand_tag_name,
--       company_code,
--       company_name,
--       first_industry_name,
--       second_industry_name,
--       track_group_id,
--       track_group_name,
--       track_industry_name,
--       track_detail_name,
--       cpc_direct_sales_dept3_name as direct_sales_dept3_name,
--       cpc_direct_sales_dept4_name as direct_sales_dept4_name,
--       cpc_direct_sales_dept5_name as direct_sales_dept5_name,
--       cpc_direct_sales_dept6_name as direct_sales_dept6_name,
--       brand_group_tag_code,
--       brand_group_tag_name,
--       cpc_direct_sales_name as direct_sales_name,
--       cpc_operator_name
--     from
--       redcdm.dim_ads_industry_account_df
--     where
--       dtm = max_dtm('redcdm.dim_ads_industry_account_df')
--   ) t2 on t1.brand_account_id = t2.brand_account_id;

-- --20231117新增小时级互动转化
-- drop table
--   if exists temp.advertiser_engage_alias{{ds_nodash}}_{{ts[11: 13]}}_campaign;

-- create table
--   temp.advertiser_engage_alias{{ds_nodash}}_{{ts[11: 13]}}_campaign
-- select
--   campaign_id,
--   hh,
--   coalesce(sum(ystd_imp_cnt), 0) as ystd_imp_cnt,
--   coalesce(sum(ystd_click_cnt), 0) as ystd_click_cnt,
--   coalesce(sum(ystd_cost), 0) as ystd_cost,
--   coalesce(sum(before_imp_cnt), 0) as before_imp_cnt,
--   coalesce(sum(before_click_cnt), 0) as before_click_cnt,
--   coalesce(sum(before_cost), 0) as before_cost,
--   coalesce(sum(imp_cnt), 0) as imp_cnt,
--   coalesce(sum(click_cnt), 0) as click_cnt,
--   coalesce(sum(cost), 0) as cost,
--   coalesce(sum(like_cnt), 0) as like_cnt,
--   coalesce(sum(comment_cnt), 0) as comment_cnt,
--   coalesce(sum(share_cnt), 0) as share_cnt,
--   coalesce(sum(follow_cnt), 0) as follow_cnt,
--   coalesce(sum(collect_cnt), 0) as collect_cnt,
--   coalesce(sum(save_cnt), 0) as save_cnt,
--   coalesce(sum(screenshot_cnt), 0) as screenshot_cnt,
--   coalesce(sum(engage_cnt), 0) as engage_cnt,
--   coalesce(sum(add_cart_cnt), 0) as add_cart_cnt,
--   coalesce(sum(buy_now_cnt), 0) as buy_now_cnt,
--   coalesce(sum(goods_view_cnt), 0) as goods_view_cnt,
--   coalesce(sum(seller_view_cnt), 0) as seller_view_cnt,
--   coalesce(sum(rgmv), 0) as rgmv,
--   coalesce(sum(leads_cnt), 0) as leads_cnt,
--   coalesce(sum(valid_leads_cnt), 0) as valid_leads_cnt,
--   coalesce(sum(leads_success_cnt), 0) as leads_success_cnt,
--   coalesce(sum(leads_success_valid_cnt), 0) as leads_success_valid_cnt,
--   coalesce(sum(msg_num), 0) as msg_num,
--   coalesce(sum(msg_open_num), 0) as msg_open_num,
--   coalesce(sum(msg_driven_open_num), 0) as msg_driven_open_num,
--   coalesce(sum(live_24h_click_rgmv), 0) as live_24h_click_rgmv,
--   coalesce(sum(live_24h_click_effective_shutdown_num), 0) as live_24h_click_effective_shutdown_num,
--   coalesce(sum(all_24h_click_rgmv), 0) as all_24h_click_rgmv,
--   coalesce(sum(out_click_goods_view_pv_7d), 0) as out_click_goods_view_pv_7d,
--   coalesce(sum(out_click_rgmv_7d), 0) as out_click_rgmv_7d,
--   coalesce(sum(total_order_num), 0) as total_order_num,
--   coalesce(sum(presale_order_gmv_7d), 0) as presale_order_gmv_7d,
--   coalesce(sum(purchase_order_gmv_7d), 0) as purchase_order_gmv_7d,
--   coalesce(sum(search_after_read_num), 0) as search_after_read_num,
--   coalesce(sum(ystd_like_cnt), 0) as ystd_like_cnt,
--   coalesce(sum(ystd_comment_cnt), 0) as ystd_comment_cnt,
--   coalesce(sum(ystd_share_cnt), 0) as ystd_share_cnt,
--   coalesce(sum(ystd_follow_cnt), 0) as ystd_follow_cnt,
--   coalesce(sum(ystd_collect_cnt), 0) as ystd_collect_cnt,
--   coalesce(sum(ystd_save_cnt), 0) as ystd_save_cnt,
--   coalesce(sum(ystd_screenshot_cnt), 0) as ystd_screenshot_cnt,
--   coalesce(sum(ystd_engage_cnt), 0) as ystd_engage_cnt,
--   coalesce(sum(ystd_add_cart_cnt), 0) as ystd_add_cart_cnt,
--   coalesce(sum(ystd_buy_now_cnt), 0) as ystd_buy_now_cnt,
--   coalesce(sum(ystd_goods_view_cnt), 0) as ystd_goods_view_cnt,
--   coalesce(sum(ystd_seller_view_cnt), 0) as ystd_seller_view_cnt,
--   coalesce(sum(ystd_rgmv), 0) as ystd_rgmv,
--   coalesce(sum(ystd_leads_cnt), 0) as ystd_leads_cnt,
--   coalesce(sum(ystd_valid_leads_cnt), 0) as ystd_valid_leads_cnt,
--   coalesce(sum(ystd_leads_success_cnt), 0) as ystd_leads_success_cnt,
--   coalesce(sum(ystd_leads_success_valid_cnt), 0) as ystd_leads_success_valid_cnt,
--   coalesce(sum(ystd_msg_num), 0) as ystd_msg_num,
--   coalesce(sum(ystd_msg_open_num), 0) as ystd_msg_open_num,
--   coalesce(sum(ystd_msg_driven_open_num), 0) as ystd_msg_driven_open_num,
--   coalesce(sum(ystd_live_24h_click_rgmv), 0) as ystd_live_24h_click_rgmv,
--   coalesce(
--     sum(ystd_live_24h_click_effective_shutdown_num),
--     0
--   ) as ystd_live_24h_click_effective_shutdown_num,
--   coalesce(sum(ystd_all_24h_click_rgmv), 0) as ystd_all_24h_click_rgmv,
--   coalesce(sum(ystd_out_click_goods_view_pv_7d), 0) as ystd_out_click_goods_view_pv_7d,
--   coalesce(sum(ystd_out_click_rgmv_7d), 0) as ystd_out_click_rgmv_7d,
--   coalesce(sum(ystd_total_order_num), 0) as ystd_total_order_num,
--   coalesce(sum(ystd_presale_order_gmv_7d), 0) as ystd_presale_order_gmv_7d,
--   coalesce(sum(ystd_purchase_order_gmv_7d), 0) as ystd_purchase_order_gmv_7d,
--   coalesce(sum(ystd_search_after_read_num), 0) as ystd_search_after_read_num,
--   coalesce(sum(before_like_cnt), 0) as before_like_cnt,
--   coalesce(sum(before_comment_cnt), 0) as before_comment_cnt,
--   coalesce(sum(before_share_cnt), 0) as before_share_cnt,
--   coalesce(sum(before_follow_cnt), 0) as before_follow_cnt,
--   coalesce(sum(before_collect_cnt), 0) as before_collect_cnt,
--   coalesce(sum(before_save_cnt), 0) as before_save_cnt,
--   coalesce(sum(before_screenshot_cnt), 0) as before_screenshot_cnt,
--   coalesce(sum(before_engage_cnt), 0) as before_engage_cnt,
--   coalesce(sum(before_add_cart_cnt), 0) as before_add_cart_cnt,
--   coalesce(sum(before_buy_now_cnt), 0) as before_buy_now_cnt,
--   coalesce(sum(before_goods_view_cnt), 0) as before_goods_view_cnt,
--   coalesce(sum(before_seller_view_cnt), 0) as before_seller_view_cnt,
--   coalesce(sum(before_rgmv), 0) as before_rgmv,
--   coalesce(sum(before_leads_cnt), 0) as before_leads_cnt,
--   coalesce(sum(before_valid_leads_cnt), 0) as before_valid_leads_cnt,
--   coalesce(sum(before_leads_success_cnt), 0) as before_leads_success_cnt,
--   coalesce(sum(before_leads_success_valid_cnt), 0) as before_leads_success_valid_cnt,
--   coalesce(sum(before_msg_num), 0) as before_msg_num,
--   coalesce(sum(before_msg_open_num), 0) as before_msg_open_num,
--   coalesce(sum(before_msg_driven_open_num), 0) as before_msg_driven_open_num,
--   coalesce(sum(before_live_24h_click_rgmv), 0) as before_live_24h_click_rgmv,
--   coalesce(
--     sum(before_live_24h_click_effective_shutdown_num),
--     0
--   ) as before_live_24h_click_effective_shutdown_num,
--   coalesce(sum(before_all_24h_click_rgmv), 0) as before_all_24h_click_rgmv,
--   coalesce(sum(before_out_click_goods_view_pv_7d), 0) as before_out_click_goods_view_pv_7d,
--   coalesce(sum(before_out_click_rgmv_7d), 0) as before_out_click_rgmv_7d,
--   coalesce(sum(before_total_order_num), 0) as before_total_order_num,
--   coalesce(sum(before_presale_order_gmv_7d), 0) as before_presale_order_gmv_7d,
--   coalesce(sum(before_purchase_order_gmv_7d), 0) as before_purchase_order_gmv_7d,
--   coalesce(sum(before_search_after_read_num), 0) as before_search_after_read_num
-- from
--   redcdm.dm_ads_rtb_creativity_product_hi
-- where
--   dtm = '{{ds_nodash}}'
--   and hh <= '24'
-- group by
--   campaign_id,
--   hh;

insert overwrite table redcdm.dm_ads_rtb_campaign_metric_hf partition (dtm, hh)
select
  campaign_id,
  virtual_seller_id,
  advertiser_id,
  product,
  bidding_strategy,
  bid_type,
  optimize_target,
  marketing_target,
  case
    when marketing_target in (3, 8, 14, 15) then '闭环电商'
    when marketing_target = 13 then '非闭环电商'
    when marketing_target in (2, 5, 9) then '线索'
    when marketing_target not in (3, 8, 2, 5, 9, 13, 14, 15) then '种草'
  end as market_target_type,
  campaign_status,
  campaign_day_budget,
  limit_day_budget,
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
  cost_1h,
  cost_1d,
  budget_amt,
  process_track_industry_name,
  process_track_group_name,
  process_track_second_name,
  process_track_third_name,
  imp_cnt_1d,
  click_cnt_1d,
  rgmv_1d,
  purchase_order_gmv_7d_1d,
  out_click_rgmv_7d_1d,
  engage_cnt_1d,
  '{{ds_nodash}}' as dtm,
  hh
from
  (
    --计划预算
    select
      '{{ds_nodash}}' as dtm,
      t0.hh,
      t0.campaign_id,
      t0.virtual_seller_id,
      t0.advertiser_id,
      t0.product,
      t0.bidding_strategy,
      t0.bid_type,
      t0.optimize_target,
      t0.marketing_target,
      t0.campaign_status,
      t0.campaign_day_budget,
      t0.limit_day_budget,
      --0不限1设限
      sum(coalesce(if(t1.hh = t0.hh, cost, 0), 0)) as cost_1h,
      sum(coalesce(if(t1.hh <= t0.hh, cost, 0), 0)) as cost_1d,
      sum(
        case
          when t1.hh = t0.hh then account_day_budget
          else 0
        end
      ) as budget_amt,
      --新增互动转化
      sum(if(t1.hh <= t0.hh, imp_cnt, 0)) as imp_cnt_1d,
      sum(if(t1.hh <= t0.hh, click_cnt, 0)) as click_cnt_1d,
      sum(if(t1.hh <= t0.hh, rgmv, 0)) as rgmv_1d,
      sum(if(t1.hh <= t0.hh, purchase_order_gmv_7d, 0)) as purchase_order_gmv_7d_1d,
      sum(if(t1.hh <= t0.hh, out_click_rgmv_7d, 0)) as out_click_rgmv_7d_1d,
      sum(if(t1.hh <= t0.hh, engage_cnt, 0)) as engage_cnt_1d
    from
      temp.dm_ads_rtb_advertiser_metric_hi_01{{ds_nodash}}_{{ts[11: 13]}}_campaign t0
      left join temp.advertiser_engage_alias{{ds_nodash}}_{{ts[11: 13]}}_campaign t1 on t0.campaign_id = t1.campaign_id
      left join temp.dm_ads_rtb_advertiser_metric_hi_03{{ds_nodash}}_{{ts[11: 13]}}_campaign t2 on t0.campaign_id = t2.campaign_id
    group by
      t0.hh,
      t0.campaign_id,
      t0.advertiser_id,
      t0.product,
      t0.bidding_strategy,
      t0.bid_type,
      t0.optimize_target,
      t0.marketing_target,
      t0.campaign_status,
      t0.campaign_day_budget,
      t0.limit_day_budget,
      t0.virtual_seller_id
  ) t0
  left join temp.advertiser_info_alias{{ds_nodash}}_{{ts[11: 13]}}_campaign t1 on t0.virtual_seller_id = t1.virtual_seller_id
where
  t0.hh <= (select max(hh)  from redcdm.dwd_ads_rtb_campaign_advertiser_budget_hf  where dtm = '{{ds_nodash}}' )