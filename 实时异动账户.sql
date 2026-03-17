
SET "kyuubi.spark.option.--conf spark.sql.crossJoin.enabled=true";
SET "kyuubi.spark.option.--conf spark.redExtension.allowBroadcastNestedLoopJoin=true";
--全量子账户产品以及当前和上小时
drop table if exists temp.dm_ads_rtb_advertiser_metric_hi_01{{ds_nodash}}_{{ts[11:13]}}_001;
create table temp.dm_ads_rtb_advertiser_metric_hi_01{{ds_nodash}}_{{ts[11:13]}}_001
select
  virtual_seller_id,
  advertiser_id,
  hh,
  product,
  market_target_type
from
  (select virtual_seller_id,
  rtb_advertiser_id as advertiser_id
  from reddw.dw_ads_crm_advertiser_day
  where dtm =max_dtm('reddw.dw_ads_crm_advertiser_day') and rtb_advertiser_id <> 0
  ) t0
  left join (
    select
      hh,
      product,
      market_target_type
    from
      redcdm.dwd_ads_rtb_creativity_product_cost_hi
    where
      dtm = '{{ds_nodash}}'
    group by
      1,
      2,
      3
  ) t1
  on 1=1
;
drop table if exists temp.dm_ads_rtb_advertiser_metric_hi_02{{ds_nodash}}_{{ts[11:13]}}_001;
create table temp.dm_ads_rtb_advertiser_metric_hi_02{{ds_nodash}}_{{ts[11:13]}}_001
 --账户小时余额
  select
    subject_id as virtual_seller_id,
    hh,
    case when hh = '23' then null else substring(from_unixtime(unix_timestamp(concat('{{ds}}',' ',hh,':00:00'))+3600),12,2) end as after_hh,
    sum(case when dtm = '{{ds_nodash}}' then cast(available_balance as double) else 0 end) as total_balance,
    sum(case when dtm = '{{ds_1_days_ago_nodash}}' then cast(available_balance as double) else 0 end) as ystd_total_balance
  from
    redods.ods_gondar_base_account_hf
  where
    dtm <= '{{ds_nodash}}'
    and dtm >= '{{ds_1_days_ago_nodash}}'
    and hh <= '{{ts[11:13]}}'
    --and account_type='CASH' --有多种类型余额，如现金和授信，crm展示现金余额
  group by
    1,
    2,3
    ;
drop table if exists temp.dm_ads_rtb_advertiser_metric_hi_03{{ds_nodash}}_{{ts[11:13]}}_001;
create table temp.dm_ads_rtb_advertiser_metric_hi_03{{ds_nodash}}_{{ts[11:13]}}_001
 -- --账户离线日预算
  -- select granularity,
  --   advertiser_id,
  --   product,
  --   coalesce(sum(min_budget), 0) as account_day_budget,
  --   coalesce(sum(advertiser_actual_no_buffer_budget), 0) as advertiser_budget
  -- from
  --   redcdm.dm_ads_rtb_budget_1d_di t0
  -- where
  --   dtm = '{{ds_1_days_ago_nodash}}'
  --   and ((granularity = '分场域'
  --   and groups = 3) or granularity = '广告主粒度' )
  -- group by granularity,
  --   advertiser_id,
  --   product
  --账户日预算
  select tag,
    advertiser_id,
    product,
    market_target,
    hh,
    case when hh = '23' then null else substring(from_unixtime(unix_timestamp(concat('{{ds}}',' ',hh,':00:00'))+3600),12,2) end as after_hh,
    coalesce(sum(case when dtm = '{{ds_nodash}}' then min_budget else 0 end), 0) as account_day_budget,
    coalesce(sum(case when dtm = '{{ds_nodash}}' then advertiser_budget else 0 end), 0) as advertiser_budget,
    coalesce(sum(case when dtm = '{{ds_nodash}}' then ystd_advertiser_budget else 0 end), 0) as ystd_advertiser_budget,
    coalesce(sum(case when dtm = '{{ds_1_days_ago_nodash}}'  then min_budget else 0 end), 0) as ystd_account_day_budget
  from
    redcdm.dwd_ads_rtb_campaign_advertiser_budget_hf t0
  where
     dtm <= '{{ds_nodash}}'
    and dtm >= '{{ds_1_days_ago_nodash}}'
    and hh <= (select max(hh) from redcdm.dwd_ads_rtb_campaign_advertiser_budget_hf where dtm = '{{ds_nodash}}')
    and (tag in (1,2,3) )
  group by tag,
    advertiser_id,
    product,
    market_target,
    hh,
    case when hh = '23' then null else substring(from_unixtime(unix_timestamp(concat('{{ds}}',' ',hh,':00:00'))+3600),12,2) end
;
drop table if exists temp.dm_ads_rtb_advertiser_metric_hi_04{{ds_nodash}}_{{ts[11:13]}}_001;
create table temp.dm_ads_rtb_advertiser_metric_hi_04{{ds_nodash}}_{{ts[11:13]}}_001
select coalesce(t1.tag,t2.tag) as tag,
    coalesce(t1.advertiser_id,t2.advertiser_id) as advertiser_id,
    coalesce(t1.product,t2.product) as product,
    coalesce(t1.market_target,t2.market_target) as market_target,
    coalesce(t1.hh,t2.after_hh) as hh,
    t1.account_day_budget,
    t1.advertiser_budget,
    t1.ystd_advertiser_budget,
    t1.ystd_account_day_budget,
    t2.account_day_budget as account_day_budget_before_1d,
    t2.advertiser_budget as advertiser_budget_before_1d
from temp.dm_ads_rtb_advertiser_metric_hi_03{{ds_nodash}}_{{ts[11:13]}}_001 t1 
full outer join 
(select *
from temp.dm_ads_rtb_advertiser_metric_hi_03{{ds_nodash}}_{{ts[11:13]}}_001
where after_hh is not null
)t2 on t1.hh=t2.after_hh and t1.advertiser_id=t2.advertiser_id and t1.product=t2.product and t1.market_target=t2.market_target and t1.tag=t2.tag
where coalesce(t1.hh,t2.after_hh) = (select max(hh) from redcdm.dwd_ads_rtb_campaign_advertiser_budget_hf where dtm = '{{ds_nodash}}')
;
drop table if exists temp.advertiser_info_alias{{ds_nodash}}_{{ts[11:13]}}_001;
create table temp.advertiser_info_alias{{ds_nodash}}_{{ts[11:13]}}_001

  -- 账户信息
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
    case when track_detail_name='其他' and direct_sales_dept4_name='美妆洗护行业' then '美妆' 
          when track_detail_name='其他'  and direct_sales_dept4_name='奢品行业' then '奢品' 
          when track_detail_name='其他'  and direct_sales_dept4_name='服饰潮流行业' then '服饰潮流' 
          when track_detail_name='其他' then '暂无赛道行业' else track_industry_name end as process_track_industry_name,
     case when track_detail_name='其他' then '暂无一级赛道' else track_group_name end as process_track_group_name,
     case when track_detail_name='其他' then '暂无二级赛道' ELSE split(track_detail_name,'-')[2] end as process_track_second_name,
     case when track_detail_name='其他' then '暂无三级赛道' ELSE split(track_detail_name,'-')[3] end as process_track_third_name
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
        reddw.dw_ads_crm_advertiser_day t0
      where
        dtm  = max_dtm('reddw.dw_ads_crm_advertiser_day')
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
        dtm =  max_dtm('redcdm.dim_ads_industry_account_df')
    ) t2 on t1.brand_account_id = t2.brand_account_id
;
drop table if exists temp.advertiser_cost_alias{{ds_nodash}}_{{ts[11:13]}}_001;
create table temp.advertiser_cost_alias{{ds_nodash}}_{{ts[11:13]}}_001

  -- 小时消耗指标
  select
    advertiser_id,
    product,
    market_target_type,
    hh,
    coalesce(sum(cost_1h), 0) as cost_1h,
    coalesce(sum(yesterday_cost_1h), 0) as yesterday_cost_1h
  from
    redcdm.dwd_ads_rtb_creativity_product_cost_hi --adfulllog ,替换聚光待定
  where
    dtm = '{{ds_nodash}}'
    and hh <= '24'
  group by advertiser_id,
    product,
    market_target_type,
    hh
;
--20231117新增小时级互动转化
drop table if exists temp.advertiser_engage_alias{{ds_nodash}}_{{ts[11:13]}}_001;
create table temp.advertiser_engage_alias{{ds_nodash}}_{{ts[11:13]}}_001


  select
    advertiser_id,
    product,
    market_target_type,
    hh,
    coalesce(sum(ystd_imp_cnt),0) as ystd_imp_cnt,
     coalesce(sum(ystd_click_cnt),0) as ystd_click_cnt,
     coalesce(sum(ystd_cost),0) as ystd_cost,
     coalesce(sum(before_imp_cnt),0) as before_imp_cnt,
     coalesce(sum(before_click_cnt),0) as before_click_cnt,
     coalesce(sum(before_cost),0) as before_cost,
     coalesce(sum(imp_cnt),0) as imp_cnt,
     coalesce(sum(click_cnt),0) as click_cnt,
     coalesce(sum(cost),0) as cost,
     coalesce(sum(like_cnt),0) as like_cnt,
     coalesce(sum(comment_cnt),0) as comment_cnt,
     coalesce(sum(share_cnt),0) as share_cnt,
     coalesce(sum(follow_cnt),0) as follow_cnt,
     coalesce(sum(collect_cnt),0) as collect_cnt,
     coalesce(sum(save_cnt),0) as save_cnt,
     coalesce(sum(screenshot_cnt),0) as screenshot_cnt,
     coalesce(sum(engage_cnt),0) as engage_cnt,
     coalesce(sum(add_cart_cnt),0) as add_cart_cnt,
     coalesce(sum(buy_now_cnt),0) as buy_now_cnt,
     coalesce(sum(goods_view_cnt),0) as goods_view_cnt,
     coalesce(sum(seller_view_cnt),0) as seller_view_cnt,
     coalesce(sum(rgmv),0) as rgmv,
     coalesce(sum(leads_cnt),0) as leads_cnt,
     coalesce(sum(valid_leads_cnt),0) as valid_leads_cnt,
     coalesce(sum(leads_success_cnt),0) as leads_success_cnt,
     coalesce(sum(leads_success_valid_cnt),0) as leads_success_valid_cnt,
     coalesce(sum(msg_num),0) as msg_num,
     coalesce(sum(msg_open_num),0) as msg_open_num,
     coalesce(sum(msg_driven_open_num),0) as msg_driven_open_num,
     coalesce(sum(live_24h_click_rgmv),0) as live_24h_click_rgmv,
     coalesce(sum(live_24h_click_effective_shutdown_num),0) as live_24h_click_effective_shutdown_num,
     coalesce(sum(all_24h_click_rgmv),0) as all_24h_click_rgmv,
     coalesce(sum(out_click_goods_view_pv_7d),0) as out_click_goods_view_pv_7d,
     coalesce(sum(out_click_rgmv_7d),0) as out_click_rgmv_7d,
     coalesce(sum(total_order_num),0) as total_order_num,
     coalesce(sum(presale_order_gmv_7d),0) as presale_order_gmv_7d,
     coalesce(sum(purchase_order_gmv_7d),0) as purchase_order_gmv_7d,
     coalesce(sum(search_after_read_num),0) as search_after_read_num,
     coalesce(sum(ystd_like_cnt),0) as ystd_like_cnt,
     coalesce(sum(ystd_comment_cnt),0) as ystd_comment_cnt,
     coalesce(sum(ystd_share_cnt),0) as ystd_share_cnt,
     coalesce(sum(ystd_follow_cnt),0) as ystd_follow_cnt,
     coalesce(sum(ystd_collect_cnt),0) as ystd_collect_cnt,
     coalesce(sum(ystd_save_cnt),0) as ystd_save_cnt,
     coalesce(sum(ystd_screenshot_cnt),0) as ystd_screenshot_cnt,
     coalesce(sum(ystd_engage_cnt),0) as ystd_engage_cnt,
     coalesce(sum(ystd_add_cart_cnt),0) as ystd_add_cart_cnt,
     coalesce(sum(ystd_buy_now_cnt),0) as ystd_buy_now_cnt,
     coalesce(sum(ystd_goods_view_cnt),0) as ystd_goods_view_cnt,
     coalesce(sum(ystd_seller_view_cnt),0) as ystd_seller_view_cnt,
     coalesce(sum(ystd_rgmv),0) as ystd_rgmv,
     coalesce(sum(ystd_leads_cnt),0) as ystd_leads_cnt,
     coalesce(sum(ystd_valid_leads_cnt),0) as ystd_valid_leads_cnt,
     coalesce(sum(ystd_leads_success_cnt),0) as ystd_leads_success_cnt,
     coalesce(sum(ystd_leads_success_valid_cnt),0) as ystd_leads_success_valid_cnt,
     coalesce(sum(ystd_msg_num),0) as ystd_msg_num,
     coalesce(sum(ystd_msg_open_num),0) as ystd_msg_open_num,
     coalesce(sum(ystd_msg_driven_open_num),0) as ystd_msg_driven_open_num,
     coalesce(sum(ystd_live_24h_click_rgmv),0) as ystd_live_24h_click_rgmv,
     coalesce(sum(ystd_live_24h_click_effective_shutdown_num),0) as ystd_live_24h_click_effective_shutdown_num,
     coalesce(sum(ystd_all_24h_click_rgmv),0) as ystd_all_24h_click_rgmv,
     coalesce(sum(ystd_out_click_goods_view_pv_7d),0) as ystd_out_click_goods_view_pv_7d,
     coalesce(sum(ystd_out_click_rgmv_7d),0) as ystd_out_click_rgmv_7d,
     coalesce(sum(ystd_total_order_num),0) as ystd_total_order_num,
     coalesce(sum(ystd_presale_order_gmv_7d),0) as ystd_presale_order_gmv_7d,
     coalesce(sum(ystd_purchase_order_gmv_7d),0) as ystd_purchase_order_gmv_7d,
     coalesce(sum(ystd_search_after_read_num),0) as ystd_search_after_read_num,
     coalesce(sum(before_like_cnt),0) as before_like_cnt,
     coalesce(sum(before_comment_cnt),0) as before_comment_cnt,
     coalesce(sum(before_share_cnt),0) as before_share_cnt,
     coalesce(sum(before_follow_cnt),0) as before_follow_cnt,
     coalesce(sum(before_collect_cnt),0) as before_collect_cnt,
     coalesce(sum(before_save_cnt),0) as before_save_cnt,
     coalesce(sum(before_screenshot_cnt),0) as before_screenshot_cnt,
     coalesce(sum(before_engage_cnt),0) as before_engage_cnt,
     coalesce(sum(before_add_cart_cnt),0) as before_add_cart_cnt,
     coalesce(sum(before_buy_now_cnt),0) as before_buy_now_cnt,
     coalesce(sum(before_goods_view_cnt),0) as before_goods_view_cnt,
     coalesce(sum(before_seller_view_cnt),0) as before_seller_view_cnt,
     coalesce(sum(before_rgmv),0) as before_rgmv,
     coalesce(sum(before_leads_cnt),0) as before_leads_cnt,
     coalesce(sum(before_valid_leads_cnt),0) as before_valid_leads_cnt,
     coalesce(sum(before_leads_success_cnt),0) as before_leads_success_cnt,
     coalesce(sum(before_leads_success_valid_cnt),0) as before_leads_success_valid_cnt,
     coalesce(sum(before_msg_num),0) as before_msg_num,
     coalesce(sum(before_msg_open_num),0) as before_msg_open_num,
     coalesce(sum(before_msg_driven_open_num),0) as before_msg_driven_open_num,
     coalesce(sum(before_live_24h_click_rgmv),0) as before_live_24h_click_rgmv,
     coalesce(sum(before_live_24h_click_effective_shutdown_num),0) as before_live_24h_click_effective_shutdown_num,
     coalesce(sum(before_all_24h_click_rgmv),0) as before_all_24h_click_rgmv,
     coalesce(sum(before_out_click_goods_view_pv_7d),0) as before_out_click_goods_view_pv_7d,
     coalesce(sum(before_out_click_rgmv_7d),0) as before_out_click_rgmv_7d,
     coalesce(sum(before_total_order_num),0) as before_total_order_num,
     coalesce(sum(before_presale_order_gmv_7d),0) as before_presale_order_gmv_7d,
     coalesce(sum(before_purchase_order_gmv_7d),0) as before_purchase_order_gmv_7d,
     coalesce(sum(before_search_after_read_num),0) as before_search_after_read_num
  from
    redcdm.dm_ads_rtb_creativity_product_hi
  where
    dtm = '{{ds_nodash}}'
    and hh <= '24'
  group by advertiser_id,
    product,
    market_target_type,
    hh
;
drop table if exists temp.advertiser_balance_alias{{ds_nodash}}_{{ts[11:13]}}_001;
create table temp.advertiser_balance_alias{{ds_nodash}}_{{ts[11:13]}}_001

select coalesce(t1.virtual_seller_id,t2.virtual_seller_id) as virtual_seller_id,
    coalesce(t1.hh,t2.after_hh) as hh,
    t1.total_balance,
    t1.ystd_total_balance as ystd_total_balance_1d,
    t2.total_balance as total_balance_before_1d
from temp.dm_ads_rtb_advertiser_metric_hi_02{{ds_nodash}}_{{ts[11:13]}}_001 t1 
full outer join 
(select *
from temp.dm_ads_rtb_advertiser_metric_hi_02{{ds_nodash}}_{{ts[11:13]}}_001
where after_hh is not null
)t2 on t1.hh=t2.after_hh and t1.virtual_seller_id=t2.virtual_seller_id
;
drop table if exists temp.dm_ads_rtb_advertiser_metric_hi_info{{ds_nodash}}_{{ts[11:13]}}_001;
create table temp.dm_ads_rtb_advertiser_metric_hi_info{{ds_nodash}}_{{ts[11:13]}}_001
select
  t0.virtual_seller_id,
  product,
  market_target_type,
  virtual_seller_name,
  advertiser_id,
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
  ystd_cost_1h,
  cost_1d,
  ystd_cost_1d,
  total_balance,
  budget_amt,
  ystd_budget_amt,
  process_track_industry_name,
  process_track_group_name,
  process_track_second_name,
  process_track_third_name,
  ystd_total_balance_1d,
  total_balance_before_1d,
  account_day_budget_before_1d as budget_amt_before_1d,
   ystd_imp_cnt_1d,
   ystd_click_cnt_1d,
   before_imp_cnt_1d,
   before_click_cnt_1d,
   before_cost_1d,
   imp_cnt_1d,
   click_cnt_1d,
   rgmv_1d,
   purchase_order_gmv_7d_1d,
   out_click_rgmv_7d_1d,
   engage_cnt_1d,
   ystd_rgmv_1d,
   ystd_purchase_order_gmv_7d_1d,
   ystd_out_click_rgmv_7d_1d,
   ystd_engage_cnt_1d,
   before_rgmv_1d,
   before_purchase_order_gmv_7d_1d,
   before_out_click_rgmv_7d_1d,
   before_engage_cnt_1d,
    '{{ds_nodash}}' as dtm,
    hh
from
  (
    select
      '{{ds_nodash}}' as dtm,
      t0.hh,
      t0.product,
      t0.market_target_type,
      t0.virtual_seller_id,
      sum(coalesce(if(t1.hh = t0.hh, cost, 0), 0)) as cost_1h,
      sum(
        coalesce(if(t1.hh = t0.hh, ystd_cost, 0), 0)
      ) as ystd_cost_1h,
      sum(coalesce(if(t1.hh <= t0.hh, cost, 0), 0)) as cost_1d,
      sum(
        coalesce(if(t1.hh <= t0.hh, ystd_cost, 0), 0)
      ) as ystd_cost_1d,
      sum(0) as total_balance,
      sum(0) as ystd_total_balance_1d,
      sum(0) as total_balance_before_1d,
      sum(case when t1.hh = t0.hh then account_day_budget else 0 end) as budget_amt,
      sum(case when t1.hh = t0.hh then ystd_account_day_budget else 0 end) as ystd_budget_amt,
      sum(case when t1.hh = t0.hh then account_day_budget_before_1d else 0 end) as account_day_budget_before_1d,
      --新增互动转化
      sum(if(t1.hh <= t0.hh,ystd_imp_cnt,0)) as ystd_imp_cnt_1d,
      sum(if(t1.hh <= t0.hh,ystd_click_cnt,0)) as ystd_click_cnt_1d,
      sum(if(t1.hh <= t0.hh,before_imp_cnt,0)) as before_imp_cnt_1d,
      sum(if(t1.hh <= t0.hh,before_click_cnt,0)) as before_click_cnt_1d,
      sum(if(t1.hh <= t0.hh,before_cost,0)) as before_cost_1d,
      sum(if(t1.hh <= t0.hh,imp_cnt,0)) as imp_cnt_1d,
      sum(if(t1.hh <= t0.hh,click_cnt,0)) as click_cnt_1d,
      sum(if(t1.hh <= t0.hh,rgmv,0)) as rgmv_1d,
      sum(if(t1.hh <= t0.hh,purchase_order_gmv_7d,0)) as purchase_order_gmv_7d_1d,
      sum(if(t1.hh <= t0.hh,out_click_rgmv_7d,0)) as out_click_rgmv_7d_1d,
      sum(if(t1.hh <= t0.hh,engage_cnt,0)) as engage_cnt_1d,
      sum(if(t1.hh <= t0.hh,ystd_rgmv,0)) as ystd_rgmv_1d,
      sum(if(t1.hh <= t0.hh,ystd_purchase_order_gmv_7d,0)) as ystd_purchase_order_gmv_7d_1d,
      sum(if(t1.hh <= t0.hh,ystd_out_click_rgmv_7d,0)) as ystd_out_click_rgmv_7d_1d,
      sum(if(t1.hh <= t0.hh,ystd_engage_cnt,0)) as ystd_engage_cnt_1d,
      sum(if(t1.hh <= t0.hh,before_rgmv,0)) as before_rgmv_1d,
      sum(if(t1.hh <= t0.hh,before_purchase_order_gmv_7d,0)) as before_purchase_order_gmv_7d_1d,
      sum(if(t1.hh <= t0.hh,before_out_click_rgmv_7d,0)) as before_out_click_rgmv_7d_1d,
      sum(if(t1.hh <= t0.hh,before_engage_cnt,0)) as before_engage_cnt_1d
    from
      temp.dm_ads_rtb_advertiser_metric_hi_01{{ds_nodash}}_{{ts[11:13]}}_001 t0
      inner join temp.advertiser_engage_alias{{ds_nodash}}_{{ts[11:13]}}_001 t1 on t0.advertiser_id = t1.advertiser_id
      and t0.product = t1.product
      and t0.market_target_type = t1.market_target_type
      left join 
      temp.dm_ads_rtb_advertiser_metric_hi_04{{ds_nodash}}_{{ts[11:13]}}_001 t2 on t0.advertiser_id = t2.advertiser_id
      and t0.product = t2.product
      and t0.market_target_type = t2.market_target
    group by
      1,
      2,
      3,
      4,
      5
    union all
    select
      '{{ds_nodash}}' as dtm,
      t0.hh,
      t0.product,
      t0.market_target_type,
      t0.virtual_seller_id,
      0 as cost_1h,
      0 as ystd_cost_1h,
      0 as cost_1d,
      0 as ystd_cost_1d,
      0 as total_balance,
      0 as ystd_total_balance_1d,
      0 as total_balance_before_1d,
      coalesce(account_day_budget, 0) as budget_amt,
      coalesce(ystd_account_day_budget,0) as ystd_budget_amt,
      coalesce(account_day_budget_before_1d,0) as account_day_budget_before_1d,
      0 as ystd_imp_cnt_1d,
      0 as ystd_click_cnt_1d,
      0 as before_imp_cnt_1d,
      0 as before_click_cnt_1d,
      0 as before_cost_1d,
      0 as imp_cnt_1d,
      0 as click_cnt_1d,
      0 as rgmv_1d,
      0 as purchase_order_gmv_7d_1d,
      0 as out_click_rgmv_7d_1d,
      0 as engage_cnt_1d,
      0 as ystd_rgmv_1d,
      0 as ystd_purchase_order_gmv_7d_1d,
      0 as ystd_out_click_rgmv_7d_1d,
      0 as ystd_engage_cnt_1d,
      0 as before_rgmv_1d,
      0 as before_purchase_order_gmv_7d_1d,
      0 as before_out_click_rgmv_7d_1d,
      0 as before_engage_cnt_1d
    from
      (
        select
          virtual_seller_id,
          advertiser_id,
          hh,
          product,
          '' as market_target_type
        from
          temp.dm_ads_rtb_advertiser_metric_hi_01{{ds_nodash}}_{{ts[11:13]}}_001
        group by
          1,
          2,
          3,
          4,
          5
      ) t0 --left join advertiser_balance_alias t1 on t0.virtual_seller_id = t1.virtual_seller_id and t0.hh = t1.hh
      left join temp.dm_ads_rtb_advertiser_metric_hi_04{{ds_nodash}}_{{ts[11:13]}}_001 t2 on t0.advertiser_id = t2.advertiser_id
      and t0.product = t2.product
      where tag = 2
    union all
    select
      '{{ds_nodash}}' as dtm,
      t0.hh,
      t0.product,
      t0.market_target_type,
      t0.virtual_seller_id,
      0 as cost_1h,
      0 as ystd_cost_1h,
      0 as cost_1d,
      0 as ystd_cost_1d,
      coalesce(total_balance, 0) as total_balance,
      coalesce(ystd_total_balance_1d, 0) as ystd_total_balance_1d,
      coalesce(total_balance_before_1d, 0) as total_balance_before_1d,
      coalesce(advertiser_budget, 0) as budget_amt,
      coalesce(ystd_advertiser_budget, 0) as ystd_budget_amt,
      coalesce(advertiser_budget_before_1d, 0) as account_day_budget_before_1d,
      0 as ystd_imp_cnt_1d,
      0 as ystd_click_cnt_1d,
      0 as before_imp_cnt_1d,
      0 as before_click_cnt_1d,
      0 as before_cost_1d,
      0 as imp_cnt_1d,
      0 as click_cnt_1d,
      0 as rgmv_1d,
      0 as purchase_order_gmv_7d_1d,
      0 as out_click_rgmv_7d_1d,
      0 as engage_cnt_1d,
      0 as ystd_rgmv_1d,
      0 as ystd_purchase_order_gmv_7d_1d,
      0 as ystd_out_click_rgmv_7d_1d,
      0 as ystd_engage_cnt_1d,
      0 as before_rgmv_1d,
      0 as before_purchase_order_gmv_7d_1d,
      0 as before_out_click_rgmv_7d_1d,
      0 as before_engage_cnt_1d
    from
      (
        select
          virtual_seller_id,
          advertiser_id,
          hh,
          '' as product,
          '' as market_target_type
        from
          temp.dm_ads_rtb_advertiser_metric_hi_01{{ds_nodash}}_{{ts[11:13]}}_001 t0
        group by
          1,
          2,
          3,
          4,
          5
      ) t0
      left join temp.advertiser_balance_alias{{ds_nodash}}_{{ts[11:13]}}_001 t1 on t0.virtual_seller_id = t1.virtual_seller_id
      and t0.hh = t1.hh
      left join 
      (select *
      from temp.dm_ads_rtb_advertiser_metric_hi_04{{ds_nodash}}_{{ts[11:13]}}_001 
      where tag = 3
      )t2 on t0.advertiser_id = t2.advertiser_id
  ) t0
  left join temp.advertiser_info_alias{{ds_nodash}}_{{ts[11:13]}}_001 t1 on t0.virtual_seller_id = t1.virtual_seller_id
  where t0.hh<=(select max(hh) from temp.advertiser_balance_alias{{ds_nodash}}_{{ts[11:13]}}_001)
  ;
insert
  overwrite table redcdm_dev.dm_ads_rtb_advertiser_metric_hi partition (dtm, hh)
select virtual_seller_id,
  t1.product,
  t1.market_target_type,
  virtual_seller_name,
  t1.advertiser_id,
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
  ystd_cost_1h,
  cost_1d,
  ystd_cost_1d,
  total_balance,
  budget_amt,
  ystd_budget_amt,
  t1.process_track_industry_name,
  t1.process_track_group_name,
  t1.process_track_second_name,
  t1.process_track_third_name,
  ystd_total_balance_1d,
  total_balance_before_1d,
  budget_amt_before_1d,
  track_budget_amt,
  track_cost_1d,
  track_group_budget_amt,
  track_group_cost_1d,
  valid_campaign_cnt,
  campaign_day_budget,
  limit_day_budget,
  campaign_day_budget as campaign_budget,
   ystd_imp_cnt_1d,
 ystd_click_cnt_1d,
 before_imp_cnt_1d,
 before_click_cnt_1d,
 before_cost_1d,
 imp_cnt_1d,
 click_cnt_1d,
 rgmv_1d,
 purchase_order_gmv_7d_1d,
 out_click_rgmv_7d_1d,
 engage_cnt_1d,
 ystd_rgmv_1d,
 ystd_purchase_order_gmv_7d_1d,
 ystd_out_click_rgmv_7d_1d,
 ystd_engage_cnt_1d,
 before_rgmv_1d,
 before_purchase_order_gmv_7d_1d,
 before_out_click_rgmv_7d_1d,
 before_engage_cnt_1d,
  dtm,
  t1.hh
from temp.dm_ads_rtb_advertiser_metric_hi_info{{ds_nodash}}_{{ts[11:13]}}_001 t1
left join 
(select process_track_industry_name,
     product,
     market_target_type,
     hh,
     sum(budget_amt) as track_budget_amt,
     sum(cost_1d) as track_cost_1d
from temp.dm_ads_rtb_advertiser_metric_hi_info{{ds_nodash}}_{{ts[11:13]}}_001
group by hh,process_track_industry_name,
     product,
     market_target_type
)t2 on t1.process_track_industry_name=t2.process_track_industry_name and t1.product=t2.product and t1.market_target_type=t2.market_target_type and t1.hh=t2.hh
left join 
(select process_track_group_name,
     product,
     market_target_type,
     hh,
     sum(budget_amt) as track_group_budget_amt,
     sum(cost_1d) as track_group_cost_1d
from temp.dm_ads_rtb_advertiser_metric_hi_info{{ds_nodash}}_{{ts[11:13]}}_001
group by hh,process_track_group_name,
     product,
     market_target_type
)t3 on t1.process_track_group_name=t3.process_track_group_name and t1.product=t3.product and t1.market_target_type=t3.market_target_type and t1.hh=t3.hh
left join 
--有效计划
(--计划预算
select
  advertiser_id,
  hh,
  '' as product,
  '' as market_target_type,
  valid_campaign_cnt,
  case
    when limit_day_budget = 0
    or valid_campaign_cnt = 0 then 0
    else campaign_day_budget
  end as campaign_day_budget,
  limit_day_budget
from
  (
    select ca.hh,
      ca.advertiser_id,
      count(
        case
          when ca.enable = 1
          and budget_state = 1
          and a.id is not null then ca.id
          else null
        end
      ) as valid_campaign_cnt,
      sum(campaign_day_budget) as campaign_day_budget,
      min(limit_day_budget) as limit_day_budget --0不限1设限
    from
      (
        select
          advertiser_id,
          enable,
          budget_state,
          id,
          limit_day_budget,
          campaign_day_budget,
          hh
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
          and placement in (1, 2, 4, 7)
          and state = 1
          and enable = 1
          and budget_state = 1
      ) ca
      left join (
        select
          id
        from
          redods.ods_shequ_feed_ads_t_advertiser_hf
        where
          dtm = '{{ds_nodash}}'
          and hh = (
            select
              max(hh)
            from
              redods.ods_shequ_feed_ads_t_advertiser_hf
            where
              dtm = '{{ds_nodash}}'
          )
          and state = 1
          and budget_state = 1
          and balance_state = 1
      ) a on ca.advertiser_id = a.id
    group by ca.hh,
      ca.advertiser_id
  ) base
  )camp 
on camp.advertiser_id=t1.advertiser_id and t1.product=camp.product and t1.market_target_type=camp.market_target_type and t1.hh=camp.hh

----------------草稿fullog----------------
select
  creative_id as creativity_id,
  case
    when placement = 1 then '信息流'
    when placement = 2 then '搜索'
    when placement = 4 then '全站智投'
    when placement = 7 then '视频内流'
  end as product,
  campaign_id,
  unit_id,
  advertiser_id,
  marketing_target,
  optimize_target,
  case
    when marketing_target in (3, 8, 14,15) then '闭环电商'
    when marketing_target = 13 then '非闭环电商'
    when marketing_target in (2, 5, 9) then '线索'
    when marketing_target not in (3, 8, 2, 5, 9, 13, 14,15) then '种草'
  end as market_target_type,
   '{{ds_nodash}}' as dtm,
  --hh,
  sum(if(dtm = '{{ds_nodash}}'  and coalesce(fee, 0) > 0, fee, 0)) / 100.0 as cost_1h,
  sum(if(dtm = '{{ds_1_days_ago_nodash}}'  and coalesce(fee, 0) > 0, fee, 0)) / 100.0 as yesterday_cost_1h,
  sum(case when dtm = '{{ds_nodash}}'  and coalesce(is_deduplicate,0)=0 and action='impression' and imp_cnt>0 then imp_cnt else 0 end) as imp_cnt,
  sum(case when dtm = '{{ds_nodash}}'  and coalesce(is_deduplicate,0)=0 and click_cnt>0 then click_cnt else 0 end) as click_cnt,
  sum(case when dtm = '{{ds_1_days_ago_nodash}}'   and coalesce(is_deduplicate,0)=0 and action='impression' and imp_cnt>0 then imp_cnt else 0 end) as yesterday_imp_cnt,
  sum(case when dtm ='{{ds_1_days_ago_nodash}}'   and coalesce(is_deduplicate,0)=0 and click_cnt>0 then click_cnt else 0 end) as yesterday_click_cnt,
  sum(case when dtm = '{{ds_nodash}}'  and coalesce(is_deduplicate,0)=0 and refer_time_interval <= 3600*1000 and like_cnt<>0 then like_cnt else 0 end) as like_cnt,--点赞:like_cnt
 sum(case when dtm = '{{ds_nodash}}'  and coalesce(is_deduplicate,0)=0 and  refer_time_interval <= 3600*1000  and collect_cnt<>0 then collect_cnt else 0 end) as fav_cnt,--收藏:collect_cnt
 sum(case when dtm = '{{ds_nodash}}'  and coalesce(is_deduplicate,0)=0 and  refer_time_interval <= 3600*1000  and share_cnt<>0 then share_cnt else 0 end) as share_cnt,--分享:share_cnt
 sum(case when dtm = '{{ds_nodash}}'  and coalesce(is_deduplicate,0)=0 and  refer_time_interval <= 3600*1000  and follow_cnt<>0 then follow_cnt else 0 end) as follow_cnt,--关注:follow_cnt
 sum(case when dtm = '{{ds_nodash}}'  and coalesce(is_deduplicate,0)=0 and  refer_time_interval <= 3600*1000  and comment_cnt<>0 then comment_cnt else 0 end) as cmt_cnt,--评论:comment_cnt
 sum(case when dtm = '{{ds_nodash}}'  and coalesce(is_deduplicate,0)=0 and  refer_time_interval <= 3600*1000  and screenshot_cnt<>0 then screenshot_cnt else 0 end) as screenshot_cnt,--截屏:screenshot_cnt
 sum(case when dtm = '{{ds_nodash}}'  and coalesce(is_deduplicate,0)=0 and  refer_time_interval <= 3600*1000  and save_cnt<>0 then save_cnt else 0 end) as save_cnt,--保存图片:save_cnt
   sum(case when dtm =  '{{ds_1_days_ago_nodash}}' and coalesce(is_deduplicate,0)=0 and refer_time_interval <= 3600*1000 and like_cnt<>0 then like_cnt else 0 end) as yesterday_like_cnt,--点赞:like_cnt
sum(case when dtm =  '{{ds_1_days_ago_nodash}}'   and coalesce(is_deduplicate,0)=0 and  refer_time_interval <= 3600*1000  and collect_cnt<>0 then collect_cnt else 0 end) as yesterday_fav_cnt,--收藏:collect_cnt
 sum(case when dtm =  '{{ds_1_days_ago_nodash}}'   and coalesce(is_deduplicate,0)=0 and  refer_time_interval <= 3600*1000  and share_cnt<>0 then share_cnt else 0 end) as yesterday_share_cnt,--分享:share_cnt
 sum(case when dtm =  '{{ds_1_days_ago_nodash}}'  and coalesce(is_deduplicate,0)=0 and  refer_time_interval <= 3600*1000  and follow_cnt<>0 then follow_cnt else 0 end) as yesterday_follow_cnt,--关注:follow_cnt
 sum(case when dtm = '{{ds_1_days_ago_nodash}}'   and coalesce(is_deduplicate,0)=0 and  refer_time_interval <= 3600*1000  and comment_cnt<>0 then comment_cnt else 0 end) as yesterday_cmt_cnt,--评论:comment_cnt
 sum(case when dtm = '{{ds_1_days_ago_nodash}}'   and coalesce(is_deduplicate,0)=0 and  refer_time_interval <= 3600*1000  and screenshot_cnt<>0 then screenshot_cnt else 0 end) as yesterday_screenshot_cnt,--截屏:screenshot_cnt
 sum(case when dtm =  '{{ds_1_days_ago_nodash}}'   and coalesce(is_deduplicate,0)=0 and  refer_time_interval <= 3600*1000  and save_cnt<>0 then save_cnt else 0 end) as yesterday_save_cnt--保存图片:save_cnt
from
  hive_prod.redods.ad_full_log
where
  dtm <= '{{ds_nodash}}'
  and dtm >= '{{ds_1_days_ago_nodash}}'
  and hh <= '24'
  and conversion_type = 'ENGAGEMENT'
  and (
    spam_level is null
    or spam_level = 0
  )
  and placement in (1, 2, 4, 7)
 
group by
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,9





  select
  --creative_id as creativity_id,
   module,
  -- campaign_id,
  -- unit_id,
  -- advertiser_id,
  marketing_target,
  optimize_target,
  dtm,
   case
    when marketing_target in (3, 8, 14,15) then '闭环电商'
    when marketing_target = 13 then '非闭环电商'
    when marketing_target in (2, 5, 9) then '线索'
    when marketing_target not in (3, 8, 2, 5, 9, 13, 14,15) then '种草'
  end as market_target_type,
  sum(imp_num) as imp_num,
  sum(click_num) as click_num,
  sum(like_num) as like_num,
  sum(fav_num) as fav_num,
  sum(comment_num) as comment_num,
  sum(share_num) as share_num,
  sum(follow_num) as follow_num
 
from redst.st_ads_wide_cpc_creativity_day_inc 
where dtm='{{ds_nodash}}'
group by  module,
  -- campaign_id,
  -- unit_id,
  -- advertiser_id,
  marketing_target,
  optimize_target,
  dtm,case
    when marketing_target in (3, 8, 14,15) then '闭环电商'
    when marketing_target = 13 then '非闭环电商'
    when marketing_target in (2, 5, 9) then '线索'
    when marketing_target not in (3, 8, 2, 5, 9, 13, 14,15) then '种草'
  end 