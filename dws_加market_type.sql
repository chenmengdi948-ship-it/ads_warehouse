with bcoo_note as
(
  -- 提升时效，从reddm.dm_soc_brand_coo_order_note_detail_day 抠出来的口径，预计202304月份会有base层的模型，需要替换上线
  select 
     t3.order_id
    ,t3.note_id
    ,t3.task_no
    ,a.report_brand_user_id
  from(
    select 
     order_id
     ,note_id
     ,task_no
    from reddw.dw_soc_tb_order_detail_day
    where dtm = '{{ds_nodash}}' and order_status in(401,402) and order_category not in(1,3) --非共创
    group by 1,2,3

    union all

     select
           t2.order_id
        ,t2.note_id
        ,t1.task_no
      from reddw.dw_soc_brand_coo_crowd_creation_task_day t1
      left join reddw.dw_soc_brand_coo_crowd_creation_note_day t2 on t1.task_no=t2.task_no and t2.dtm='{{ds_nodash}}' and t2.del=0
      where t1.dtm='{{ds_nodash}}' 
      and t1.task_status not in ('501','502','503') 
      and t1.task_title not like '%测试%'
      and t2.publish_mark=1
      group by 1,2,3  

  )t3
  left join
  (
  select order_id,
    report_brand_user_id
  from redods.ods_note_trade_tb_order
  WHERE dtm='{{ds_nodash}}'
  ) a
  on t3.order_id = a.order_id
  where note_id is not null and note_id<>'' and note_id<>' '
  group by t3.order_id
    ,t3.note_id
    ,t3.task_no
    ,a.report_brand_user_id
)
insert overwrite table redcdm_dev.dws_ads_industry_product_account_1d_di partition(dtm = '{{ ds_nodash }}')
select
  date_key
  ,module
  ,product
  ,brand_account_id
  ,sum(a.imp_cnt) as imp_cnt
  ,sum(a.click_cnt) as click_cnt
  ,sum(a.like_cnt) as like_cnt
  ,sum(a.fav_cnt) as fav_cnt
  ,sum(a.cmt_cnt) as cmt_cnt
  ,sum(a.share_cnt) as share_cnt
  ,sum(a.follow_cnt) as follow_cnt
  ,sum(a.mkt_ecm_cost) as mkt_ecm_cost
  ,sum(a.mkt_leads_cost) as mkt_leads_cost
  ,sum(a.mkt_zc_cost) as mkt_zc_cost
  ,sum(a.open_sale_num) as open_sale_num
  ,sum(a.campaign_cnt) as campaign_cnt
  ,sum(a.unit_cnt) as unit_cnt
  ,sum(a.brand_campaign_cnt) as brand_campaign_cnt
  ,sum(a.cpc_cost_budget_rate) as cpc_cost_budget_rate
  ,sum(a.cpc_budget) as cpc_budget
  ,sum(a.mkt_ecm_cash_cost) as mkt_ecm_cash_cost
  ,sum(a.mkt_leads_cash_cost) as mkt_leads_cash_cost
  ,sum(a.mkt_zc_cash_cost) as mkt_zc_cash_cost
  ,sum(a.mkt_ecm_direct_cost) as mkt_ecm_direct_cost
  ,sum(a.mkt_leads_direct_cost) as mkt_leads_direct_cost
  ,sum(a.mkt_zc_direct_cost) as mkt_zc_direct_cost
  ,sum(a.mkt_ecm_direct_cash_cost) as mkt_ecm_direct_cash_cost
  ,sum(a.mkt_leads_direct_cash_cost) as mkt_leads_direct_cash_cost
  ,sum(a.mkt_zc_direct_cash_cost) as mkt_zc_direct_cash_cost
  ,sum(a.mkt_ecm_channel_cost) as mkt_ecm_channel_cost
  ,sum(a.mkt_leads_channel_cost) as mkt_leads_channel_cost
  ,sum(a.mkt_zc_channel_cost) as mkt_zc_channel_cost
  ,sum(a.mkt_ecm_channel_cash_cost) as mkt_ecm_channel_cash_cost
  ,sum(a.mkt_leads_channel_cash_cost) as mkt_leads_channel_cash_cost
  ,sum(a.mkt_zc_channel_cash_cost) as mkt_zc_channel_cash_cost
  ,market_target
from (
  -- 品牌广告
  select
    '{{ds}}' as date_key
    ,'品牌' as module
    ,case 
      when a.ads_container = 'app_open' then '开屏' 
      when a.ads_container = 'search_third' then '搜索第三位' 
      when a.ads_container = 'feed' then '信息流GD' 
      when a.ads_container = 'search_brand_area' then '品牌专区' 
      -- when a.ads_container = 'fire_topic' then '火焰话题' 
      else '品牌其他' 
    end as product
    ,a.advertiser_id as brand_account_id
    ,'整体' as market_target
    ,sum(a.imp_num) as imp_cnt
    ,sum(a.click_num) as click_cnt
    ,sum(a.like_num) as like_cnt
    ,sum(a.fav_num) as fav_cnt
    ,sum(a.cmt_num) as cmt_cnt
    ,sum(a.share_num) as share_cnt
    ,sum(a.follow_num) as follow_cnt
    ,0 as mkt_ecm_cost
    ,0 as mkt_leads_cost
    ,0 as mkt_zc_cost
    ,0 as open_sale_num
    ,count(distinct campaign_id) as campaign_cnt
    ,count(distinct unit_id) as unit_cnt
    ,max(coalesce(b.campaign_cnt,0)) as brand_campaign_cnt
    ,0 as cpc_cost_budget_rate
    ,0 as cpc_budget
    ,0 as mkt_ecm_cash_cost
    ,0 as mkt_leads_cash_cost
    ,0 as mkt_zc_cash_cost
    ,0 as mkt_ecm_direct_cost
    ,0 as mkt_leads_direct_cost
    ,0 as mkt_zc_direct_cost
    ,0 as mkt_ecm_direct_cash_cost
    ,0 as mkt_leads_direct_cash_cost
    ,0 as mkt_zc_direct_cash_cost
    ,0 as mkt_ecm_channel_cost
    ,0 as mkt_leads_channel_cost
    ,0 as mkt_zc_channel_cost
    ,0 as mkt_ecm_channel_cash_cost
    ,0 as mkt_leads_channel_cash_cost
    ,0 as mkt_zc_channel_cash_cost
  from 
    redst.st_ads_brand_creativity_loc_metrics_day_inc a
  left join (
    select
       advertiser_id as brand_account_id
      ,count(distinct campaign_id) as campaign_cnt
    from 
      redst.st_ads_brand_creativity_loc_metrics_day_inc
    where
      dtm = '{{ds_nodash}}'
    group by
      1
  ) b on a.advertiser_id = b.brand_account_id
  join 
    reddw.dw_ads_account_day c on c.dtm = '{{ds_nodash}}' and a.advertiser_id = c.user_id and coalesce(c.company_name,'') <> 'offlineMockCompanyName' --剔除内广
  where
    a.dtm = '{{ds_nodash}}'
  group by 
    3,4
  union all 
  -- 开屏售卖轮次
  select
    '{{ds}}' as date_key
    ,'品牌' as module
    ,'开屏' as product
    ,brand_account_id
    ,'整体' as market_target
    ,0 as imp_cnt
    ,0 as click_cnt
    ,0 as like_cnt
    ,0 as fav_cnt
    ,0 as cmt_cnt
    ,0 as share_cnt
    ,0 as follow_cnt
    ,0 as mkt_ecm_cost
    ,0 as mkt_leads_cost
    ,0 as mkt_zc_cost  
    ,sum(
      case
        when unit_type = 'CPM' then ads_num * 1000
        when unit_type = '天/轮' then ads_num * 10000000
        when unit_type = '天/半轮' then ads_num * 5000000
        else ads_num end
    ) / 10000000  as open_sale_num
    ,0 as campaign_cnt
    ,0 as unit_cnt
    ,0 as brand_campaign_cnt
    ,0 as cpc_cost_budget_rate
    ,0 as cpc_budget
    ,0 as mkt_ecm_cash_cost
    ,0 as mkt_leads_cash_cost
    ,0 as mkt_zc_cash_cost
    ,0 as mkt_ecm_direct_cost
    ,0 as mkt_leads_direct_cost
    ,0 as mkt_zc_direct_cost
    ,0 as mkt_ecm_direct_cash_cost
    ,0 as mkt_leads_direct_cash_cost
    ,0 as mkt_zc_direct_cash_cost
    ,0 as mkt_ecm_channel_cost
    ,0 as mkt_leads_channel_cost
    ,0 as mkt_zc_channel_cost
    ,0 as mkt_ecm_channel_cash_cost
    ,0 as mkt_leads_channel_cash_cost
    ,0 as mkt_zc_channel_cash_cost
  from
    reddw.dw_ads_crm_brand_stats_day a 
  join 
    reddw.dw_ads_account_day b on b.dtm = '{{ds_nodash}}' and a.brand_account_id = b.user_id and coalesce(b.company_name,'') <> 'offlineMockCompanyName' --剔除内广
  where
    a.dtm = '{{ds_nodash}}'
    and a.product_name = '开屏'
    and coalesce(a.company_name,'') <> 'offlineMockCompanyName'
    and case when a.launch_start_date >= '2021-01-01' then a.settle_date else a.launch_start_date end = '{{ds}}'
  group by
    4
  union all 
  -- 效果
  select 
    '{{ds}}' as date_key
    ,'效果' as module
    ,case 
      when a.module = '发现feed' then '竞价-信息流' 
      when a.module = '搜索feed' then '竞价-搜索' 
      when a.module = '视频内流' then '竞价-视频内流'
    end as product
    ,a.brand_account_id
    ,case when a.marketing_target in (3,8) then '闭环电商'
     when a.marketing_target in (13) then '非闭环电商'
     when a.marketing_target in (2,5,9) then '线索'
     when a.marketing_target not in (3,8,2,5,9,13) then '种草'
     end as market_target
    ,sum(a.imp_num) as imp_cnt
    ,sum(a.click_num) as click_cnt
    ,sum(a.like_num) as like_cnt
    ,sum(a.fav_num) as fav_cnt
    ,sum(a.comment_num) as cmt_cnt
    ,sum(a.share_num) as share_cnt
    ,sum(a.follow_num) as follow_cnt
    ,sum(case when coalesce(a.marketing_target,0) in (3,8) then coalesce(cash_cost,0.0) else 0.0 end) as mkt_ecm_cost -- 电商
    ,sum(case when coalesce(a.marketing_target,0) in (2,5,9) then coalesce(cash_cost,0.0) else 0.0 end) as mkt_leads_cost -- 线索
    ,sum(case when coalesce(a.marketing_target,0) not in (3,8,2,5,9,13) then coalesce(cash_cost,0.0) else 0.0 end) as mkt_zc_cost -- 种草
    ,0 as open_sale_num
    ,count(distinct campaign_id) as campaign_cnt
    ,count(distinct unit_id) as unit_cnt
    ,0 as brand_campaign_cnt
    ,0 as cpc_cost_budget_rate
    ,0 as cpc_budget
    ,sum(case when coalesce(a.marketing_target,0) in (3,8) then coalesce(cash_cost,0.0) else 0.0 end) as mkt_ecm_cash_cost
    ,sum(case when coalesce(a.marketing_target,0) in (2,5,9) then coalesce(cash_cost,0.0) else 0.0 end) as mkt_leads_cash_cost
    ,sum(case when coalesce(a.marketing_target,0) not in (3,8,2,5,9,13) then coalesce(cash_cost,0.0) else 0.0 end) as mkt_zc_cash_cost
    ,sum(case when coalesce(a.marketing_target,0) in (3,8) and coalesce(b.sales_system,'') <> '渠道业务部' then coalesce(cost,0.0) else 0.0 end) as mkt_ecm_direct_cost
    ,sum(case when coalesce(a.marketing_target,0) in (2,5,9) and coalesce(b.sales_system,'') <> '渠道业务部' then coalesce(cost,0.0) else 0.0 end) as mkt_leads_direct_cost
    ,sum(case when coalesce(a.marketing_target,0) not in (3,8,2,5,9,13) and coalesce(b.sales_system,'') <> '渠道业务部' then coalesce(cost,0.0) else 0.0 end) as mkt_zc_direct_cost
    ,sum(case when coalesce(a.marketing_target,0) in (3,8) and coalesce(b.sales_system,'') <> '渠道业务部' then coalesce(cash_cost,0.0) else 0.0 end) as mkt_ecm_direct_cash_cost
    ,sum(case when coalesce(a.marketing_target,0) in (2,5,9) and coalesce(b.sales_system,'') <> '渠道业务部' then coalesce(cash_cost,0.0) else 0.0 end) as mkt_leads_direct_cash_cost
    ,sum(case when coalesce(a.marketing_target,0) not in (3,8,2,5,9,13) and coalesce(b.sales_system,'') <> '渠道业务部' then coalesce(cash_cost,0.0) else 0.0 end) as mkt_zc_direct_cash_cost
    ,sum(case when coalesce(a.marketing_target,0) in (3,8) and coalesce(b.sales_system,'') = '渠道业务部' then coalesce(cost,0.0) else 0.0 end) as mkt_ecm_channel_cost
    ,sum(case when coalesce(a.marketing_target,0) in (2,5,9) and coalesce(b.sales_system,'') = '渠道业务部' then coalesce(cost,0.0) else 0.0 end) as mkt_leads_channel_cost
    ,sum(case when coalesce(a.marketing_target,0) not in (3,8,2,5,9,13) and coalesce(b.sales_system,'') = '渠道业务部' then coalesce(cost,0.0) else 0.0 end) as mkt_zc_channel_cost
    ,sum(case when coalesce(a.marketing_target,0) in (3,8) and coalesce(b.sales_system,'') = '渠道业务部' then coalesce(cash_cost,0.0) else 0.0 end) as mkt_ecm_channel_cash_cost
    ,sum(case when coalesce(a.marketing_target,0) in (2,5,9) and coalesce(b.sales_system,'') = '渠道业务部' then coalesce(cash_cost,0.0) else 0.0 end) as mkt_leads_channel_cash_cost
    ,sum(case when coalesce(a.marketing_target,0) not in (3,8,2,5,9,13) and coalesce(b.sales_system,'') = '渠道业务部' then coalesce(cash_cost,0.0) else 0.0 end) as mkt_zc_channel_cash_cost
  from 
    reddw.dw_ads_wide_cpc_creativity_base_day_inc a
  left join (
    select 
       advertiser_id
      ,sales_system
    from reddm.dm_ads_crm_rtb_virtual_seller_income_wide_day 
    where dtm = '{{ds_nodash}}'
    group by 
      1,2
  ) b on a.advertiser_id = b.advertiser_id
  where
    a.dtm = '{{ds_nodash}}'
    and a.is_effective = 1
  group by 
    3,4,5
  union all 
  -- 品合
  select 
    '{{ds}}' as date_key
    ,'品合' as module
    ,'品合' as product
    ,report_brand_user_id as brand_account_id
    ,'整体' as market_target
    ,sum(b.imp_num) as imp_cnt
    ,sum(b.click_num) as click_cnt
    ,sum(b.like_num) as like_cnt
    ,sum(b.fav_num) as fav_cnt
    ,sum(b.cmt_num) as cmt_cnt
    ,sum(b.share_num) as share_cnt
    ,sum(b.follow_from_discovery_num) as follow_cnt
    ,0 as mkt_ecm_cost
    ,0 as mkt_leads_cost
    ,0 as mkt_zc_cost
    ,0 as open_sale_num
    ,0 as campaign_cnt
    ,0 as unit_cnt
    ,0 as brand_campaign_cnt
    ,0 as cpc_cost_budget_rate
    ,0 as cpc_budget
    ,0 as mkt_ecm_cash_cost
    ,0 as mkt_leads_cash_cost
    ,0 as mkt_zc_cash_cost
    ,0 as mkt_ecm_direct_cost
    ,0 as mkt_leads_direct_cost
    ,0 as mkt_zc_direct_cost
    ,0 as mkt_ecm_direct_cash_cost
    ,0 as mkt_leads_direct_cash_cost
    ,0 as mkt_zc_direct_cash_cost
    ,0 as mkt_ecm_channel_cost
    ,0 as mkt_leads_channel_cost
    ,0 as mkt_zc_channel_cost
    ,0 as mkt_ecm_channel_cash_cost
    ,0 as mkt_leads_channel_cash_cost
    ,0 as mkt_zc_channel_cash_cost
  from 
    bcoo_note a 
  left join 
    reddm.dm_soc_discovery_engagement_new_day_inc b 
  on 
    b.dtm = '{{ds_nodash}}'
    and a.note_id = b.discovery_id
  -- where 
  --   a.dtm = '{{ds_nodash}}'
  --   and f_getdate(a.note_publish_time) <= '{{ds}}' -- 发布在今天之前的
  group by 
    4
  union all 
  -- 薯条
  select 
    '{{ds}}' as date_key
    ,'薯条' as module
    ,'薯条' as product
    ,chips_user_id as brand_account_id
    ,'整体' as market_target
    ,sum(chips_imp_num) as imp_cnt
    ,sum(chips_click_num) as click_cnt
    ,sum(chips_like_num) as like_cnt
    ,sum(chips_fav_num) as fav_cnt
    ,sum(chips_cmt_num) as cmt_cnt
    ,sum(chips_share_num) as share_cnt
    ,sum(chips_follow_num) as follow_cnt
    ,0 as mkt_ecm_cost
    ,0 as mkt_leads_cost
    ,0 as mkt_zc_cost
    ,0 as open_sale_num
    ,0 as campaign_cnt
    ,0 as unit_cnt
    ,0 as brand_campaign_cnt
    ,0 as cpc_cost_budget_rate
    ,0 as cpc_budget
    ,0 as mkt_ecm_cash_cost
    ,0 as mkt_leads_cash_cost
    ,0 as mkt_zc_cash_cost
    ,0 as mkt_ecm_direct_cost
    ,0 as mkt_leads_direct_cost
    ,0 as mkt_zc_direct_cost
    ,0 as mkt_ecm_direct_cash_cost
    ,0 as mkt_leads_direct_cash_cost
    ,0 as mkt_zc_direct_cash_cost
    ,0 as mkt_ecm_channel_cost
    ,0 as mkt_leads_channel_cost
    ,0 as mkt_zc_channel_cost
    ,0 as mkt_ecm_channel_cash_cost
    ,0 as mkt_leads_channel_cash_cost
    ,0 as mkt_zc_channel_cash_cost
  from 
    redst.st_ads_chips_engagement_day_inc a 
  where
    dtm = '{{ds_nodash}}'
  group by 
    4
  union all 
  select
    '{{ds}}' as date_key
    ,'效果' as module
    ,case 
        when a.module = '发现feed' then '竞价-信息流' 
        when a.module = '搜索feed' then '竞价-搜索' 
        when a.module = '视频内流' then '竞价-视频内流'
    end as product
    ,brand_account_id
    ,case when ads_purpose in ('商品销量','直播推广') then '闭环电商' 
      when ads_purpose in ('13','非闭环电商' ) then '非闭环电商' 
      when ads_purpose in ('销售线索收集','私信营销','9') then '线索' 
      when ads_purpose not in ('商品销量','直播推广','9','13','非闭环电商' ,'销售线索收集','私信营销') then '种草' end as market_target --ads_purpose字段就是marketing_type对应中文描述
    ,0 as imp_cnt
    ,0 as click_cnt
    ,0 as like_cnt
    ,0 as fav_cnt
    ,0 as cmt_cnt
    ,0 as share_cnt
    ,0 as follow_cnt
    ,0 as mkt_ecm_cost
    ,0 as mkt_leads_cost
    ,0 as mkt_zc_cost
    ,0 as open_sale_num
    ,0 as campaign_cnt
    ,0 as unit_cnt
    ,0 as brand_campaign_cnt
    ,sum(cost_special_campaign) as cpc_cost_budget_rate
    ,sum(min_budget) as cpc_budget
    ,0 as mkt_ecm_cash_cost
    ,0 as mkt_leads_cash_cost
    ,0 as mkt_zc_cash_cost
    ,0 as mkt_ecm_direct_cost
    ,0 as mkt_leads_direct_cost
    ,0 as mkt_zc_direct_cost
    ,0 as mkt_ecm_direct_cash_cost
    ,0 as mkt_leads_direct_cash_cost
    ,0 as mkt_zc_direct_cash_cost
    ,0 as mkt_ecm_channel_cost
    ,0 as mkt_leads_channel_cost
    ,0 as mkt_zc_channel_cost
    ,0 as mkt_ecm_channel_cash_cost
    ,0 as mkt_leads_channel_cash_cost
    ,0 as mkt_zc_channel_cash_cost
  from 
    redapp.app_ads_overall_budget_1d_di a
  where
    dtm = '{{ds_nodash}}'
    and granularity = '分场域'
    and groups = 3
  group by 
    3,4,5
) a
group by 
  date_key
  ,module
  ,product
  ,brand_account_id
  ,market_target
;