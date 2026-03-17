--人群包：点击数据需二次校验
insert overwrite table redapp_dev.app_ads_insight_account_agent_group_df partition(dtm = '{{ ds_nodash }}')
select date_key,
  brand_account_id,
  agent_user_id,
  agent_name,
  group_id,
  group_name,
  imp_cnt,
  click_cnt,
  cash_cost,
  cost
from 
--前一天全量
(select date_key,
  brand_account_id,
  agent_user_id,
  agent_name,
  group_id,
  group_name,
  imp_cnt,
  click_cnt,
  cash_cost,
  cost
from redapp.app_ads_insight_account_agent_group_df
where dtm = '{{yesterday_ds_nodash}}'
union all --最近一天增量
select
  a.date_key,
  a.brand_account_id,
  b.agent_user_id,
  b.agent_name,
  b.group_id,
  c.group_name,
  sum(a.imp_num) as imp_cnt,
  sum(a.click_num) as click_cnt,
  sum(a.cash_cost) as cash_cost,
  sum(a.cost) as cost
from
  (
    select
      f_getdate(dtm) as date_key,
      brand_account_id,
      creativity_id,
      imp_cnt as imp_num,
      click_cnt as click_num,
      0 as cost,
      0 as cash_cost
    from
      redcdm.dws_ads_log_creativity_cube_1d_di --流量中间层
    where
      dtm = '{{ds_nodash}}' and cube_type = '创意'
      and module='效果广告'
      union all
    select date_key,
      brand_account_id,
      virtual_object_id as creativity_id,
      0 as imp_num,
      0 as click_num,
      income_amt as cost,
      cash_income_amt as cash_cost
      from redcdm.dws_ads_creativity_order_share_income_nd_df --收入中间层-创意分摊粒度
      where  dtm = '{{ds_nodash}}' and date_key='{{ds}}'
      and module='效果'
  ) a
  left join (
    select
      creativity_id,
      dmp_group_id,
      group_id,
      agent_user_id,
      agent_name,
      dtm
    from
      redcdm.dim_ads_creativity_core_df --中间层创意维表
   lateral view outer explode(split(dmp_group_id, ',')) tmp as group_id
    where
      dtm = '{{ds_nodash}}'
  ) b on a.creativity_id = b.creativity_id
  join 
  (SELECT
    group_id
  FROM
    redcdm.dim_industry_group_id_df
  WHERE
    dtm='all'
  group by group_id
  )group_id 
  on group_id.group_id = b.group_id
  join (
    select
      group_id,
      group_name
    from
      reddim.dim_ads_dmp_group_day
    where
      dtm = '{{ds_nodash}}'
      and group_state = 1
  ) c on c.group_id = b.group_id
group by
  date_key,
  brand_account_id,
  b.agent_user_id,
  b.agent_name,
  b.group_id,
  c.group_name
)detail



--spu绑定
insert overwrite table redapp_dev.app_insight_account_note_spu_info_td_df partition(dtm = '{{ ds_nodash }}')
select 
  a.date_key as date_key
  ,a.note_id
  ,case 
    when coalesce(d.is_brand,0) = 1 then '企业号' 
    when c.order_type_cate1_id = 1 then '定制'
    when c.order_type_cate1_id = 2 then '招募'
    when c.order_type_cate1_id = 3 then '共创'
    when c.order_type_cate1_id = 4 then '新芽'
  end as note_type 
  ,a.v_seller_id 
  ,a.v_seller_name
  ,a.brand_account_id
  ,e.brand_user_name as brand_account_name
  ,case when f.brand_account_id is not null then 1 else 0 end as is_spu_white_list
  ,f.update_time as white_list_time
  ,e.first_ad_industry_name as first_ad_industry_name
  ,e.second_ad_industry_name as second_ad_industry_name
  ,e.company_name
  ,b.spu_id
  ,b.spu_name
  ,b.bind_time
  ,b.bind_update_time
  ,case when b.spu_id is not null then 1 else 0 end as bind_status 
  ,b.brand_id
  ,b.brand_name
  ,e.agent_user_name
  ,a.marketing_target
  ,e.sales_system
  ,e.direct_sales_code
  ,e.direct_sales_name
  ,e.direct_sales_dept_code
  ,e.direct_sales_dept_name
  ,e.direct_sales_parent_dept1_code
  ,e.direct_sales_parent_dept1_name
  ,e.direct_sales_parent_dept2_code
  ,e.direct_sales_parent_dept2_name
  ,e.channel_sales_code             
  ,e.channel_sales_name             
  ,e.channel_sales_dept_code        
  ,e.channel_sales_dept_name        
  ,e.channel_sales_parent_dept1_code
  ,e.channel_sales_parent_dept1_name
  ,e.channel_sales_parent_dept2_code
  ,e.channel_sales_parent_dept2_name
  ,a.cash_cost 
  ,a.cost
  ,e.agent_type
from (
  select
     a.date_key
    ,a.note_id as note_id
    ,b.v_seller_id as v_seller_id
    ,b.shadow_name as v_seller_name
    ,a.brand_account_id
    ,a.brand_account_name
    ,a.marketing_target
    ,sum(a.cash_income_amt) as cash_cost 
    ,sum(a.income_amt) as cost
  from 
  (select virtual_object_id,
    note_id,
    brand_account_id,
    brand_account_name,
    marketing_target,
    cash_income_amt,
    income_amt,
    advertiser_id,
    date_key
  from redcdm.dws_ads_creativity_order_share_income_nd_df  --收入中间层-创意分摊粒度    
  where
    dtm = '{{ds_nodash}}'
    and module='效果'
  )a 
  join 
  (select creativity_id
  from redcdm.dim_ads_creativity_core_df --中间层创意维表
  where
    dtm = '{{ds_nodash}}' and ads_material_type = 'post'
  group by creativity_id
  )creativity 
  on creativity.creativity_id = a.virtual_object_id
  left join
    reddw.dw_ads_cpc_advertiser_day b on b.dtm = '{{ds_nodash}}' and a.advertiser_id = b.advertiser_id
  group by
    1,2,3,4,5,6,7
) a 
left join 
  ads_databank.dim_spu_note_df b on b.dtm = '{{ds_nodash}}' and a.note_id = b.note_id and b.bind_type = 2 
left join 
  reddm.dm_soc_brand_coo_order_note_detail_day c on c.dtm = '{{ds_nodash}}' and a.note_id = c.note_id
left join 
  reddw.dw_soc_discovery_delta_7_day d on d.dtm = '{{ds_nodash}}' and a.note_id = d.discovery_id
left join(
  select --crm获取组织架构
    virtual_seller_id
    ,company_name
    ,agent_company_name as agent_user_name
    ,sales_system
    ,direct_sales_code
    ,direct_sales_name
    ,direct_sales_dept_code
    ,direct_sales_dept_name
    ,direct_sales_parent_dept1_code
    ,direct_sales_parent_dept1_name
    ,direct_sales_parent_dept2_code
    ,direct_sales_parent_dept2_name
    ,channel_sales_code             
    ,channel_sales_name             
    ,channel_sales_dept_code        
    ,channel_sales_dept_name        
    ,channel_sales_parent_dept1_code
    ,channel_sales_parent_dept1_name
    ,channel_sales_parent_dept2_code
    ,channel_sales_parent_dept2_name
    ,agent_type
    ,first_ad_industry_name
    ,second_ad_industry_name
    ,brand_user_name
  from reddm.dm_ads_crm_advertiser_income_wide_day e 
  where 
    e.dtm = '{{ds_nodash}}' 
    and e.module = '效果'
  group by     
     virtual_seller_id
    ,company_name
    ,agent_company_name
    ,sales_system
    ,direct_sales_code
    ,direct_sales_name
    ,direct_sales_dept_code
    ,direct_sales_dept_name
    ,direct_sales_parent_dept1_code
    ,direct_sales_parent_dept1_name
    ,direct_sales_parent_dept2_code
    ,direct_sales_parent_dept2_name
    ,channel_sales_code             
    ,channel_sales_name             
    ,channel_sales_dept_code        
    ,channel_sales_dept_name        
    ,channel_sales_parent_dept1_code
    ,channel_sales_parent_dept1_name
    ,channel_sales_parent_dept2_code
    ,channel_sales_parent_dept2_name
    ,agent_type
    ,first_ad_industry_name
    ,second_ad_industry_name
    ,brand_user_name
) e on a.v_seller_id = e.virtual_seller_id 
left join (
  select 
    item as brand_account_id
    ,max(update_time) as update_time
  from 
    redods.ods_brand_account_tb_common_white_list
  where 
    dtm = '{{ds_nodash}}' 
    and type in ('spu_brand_user_id','spu_brand_user_id_new')
    and status = 1
  group by 1 
) f on a.brand_account_id = f.brand_account_id
;



--------------------
--新产品渗透
with rtb as 
(select
  datekey as date_key,
  t1.brand_account_id,
  module as page_instance,
  case
    when platform = 0 then '专业号平台'
    when platform = 1 then '聚光'
    when platform = 2 then 'ark'
    else '其他'
  end as platform,
  --平台
  case
    when biz_product_type = -1 then '其他'
    when biz_product_type = 2 then '全站智投'
    when biz_product_type = 1 then '搜索追投'
    else '其他'
  end as biz_product_type,
  --全站智投
  marketing_target,
  optimize_target,
  optimize_target_msg,
  target_type_msg_list,
  bidding_strategy_msg,
  bid_type_msg,
  case
    when marketing_target = 0 then '老计划'
    when marketing_target = 1 then '应用推广'
    when marketing_target = 2 then '销售线索收集'
    when marketing_target = 3 then '商品销量'
    when marketing_target = 4 then '笔记种草'
    when marketing_target = 5 then '私信营销'
    when marketing_target = 6 then '品牌知名度'
    when marketing_target = 7 then '品牌意向'
    when marketing_target = 8 then '直播推广'
    when marketing_target = 9
    and optimize_target in (5, 9, 13) then '私信营销'
    when marketing_target = 9
    and optimize_target in (3, 12) then '销售线索收集'
    when marketing_target = 9 then '线索广告'
    when marketing_target = 10 then '抢占关键词'
    when marketing_target = 11 then '抢占人群'
    when marketing_target = 12 then '加粉'
    when marketing_target = 13 then '行业商品推广'
    else marketing_target
  end as ads_purpose,
  --营销目的
  case
    when optimize_target = 18
    and brief.note_id is not null then 1
    else 0
  end as is_redstar_optimize,
  is_effective,
  -- target_audience_groups,
  sum(cost) as cost,
  sum(cash_cost) as cash_cost,
  sum(cash_cost) as rtb_gd_cash_cost,
  --竞价+gd
  sum(cash_cost) as rtb_cash_cost, --竞价+授信
  agent_user_id as agent_id,
  agent_name,
  case when comp.creativity_id is not null then 1 else 0 end as is_comp,
  0 as rtb_cost,
  0 as brand_cost
from
  (--增量
    select
      datekey,
      creativity_id,
      brand_account_id,
      module,
      platform,
      --平台
      biz_product_type,
      --全站智投
      marketing_target,
      optimize_target,
      optimize_target_msg,
      target_type_msg_list,
      bidding_strategy_msg,
      bid_type_msg,
      case
        when ads_material_type = 'post' then ads_material_id
        else null
      end as note_id,
      is_effective,
      cost,
      cash_cost,
      dtm
    from
      redst.st_ads_wide_cpc_creativity_day_inc --纵向模型输出后再调整
    where 
      dtm = '{{ds_nodash}}'
  ) t1
  left join 
  (select
      creativity_id,
      agent_user_id,
      agent_name
    from
      redcdm.dim_ads_creativity_core_df
    where dtm = '{{ds_nodash}}'
    group by creativity_id,
      agent_user_id,
      agent_name
    )dim 
  on dim.creativity_id = t1.creativity_id
  left join (
    select
      note_id
    from
      redcdm.dim_ads_taolian_brief_overall_df
    where
      dtm = '{{ds_nodash}}'
      and note_id <> '' -- and replace(start_time, '-', '') >= '20220901'
      and replace(end_time, '-', '') >= '20220901'
    group by
      note_id
  ) as brief on brief.note_id = t1.note_id
  left join (
    select dtm,
      cast(id as string) as creativity_id
    from
      redcdm.dwd_ads_rtb_creativity_df
    where
      dtm >= '20230520' --搜索组件上线时间
      and get_json_object(material,'$.conversion_component_types') is not null --聚光有评论区组件
    group by dtm,
      cast(id as string)
  ) as comp on comp.creativity_id = t1.creativity_id
  and comp.dtm=t1.dtm
group by
  datekey,
  module,
  case
    when platform = 0 then '专业号平台'
    when platform = 1 then '聚光'
    when platform = 2 then 'ark'
    else '其他'
  end,
  --平台
  case
    when biz_product_type = -1 then '其他'
    when biz_product_type = 2 then '全站智投'
    when biz_product_type = 1 then '搜索追投'
    else '其他'
  end,
  --全站智投
  marketing_target,
  optimize_target,
  optimize_target_msg,
  target_type_msg_list,
  bidding_strategy_msg,
  bid_type_msg,
  case
    when marketing_target = 0 then '老计划'
    when marketing_target = 1 then '应用推广'
    when marketing_target = 2 then '销售线索收集'
    when marketing_target = 3 then '商品销量'
    when marketing_target = 4 then '笔记种草'
    when marketing_target = 5 then '私信营销'
    when marketing_target = 6 then '品牌知名度'
    when marketing_target = 7 then '品牌意向'
    when marketing_target = 8 then '直播推广'
    when marketing_target = 9
    and optimize_target in (5, 9, 13) then '私信营销'
    when marketing_target = 9
    and optimize_target in (3, 12) then '销售线索收集'
    when marketing_target = 9 then '线索广告'
    when marketing_target = 10 then '抢占关键词'
    when marketing_target = 11 then '抢占人群'
    when marketing_target = 12 then '加粉'
    when marketing_target = 13 then '行业商品推广'
    else marketing_target
  end,
  --营销目的
  brand_account_id,
  agent_user_id,
  agent_name,
  case
    when optimize_target = 18
    and brief.note_id is not null then 1
    else 0
  end,
  is_effective,
  case when comp.creativity_id is not null then 1 else 0 end
)
insert overwrite table redapp.app_ads_insight_account_cost_detail_df partition(dtm = '{{ ds_nodash }}') --效果广告
select
  date_key,
  rtb.brand_account_id,
  page_instance,
  platform,
  --平台
  biz_product_type,
  --全站智投
  marketing_target,
  optimize_target,
  optimize_target_msg,
  target_type_msg_list,
  bidding_strategy_msg,
  bid_type_msg,
  ads_purpose,
  --营销目的
  is_redstar_optimize,
  is_effective,
  -- target_audience_groups,
  sum(cost) as cost,
  sum(cash_cost) as cash_cost,
  sum(cash_cost) as rtb_gd_cash_cost,
  --竞价+gd
  sum(cash_cost) as rtb_cash_cost, --竞价+授信
  agent_id,
  agent_name,
  0 as rtb_cost,
  0 as brand_cost
from rtb 
group by date_key,
  rtb.brand_account_id,
  page_instance,
  platform,
  --平台
  biz_product_type,
  --全站智投
  marketing_target,
  optimize_target,
  optimize_target_msg,
  target_type_msg_list,
  bidding_strategy_msg,
  bid_type_msg,
  ads_purpose,
  --营销目的
  is_redstar_optimize,
  is_effective,
  agent_id,
  agent_name
union all 
select
  date_key,
  rtb.brand_account_id,
  page_instance,
  platform,
  --平台
  '聚光-搜索组件' as biz_product_type,
  marketing_target,
  optimize_target,
  optimize_target_msg,
  target_type_msg_list,
  bidding_strategy_msg,
  bid_type_msg,
  ads_purpose,
  --营销目的
  is_redstar_optimize,
  is_effective,
  -- target_audience_groups,
  sum(cost) as cost,
  sum(cash_cost) as cash_cost,
  sum(cash_cost) as rtb_gd_cash_cost,
  --竞价+gd
  sum(cash_cost) as rtb_cash_cost, --竞价+授信
  agent_id,
  agent_name,
  0 as rtb_cost,
  0 as brand_cost
from rtb
where is_comp=1 
group by date_key,
  rtb.brand_account_id,
  page_instance,
  platform,
  --平台
  biz_product_type,
  --全站智投
  marketing_target,
  optimize_target,
  optimize_target_msg,
  target_type_msg_list,
  bidding_strategy_msg,
  bid_type_msg,
  ads_purpose,
  --营销目的
  is_redstar_optimize,
  is_effective,
  agent_id,
  agent_name
union all
  --蒲公英
select
  substr(note_publish_time, 1, 10) as date_key,
  report_brand_user_id as brand_account_id,
  '整体' as page_instance,
  '其他' as platform,
  '蒲公英' as biz_product_type,
  '整体' as marketing_target,
  -1 as optimize_target,
  '整体' as optimize_target_msg,
  '整体' as target_type_msg_list,
  '整体' as bidding_strategy_msg,
  '整体' as bid_type_msg,
  '整体' as ads_purpose,
  0 as is_redstar_optimize,
  1 as is_effective,
  sum(content_price + service_fee) as cost,
  sum(content_price + service_fee) as cash_cost,
  sum(content_price + service_fee) as rtb_gd_cash_cost,
  --竞价+gd
  sum(content_price + service_fee) as rtb_cash_cost, --竞价+授信
  case when is_agent_order=1 then brand_id else null end as agent_id,
  case when is_agent_order=1 then brand_nickname else null end agent_name,
  0 as rtb_cost,
  0 as brand_cost
from
  reddw.dw_soc_tb_order_note_detail_day
where
  dtm = '{{ds_nodash}}'
  and order_status in (401, 402)
group by
  substr(note_publish_time, 1, 10),
  report_brand_user_id,
  case when is_agent_order=1 then brand_id else null end ,
  case when is_agent_order=1 then brand_nickname else null end
union all
  --蒲公英组件
select
  substr(note_publish_time, 1, 10) as date_key,
  report_brand_user_id as brand_account_id,
  '整体' as page_instance,
  '其他' as platform,
  '蒲公英-评论区组件' as biz_product_type,
  '整体' as marketing_target,
  -1 as optimize_target,
  '整体' as optimize_target_msg,
  '整体' as target_type_msg_list,
  '整体' as bidding_strategy_msg,
  '整体' as bid_type_msg,
  '整体' as ads_purpose,
  0 as is_redstar_optimize,
  1 as is_effective,
  sum(content_price + service_fee) as cost,
  sum(content_price + service_fee) as cash_cost,
  sum(content_price + service_fee) as rtb_gd_cash_cost,
  --竞价+gd
  sum(content_price + service_fee) as rtb_cash_cost, --竞价+授信
  agent_id,
  agent_name,
  0 as rtb_cost,
  0 as brand_cost
from
  reddm.dm_ads_bind_note_component_metrics_df
where
  dtm = '{{ds_nodash}}'
  and order_status in (401, 402)
  and comp_type in (1, 2, 3)
group by
  substr(note_publish_time, 1, 10),
  report_brand_user_id,
  agent_id,
  agent_name
union all
select
  substr(note_publish_time, 1, 10) as date_key,
  report_brand_user_id as brand_account_id,
  '整体' as page_instance,
  '其他' as platform,
  '蒲公英-搜索组件' as biz_product_type,
  '整体' as marketing_target,
  -1 as optimize_target,
  '整体' as optimize_target_msg,
  '整体' as target_type_msg_list,
  '整体' as bidding_strategy_msg,
  '整体' as bid_type_msg,
  '整体' as ads_purpose,
  0 as is_redstar_optimize,
  1 as is_effective,
  sum(content_price + service_fee) as cost,
  sum(content_price + service_fee) as cash_cost,
  sum(content_price + service_fee) as rtb_gd_cash_cost,
  --竞价+gd
  sum(content_price + service_fee) as rtb_cash_cost, --竞价+授信
  agent_id,
  agent_name,
  0 as rtb_cost,
  0 as brand_cost
from
reddm.dm_ads_bind_note_component_metrics_df
where
  dtm = '{{ds_nodash}}'
  and order_status in (401, 402)
  and content_comp_type in (2)
group by
  substr(note_publish_time, 1, 10),
  report_brand_user_id,
  agent_id,
  agent_name

union all
  --蒲公英笔记预约直播
select
  substr(note_publish_time, 1, 10) as date_key,
  report_brand_user_id as brand_account_id,
  '整体' as page_instance,
  '其他' as platform,
  '蒲公英-预约组件' as biz_product_type,
  '整体' as marketing_target,
  -1 as optimize_target,
  '整体' as optimize_target_msg,
  '整体' as target_type_msg_list,
  '整体' as bidding_strategy_msg,
  '整体' as bid_type_msg,
  '整体' as ads_purpose,
  0 as is_redstar_optimize,
  1 as is_effective,
  sum(content_price + service_fee) as cost,
  sum(content_price + service_fee) as cash_cost,
  sum(content_price + service_fee) as rtb_gd_cash_cost,
  --竞价+gd
  sum(content_price + service_fee) as rtb_cash_cost, --竞价+授信
  agent_id,
  agent_name,
  0 as rtb_cost,
  0 as brand_cost
from
  (
    select
      carrier_id as note_id
    from
      redcdm.dwd_liv_live_report_carrier_relation_df
    where
      dtm = '{{ds_nodash}}'
      and carrier_type = 3
    group by
      1
  ) t1
  join --蒲公英笔记
  (
    select
      note_id,
      note_publish_time,
      content_price,
      service_fee,
      report_brand_user_id,
      case when is_agent_order=1 then brand_id else null end as agent_id,
      case when is_agent_order=1 then brand_nickname else null end agent_name
    from
      reddw.dw_soc_tb_order_note_detail_day
    where
      dtm = '{{ds_nodash}}'
      and order_status in (401, 402)
  ) t2 on t1.note_id = t2.note_id
group by
  substr(note_publish_time, 1, 10),
  report_brand_user_id,
  agent_id,
  agent_name
union all
  --小红星（淘联）
select
  from_unixtime(
    unix_timestamp(settle_date, 'yyyyMMdd'),
    'yyyy-MM-dd'
  ) as date_key,
  brand_account_user_id as brand_account_id,
  '整体' as page_instance,
  '其他' as platform,
  '小红星' as biz_product_type,
  '整体' as marketing_target,
  -1 as optimize_target,
  '整体' as optimize_target_msg,
  '整体' as target_type_msg_list,
  '整体' as bidding_strategy_msg,
  '整体' as bid_type_msg,
  '整体' as ads_purpose,
  0 as is_redstar_optimize,
  1 as is_effective,
  sum(task_period_ads_cost) as cost,
  sum(coalesce(task_period_ads_cash_cost, 0)) as cash_cost,
  0 as rtb_gd_cash_cost,
  --竞价+gd口径确认后补充
  0 as rtb_cash_cost, --竞价+授信口径确认后补充
  agent_id,
  agent_name,
  sum(coalesce(task_period_ads_feed_cpc_cost,0)+coalesce(task_period_ads_search_cpc_cost,0)+coalesce(task_period_ads_internal_flow_cost,0)) as rtb_cost,
  sum(coalesce(task_period_ads_feed_gd_cost,0)+coalesce(task_period_ads_open_cost,0)+coalesce(task_period_ads_search_third_cost,0)) as brand_cost
from
(select brief_id,
  brief_name,
  brand_account_user_id,
  brand_account_user_name,
  note_id,
  bcoo_order_id,
  ads_cost,
  ads_cash_cost,
  settle_date,
  bcoo_create_content_price,
  bcoo_create_service_fee,
  ads_chips_cost,
  ads_search_third_cost,
  ads_open_cost,
  ads_feed_gd_cost,
  ads_feed_cpc_cost,
  ads_search_cpc_cost,
  ads_internal_flow_cost,
  task_period_ads_chips_cost,
  task_period_ads_search_third_cost,
  task_period_ads_open_cost,
  task_period_ads_feed_gd_cost,
  task_period_ads_feed_cpc_cost,
  task_period_ads_search_cpc_cost,
  task_period_ads_internal_flow_cost,
  task_period_ads_cost,
  task_period_ads_cash_cost
from
  redapp.app_ads_taolian_brief_note_cost_df
where
  dtm = f_getdate('{{ds_nodash}}',-1)
  )t1  
left join 
(select note_id,
  brief_id,
  max(agent_user_id) as agent_id,
  max(agent_user_name) as agent_name --以防主键重复，进行兜底。目前不加max也不重复
FROM
  redcdm.dim_ads_taolian_brief_note_v2_df
WHERE
  dtm = '{{ds_nodash}}' and note_id<>''
group by note_id,
  brief_id
  )t2 
on t1.note_id = t2.note_id and t1.brief_id=t2.brief_id
group by
  from_unixtime(
    unix_timestamp(settle_date, 'yyyyMMdd'),
    'yyyy-MM-dd'
  ),
  brand_account_user_id,
  agent_id,
  agent_name