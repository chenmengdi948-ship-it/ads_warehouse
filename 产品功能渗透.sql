--效果广告
select
  datekey as date_key,
  brand_account_id,
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
  sum(cash_cost) as rtb_cash_cost --竞价+授信
from
  (
    select
      datekey,
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
      cash_cost
    from
      redst.st_ads_wide_cpc_creativity_day_inc
    where
      dtm = '{{ds_nodash}}'
  ) t1
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
  case
    when optimize_target = 18
    and brief.note_id is not null then 1
    else 0
  end,
  is_effective
union all
  --蒲公英
select
  substr(note_publish_time, 1, 10) as date_key,
  report_brand_user_id as brand_account_id,
  '整体' as page_instance,
  '蒲公英' as platform,
  '整体' as biz_product_type,
  '整体' as marketing_target,
  '整体' as optimize_target,
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
  sum(content_price + service_fee) as rtb_cash_cost --竞价+授信
from
  reddw.dw_soc_tb_order_note_detail_day
where
  dtm = '{{ds_nodash}}'
  and order_status in (401, 402)
group by
  substr(note_publish_time, 1, 10),
  report_brand_user_id
union all
  --蒲公英组件
select
  substr(note_publish_time, 1, 10) as date_key,
  report_brand_user_id as brand_account_id,
  '整体' as page_instance,
  '蒲公英评论区组件' as platform,
  '整体' as biz_product_type,
  '整体' as marketing_target,
  '整体' as optimize_target,
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
  sum(content_price + service_fee) as rtb_cash_cost --竞价+授信
from
  reddm.dm_ads_bind_note_component_metrics_df
where
  dtm = '{{ds_nodash}}'
  and order_status in (401, 402)
  and comp_type in (1, 2, 3)
group by
  substr(note_publish_time, 1, 10),
  report_brand_user_id
union all
  --蒲公英笔记预约直播
select
  substr(note_publish_time, 1, 10) as date_key,
  report_brand_user_id as brand_account_id,
  '整体' as page_instance,
  '蒲公英直播预约组件' as platform,
  '整体' as biz_product_type,
  '整体' as marketing_target,
  '整体' as optimize_target,
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
  sum(content_price + service_fee) as rtb_cash_cost --竞价+授信
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
      report_brand_user_id
    from
      reddw.dw_soc_tb_order_note_detail_day
    where
      dtm = '{{ds_nodash}}'
      and order_status in (401, 402)
  ) t2 on t1.note_id = t2.note_id
group by substr(note_publish_time, 1, 10) ,
  report_brand_user_id
union all
  --小红星（淘联）
select
  from_unixtime(
    unix_timestamp(settle_date, 'yyyyMMdd'),
    'yyyy-MM-dd'
  ) as date_key,
  brand_account_user_id as brand_account_id,
  '整体' as page_instance,
  '小红星' as platform,
  '整体' as biz_product_type,
  '整体' as marketing_target,
  '整体' as optimize_target,
  '整体' as optimize_target_msg,
  '整体' as target_type_msg_list,
  '整体' as bidding_strategy_msg,
  '整体' as bid_type_msg,
  '整体' as ads_purpose,
  0 as is_redstar_optimize,
  1 as is_effective,
  sum(ads_cost) as cost,
  sum(coalesce(ads_cash_cost, 0)) as cash_cost,
  sum(
    coalesce(feed_gd_cost, 0) + coalesce(feed_cpc_cost, 0) + coalesce(search_cpc_cost, 0) + coalesce(internal_flow_cost, 0)
  ) as rtb_gd_cash_cost,
  --竞价+gd
  sum(
    coalesce(feed_cpc_cost, 0) + coalesce(search_cpc_cost, 0) + coalesce(internal_flow_cost, 0)
  ) as rtb_cash_cost --竞价+授信
from
  redapp.app_ads_taolian_brief_note_cost_df
where
  dtm = '{{ds_nodash}}'
group by
  from_unixtime(
    unix_timestamp(settle_date, 'yyyyMMdd'),
    'yyyy-MM-dd'
  ),
  brand_account_user_id