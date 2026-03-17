-- ************************************************
-- Author: chenmengdi
-- CreateTime:2023-11-28T16:41:26+08:00
-- Update: Task Update Description
-- ************************************************
drop table if exists temp.temp_app_ads_industry_ecm_target_metric_df_{{ds_nodash}}_online;

create table
  temp.temp_app_ads_industry_ecm_target_metric_df_{{ds_nodash}}_online as
select
  brand_account_id,
  brand_user_name,
  track_industry_dept_group_name,
  direct_sales_dept5_name,
  company_name,
  company_code,
  brand_group_tag_code,
  brand_group_tag_name,
  day_dtm,
  dt,
  if(month(dt) & 1 = 1,trunc(dt,'MM'),trunc(add_months(dt,-1),'MM')) as bimonth_dt
from
  ( --账户基础信息
    select
      brand_account_id,
      brand_user_name,
      track_industry_dept_group_name,
      direct_sales_dept5_name,
      company_name,
      company_code,
      brand_group_tag_code,
      brand_group_tag_name
    from
      redapp.app_ads_insight_industry_product_account_td_df
    where
      dtm = '{{ds_nodash}}'
      and track_industry_dept_group_name in ('奢品', '美妆', '服饰潮流')
      and date_key = '{{ds}}'
    group by
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8
  ) a
  join (
    select
      day_dtm,
      dt
    from
      redcdm.dim_ads_date_df
    where
      dtm = 'all'
      and day_dtm between '20221001' and '{{ds_nodash}}'
  ) dt on 1 = 1;
drop table if exists temp.temp_app_ads_industry_ecm_target_metric_df_{{ds_nodash}}_bimonth_online;

create table
  temp.temp_app_ads_industry_ecm_target_metric_df_{{ds_nodash}}_bimonth_online as
 --截止当日双月累计指标
    select
      date_key,
      dtm,
      brand_account_id,
      zc_cash_cost,
      cid_cash_cost,
      ds_cash_cost,
      spxl_cash_cost,
      zbtg_cash_cost,
      zbyr_cash_cost, 
      dplx_cash_cost,
      sum(zc_cash_cost) over (
        partition by
          brand_account_id,bimonth_dt
        order by
          cast(date_key as date) asc rows between unbounded  PRECEDING
          and current row
      ) as zc_cash_cost_2m,
      sum(cid_cash_cost) over (
        partition by
          brand_account_id,bimonth_dt
        order by
          cast(date_key as date) asc rows between unbounded  PRECEDING
          and current row
      ) as cid_cash_cost_2m,
      sum(ds_cash_cost) over (
        partition by
          brand_account_id,bimonth_dt
        order by
          cast(date_key as date) asc rows between unbounded  PRECEDING
          and current row
      ) as ds_cash_cost_2m,
      sum(spxl_cash_cost) over (
        partition by
          brand_account_id,bimonth_dt
        order by
          cast(date_key as date) asc rows between unbounded  PRECEDING
          and current row
      ) as spxl_cash_cost_2m,
      sum(zbtg_cash_cost) over (
        partition by
          brand_account_id,bimonth_dt
        order by
          cast(date_key as date) asc rows between unbounded  PRECEDING
          and current row
      ) as zbtg_cash_cost_2m,
      sum(zbyr_cash_cost) over (
        partition by
          brand_account_id,bimonth_dt
        order by
          cast(date_key as date) asc rows between unbounded  PRECEDING
          and current row
      ) as zbyr_cash_cost_2m,
      sum(dplx_cash_cost) over (
        partition by
          brand_account_id,bimonth_dt
        order by
          cast(date_key as date) asc rows between unbounded  PRECEDING
          and current row
      ) as dplx_cash_cost_2m
    from
      (
        select
          dt as date_key,
          day_dtm as dtm,
          bimonth_dt,
          a.brand_account_id,
          --coalesce(deal_gmv, 0) as deal_gmv,
          coalesce(zc_cash_cost,0) as zc_cash_cost,
          coalesce(cid_cash_cost,0) as cid_cash_cost,
          coalesce(ds_cash_cost,0) as ds_cash_cost,
          coalesce(spxl_cash_cost,0) as spxl_cash_cost,
          coalesce(zbtg_cash_cost,0) as zbtg_cash_cost,
          coalesce(zbyr_cash_cost,0) as zbyr_cash_cost, 
          coalesce(dplx_cash_cost,0) as dplx_cash_cost
        from
    (
    select
      *
    from
      temp.temp_app_ads_industry_ecm_target_metric_df_{{ds_nodash}}_online
    where
      day_dtm >= '20231101' --从11月加双月指标
    ) a
    left join ( --电商广告投放情况
    select
      dtm,
      brand_account_id,
      sum(
        if (
          marketing_target not in (3, 8, 14, 15, 13, 2, 5, 9),
          cost_amount,
          0
        )
      ) as zc_cash_cost,
      sum(if (marketing_target = 13, cost_amount, 0)) as cid_cash_cost,
      sum(if (marketing_target = 13, total_amount, 0)) as cid_cost,
      sum(
        if (
          marketing_target in (3, 8, 14, 15),
          cost_amount,
          0
        )
      ) as ds_cash_cost,
      sum(
        if (
          marketing_target in (3, 8, 14, 15),
          total_amount,
          0
        )
      ) as ds_cost,
      sum(
        if (
          marketing_target in (3, 8, 14, 15),
          click_rgmv_7d,
          0
        )
      ) as ds_rgmv,
      sum(if (marketing_target in (3), cost_amount, 0)) as spxl_cash_cost,
      sum(if (marketing_target in (3), total_amount, 0)) as spxl_cost,
      sum(if (marketing_target in (3), click_rgmv_7d, 0)) as spxl_rgmv,
      sum(if (marketing_target in (8), cost_amount, 0)) as zbtg_cash_cost,
      sum(if (marketing_target in (8), total_amount, 0)) as zbtg_cost,
      sum(if (marketing_target in (8), click_rgmv_7d, 0)) as zbtg_rgmv,
      sum(if (marketing_target in (14), cost_amount, 0)) as zbyr_cash_cost,
      sum(if (marketing_target in (14), total_amount, 0)) as zbyr_cost,
      sum(if (marketing_target in (14), click_rgmv_7d, 0)) as zbyr_rgmv,
      sum(if (marketing_target in (15), cost_amount, 0)) as dplx_cash_cost,
      sum(if (marketing_target in (15), total_amount, 0)) as dplx_cost,
      sum(if (marketing_target in (15), click_rgmv_7d, 0)) as dplx_rgmv,
      sum(
        if (
          marketing_target = 8,
          live_24h_click_effective_shutdown_distinct_eventid_num,
          0
        )
      ) as live_24h_click_effective_shutdown_distinct_eventid_num, --有效观播uv
      sum(
        if (
          marketing_target = 8,
          live_24h_click_effective_shutdown_num,
          0
        )
      ) as live_24h_click_effective_shutdown_num, --有效观播uv
      sum(if (marketing_target = 8, live_order_num, 0)) as live_order_num --直播间订单数
    from
      redcdm.dm_ads_rtb_creativity_1d_di
    where
      dtm between '20231101' and '{{ds_nodash}}'
    group by
      1,
      2
  ) b on a.brand_account_id = b.brand_account_id
  and a.day_dtm = b.dtm
  )info;  
drop table if exists temp.temp_app_ads_industry_ecm_target_metric_df_{{ds_nodash}}_bimonth_02_online;

create table
  temp.temp_app_ads_industry_ecm_target_metric_df_{{ds_nodash}}_bimonth_02_online as
 --截止当日双月累计指标
    select
      date_key,
      dtm,
      brand_account_id,
      brand_ti_user_cost,
      brand_ti_target_cost,
      sum(brand_ti_user_cost) over (
        partition by
          brand_account_id,bimonth_dt
        order by
          cast(date_key as date) asc rows between unbounded  PRECEDING
          and current row
      ) as brand_ti_user_cost_2m,
      sum(brand_ti_target_cost) over (
        partition by
          brand_account_id,bimonth_dt
        order by
          cast(date_key as date) asc rows between unbounded  PRECEDING
          and current row
      ) as brand_ti_target_cost_2m
      
    from
      (
        select
          dt as date_key,
          day_dtm as dtm,
          bimonth_dt,
          a.brand_account_id,
          --coalesce(deal_gmv, 0) as deal_gmv,
          coalesce(brand_ti_user_cost,0) as brand_ti_user_cost,
          coalesce(brand_ti_target_cost,0) as brand_ti_target_cost
        from
    (
    select
      *
    from
      temp.temp_app_ads_industry_ecm_target_metric_df_{{ds_nodash}}_online
    where
      day_dtm between '20231101' and '{{ds_nodash}}'
    ) a
    left join ( --电商广告投放情况
    select
      dtm,
      brand_account_id,
      sum(if (ti_user_type = '品牌种草人群', cost, 0)) as brand_ti_user_cost,
      sum(
        if (detail_target_type='品牌种草人群定向', cost, 0) 
      ) as brand_ti_target_cost--品牌种草人群定向
    from
      redapp.app_ads_cvr_account_detail_di
    where
      marketing_target = 3
      and dtm between '20231101' and '{{ds_nodash}}'
    group by
      1,
      2
  ) b on a.brand_account_id = b.brand_account_id
  and a.day_dtm = b.dtm
  )info; 
drop table if exists temp.temp_app_ads_industry_ecm_target_metric_df_{{ds_nodash}}_bimonth_03_online;

create table
  temp.temp_app_ads_industry_ecm_target_metric_df_{{ds_nodash}}_bimonth_03_online as
 --过去30天dgmv
 select
      date_key,
      dtm,
      brand_account_id,
      deal_gmv,
      bimonth_dt,
      dianbo_deal_gmv,
      sum(deal_gmv) over (
        partition by
          brand_account_id
        order by
          cast(date_key as date) asc rows between 29 PRECEDING
          and 0 FOLLOWING
      ) as deal_gmv_30d,
      sum(deal_gmv) over (
        partition by
          brand_account_id,bimonth_dt
        order by
          cast(date_key as date) asc rows between unbounded  PRECEDING
          and current row
      ) as deal_gmv_2m,
      sum(dianbo_deal_gmv) over (
        partition by
          brand_account_id,bimonth_dt
        order by
          cast(date_key as date) asc rows between unbounded  PRECEDING
          and current row
      ) as dianbo_deal_gmv_2m
    from
      (
        select
          dt as date_key,
          day_dtm as dtm,
          a.brand_account_id,
          bimonth_dt,
          coalesce(deal_gmv, 0) as deal_gmv,
          coalesce(dianbo_deal_gmv, 0) as  dianbo_deal_gmv
        from
          (
          select
            *
          from
            temp.temp_app_ads_industry_ecm_target_metric_df_{{ds_nodash}}_online
          where
            day_dtm between '20231001' and '{{ds_nodash}}'
          )  a
          left join (
            select
              f_getdate (dtm) as date_key,
              dtm,
              brand_account_id,
              sum(deal_gmv) as deal_gmv,
              sum(if (channel = '店播', deal_gmv, 0)) as dianbo_deal_gmv
            from
              redapp.app_ads_trd_user_seller_account_detail_df
            where
              dtm between '20231001' and '{{ds_nodash}}'
            group by
              1,
              2,
              3
          ) base on a.brand_account_id = base.brand_account_id
          and a.day_dtm = base.dtm
      ) info
    group by
      date_key,
      dtm,
      brand_account_id,
      deal_gmv,
      bimonth_dt,
      dianbo_deal_gmv
  ;
insert overwrite table redapp.app_ads_industry_ecm_target_metric_df partition (dtm='{{ds_nodash}}')
select a.dt as date_key,
  a.brand_account_id,
  a.brand_user_name,
  track_industry_dept_group_name,
  direct_sales_dept5_name,
  company_name,
  company_code,
  brand_group_tag_code,
  brand_group_tag_name,
  if (seller_cnt > 0, 1, 0) as kaidian_or_not,
  zc_cash_cost as ti_cash_cost,
  cid_cash_cost as ecm_unclosed_cash_cost,
  cid_cost as ecm_unclosed_cost,
  out_click_rgmv_15d,
  out_click_goods_view_pv_15d,
  ds_cash_cost as ecm_closed_cash_cost,
  ds_cost as ecm_closed_cost,
  ds_rgmv as ecm_closed_rgmv,
  spxl_cash_cost as sx_cash_cost,
  spxl_cost as sx_cost,
  spxl_rgmv as sx_rgmv,
  zbtg_cash_cost,
  zbtg_cost,
  zbtg_rgmv,
  zbyr_cash_cost,
  zbyr_cost,
  zbyr_rgmv,
  dplx_cash_cost,
  dplx_cost,
  dplx_rgmv,
  live_24h_click_effective_shutdown_distinct_eventid_num,
  live_24h_click_effective_shutdown_num,
  live_order_num,
  c.deal_gmv,
  ads_deal_gmv,
  dianbo_deal_gmv as s_deal_gmv,
  ads_dianbo_deal_gmv as ads_s_deal_gmv,
  kbo_deal_gmv as k_deal_gmv,
  biji_deal_gmv as note_deal_gmv,
  deal_gmv_30d,
  if (deal_gmv_30d > 100000, 1, 0) as is_high_sales,
  brand_ti_user_cost,
  brand_ti_target_cost,
  deal_gmv_2m,
  dianbo_deal_gmv_2m as s_deal_gmv_2m, 
  brand_ti_user_cost_2m,
  brand_ti_target_cost_2m,
  zc_cash_cost_2m as ti_cash_cost_2m,
  cid_cash_cost_2m as ecm_unclosed_cash_cost_2m,
  ds_cash_cost_2m as ecm_closed_cash_cost_2m,
  spxl_cash_cost_2m as sx_cash_cost_2m,
  zbtg_cash_cost_2m,
  zbyr_cash_cost_2m,
  dplx_cash_cost_2m,
  cvr_cost,
  cvr_click_rgmv_7d
 -- a.day_dtm as dtm
from
  (
    select
      *,
      day_dtm
    from
      temp.temp_app_ads_industry_ecm_target_metric_df_{{ds_nodash}}_online
    where
      day_dtm >= '20221001'
  ) a
  left join ( --电商广告投放情况
    select
      dtm,
      brand_account_id,
      sum(
        if (
          marketing_target not in (3, 8, 14, 15, 13, 2, 5, 9),
          cost_amount,
          0
        )
      ) as zc_cash_cost,
      sum(if (marketing_target = 13, cost_amount, 0)) as cid_cash_cost,
      sum(if (marketing_target = 13, total_amount, 0)) as cid_cost,
      sum(
        if (
          marketing_target in (3, 8, 14, 15),
          cost_amount,
          0
        )
      ) as ds_cash_cost,
      sum(
        if (
          marketing_target in (3, 8, 14, 15),
          total_amount,
          0
        )
      ) as ds_cost,
      sum(
        if (
          marketing_target in (3, 8, 14, 15),
          click_rgmv_7d,
          0
        )
      ) as ds_rgmv,
      sum(
        if (
          marketing_target in (3, 8,  15),
          total_amount,
          0
        )
      ) as cvr_cost,
      sum(
        if (
          marketing_target in (3, 8, 15),
          click_rgmv_7d,
          0
        )
      ) as cvr_click_rgmv_7d,
      sum(if (marketing_target in (3), cost_amount, 0)) as spxl_cash_cost,
      sum(if (marketing_target in (3), total_amount, 0)) as spxl_cost,
      sum(if (marketing_target in (3), click_rgmv_7d, 0)) as spxl_rgmv,
      sum(if (marketing_target in (8), cost_amount, 0)) as zbtg_cash_cost,
      sum(if (marketing_target in (8), total_amount, 0)) as zbtg_cost,
      sum(if (marketing_target in (8), click_rgmv_7d, 0)) as zbtg_rgmv,
      sum(if (marketing_target in (14), cost_amount, 0)) as zbyr_cash_cost,
      sum(if (marketing_target in (14), total_amount, 0)) as zbyr_cost,
      sum(if (marketing_target in (14), click_rgmv_7d, 0)) as zbyr_rgmv,
      sum(if (marketing_target in (15), cost_amount, 0)) as dplx_cash_cost,
      sum(if (marketing_target in (15), total_amount, 0)) as dplx_cost,
      sum(if (marketing_target in (15), click_rgmv_7d, 0)) as dplx_rgmv,
      sum(
        if (
          marketing_target = 8,
          live_24h_click_effective_shutdown_distinct_eventid_num,
          0
        )
      ) as live_24h_click_effective_shutdown_distinct_eventid_num, --有效观播uv
      sum(
        if (
          marketing_target = 8,
          live_24h_click_effective_shutdown_num,
          0
        )
      ) as live_24h_click_effective_shutdown_num, --有效观播uv
      sum(if (marketing_target = 8, live_order_num, 0)) as live_order_num --直播间订单数
    from
      redcdm.dm_ads_rtb_creativity_1d_di
    where
      dtm between '20221001' and '{{ds_nodash}}'
    group by
      1,
      2
  ) b on a.brand_account_id = b.brand_account_id
  and a.day_dtm = b.dtm
  left join ( --电商成交数据
    select
      dtm,
      brand_account_id,
      sum(deal_gmv) as deal_gmv,
      sum(if (traffic_type = '广告', deal_gmv, 0)) as ads_deal_gmv,
      sum(if (channel = '店播', deal_gmv, 0)) as dianbo_deal_gmv,
      sum(
        if (
          traffic_type = '广告'
          and channel = '店播',
          deal_gmv,
          0
        )
      ) as ads_dianbo_deal_gmv,
      sum(if (channel = 'K播', deal_gmv, 0)) as kbo_deal_gmv,
      sum(if (channel_group = '笔记', deal_gmv, 0)) as biji_deal_gmv
    from
      redapp.app_ads_trd_user_seller_account_detail_df
    where
      dtm between '20221001' and '{{ds_nodash}}'
    group by
      1,
      2
  ) c on a.brand_account_id = c.brand_account_id
  and a.day_dtm = c.dtm
  left join ( --过去30天dgmv
    --过去30天dgmv
    select
      date_key,
      dtm,
      brand_account_id,
      deal_gmv,
      sum(deal_gmv) over (
        partition by
          brand_account_id
        order by
          cast(date_key as date) asc rows between 29 PRECEDING
          and 0 FOLLOWING
      ) as deal_gmv_30d
    from
      (
        select
          dt as date_key,
          day_dtm as dtm,
          a.brand_account_id,
          coalesce(deal_gmv, 0) as deal_gmv
        from
          temp.temp_app_ads_industry_ecm_target_metric_df_{{ds_nodash}}_online a
          left join (
            select
              f_getdate (dtm) as date_key,
              dtm,
              brand_account_id,
              sum(deal_gmv) as deal_gmv
            from
              redapp.app_ads_trd_user_seller_account_detail_df
            where
              dtm between '20221001' and '{{ds_nodash}}'
            group by
              1,
              2,
              3
          ) base on a.brand_account_id = base.brand_account_id
          and a.day_dtm = base.dtm
      ) info
    group by
      date_key,
      dtm,
      brand_account_id,
      deal_gmv
  ) d on a.brand_account_id = d.brand_account_id
  and a.day_dtm = d.dtm
  left join ( --商家数
    select
      dtm,
      user_id,
      count(distinct seller_id) as seller_cnt
    from
      redcdm.dim_seller_account_relation_df
    where
      bind_status > 0
      and dtm between '20221001' and '{{ds_nodash}}'
    group by
      1,
      2
  ) e on a.brand_account_id = e.user_id
  and a.day_dtm = e.dtm
  left join ( --cid投放情况
    select
      dtm,
      brand_account_id,
      sum(cost) as cid_cost_cpc,
      sum(out_click_rgmv_15d) / 100 as out_click_rgmv_15d,
      sum(out_click_goods_view_pv_15d) as out_click_goods_view_pv_15d
    from
      redst.st_ads_wide_cpc_creativity_day_inc_slow_view
    where
      dtm between '20221001' and '{{ds_nodash}}'
      and marketing_target = 13
    group by
      1,
      2
  ) f on a.brand_account_id = f.brand_account_id
  and a.day_dtm = f.dtm
  left join ( --品牌种草+转化
    select
      dtm,
      brand_account_id,
      sum(if (ti_user_type = '品牌种草人群', cost, 0)) as brand_ti_user_cost
    from
      redapp.app_ads_cvr_account_detail_di
    where
      marketing_target = 3
      and dtm between '20221001' and '{{ds_nodash}}'
    group by
      1,
      2
  ) t1 on a.brand_account_id = t1.brand_account_id
  and a.day_dtm = t1.dtm
  left join ( --品牌种草人群定向
    select
      dtm,
      brand_account_id,
      sum(
        if (target_type_msg_list like '%品牌种草人群定向%', cost, 0)
      ) as brand_ti_target_cost
    from
      redapp.app_ads_cvr_creativity_user_account_di
    where
      marketing_target = 3
      and  dtm between '20221001' and '{{ds_nodash}}'
    group by
      1,
      2
  ) t2 on a.brand_account_id = t2.brand_account_id
  and a.day_dtm = t2.dtm
  left join ( --双月收入

    select
      dtm,
      brand_account_id,
      zc_cash_cost_2m,
      cid_cash_cost_2m ,
      ds_cash_cost_2m ,
      spxl_cash_cost_2m ,
      zbtg_cash_cost_2m,
      zbyr_cash_cost_2m,
      dplx_cash_cost_2m
  from
    temp.temp_app_ads_industry_ecm_target_metric_df_{{ds_nodash}}_bimonth_online
  where dtm between '20231101' and '{{ds_nodash}}'
  ) t3 on a.brand_account_id = t3.brand_account_id
  and a.day_dtm = t3.dtm
  left join (
    select
      dtm,
      brand_account_id,
       brand_ti_user_cost_2m,
       brand_ti_target_cost_2m--品牌种草人群定向
    from
      temp.temp_app_ads_industry_ecm_target_metric_df_{{ds_nodash}}_bimonth_02_online
   
  where dtm between '20231101' and '{{ds_nodash}}'
  ) t4 on a.brand_account_id = t4.brand_account_id
  and a.day_dtm = t4.dtm
left join ( 

    select
      dtm,
      brand_account_id,
      deal_gmv_2m,
     dianbo_deal_gmv_2m
  from
    temp.temp_app_ads_industry_ecm_target_metric_df_{{ds_nodash}}_bimonth_03_online
  where dtm between '20231101' and '{{ds_nodash}}'
  ) t5 on a.brand_account_id = t5.brand_account_id
  and a.day_dtm = t5.dtm

