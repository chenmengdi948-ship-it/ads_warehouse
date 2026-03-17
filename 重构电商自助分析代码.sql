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
  if(month(dt) & 1 = 1,trunc(dt,'MM'),trunc(add_months(dt,-1),'MM')) as bimonth_dt,
  week_label as week_dt
from
  ( --账户基础信息
    select
      brand_account_id,
      brand_user_name,
      direct_sales_dept5_name,
      company_name,
      company_code,
      brand_group_tag_code,
      brand_group_tag_name,
      max(track_industry_dept_group_name) as track_industry_dept_group_name
    from
      redapp.app_ads_insight_industry_product_account_td_df
    where
      dtm = '{{ds_nodash}}'
      --and track_industry_dept_group_name in ('奢品', '美妆', '服饰潮流','美护')
      --and direct_sales_dept2_name in ('行业团队','生态客户业务部','渠道业务部')
      and date_key = '{{ds}}'
    group by
      1,
      2,
      3,
      4,
      5,
      6,
      7
  ) a
  join (
    select
      day_dtm,
      dt,
      week_label
    from
      redcdm.dim_ads_date_df
    where
      dtm = 'all'
      and day_dtm between '20221201' and '{{ds_nodash}}'
  ) dt on 1 = 1;

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
      bimonth_dt,
      week_dt
    from
      temp.temp_app_ads_industry_ecm_target_metric_df_{{ds_nodash}}_online
    where
      day_dtm between   f_getdate('{{ds_nodash}}', -7) and '{{ds_nodash}}'
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
      and dtm between   f_getdate('{{ds_nodash}}', -7) and '{{ds_nodash}}'
    group by
      1,
      2
  ) b on a.brand_account_id = b.brand_account_id
  and a.day_dtm = b.dtm
  )info; 

drop table if exists temp.temp_app_ads_industry_ecm_target_metric_df_{{ds_nodash}}_bimonth_04_online;

create table
  temp.temp_app_ads_industry_ecm_target_metric_df_{{ds_nodash}}_bimonth_04_online as 
--新增计划&素材供给字段（限制电商闭环广告),对齐 https://redbi.devops.xiaohongshu.com/dashboard/list?type=1&dashboardId=4723&projectId=4&pageId=page_UkuCnkWs9a  看板中相关口径
select coalesce(t1.dtm,t2.create_dt) as dtm,
    f_getdate(coalesce(t1.dtm,t2.create_dt)) as date_key,
    coalesce(t1.brand_account_id,t2.brand_account_id) as brand_account_id,
    count(distinct case when total_amount>0 then t1.campaign_id else null end) as cost_campaign_cnt,
    count(distinct  t1.campaign_id ) as valid_campaign_cnt,
    count(distinct case when cost_amount>0 then t1.campaign_id else null end) as cash_cost_campaign_cnt,
    count(distinct case when campaign_first_cost_date=f_getdate(dtm) and  total_amount>0 then t1.campaign_id else null end) as new_cost_campaign_cnt,
    count(distinct case when create_dt=dtm and cost_amount>0 then t1.campaign_id else null end) as new_cash_cost_campaign_cnt,
    count(distinct case when create_dt=dtm  then t1.campaign_id else null end) as new_valid_campaign_cnt,
    count(distinct  t2.campaign_id ) as new_campaign_cnt,
    count(distinct case when total_amount>0 then t1.note_id else null end) as cost_note_cnt,
    count(distinct  t1.note_id ) as valid_note_cnt,
    count(distinct case when cost_amount>0 then t1.note_id else null end) as cash_cost_note_cnt,
    count(distinct case when note_first_cost_date=f_getdate(dtm) and  total_amount>0 then t1.note_id else null end) as new_cost_note_cnt
from 
(select  brand_account_id,
    dtm,
    campaign_id,
    case when ads_material_type='post' then ads_material_id else null end as note_id,
    sum(total_amount) as total_amount,
    sum(cost_amount) as cost_amount
from redcdm.dm_ads_rtb_creativity_1d_di
where dtm>='20240801' and dtm<='{{ds_nodash}}'
and (is_effective =1 
    or total_amount>0)
and marketing_target in (3,8,14,15)
group by 1,2,3,4
)t1 
left join 
(select campaign_id,min(first_cost_date) as campaign_first_cost_date
from redapp.app_ads_industry_rtb_creativity_di
where dtm='{{ds_nodash}}'
group by campaign_id
)cam
on t1.campaign_id = cam.campaign_id
left join 
(select ads_material_id as note_id,min(first_cost_date) as note_first_cost_date
from redapp.app_ads_industry_rtb_creativity_di
where dtm='{{ds_nodash}}'
group by 1
)note 
on t1.note_id = note.note_id
full outer join 
(select a.id as campaign_id,
    advertiser_id,
    brand_account_id,
    from_unixtime(floor(a.create_time / 1000 + 28800), 'yyyyMMdd') as create_dt
from redcdm.dwd_ads_rtb_campaign_df a
left join redcdm.dim_ads_advertiser_df b on a.v_seller_id = b.virtual_seller_id and  b.dtm='{{ds_nodash}}'
where a.dtm='{{ds_nodash}}' and a.marketing_target in (3,8,14,15) and a.platform in (0,1,2,7,9)
group by 1,2,3,4
)t2 
on t1.campaign_id = t2.campaign_id and t1.dtm=t2.create_dt
group by coalesce(t1.dtm,t2.create_dt),
    coalesce(t1.brand_account_id,t2.brand_account_id)
;
insert overwrite table redapp.app_ads_industry_ecm_target_metric_df partition (dtm='{{ds_nodash}}')
select t1.date_key,
  t1.brand_account_id,
  dim.brand_user_name,
  dim.track_industry_dept_group_name,
  dim.direct_sales_dept5_name,
  dim.company_name,
  dim.company_code,
  dim.brand_group_tag_code,
  dim.brand_group_tag_name,
  is_open_store,
  ti_cash_cost,
  ecm_unclosed_cash_cost,
  ecm_unclosed_cost,
  out_click_rgmv_15d,
  out_click_goods_view_pv_15d,
  ecm_closed_cash_cost,
  ecm_closed_cost,
  ecm_closed_rgmv,
  sx_cash_cost,
  sx_cost,
  sx_rgmv,
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
  deal_gmv,
  ads_deal_gmv,
  s_deal_gmv,
  ads_s_deal_gmv,
  k_deal_gmv,
  note_deal_gmv,
  deal_gmv_30d,
  is_high_sales,
  brand_ti_user_cost,
  brand_ti_target_cost,
  deal_gmv_2m,
  s_deal_gmv_2m,
  brand_ti_user_cost_2m,
  brand_ti_target_cost_2m,
  ti_cash_cost_2m,
  ecm_unclosed_cash_cost_2m,
  ecm_closed_cash_cost_2m,
  sx_cash_cost_2m,
  zbtg_cash_cost_2m,
  zbyr_cash_cost_2m,
  dplx_cash_cost_2m,
  cvr_cost,
  cvr_click_rgmv_7d,
  day_of_week,
  bimonth_dt,
  days_of_bimonth,
  bimonth_days,
  ecm_closed_cash_cost_1w,
  cost_campaign_cnt,
  valid_campaign_cnt,
  cash_cost_campaign_cnt,
  new_cost_campaign_cnt,
  new_cash_cost_campaign_cnt,
  new_valid_campaign_cnt,
  new_campaign_cnt,
  cost_note_cnt,
  cash_cost_note_cnt,
  valid_note_cnt,
  new_cost_note_cnt,
  live_num,
  live_duration,
  store_dgmv,
  mini_deal_gmv,
  is_open_mini_store
from 
(
select a.dt as date_key,
  a.brand_account_id,
  a.brand_user_name,
  track_industry_dept_group_name,
  direct_sales_dept5_name,
  company_name,
  company_code,
  brand_group_tag_code,
  brand_group_tag_name,
  if (seller_cnt > 0, 1, 0) as is_open_store,
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
  cvr_click_rgmv_7d,
  ds_cash_cost_1w as ecm_closed_cash_cost_1w,
  live_duration,
  live_num,
  store_dgmv,
  if (mini_seller_cnt > 0, 1, 0) as is_open_mini_store,
  cost_campaign_cnt,

  valid_campaign_cnt,

  cash_cost_campaign_cnt,

  new_cost_campaign_cnt,

  new_cash_cost_campaign_cnt,

  new_valid_campaign_cnt,

  new_campaign_cnt,

  cost_note_cnt,

  cash_cost_note_cnt,

  valid_note_cnt,

  new_cost_note_cnt
 -- a.day_dtm as dtm
from
  (
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
      bimonth_dt,
      week_dt
     
    from
      temp.temp_app_ads_industry_ecm_target_metric_df_{{ds_nodash}}_online
    where
      day_dtm >=  f_getdate('{{ds_nodash}}', -7) 
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
      dtm between   f_getdate('{{ds_nodash}}', -7)  and '{{ds_nodash}}'
    group by
      1,
      2
  ) b on a.brand_account_id = b.brand_account_id
  and a.day_dtm = b.dtm
  left join ( --电商成交数据
    select
      dtm,
      seller_user_id as brand_account_id,
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
      redapp.app_ads_ecm_seller_account_detail_di
    where
      dtm between   f_getdate('{{ds_nodash}}', -7) and '{{ds_nodash}}'
    group by
      1,
      2
  ) c on a.brand_account_id = c.brand_account_id
  and a.day_dtm = c.dtm

  left join 
  (select dtm,
    anchor_id as brand_account_id,
    sum(live_duration) as live_duration,
    sum(live_num) as live_num,
    sum(store_dgmv) as store_dgmv
  from redcdm.dm_ads_ecm_live_seller_metrics_2d_di 
  where dtm between   f_getdate('{{ds_nodash}}', -7) and dtm<='{{ds_nodash}}'
  group by 1,2
  )d 
  on a.brand_account_id = d.brand_account_id
   and a.day_dtm = d.dtm
  left join ( --商家数
    select
      dtm,
      user_id,
      count(distinct seller_id) as seller_cnt,
      count(distinct case when seller_type = 90 then seller_id else null end) as mini_seller_cnt
    from
      reddw.dw_trd_seller_base_metrics_day
    where
      state in (100,200,300)
      and dtm between   f_getdate('{{ds_nodash}}', -7)  and '{{ds_nodash}}'
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
      dtm between   f_getdate('{{ds_nodash}}', -7)  and '{{ds_nodash}}'
      and marketing_target = 13
    group by
      1,
      2
  ) f on a.brand_account_id = f.brand_account_id
  and a.day_dtm = f.dtm
left join  temp.temp_app_ads_industry_ecm_target_metric_df_{{ds_nodash}}_bimonth_04_online tt
on tt.brand_account_id=a.brand_account_id and a.dt = tt.date_key
  left join (
    select
      dtm,
      brand_account_id,
      brand_ti_target_cost,
      brand_ti_user_cost,
       brand_ti_user_cost_2m,
       brand_ti_target_cost_2m--品牌种草人群定向
    from
      temp.temp_app_ads_industry_ecm_target_metric_df_{{ds_nodash}}_bimonth_02_online
   
  where dtm between   f_getdate('{{ds_nodash}}', -7) and '{{ds_nodash}}'
  ) t4 on a.brand_account_id = t4.brand_account_id
  and a.day_dtm = t4.dtm
left join ( 

    select
      date_key,
      brand_account_id,
      deal_gmv_2m,
      s_deal_gmv_2m as dianbo_deal_gmv_2m,
      deal_gmv_30d,
      ecm_unclosed_cash_cost_2m as cid_cash_cost_2m ,
      ecm_closed_cash_cost_2m as ds_cash_cost_2m ,
      sx_cash_cost_2m as spxl_cash_cost_2m ,
      zbtg_cash_cost_2m,
      zbyr_cash_cost_2m,
      dplx_cash_cost_2m,
      zc_cash_cost_2m as zc_cash_cost_2m ,
      ecm_closed_cash_cost_1w as ds_cash_cost_1w
  from
    redapp.app_ads_industry_ecm_target_metric_mid_df
  where dtm ='{{ds_nodash}}' and date_key  between   f_getdate('{{ds}}', -7) and '{{ds}}'
  ) t5 on a.brand_account_id = t5.brand_account_id
  and a.dt= t5.date_key
  
union all 
--20220101-20231101之前数据
SELECT
  date_key,
  brand_account_id,
  brand_user_name,
  case when track_industry_dept_group_name ='美妆' then '美护' else track_industry_dept_group_name end as  track_industry_dept_group_name,
  direct_sales_dept5_name,
  company_name,
  company_code,
  brand_group_tag_code,
  brand_group_tag_name,
  is_open_store,
  ti_cash_cost,
  ecm_unclosed_cash_cost,
  ecm_unclosed_cost,
  out_click_rgmv_15d,
  out_click_goods_view_pv_15d,
  ecm_closed_cash_cost,
  ecm_closed_cost,
  ecm_closed_rgmv,
  sx_cash_cost,
  sx_cost,
  sx_rgmv,
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
  deal_gmv,
  ads_deal_gmv,
  s_deal_gmv,
  ads_s_deal_gmv,
  k_deal_gmv,
  note_deal_gmv,
  deal_gmv_30d,
  is_high_sales,
  brand_ti_user_cost,
  brand_ti_target_cost,
  deal_gmv_2m,
  s_deal_gmv_2m,
  brand_ti_user_cost_2m,
  brand_ti_target_cost_2m,
  ti_cash_cost_2m,
  ecm_unclosed_cash_cost_2m,
  ecm_closed_cash_cost_2m,
  sx_cash_cost_2m,
  zbtg_cash_cost_2m,
  zbyr_cash_cost_2m,
  dplx_cash_cost_2m,
  cvr_cost,
  cvr_click_rgmv_7d,
  ecm_closed_cash_cost_1w,
  live_duration,
  live_num,
  store_dgmv,
  is_open_mini_store,
  cost_campaign_cnt,

  valid_campaign_cnt,

  cash_cost_campaign_cnt,

  new_cost_campaign_cnt,

  new_cash_cost_campaign_cnt,

  new_valid_campaign_cnt,

  new_campaign_cnt,

  cost_note_cnt,

  cash_cost_note_cnt,

  valid_note_cnt,

  new_cost_note_cnt
FROM
  redapp.app_ads_industry_ecm_target_metric_df
where dtm=f_getdate ('{{ds_nodash}}', -1)
and date_key<   f_getdate('{{ds}}', -7) 
)t1 
left join 
(select
  dt,
  day_of_week,
  bimonth_dt,
  sum(1) over(
    partition by bimonth_dt
    order by
      dt asc rows between unbounded PRECEDING
      and current row
  ) as days_of_bimonth,
  sum(1) over(partition by bimonth_dt) as bimonth_days
from
  (
    select
      dt,
      day_of_week,
      if(
        month(dt) & 1 = 1,
        trunc(dt, 'MM'),
        trunc(add_months(dt, -1), 'MM')
      ) as bimonth_dt
    from
      redcdm.dim_ads_date_df
    where
      dtm = 'all'
      and dt >= '2022-01-01'
      and dt <= f_getdate('{{ds}}',70) --需包含最新完整双月
  ) t1
)t2 on t1.date_key=t2.dt

left join 

--小程序gmv 
(SELECT
  t2.pro_user_id,
  f_getdate(t1.dtm) as date_key,
  sum(dgmv) as mini_deal_gmv
FROM
  reddw.dw_trd_o2o_order_package_day_inc t1
  left join 
  redcdm.dim_seller_base_df t2 on t1.seller_id = t2.seller_id and t2.dtm='{{ds_nodash}}'
WHERE
 t1.dtm>='20230101' and t1.dtm<='{{ds_nodash}}'
  and t1.is_valid=1 
  and t1.channel_type=202
group by 1,2
)mini 
on mini.pro_user_id =t1.brand_account_id and t1.date_key = mini.date_key
left join temp.temp_app_ads_industry_ecm_target_metric_df_{{ds_nodash}}_online dim 
on dim.brand_account_id=t1.brand_account_id and dim.dt=t1.date_key
