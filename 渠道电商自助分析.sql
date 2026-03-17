-- ************************************************
-- Author: chenmengdi
-- CreateTime:2023-11-28T16:41:26+08:00
-- Update: Task Update Description
-- ************************************************
drop table if exists temp.temp_app_ads_industry_agent_ecm_target_metric_df_{{ds_nodash}}_online;

create table
  temp.temp_app_ads_industry_agent_ecm_target_metric_df_{{ds_nodash}}_online as
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
  week_label as week_dt,
  agent_type,
  gp
from
  ( --账户基础信息
    select
      a.brand_account_id,
      a.brand_account_name as brand_user_name,
      case when (coalesce(cpc_direct_sales_dept2_name,cpc_operator_dept2_name,if(a.company_name is null,'创作者商业化部','未挂接'))='行业团队') then 
      (case when (track_industry_name is not null) then
        ( case when track_industry_name='生活服务' then track_group_name
           when  track_industry_name!='生活服务' then  track_industry_name end)
        when (track_industry_name is null) then (
            case when cpc_direct_sales_dept4_name='美妆洗护行业' then '美护'
            when  cpc_direct_sales_dept4_name='奢品行业' then '奢品'
            when  cpc_direct_sales_dept4_name='服饰潮流行业' then '服饰潮流'
           else '行业团队其他' end
           )end
        )
        when coalesce(cpc_direct_sales_dept2_name,cpc_operator_dept2_name,if(a.company_name is null,'创作者商业化部','未挂接')) in ('生态客户业务部','创作者商业化部') then 
        coalesce(cpc_direct_sales_dept2_name,cpc_operator_dept2_name,if(a.company_name is null,'创作者商业化部','未挂接'))
        else '自闭环及其他' end as track_industry_dept_group_name,
      a.cpc_direct_sales_dept5_name as direct_sales_dept5_name,
      a.company_name,
      a.company_code,
      t1.brand_tag_code as  brand_group_tag_code,
      t1.brand_tag_name as brand_group_tag_name,
case
            when t3.agent_type in ('SME服务商', '直签', '内广', '家居渠道') then t3.agent_type
            when t1.second_ad_industry_name in ('汽车厂商', '汽车经销商')
            and t1.agent_company_name is not null then '汽车代理'
            when t3.agent_type in ('汽车代理', '汽车行代', '汽车服务商')
            and t1.second_ad_industry_name not in ('汽车厂商', '汽车经销商') then '汽车代理'
            when t1.agent_company_name is null
            or t1.agent_company_name = '' then '直签'
            when t3.agent_type is not null then t3.agent_type
            else '未挂接'
          end as agent_type,
           case
        when t2.agnet_name is not null then t2.gp
        else t1.agent_company_name
      end as gp
    from
      redcdm.dim_ads_advertiser_df a 
     join ads_data_crm.dim_ads_crm_virtual_seller_id_info_df t1 on a.virtual_seller_id = t1.virtual_seller_id and t1.dtm='{{ds_nodash}}'
     left join (
        select
          agnet_name,
          gp
        from
          reddim.dim_ads_agent_list_lengte_2201_month
        where
          dtm = '20220124'
        group by 1,2
      ) t2 on t1.agent_company_name = t2.agnet_name
    left join 
    ( select
          1 as tag, --效果品牌
          agnet_name,
          agent_type --只有agnet_name='广州鼎承文化传媒科技有限公司'有重复，兜底处理下已和bi对齐
        from
          reddim.dim_ads_agent_list_lengte_2201_month
        where
          dtm = '20220124'
          and agent_type <> '品合任务代理'
          and agent_type not in ('蒲公英-自助代理', '蒲公英-框架代理')
        group by 1,2,3
    )t3 on t3.agnet_name = t1.agent_company_name
    
    where
      a.dtm = '{{ds_nodash}}'
      and a.agent_user_name<>''
    
    group by
      1,
      2,
      3,
      4,
      5,
      6,
      7,8,9,10
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


drop table if exists temp.temp_app_ads_industry_agent_ecm_target_metric_df_{{ds_nodash}}_bimonth_online;

create table
  temp.temp_app_ads_industry_agent_ecm_target_metric_df_{{ds_nodash}}_bimonth_online as
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
      sum(ds_cash_cost) over (
        partition by
          brand_account_id,week_dt
        order by
          cast(date_key as date) asc rows between unbounded  PRECEDING
          and current row
      ) as ds_cash_cost_1w,
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
          week_dt,
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
      temp.temp_app_ads_industry_agent_ecm_target_metric_df_{{ds_nodash}}_online
    where
      day_dtm >= '20230101' --从11月加双月指标
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
      dtm between '20230101' and '{{ds_nodash}}'
    group by
      1,
      2
  ) b on a.brand_account_id = b.brand_account_id
  and a.day_dtm = b.dtm
  )info;  
drop table if exists temp.temp_app_ads_industry_agent_ecm_target_metric_df_{{ds_nodash}}_bimonth_02_online;

create table
  temp.temp_app_ads_industry_agent_ecm_target_metric_df_{{ds_nodash}}_bimonth_02_online as
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
      temp.temp_app_ads_industry_agent_ecm_target_metric_df_{{ds_nodash}}_online
    where
      day_dtm between '20231201' and '{{ds_nodash}}'
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
      and dtm between '20231201' and '{{ds_nodash}}'
    group by
      1,
      2
  ) b on a.brand_account_id = b.brand_account_id
  and a.day_dtm = b.dtm
  )info; 
drop table if exists temp.temp_app_ads_industry_agent_ecm_target_metric_df_{{ds_nodash}}_bimonth_03_online;

create table
  temp.temp_app_ads_industry_agent_ecm_target_metric_df_{{ds_nodash}}_bimonth_03_online as
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
            temp.temp_app_ads_industry_agent_ecm_target_metric_df_{{ds_nodash}}_online
          where
            day_dtm between '20221201' and '{{ds_nodash}}'
          )  a
          left join (
            select
              f_getdate (dtm) as date_key,
              dtm,
              brand_account_id,
              sum(deal_gmv) as deal_gmv,
              sum(if (channel = '店播', deal_gmv, 0)) as dianbo_deal_gmv
            from
              redapp.app_ads_ecm_seller_account_detail_di
            where
              dtm between '20221201' and '{{ds_nodash}}'
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
-- drop table if exists temp.temp_app_ads_industry_agent_ecm_target_metric_df_{{ds_nodash}}_bimonth_04_online;

-- create table
--   temp.temp_app_ads_industry_agent_ecm_target_metric_df_{{ds_nodash}}_bimonth_04_online as 
-- --新增计划&素材供给字段（限制电商闭环广告),对齐 https://redbi.devops.xiaohongshu.com/dashboard/list?type=1&dashboardId=4723&projectId=4&pageId=page_UkuCnkWs9a  看板中相关口径
-- select coalesce(t1.dtm,t2.create_dt) as dtm,
--     f_getdate(coalesce(t1.dtm,t2.create_dt)) as date_key,
--     coalesce(t1.brand_account_id,t2.brand_account_id) as brand_account_id,
--     count(distinct case when total_amount>0 then t1.campaign_id else null end) as cost_campaign_cnt,
--     count(distinct  t1.campaign_id ) as valid_campaign_cnt,
--     count(distinct case when cost_amount>0 then t1.campaign_id else null end) as cash_cost_campaign_cnt,
--     count(distinct case when campaign_first_cost_date=f_getdate(dtm) and  total_amount>0 then t1.campaign_id else null end) as new_cost_campaign_cnt,
--     count(distinct case when create_dt=dtm and cost_amount>0 then t1.campaign_id else null end) as new_cash_cost_campaign_cnt,
--     count(distinct case when create_dt=dtm  then t1.campaign_id else null end) as new_valid_campaign_cnt,
--     count(distinct  t2.campaign_id ) as new_campaign_cnt
-- from 
-- (select  brand_account_id,
--     dtm,
--     campaign_id,
--     sum(total_amount) as total_amount,
--     sum(cost_amount) as cost_amount
-- from redcdm.dm_ads_rtb_creativity_1d_di
-- where dtm>='20230101' and dtm<='{{ds_nodash}}'
-- and (is_effective =1 
--     or total_amount>0)
-- and marketing_target in (3,8,14,15)
-- group by 1,2,3
-- )t1 
-- left join 
-- (select campaign_id,min(first_cost_date) as campaign_first_cost_date
-- from redapp.app_ads_industry_rtb_creativity_di
-- where dtm='{{ds_nodash}}'
-- group by campaign_id
-- )cam
-- on t1.campaign_id = cam.campaign_id
-- full outer join 
-- (select a.id as campaign_id,
--     advertiser_id,
--     brand_account_id,
--     from_unixtime(floor(a.create_time / 1000 + 28800), 'yyyyMMdd') as create_dt
-- from redcdm.dwd_ads_rtb_campaign_df a
-- left join redcdm.dim_ads_advertiser_df b on a.v_seller_id = b.virtual_seller_id and  b.dtm='{{ds_nodash}}'
-- where a.dtm='{{ds_nodash}}' and a.marketing_target in (3,8,14,15)
-- group by 1,2,3,4
-- )t2 
-- on t1.campaign_id = t2.campaign_id and t1.dtm=t2.create_dt
-- group by coalesce(t1.dtm,t2.create_dt),
--     coalesce(t1.brand_account_id,t2.brand_account_id)
-- ;
insert overwrite table redapp.app_ads_industry_agent_ecm_target_metric_df partition (dtm='{{ds_nodash}}')
select t1.date_key,
  t1.brand_account_id,
  brand_user_name,
  track_industry_dept_group_name,
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
  day_of_week,
  bimonth_dt,
  days_of_bimonth,
  bimonth_days,
  ecm_closed_cash_cost_1w
  -- cost_campaign_cnt,
  -- valid_campaign_cnt,
  -- cash_cost_campaign_cnt,
  -- new_cost_campaign_cnt,
  -- new_cash_cost_campaign_cnt,
  -- new_valid_campaign_cnt,
  -- new_campaign_cnt
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
  ds_cash_cost_1w as ecm_closed_cash_cost_1w
 -- a.day_dtm as dtm
from
  (
    select
      *,
      day_dtm
    from
      temp.temp_app_ads_industry_agent_ecm_target_metric_df_{{ds_nodash}}_online
    where
      day_dtm >= '20230101'
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
      dtm between '20230101' and '{{ds_nodash}}'
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
      dtm between '20230101' and '{{ds_nodash}}'
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
          temp.temp_app_ads_industry_agent_ecm_target_metric_df_{{ds_nodash}}_online a
          left join (
            select
              f_getdate (dtm) as date_key,
              dtm,
              brand_account_id,
              sum(deal_gmv) as deal_gmv
            from
              redapp.app_ads_ecm_seller_account_detail_di
            where
              dtm between '20230101' and '{{ds_nodash}}'
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
      and dtm between '20230101' and '{{ds_nodash}}'
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
      dtm between '20231101' and '{{ds_nodash}}'
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
      and dtm between '20240101' and '{{ds_nodash}}'
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
      and  dtm between '20240101' and '{{ds_nodash}}'
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
      ds_cash_cost_1w,
      spxl_cash_cost_2m ,
      zbtg_cash_cost_2m,
      zbyr_cash_cost_2m,
      dplx_cash_cost_2m
  from
    temp.temp_app_ads_industry_agent_ecm_target_metric_df_{{ds_nodash}}_bimonth_online
  where dtm between '20230101' and '{{ds_nodash}}'
  ) t3 on a.brand_account_id = t3.brand_account_id
  and a.day_dtm = t3.dtm
  left join (
    select
      dtm,
      brand_account_id,
       brand_ti_user_cost_2m,
       brand_ti_target_cost_2m--品牌种草人群定向
    from
      temp.temp_app_ads_industry_agent_ecm_target_metric_df_{{ds_nodash}}_bimonth_02_online
   
  where dtm between '20240101' and '{{ds_nodash}}'
  ) t4 on a.brand_account_id = t4.brand_account_id
  and a.day_dtm = t4.dtm
left join ( 

    select
      dtm,
      brand_account_id,
      deal_gmv_2m,
     dianbo_deal_gmv_2m
  from
    temp.temp_app_ads_industry_agent_ecm_target_metric_df_{{ds_nodash}}_bimonth_03_online
  where dtm between '20230101' and '{{ds_nodash}}'
  ) t5 on a.brand_account_id = t5.brand_account_id
  and a.day_dtm = t5.dtm

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
-- left join  temp.temp_app_ads_industry_agent_ecm_target_metric_df_{{ds_nodash}}_bimonth_04_online t3
-- on t3.brand_account_id=t1.brand_account_id and t1.date_key = t3.date_key


































