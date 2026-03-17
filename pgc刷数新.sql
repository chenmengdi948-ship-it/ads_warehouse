
    -- ************************************************
    -- Author: chenmengdi
    -- CreateTime:2024-01-03T20:13:35+08:00
    -- Update: Task Update Description
    -- ************************************************
    -----------------temp.temp_app_ads_ecm_note_spu_{{ds_nodash}}_nt_his在dor上单独执行，记得修改 temp.temp_app_ads_ecm_note_spu_{{ds_nodash}}_1_his的temp.temp_app_ads_ecm_note_spu_20240220_nt_his
-- drop table if exists temp.temp_app_ads_ecm_note_spu_{{ds_nodash}}_nt_his;
-- create table temp.temp_app_ads_ecm_note_spu_{{ds_nodash}}_nt_his as 
-- select '{{ds_nodash}}' as dtm,
--   b.type,
--   a.user_id as brand_user_id,
--   a.note_id,
--   min(create_time) as create_time,
--   if(concat_ws(',', collect_set(a.type)) like '%pugongying%',
--   'pugongying',
--     if(
--     concat_ws(',', collect_set(a.type)) like '%kos%',
--     'kos',
--     if(
--       concat_ws(',', collect_set(a.type)) like '%cps%',
--       'cps',
--       'gh'
--     )
--   )
--   ) as note_type
-- from
--   (
--     select dtm,
--       discovery_id as note_id,
--       user_id,
--       substring(create_time, 1, 10) as create_time,
--       'guanhao' as type
--     from
--       reddw.dw_soc_discovery_delta_7_day
--     where
--    dtm = '{{ds_nodash}}'
--       and (
--         is_brand = 1
--         -- or is_cps_note = 1
--         -- or is_bind = 1
--       )
--       and substring(publish_time, 1, 10) >= '2023-01-01'
--       and substring(publish_time, 1, 10) <= f_getdate(dtm)
--     union all
--     select dtm,
--       note_id,
--       report_brand_user_id as user_id,
--       substring(note_publish_time, 1, 10) as create_time,
--       'pugongying' as type
--     from
--       redcdm.dwd_ads_bcoo_ord_note_df --蒲公英笔记
--     where
--       dtm = '{{ds_nodash}}'
--       and order_status in (401, 402,420,421)
--       and substr(order_create_time, 1, 4) >= '2023'
--     group by dtm,
--       note_id,
--       substring(note_publish_time, 1, 10),
--       report_brand_user_id
--     union all
--     select dtm,
--       note_id,
--       brand_account_id as user_id,
--       substring(create_time, 1, 10) as create_time,
--       'cps' as type
--     from
--       redapp.app_ads_matrix_user_shopping_note_info_1d_df
--     where
--       dtm = '{{ds_nodash}}'
--       and substr(create_time, 1, 4) >= '2023'
--     group by dtm,
--       note_id,
--       brand_account_id,
--       substring(create_time, 1, 10)
--     union all 
--     --kos笔记
--     select dtm,
--       note_id,
--       brand_user_id as user_id,
--       substr(note_publish_time,1,10) as create_time,
--       'kos' as type
--     from  redods.ods_note_trade_tb_grant_note_df
--     where dtm = '{{ds_nodash}}'
--       and grant_type =1
--       and grant_status in (6,10)
--       and substr(note_publish_time,1,4)>= '2023'
--   ) a
--   left join 
--   (select  discovery_id as note_id,
--       type,
--       user_id
--     from
--       reddw.dw_soc_discovery_delta_7_day
--     where
--       dtm = '{{ds_nodash}}'
--      )b 
--      on a.note_id =b.note_id
-- group by a.dtm,
-- b.type,
--   a.user_id,
--   a.note_id;
drop table
  if exists temp.temp_app_ads_ecm_note_spu_{{ds_nodash}}_1_his;

create table
  temp.temp_app_ads_ecm_note_spu_{{ds_nodash}}_1_his as
select
  t2.brand_account_id,
  brand_account_name,
  company_code,
  company_name,
  group_code,
  group_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  cpc_direct_sales_code,
  cpc_direct_sales_name,
  cpc_direct_sales_dept1_name,
  cpc_direct_sales_dept2_name,
  cpc_direct_sales_dept3_name,
  cpc_direct_sales_dept4_name,
  cpc_direct_sales_dept5_name,
  cpc_direct_sales_dept6_name,
  brand_tag_name,
  brand_group_tag_name,
  cpc_operator_code,
  cpc_operator_name,
  cpc_operator_dept1_name,
  cpc_operator_dept2_name,
  cpc_operator_dept3_name,
  cpc_operator_dept4_name,
  cpc_operator_dept5_name,
  cpc_operator_dept6_name,
  0 as campaign_id,
  0 as unit_id,
  'ALL' as note_id,
  '' as agent_user_id,
  '' as agent_name,
  -911 as build_type,
  -911 as biz_product_type,
  '' as target_type_msg_list,
  -911 as marketing_target,
  -911 as optimize_target,
  '' as optimize_target_msg,
  '' as publish_time,
  '' as campaign_name,
  '' as campaign_placement,
  '' as product,
  type,
  is_brand,
  is_cps_note,
  is_bind,
  is_kos,
  0 as click_cnt,
  0 as imp_cnt,
  0 as cost,
  0 as cash_cost,
  0 as purchase_order_num,
  0 as click_rgmv_7d,
  0 as engage_cnt,
  1 as tag,
  note_num,
  new_note_num,
  cost_spu_cnt, 
  cost_note_num,
  ecm_closed_cost_note_num,
  note.dtm
from
  (
    select
      brand_account_id,
      brand_account_name,
      company_code,
      company_name,
      group_code,
      group_name,
      track_group_id,
      track_group_name,
      track_industry_name,
      track_detail_name,
      cpc_direct_sales_code,
      cpc_direct_sales_name,
      cpc_direct_sales_dept1_code,
      cpc_direct_sales_dept1_name,
      cpc_direct_sales_dept2_code,
      cpc_direct_sales_dept2_name,
      cpc_direct_sales_dept3_code,
      cpc_direct_sales_dept3_name,
      cpc_direct_sales_dept4_code,
      cpc_direct_sales_dept4_name,
      cpc_direct_sales_dept5_code,
      cpc_direct_sales_dept5_name,
      cpc_direct_sales_dept6_code,
      cpc_direct_sales_dept6_name,
      brand_tag_code,
      brand_tag_name,
      brand_group_tag_code,
      brand_group_tag_name,
      cpc_operator_code,
      cpc_operator_name,
      cpc_operator_dept1_code,
      cpc_operator_dept1_name,
      cpc_operator_dept2_code,
      cpc_operator_dept2_name,
      cpc_operator_dept3_code,
      cpc_operator_dept3_name,
      cpc_operator_dept4_code,
      cpc_operator_dept4_name,
      cpc_operator_dept5_code,
      cpc_operator_dept5_name,
      cpc_operator_dept6_code,
      cpc_operator_dept6_name
    from
      redcdm.dim_ads_industry_brand_account_df
    where
      dtm = max_dtm('redcdm.dim_ads_industry_brand_account_df')
  ) t2
  join 
  (select coalesce(note.user_id,all_cost.brand_account_id) as user_id,
      coalesce(note.type,all_cost.type) as type,
      coalesce(note.is_brand,all_cost.is_brand) as  is_brand,
      coalesce(note.is_cps_note,all_cost.is_cps_note) as is_cps_note,
      coalesce(note.is_bind,all_cost.is_bind) as  is_bind,
      coalesce(note.is_kos,all_cost.is_kos) as  is_kos,
      coalesce(note.dtm,all_cost.dtm) as dtm,
      note_num,
      new_note_num,
      cost_note_num,
      ecm_closed_cost_note_num
  from 
  (
    select
      brand_user_id as user_id,
      type,
      if(note_type = 'gh',1, 0) as is_brand,
      if(note_type = 'cps',1, 0) as is_cps_note,
      if(note_type = 'pugongying',1, 0) as is_bind,
      if(note_type = 'kos',1, 0) as is_kos,
      '{{ds_nodash}}' as dtm,
      count(case when substring(create_time, 1, 10)>=f_getdate('{{ds}}',-364) and  substring(create_time, 1, 10)<='{{ds}}' then 1 else null end) as note_num,
      COUNT(
        CASE
          WHEN substring(create_time, 1, 10) = f_getdate(dtm) then 1
          else null
        end
      ) as new_note_num
    from
      temp.temp_app_ads_ecm_note_spu_20240220_nt_his
    group by user_id,
      type,
      if(note_type = 'gh',1, 0) ,
      if(note_type = 'cps',1, 0) ,
      if(note_type = 'pugongying',1, 0) ,
      if(note_type = 'kos',1, 0) 
     -- dtm
  ) note 
  
  full outer join 
  (select   brand_account_id,
    '{{ds_nodash}}' as dtm,
    type,
    coalesce(is_brand, 0) as is_brand,
    coalesce(is_cps_note, 0) as is_cps_note,
    coalesce(is_bind, 0) as is_bind,
    coalesce(is_kos, 0) as is_kos,
    count(distinct ads_material_id ) as cost_note_num,
    count(distinct case when marketing_target in (3, 8, 14, 15) then ads_material_id else null end) as ecm_closed_cost_note_num
  from 
    (select brand_account_id,
        dtm,
        ads_material_id,
        marketing_target,
        type,
        is_brand,
        is_cps_note,
        is_bind,
        is_kos
    from 
      (select brand_account_id,
        dtm,
        ads_material_id,
        marketing_target
    from  redcdm.dm_ads_rtb_creativity_1d_di
    WHERE
      --dtm>='20230101' 
      --2024年改成近365日累计
     dtm>=f_getdate('{{ds_nodash}}',-364)
      and dtm<='{{ds_nodash}}'
      and (
        is_effective = 1
        or total_amount > 0
      )
     -- and marketing_target in (3, 8, 14, 15)
      and ads_material_type = 'post'
      and total_amount>0
      group by brand_account_id,
        dtm,
        ads_material_id,
        marketing_target
      )t1 
      left join 
    ( select
      discovery_id,
      --user_id,
      --substring(publish_time, 1, 10) as publish_time,
      type,
      is_brand,
      is_cps_note,
      is_bind
    from
      reddw.dw_soc_discovery_delta_7_day
    where
      dtm = max_dtm('reddw.dw_soc_discovery_delta_7_day')
    ) note on note.discovery_id = t1.ads_material_id
    left join 
    (select 
      note_id,
      1 as is_kos
    from  redods.ods_note_trade_tb_grant_note_df
    where dtm = max_dtm('redods.ods_note_trade_tb_grant_note_df')
      and grant_type =1
      and grant_status in (6,10)
    group by 1  
    )kos 
    on kos.note_id =t1.ads_material_id
    )t1
    -- join (
    --   select
    --     day_dtm,
    --     dt,
    --     week_label
    --   from
    --     redcdm.dim_ads_date_df
    --   where
    --     dtm = 'all'
    --     and day_dtm between f_getdate('{{ds_nodash}}', -7) and '{{ds_nodash}}'
    -- ) dt on 1 = 1
    -- where t1.dtm>=f_getdate(day_dtm,-364) and t1.dtm<=day_dtm
    group by brand_account_id,
        --day_dtm,
        type,
        coalesce(is_brand, 0) ,
        coalesce(is_cps_note, 0) ,
        coalesce(is_bind, 0),
        coalesce(is_kos, 0)
    )all_cost 
    on all_cost.dtm=note.dtm and all_cost.brand_account_id = note.user_id
    and all_cost.is_brand=note.is_brand and all_cost.is_bind = note.is_bind
    and all_cost.is_cps_note=note.is_cps_note and all_cost.type = note.type and all_cost.is_kos = note.is_kos
    
  )note
  on note.user_id = t2.brand_account_id 
  left join 
  (select regexp_replace(date_key,'-','') as dtm, 
    brand_user_id,
    count(distinct spu_id) as cost_spu_cnt
  from redcdm.dws_ads_note_spu_product_income_detail_td_df 
  where dtm = max_dtm('redcdm.dws_ads_note_spu_product_income_detail_td_df')
    and cash_income_amt <> 0
    and spu_id is not null
    and module='效果'
    and marketing_target in (3,8,14,15)
    and date_key = '{{ds}}'
  group by regexp_replace(date_key,'-',''), 
    brand_user_id
  )spu 
  on spu.brand_user_id=note.user_id and spu.dtm=note.dtm
;

insert overwrite table redapp.app_ads_insight_ecm_closed_account_note_unit_detail_di   partition( dtm )
select   brand_account_id,
  t1.note_id,
  unit_id,
  t1.campaign_id,
  tag,
  campaign_name,
  campaign_placement,
  agent_user_id,
  agent_name,
  product,
  brand_account_name,
  company_code,
  company_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  cpc_direct_sales_code,
  cpc_direct_sales_name,
  cpc_direct_sales_dept1_name,
  cpc_direct_sales_dept2_name,
  cpc_direct_sales_dept3_name,
  cpc_direct_sales_dept4_name,
  cpc_direct_sales_dept5_name,
  cpc_direct_sales_dept6_name,
  brand_tag_name,
  brand_group_tag_name,
  cpc_operator_code,
  cpc_operator_name,
  cpc_operator_dept1_name,
  cpc_operator_dept2_name,
  cpc_operator_dept3_name,
  cpc_operator_dept4_name,
  cpc_operator_dept5_name,
  cpc_operator_dept6_name,
  build_type,
  biz_product_type,
  --target_type_msg_list,
  marketing_target,
  optimize_target,
  optimize_target_msg,
  publish_time,
  type,
  is_brand,
  is_cps_note,
  is_bind,
  click_cnt,
  imp_cnt,
  cost,
  cash_cost,
  purchase_order_num,
  click_rgmv_7d,
  engage_cnt,
  note_num,
  new_note_num,
  cost_spu_cnt,
  campaign_create_date,
  campaign_start_date,
  note_first_cost_date,
  cam.v_seller_id as virtual_seller_id,
  dim.virtual_seller_name,
  cam.advertiser_id,
  note_first_cost_date as note_ecm_first_cost_date,
  campaign_ecm_first_cost_date,
  campaign_first_cost_date,
  cost_note_num,
  ecm_closed_cost_note_num,
  is_kos,
  dtm
from 
(
select
  coalesce(t1.brand_account_id, t2.brand_account_id) as brand_account_id,
  t1.note_id,
  unit_id,
  campaign_id,
  tag,
  campaign_name,
  campaign_placement,
  agent_user_id,
  agent_name,
  product,
  brand_account_name,
  company_code,
  company_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  cpc_direct_sales_code,
  cpc_direct_sales_name,
  cpc_direct_sales_dept1_name,
  cpc_direct_sales_dept2_name,
  cpc_direct_sales_dept3_name,
  cpc_direct_sales_dept4_name,
  cpc_direct_sales_dept5_name,
  cpc_direct_sales_dept6_name,
  brand_tag_name,
  brand_group_tag_name,
  cpc_operator_code,
  cpc_operator_name,
  cpc_operator_dept1_name,
  cpc_operator_dept2_name,
  cpc_operator_dept3_name,
  cpc_operator_dept4_name,
  cpc_operator_dept5_name,
  cpc_operator_dept6_name,
  build_type,
  biz_product_type,
  -- target_type_msg_list,
  marketing_target,
  optimize_target,
  optimize_target_msg,
  publish_time,
  type,
  coalesce(is_brand, 0) as is_brand,
  coalesce(is_cps_note, 0) as is_cps_note,
  coalesce(is_bind, 0) as is_bind,
  coalesce(is_kos, 0) as is_kos,
  sum(unique_click_cnt) as click_cnt,
  sum(unique_imp_cnt) as imp_cnt,
  sum(
    case
      when marketing_target in (3, 8, 15) then total_amount
      else 0
    end
  ) as cost,
  sum(cost_amount) as cash_cost,
  sum(
    case
      when marketing_target in (3, 8, 15) then purchase_order_num
      else 0
    end
  ) as purchase_order_num,
  sum(
    case
      when marketing_target in (3, 8, 15) then click_rgmv_7d
      else 0
    end
  ) as click_rgmv_7d,
  sum(engage_cnt) as engage_cnt,
  0 as note_num,
  0 as new_note_num,
  0 as cost_spu_cnt,
  0 as cost_note_num,
  0 as ecm_closed_cost_note_num,
  dtm
from
  (
    SELECT
      campaign_id,
      case
        when ads_material_type = 'post' then ads_material_id
        else null
      end as note_id,
      brand_account_id,
      agent_user_id,
      agent_name,
      --brand_account_name,
      -- dmp_group_id,
      -- campaign_build_type,
      product,
      build_type,
      biz_product_type,
      target_type_msg_list,
      marketing_target,
      optimize_target,
      optimize_target_msg,
      unique_click_cnt,
      unique_imp_cnt,
      total_amount,
      cost_amount,
      coalesce(purchase_order_num, 0) + coalesce(mini_purchase_order_num, 0) as purchase_order_num,
      purchase_rgmv,
      mini_purchase_rgmv,
      coalesce(purchase_rgmv, 0) + coalesce(mini_purchase_rgmv, 0) as click_rgmv_7d,
      like_cnt,
      fav_cnt,
      cmt_cnt,
      follow_cnt,
      share_cnt,
      unit_id,
      0 as tag,
      campaign_name,
      campaign_placement,
      coalesce(like_cnt, 0) + coalesce(cmt_cnt, 0) + coalesce(fav_cnt, 0) + coalesce(follow_cnt, 0) + coalesce(share_cnt, 0) as engage_cnt,
      dtm
    FROM
      redcdm.dm_ads_rtb_creativity_1d_di
    WHERE
      dtm = '{{ds_nodash}}'
      and (
        is_effective = 1
        or total_amount > 0
      )
      and marketing_target in (3, 8, 14, 15)
  ) t1
  left join (
    select
      brand_account_id,
      brand_account_name,
      company_code,
      company_name,
      group_code,
      group_name,
      track_group_id,
      track_group_name,
      track_industry_name,
      track_detail_name,
      cpc_direct_sales_code,
      cpc_direct_sales_name,
      cpc_direct_sales_dept1_code,
      cpc_direct_sales_dept1_name,
      cpc_direct_sales_dept2_code,
      cpc_direct_sales_dept2_name,
      cpc_direct_sales_dept3_code,
      cpc_direct_sales_dept3_name,
      cpc_direct_sales_dept4_code,
      cpc_direct_sales_dept4_name,
      cpc_direct_sales_dept5_code,
      cpc_direct_sales_dept5_name,
      cpc_direct_sales_dept6_code,
      cpc_direct_sales_dept6_name,
      brand_tag_code,
      brand_tag_name,
      brand_group_tag_code,
      brand_group_tag_name,
      cpc_operator_code,
      cpc_operator_name,
      cpc_operator_dept1_code,
      cpc_operator_dept1_name,
      cpc_operator_dept2_code,
      cpc_operator_dept2_name,
      cpc_operator_dept3_code,
      cpc_operator_dept3_name,
      cpc_operator_dept4_code,
      cpc_operator_dept4_name,
      cpc_operator_dept5_code,
      cpc_operator_dept5_name,
      cpc_operator_dept6_code,
      cpc_operator_dept6_name
    from
      redcdm.dim_ads_industry_brand_account_df
    where
      dtm = max_dtm('redcdm.dim_ads_industry_brand_account_df')
  ) t2 on t1.brand_account_id = t2.brand_account_id
  left join (
    select
      discovery_id,
      user_id,
      substring(publish_time, 1, 10) as publish_time,
      type,
      is_brand,
      is_cps_note,
      is_bind
    from
      reddw.dw_soc_discovery_delta_7_day
    where
      dtm = max_dtm('reddw.dw_soc_discovery_delta_7_day')
  ) note on note.discovery_id = t1.note_id
  left join 
    (select 
      note_id,
      1 as is_kos
    from  redods.ods_note_trade_tb_grant_note_df
    where dtm = max_dtm('redods.ods_note_trade_tb_grant_note_df')
      and grant_type =1
      and grant_status in (6,10)
    group by 1  
    )kos 
    on kos.note_id =t1.note_id
group by
  coalesce(t1.brand_account_id, t2.brand_account_id),
  t2.brand_account_name,
  company_code,
  company_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  cpc_direct_sales_code,
  cpc_direct_sales_name,
  cpc_direct_sales_dept1_name,
  cpc_direct_sales_dept2_name,
  cpc_direct_sales_dept3_name,
  cpc_direct_sales_dept4_name,
  cpc_direct_sales_dept5_name,
  cpc_direct_sales_dept6_name,
  brand_tag_name,
  brand_group_tag_name,
  cpc_operator_code,
  cpc_operator_name,
  cpc_operator_dept1_name,
  cpc_operator_dept2_name,
  cpc_operator_dept3_name,
  cpc_operator_dept4_name,
  cpc_operator_dept5_name,
  cpc_operator_dept6_name,
  campaign_id,
  t1.note_id,
  agent_user_id,
  agent_name,
  product,
  build_type,
  biz_product_type,
  --target_type_msg_list,
  marketing_target,
  optimize_target,
  optimize_target_msg,
  publish_time,
  type,
  coalesce(is_brand, 0),
  coalesce(is_cps_note, 0),
  coalesce(is_bind, 0),
  coalesce(is_kos, 0),
  tag,
  campaign_name,
  campaign_placement,
  unit_id,
  dtm
union all
--企业号粒度总的投放笔记
select
  brand_account_id,
  note_id,
  unit_id,
  campaign_id,
  tag,
  campaign_name,
  campaign_placement,
  agent_user_id,
  agent_name,
  product,
  brand_account_name,
  company_code,
  company_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  cpc_direct_sales_code,
  cpc_direct_sales_name,
  cpc_direct_sales_dept1_name,
  cpc_direct_sales_dept2_name,
  cpc_direct_sales_dept3_name,
  cpc_direct_sales_dept4_name,
  cpc_direct_sales_dept5_name,
  cpc_direct_sales_dept6_name,
  brand_tag_name,
  brand_group_tag_name,
  cpc_operator_code,
  cpc_operator_name,
  cpc_operator_dept1_name,
  cpc_operator_dept2_name,
  cpc_operator_dept3_name,
  cpc_operator_dept4_name,
  cpc_operator_dept5_name,
  cpc_operator_dept6_name,
  build_type,
  biz_product_type,
  --target_type_msg_list,
  marketing_target,
  optimize_target,
  optimize_target_msg,
  publish_time,
  type,
  coalesce(is_brand, 0) as is_brand,
  coalesce(is_cps_note, 0) as is_cps_note,
  coalesce(is_bind, 0) as is_bind,
  coalesce(is_kos, 0) as is_kos,
  click_cnt,
  imp_cnt,
  cost,
  cash_cost,
  purchase_order_num,
  click_rgmv_7d,
  engage_cnt,
  note_num,
  new_note_num,
  cost_spu_cnt,
  cost_note_num,
  ecm_closed_cost_note_num,
  dtm
from
  temp.temp_app_ads_ecm_note_spu_{{ds_nodash}}_1
  )t1 
  left join 
  --计划创建时间
  (select id,
    from_unixtime(floor(create_time / 1000) + 28800, 'yyyy-MM-dd') as campaign_create_date,
    from_unixtime(floor(start_time / 1000) + 28800, 'yyyy-MM-dd') as campaign_start_date,
    v_seller_id,
    advertiser_id
  from redcdm.dwd_ads_rtb_campaign_df
  where
    dtm = greatest('{{ds_nodash}}', '20231205')
  )cam 
  on cam.id = t1.campaign_id
  left join 
  (select virtual_seller_id,rtb_advertiser_id,virtual_seller_name
  from redcdm.dim_ads_advertiser_df 
  where dtm=max_dtm('redcdm.dim_ads_advertiser_df')
  )dim 
  on dim.virtual_seller_id=cam.v_seller_id
  left join 
  (--笔记首次效果消耗时间
  select  ads_material_id as note_id,f_getdate(min(dtm)) as note_first_cost_date
  from 
      redcdm.dm_ads_rtb_creativity_1d_di
    WHERE
      dtm>='20230101' and dtm<='{{ds_nodash}}'
      and (
        is_effective = 1
        or total_amount > 0
      )
      and marketing_target in (3, 8, 14, 15)
      and ads_material_type = 'post'
      and total_amount>0
      group by ads_material_id 
      )cost 
      on cost.note_id = t1.note_id
left join 
  (--笔记首次效果消耗时间
  select  campaign_id,f_getdate(min(dtm)) as campaign_first_cost_date
  ,f_getdate(min(case when marketing_target in (3, 8, 14, 15) then dtm else '9999-12-31' end)) as campaign_ecm_first_cost_date
  from 
      redcdm.dm_ads_rtb_creativity_1d_di
    WHERE
      dtm>='20230101' and dtm<='{{ds_nodash}}'
      and (
        is_effective = 1
        or total_amount > 0
      )
     -- and marketing_target in (3, 8, 14, 15)
      --and ads_material_type = 'post'
      and total_amount>0
      group by campaign_id
      )cam_cost 
      on cam_cost.campaign_id = t1.campaign_id