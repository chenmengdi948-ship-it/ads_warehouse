SELECT
  module,
  product,
  creativity_id,
  creativity_name,
  ads_material_type,
  ads_material_id,
  build_type,
  biz_product_type,
  target_type_list,
  target_type_msg_list,
  campaign_id,
  campaign_name,
  campaign_placement,
  marketing_target,
  optimize_target,
  optimize_target_msg,
  brand_account_id,
  agent_user_id,
  agent_name,
  brand_account_name,
  dmp_group_id,
  campaign_build_type,
  unique_click_cnt,
  unique_imp_cnt,
  total_amount,
  cost_amount,
  purchase_order_num,
  mini_purchase_order_num,
  ecm_unclosed_purchase_order_num,
  purchase_rgmv,
  mini_purchase_rgmv,
  like_cnt,
  fav_cnt,
  cmt_cnt,
  follow_cnt,
  share_cnt,
  screenshot_cnt,
  coalesce(like_cnt,0)+coalesce(cmt_cnt,0)+coalesce(fav_cnt,0)+coalesce(follow_cnt,0)+coalesce(share_cnt,0) as engage_cnt
FROM
  redcdm.dm_ads_rtb_creativity_1d_di
WHERE
  dtm = '{{ds_nodash}}'
  and (
    is_effective = 1
    or total_amount > 0
  )
select split(name,'-')[0] as name,count(1)
         from redods.ods_shequ_feed_ads_t_ads_rtb_unit_target
         where dtm = '20240102'
         group by split(name,'-')[0]

drop table
  if exists temp.temp_app_ads_ecm_note_spu_{{ds_nodash}}_1;

create table
  temp.temp_app_ads_ecm_note_spu_{{ds_nodash}}_1 as
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
  coalesce(is_brand,0) as is_brand,
  coalesce(is_cps_note,0) as is_cps_note,
  coalesce(is_bind,0) as is_bind,
  0 as click_cnt,
  0 as imp_cnt,
  0 as cost,
  0 as cash_cost,
  0 as purchase_order_num,
  0 as click_rgmv_7d,
  0 as engage_cnt,
  1 as tag,
  count(1) as note_num,
  COUNT(CASE WHEN substring(publish_time, 1, 10)='{{ds}}' then 1 else null end) as new_note_num
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
      dtm = '{{ds_nodash}}'
  ) t2
  join (
    select
      discovery_id,
      user_id,
      publish_time,
      type,
      is_brand,
      is_cps_note,
      is_bind
    from
      reddw.dw_soc_discovery_delta_7_day
    where
      dtm = '{{ds_nodash}}'
      and substring(publish_time, 1, 10) >= '2023-01-01'
      and substring(publish_time, 1, 10) <= '{{ds}}'
      and (
        is_brand = 1
        or is_cps_note = 1
        or is_bind = 1
      )
  ) note on note.user_id = t2.brand_account_id
group by
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
  -- campaign_id,
  --  note_id,
  -- agent_user_id,
  -- agent_name,
  -- marketing_target,
  -- publish_time,
  type,
  coalesce(is_brand,0),
  coalesce(is_cps_note,0) ,
  coalesce(is_bind,0) ;

drop table
  if exists temp.temp_app_ads_ecm_note_spu_{{ds_nodash}}_2;

create table
  temp.temp_app_ads_ecm_note_spu_{{ds_nodash}}_2 as
select
  coalesce(t1.brand_account_id, t2.brand_account_id) as brand_account_id,

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
  target_type_msg_list,
  marketing_target,
  optimize_target,
  optimize_target_msg,
  publish_time,
  type,
  coalesce(is_brand,0) as is_brand,
  coalesce(is_cps_note,0) as is_cps_note,
  coalesce(is_bind,0) as is_bind,
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
  0 as new_note_num
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
      coalesce(like_cnt, 0) + coalesce(cmt_cnt, 0) + coalesce(fav_cnt, 0) + coalesce(follow_cnt, 0) + coalesce(share_cnt, 0) as engage_cnt
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
      dtm = '{{ds_nodash}}'
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
      dtm = '{{ds_nodash}}' 
  ) note on note.discovery_id = t1.note_id
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
  note_id,
  agent_user_id,
  agent_name,
  product,
  build_type,
  biz_product_type,
  target_type_msg_list,
  marketing_target,
  optimize_target,
  optimize_target_msg,
  publish_time,
  type,
  coalesce(is_brand,0) ,
  coalesce(is_cps_note,0),
  coalesce(is_bind,0) ,
  tag,
  campaign_name,
  campaign_placement,
  unit_id
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
  coalesce(is_brand,0) ,
  coalesce(is_cps_note,0),
  coalesce(is_bind,0) ,
  click_cnt,
  imp_cnt,
  cost,
  cash_cost,
  purchase_order_num,
  click_rgmv_7d,
  engage_cnt,
  note_num,
  new_note_num
from
  temp.temp_app_ads_ecm_note_spu_{{ds_nodash}}_1