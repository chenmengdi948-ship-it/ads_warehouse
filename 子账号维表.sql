insert overwrite table  redcdm.dim_ads_advertiser_df partition(dtm = '{{ ds_nodash }}') 
select
  coalesce(b.virtual_seller_id, a.v_seller_id) as virtual_seller_id,
  coalesce(type, '其他') as type,
  coalesce(
    if(b.brand_user_id = '', null, brand_user_id),
    a.brand_account_id
  ) as brand_account_id,
  agent_user_id,
  agent_user_name,
  agent_virtual_seller_id,
  agent_company_code,
  agent_company_name,
  sub_virtual_seller_name as virtual_seller_name,
  coalesce(b.rtb_advertiser_id, a.advertiser_id) as rtb_advertiser_id,
  coalesce(b.state, a.state) as state,
  coalesce(b.create_time, a.create_time) as create_time,
  coalesce(b.update_time, a.modify_time) as update_time,
  balance_account_id,
  sub_virtual_seller_email,
  account_type,
  brand_account_name,
  company_code,
  company_name,
  track_group_name,
  track_industry_name,
  track_detail_name,
  cpc_direct_sales_code,5ec619560000000001000bd4
  cpc_direct_sales_name,
  cpc_direct_sales_dept1_name,
  cpc_direct_sales_dept2_name,
  cpc_direct_sales_dept3_name,
  cpc_direct_sales_dept4_name,
  cpc_direct_sales_dept5_name,
  cpc_direct_sales_dept6_name,
  cpc_operator_code,
  cpc_operator_name,
  cpc_operator_dept1_name,
  cpc_operator_dept2_name,
  cpc_operator_dept3_name,
  cpc_operator_dept4_name,
  cpc_operator_dept5_name,
  cpc_operator_dept6_name,
  rtb_channel_code as channel_sales_code,
  rtb_channel_name as channel_salses_name,
  channel_op_code as channel_operator_code,
  channel_op_name as channel_operator_name
from
  (
    select
      v_seller_id,
      advertiser_id,
      brand_account_id,
      state,
      create_time,
      modify_time
    from
      reddw.dw_ads_cpc_advertiser_new_day
    where
      dtm = '{{ds_nodash}}'
    group by
      v_seller_id,
      advertiser_id,
      brand_account_id,
      state,
      create_time,
      modify_time
  ) a
  full outer join (
    select
      virtual_seller_id,
      type,
      brand_user_id,
      agent_user_id,
      agent_user_name,
      agent_virtual_seller_id,
      agent_company_code,
      agent_company_name,
      sub_virtual_seller_name,
      rtb_advertiser_id,
      state,
      create_time,
      update_time,
      balance_account_id,
      sub_virtual_seller_email,
      account_type
    from
      reddw.dw_ads_crm_advertiser_day
    where
      dtm = '{{ds_nodash}}'
  ) b on a.v_seller_id = b.virtual_seller_id
  left join (
    select
      virtual_seller_id,
      rtb_channel_code,
      rtb_channel_name,
      channel_op_code,
      channel_op_name
    from
      reddw.dw_ads_crm_virtual_seller_relation_day
    where
      dtm = '{{ds_nodash}}'
  ) c on coalesce(b.virtual_seller_id, a.v_seller_id) = c.virtual_seller_id
  left join (
    select
      brand_account_id,
      brand_account_name,
      company_code,
      company_name,
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
      cpc_operator_code,
      cpc_operator_name,
      cpc_operator_dept1_name,
      cpc_operator_dept2_name,
      cpc_operator_dept3_name,
      cpc_operator_dept4_name,
      cpc_operator_dept5_name,
      cpc_operator_dept6_name
    from
      redcdm.dim_ads_industry_brand_account_df
    where
      dtm = '{{ds_nodash}}'
  ) d on coalesce(
    if(b.brand_user_id = '', null, brand_user_id),
    a.brand_account_id
  ) = d.brand_account_id