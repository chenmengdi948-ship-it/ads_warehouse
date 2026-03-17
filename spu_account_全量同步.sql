select t1.date_key,
  t1.spu_id,
  t1.brand_account_id,
  t1.module,
  t1.product,
  t1.marketing_target,
  spu_name,
  brand_id,
  brand_name,
  commercial_taxonomy_name1,
  commercial_code2,
  commercial_taxonomy_name2,
  commercial_code3,
  commercial_taxonomy_name3,
  commercial_code4,
  commercial_taxonomy_name4,
  brand_account_name,
  operator_code,
  operator_name,
  direct_sales_code,
  direct_sales_name,
  direct_sales_dept1_name,
  direct_sales_dept2_name,
  direct_sales_dept3_name,
  imp_num,
  click_num,
  like_num,
  fav_num,
  read_feed_num,
  share_num,
  t2.cash_cost,
  t2.bind_cash_cost,
  dtm,
  t1.agent_user_id,
  agent_user_name,
  channel_sales_name,
  channel_operator_name,
  planner_name,
  staff_name,
  t2.cost,
  t2.bind_cost,
  is_cspu
from 
(SELECT
  date_key,
  spu_id,
  brand_account_id,
  module,
  product,
  marketing_target,
  spu_name,
  brand_id,
  brand_name,
  commercial_taxonomy_name1,
  commercial_code2,
  commercial_taxonomy_name2,
  commercial_code3,
  commercial_taxonomy_name3,
  commercial_code4,
  commercial_taxonomy_name4,
  brand_account_name,
  operator_code,
  operator_name,
  direct_sales_code,
  direct_sales_name,
  direct_sales_dept1_name,
  direct_sales_dept2_name,
  direct_sales_dept3_name,
  imp_num,
  click_num,
  like_num,
  fav_num,
  read_feed_num,
  share_num,
  cash_cost,
  bind_cash_cost,
  cast('{{ds_nodash}}' as int) as dtm,
  agent_user_id,
  agent_user_name,
  channel_sales_name,
  channel_operator_name,
  planner_name,
  concat_ws(',',operator_name,direct_sales_name,channel_sales_name,channel_operator_name,planner_name) as staff_name,
  cost,
  bind_cost,
  is_cspu
FROM
  redcdm.dm_ads_spu_account_detail_1d_di
WHERE
  dtm >= '20230101' and dtm<='{{ds_nodash}}'
  )t1 
  full join 
  (select ----绑定spu的流水分摊 
    date_key,
    t1.spu_id,
    brand_user_id as brand_account_id,
    case when module = '蒲公英' then '品合' 
    when module =  '竞价' then '效果' 
    when module =  '品牌广告' then '品牌' else module end as module,
    case when module = '薯条' then '薯条' when module = '蒲公英' then '品合' 
    when product='发现feed' then '竞价-信息流' 
    when product='搜索feed' then '竞价-搜索'
    when product='视频内流' then '竞价-视频内流' 
    else product end as product,
    case when module = '效果'
        then
          case
            when marketing_target in (3, 8,14,15) then '闭环电商广告'
            when marketing_target in (13) then '非闭环电商广告'
            when marketing_target in (2, 5, 9) then '线索广告'
            when marketing_target in (16) then '平台UG'
            when marketing_target in (20) then '应用下载'
            else '种草广告'
          end
      else '其他'
      end as marketing_target,
    agent_user_id,
    -- agent_name,
    sum(cash_income_amt) as cash_cost,
    sum(case when bind_type=2 then cash_income_amt else 0 end) as bind_cash_cost,
    
    sum(income_amt) as cost,
    sum(case when bind_type=2 then income_amt else 0 end) as bind_cost
  from
      redcdm.dws_ads_note_spu_product_income_detail_td_df t1
    where
      t1.dtm = '{{ds_nodash}}' 
  group by  date_key,
    t1.spu_id,
    brand_user_id ,
    case when module = '蒲公英' then '品合' 
    when module =  '竞价' then '效果' 
    when module =  '品牌广告' then '品牌' else module end ,
    case when module = '薯条' then '薯条' when module = '蒲公英' then '品合' 
    when product='发现feed' then '竞价-信息流' 
    when product='搜索feed' then '竞价-搜索'
    when product='视频内流' then '竞价-视频内流' 
    else product end ,
    case when module = '效果'
        then
          case
            when marketing_target in (3, 8,14,15) then '闭环电商广告'
            when marketing_target in (13) then '非闭环电商广告'
            when marketing_target in (2, 5, 9) then '线索广告'
            when marketing_target in (16) then '平台UG'
            when marketing_target in (20) then '应用下载'
            else '种草广告'
          end
      else '其他'
      end ,
    agent_user_id
  )t2 
  on t1.date_key = t2.date_key and t1.spu_id = t2.spu_id and t1.brand_account_id= t2.brand_account_id and t1.module = t2.module and t1.product= t2.product and 
 t1.marketing_target=t2.marketing_target and t1.agent_user_id=t2.agent_user_id

 left join (
    select
      spu_id,
      brand_id,
      brand_name,
      name as spu_name,
      commercial_taxonomy_name1,
      commercial_code2,
      commercial_taxonomy_name2,
      commercial_code3,
      commercial_taxonomy_name3,
      commercial_code4,
      commercial_taxonomy_name4
    from
      ads_databank.dim_spu_df
    where
      dtm =greatest('{{ds_nodash}}', '20240530')
  ) tt1 on tt1.spu_id = detail.spu_id
left join 
  (select brand_account_id,
      direct_sales_dept1_name,
      direct_sales_dept2_name,
      direct_sales_dept3_name,
      direct_sales_name,
      operator_name,
      operator_code ,
      direct_sales_code,
      brand_user_name as brand_account_name,
      planner_name
  from redapp.app_ads_insight_industry_account_df
  where dtm= greatest('20240530','{{ds_nodash}}')
  )spu_account 
  on spu_account.brand_account_id=detail.brand_account_id
  --代理对应渠道销售20230913
  left join 
  (select date_key,agent_user_id,brand_user_id,concat_ws(',',collect_set(t5.name)) as channel_sales_name,concat_ws(',',collect_set(t6.name)) as channel_operator_name
  from reddm.dm_ads_crm_advertiser_income_wide_day t1
  -- left join 
  -- reddw.dw_ads_crm_virtual_seller_hook_relation_day t2 
  left join 
  (select virtual_seller_id,
    channel_op_code,
    primary_channel_code
  from ads_data_crm.dim_ads_crm_virtual_seller_id_info_df
  where dtm=greatest('20240530','{{ds_nodash}}')
  )t2
  on t1.virtual_seller_id=t2.virtual_seller_id
  left join 
  (select red_name,
   name
  from
    (select red_name,
      concat(red_name, '(', true_name, ')') as name,
      row_number()over(partition by  red_name order by create_time desc) as rn --兜底历史脏数据56条署名重复
    from redods.ods_ads_crm_ads_crm_user 
    where dtm=max_dtm('redods.ods_ads_crm_ads_crm_user ')
    )a 
    where rn=1
  )t5 
  on t5.red_name = t1.channel_sales_name
  left join 
  (select code,
   name
  from
    (select code,
      concat(red_name, '(', true_name, ')') as name,
      row_number()over(partition by  red_name order by create_time desc) as rn --兜底历史脏数据56条署名重复
    from redods.ods_ads_crm_ads_crm_user 
    where dtm=max_dtm('redods.ods_ads_crm_ads_crm_user')
    )a 
    where rn=1
  )t6
  on t6.code = t2.channel_op_code
  where t1.dtm=greatest('20240530','{{ds_nodash}}') and date_key='{{ds}}'
  group by  date_key,agent_user_id,brand_user_id
  )channel 
on channel.agent_user_id=detail.agent_user_id and channel.brand_user_id=detail.brand_account_id