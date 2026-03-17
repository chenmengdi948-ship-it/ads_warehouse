select *
from 
(select date_key,
  spu_id,
  brand_account_id,
  module,
  product,
  marketing_target,
  agent_user_id,
  agent_user_name,
  channel_sales_name,
  channel_operator_name,
  is_cspu,
  sum(imp_num) as imp_num,
  sum(click_num) as click_num,
  sum(like_num) as like_num,
  sum(fav_num) as fav_num,
  sum(read_feed_num) as read_feed_num,
  sum(share_num) as share_num,
  sum(cash_cost) as cash_cost,
  sum(bind_cash_cost) as bind_cash_cost,
  
  
  sum(cost) as  cost,
  sum(bind_cost) as  bind_cost
from 
(SELECT
  date_key,
  spu_id,
  brand_account_id,
  module,
  product,
  marketing_target,
  
  imp_num,
  click_num,
  like_num,
  fav_num,
  read_feed_num,
  share_num,
  0 as cash_cost,
  0 as bind_cash_cost,
  agent_user_id,
  agent_user_name,
  channel_sales_name,
  channel_operator_name,
  
  0 as cost,
  0 as bind_cost,
  is_cspu
FROM
  redcdm.dm_ads_spu_account_detail_1d_di
where dtm>='20230101' and  dtm<='{{ds_nodash}}' 
union all 
--spu
SELECT
  date_key,
  spu_id,
  t1.brand_user_id as brand_account_id,
  module,
   case when product='信息流' then '竞价-信息流' when product='搜索' then '竞价-搜索' 
  when product='视频内流' then '竞价-视频内流' else product end as  product,
  case
        when marketing_target in (3, 8, 14, 15) then '闭环电商广告'
        when marketing_target in (13) then '非闭环电商广告'
        when marketing_target in (2, 5, 9) then '线索广告'
        when marketing_target in (16) then '平台UG'
        when marketing_target not in (3, 8, 2, 5, 9, 13, 14, 15)
        and module in ('效果') then '种草广告'
        when module in ('品牌', '薯条', '品合') then '整体'
        else null
      end as marketing_target,
  
  0 as imp_num,
  0 as click_num,
  0 as like_num,
  0 as fav_num,
  0 as read_feed_num,
  0 as share_num,
  sum(cash_income_amt) as cash_cost,
  sum(case when bind_type=2 then cash_income_amt else 0 end) as bind_cash_cost,
  t1.agent_user_id,
  t1.agent_name as agent_user_name,
  t2.channel_sales_name,
  t2.channel_operator_name,
  
  sum(income_amt) as  cost,
  sum(case when bind_type=2 then income_amt else 0 end) as  bind_cost,
  0 as is_cspu
FROM
  redcdm.dws_ads_note_spu_product_income_detail_td_df t1 
left join redcdm.dim_ads_advertiser_df t2 on t1.v_seller_id=t2.virtual_seller_id and t2.dtm='{{ds_nodash}}'
where t1.dtm='{{ds_nodash}}' and t1.date_key>='2023-01-01' and  t1.date_key<='{{ds}}' 
group by date_key,
  spu_id,
  t1.brand_user_id,
  module,
   case when product='信息流' then '竞价-信息流' when product='搜索' then '竞价-搜索' 
  when product='视频内流' then '竞价-视频内流' else product end ,
  case
        when marketing_target in (3, 8, 14, 15) then '闭环电商广告'
        when marketing_target in (13) then '非闭环电商广告'
        when marketing_target in (2, 5, 9) then '线索广告'
        when marketing_target in (16) then '平台UG'
        when marketing_target not in (3, 8, 2, 5, 9, 13, 14, 15)
        and module in ('效果') then '种草广告'
        when module in ('品牌', '薯条', '品合') then '整体'
        else null
      end,
  t1.agent_user_id,
  t1.agent_name ,
  t2.channel_sales_name,
  t2.channel_operator_name
)t11
group by date_key,
  spu_id,
  brand_account_id,
  module,
  product,
  marketing_target,
  agent_user_id,
  agent_user_name,
  channel_sales_name,
  channel_operator_name,
  is_cspu
)t1
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
      dtm =greatest('{{ds_nodash}}', '20231018')
  ) tt1 on tt1.spu_id =t1.spu_id
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
  where dtm= greatest('20231030','{{ds_nodash}}')
  )spu_account 
  on spu_account.brand_account_id=t1.brand_account_id