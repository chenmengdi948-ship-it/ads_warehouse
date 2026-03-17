insert overwrite table redcdm_dev.dm_ads_industry_product_account_td_df partition(dtm = '{{ ds_nodash }}')
select 
   date_key
  ,module
  ,product
  ,brand_account_id
  ,sum(imp_cnt) as imp_cnt
  ,sum(click_cnt) as click_cnt
  ,sum(like_cnt) as like_cnt
  ,sum(fav_cnt) as fav_cnt
  ,sum(cmt_cnt) as cmt_cnt
  ,sum(share_cnt) as share_cnt
  ,sum(follow_cnt) as follow_cnt
  ,sum(cash_cost) as cash_cost
  ,sum(cost) as cost
  ,sum(mkt_ecm_cost) as mkt_ecm_cost
  ,sum(mkt_leads_cost) as mkt_leads_cost
  ,sum(mkt_zc_cost) as mkt_zc_cost
  ,sum(open_sale_num) as open_sale_num
  ,sum(direct_cash_cost) as direct_cash_cost
  ,sum(direct_cost) as direct_cost
  ,sum(channel_cash_cost) as channel_cash_cost
  ,sum(channel_cost) as channel_cost
  ,sum(campaign_cnt) as campaign_cnt
  ,sum(unit_cnt) as unit_cnt
  ,sum(brand_campaign_cnt) as brand_campaign_cnt
  ,sum(cpc_cost_budget_rate) as cpc_cost_budget_rate
  ,sum(cpc_budget) as cpc_budget
  ,sum(mkt_ecm_cash_cost) as mkt_ecm_cash_cost
  ,sum(mkt_leads_cash_cost) as mkt_leads_cash_cost
  ,sum(mkt_zc_cash_cost) as mkt_zc_cash_cost
  ,sum(mkt_ecm_direct_cost) as mkt_ecm_direct_cost
  ,sum(mkt_leads_direct_cost) as mkt_leads_direct_cost
  ,sum(mkt_zc_direct_cost) as mkt_zc_direct_cost
  ,sum(mkt_ecm_direct_cash_cost) as mkt_ecm_direct_cash_cost
  ,sum(mkt_leads_direct_cash_cost) as mkt_leads_direct_cash_cost
  ,sum(mkt_zc_direct_cash_cost) as mkt_zc_direct_cash_cost
  ,sum(mkt_ecm_channel_cost) as mkt_ecm_channel_cost
  ,sum(mkt_leads_channel_cost) as mkt_leads_channel_cost
  ,sum(mkt_zc_channel_cost) as mkt_zc_channel_cost
  ,sum(mkt_ecm_channel_cash_cost) as mkt_ecm_channel_cash_cost
  ,sum(mkt_leads_channel_cash_cost) as mkt_leads_channel_cash_cost
  ,sum(mkt_zc_channel_cash_cost) as mkt_zc_channel_cash_cost
  ,sum(mkt_ecm_unclosed_cost) as mkt_ecm_unclosed_cost
  ,sum(mkt_ecm_unclosed_cash_cost) as mkt_ecm_unclosed_cash_cost
  ,sum(mkt_ecm_unclosed_direct_cost) as mkt_ecm_unclosed_direct_cost
  ,sum(mkt_ecm_unclosed_direct_cash_cost) as mkt_ecm_unclosed_direct_cash_cost
  ,sum(mkt_ecm_unclosed_channel_cost) as mkt_ecm_unclosed_channel_cost
  ,sum(mkt_ecm_unclosed_channel_cash_cost) as mkt_ecm_unclosed_channel_cash_cost
  ,market_target
from ( 
    -- 除收入以外，从结果表前天的全量快照中出
    select
       date_key
      ,module
      ,product
      ,brand_account_id
      ,null as market_target
      ,imp_cnt
      ,click_cnt
      ,like_cnt
      ,fav_cnt
      ,cmt_cnt
      ,share_cnt
      ,follow_cnt
      ,0 as cash_cost
      ,0 as cost
      ,0 as mkt_ecm_cost
      ,0 as mkt_leads_cost
      ,0 as mkt_zc_cost
      ,open_sale_num
      ,0 as direct_cash_cost
      ,0 as direct_cost
      ,0 as channel_cash_cost
      ,0 as channel_cost
      ,campaign_cnt
      ,unit_cnt
      ,brand_campaign_cnt
      ,cpc_cost_budget_rate
      ,cpc_budget
      ,0 as mkt_ecm_cash_cost
      ,0 as mkt_leads_cash_cost
      ,0 as mkt_zc_cash_cost
      ,0 as mkt_ecm_direct_cost
      ,0 as mkt_leads_direct_cost
      ,0 as mkt_zc_direct_cost
      ,0 as mkt_ecm_direct_cash_cost
      ,0 as mkt_leads_direct_cash_cost
      ,0 as mkt_zc_direct_cash_cost
      ,0 as mkt_ecm_channel_cost
      ,0 as mkt_leads_channel_cost
      ,0 as mkt_zc_channel_cost
      ,0 as mkt_ecm_channel_cash_cost
      ,0 as mkt_leads_channel_cash_cost
      ,0 as mkt_zc_channel_cash_cost
      ,0 as mkt_ecm_unclosed_cost
      ,0 as mkt_ecm_unclosed_cash_cost
      ,0 as mkt_ecm_unclosed_direct_cost
      ,0 as mkt_ecm_unclosed_direct_cash_cost
      ,0 as mkt_ecm_unclosed_channel_cost
      ,0 as mkt_ecm_unclosed_channel_cash_cost
    from
      redcdm.dm_ads_industry_product_account_td_df
    where
      dtm = '{{yesterday_ds_nodash}}'
    union all
    select
       date_key
      ,module
      ,product
      ,brand_account_id
      ,market_target
      ,imp_cnt
      ,click_cnt
      ,like_cnt
      ,fav_cnt
      ,cmt_cnt
      ,share_cnt
      ,follow_cnt
      ,0 as cash_cost
      ,0 as cost
      ,0 as mkt_ecm_cost
      ,0 as mkt_leads_cost
      ,0 as mkt_zc_cost
      ,open_sale_num
      ,0 as direct_cash_cost
      ,0 as direct_cost
      ,0 as channel_cash_cost
      ,0 as channel_cost
      ,campaign_cnt
      ,unit_cnt
      ,brand_campaign_cnt
      ,cpc_cost_budget_rate
      ,cpc_budget
      ,0 as mkt_ecm_cash_cost
      ,0 as mkt_leads_cash_cost
      ,0 as mkt_zc_cash_cost
      ,0 as mkt_ecm_direct_cost
      ,0 as mkt_leads_direct_cost
      ,0 as mkt_zc_direct_cost
      ,0 as mkt_ecm_direct_cash_cost
      ,0 as mkt_leads_direct_cash_cost
      ,0 as mkt_zc_direct_cash_cost
      ,0 as mkt_ecm_channel_cost
      ,0 as mkt_leads_channel_cost
      ,0 as mkt_zc_channel_cost
      ,0 as mkt_ecm_channel_cash_cost
      ,0 as mkt_leads_channel_cash_cost
      ,0 as mkt_zc_channel_cash_cost
      ,0 as mkt_ecm_unclosed_cost
      ,0 as mkt_ecm_unclosed_cash_cost
      ,0 as mkt_ecm_unclosed_direct_cost
      ,0 as mkt_ecm_unclosed_direct_cash_cost
      ,0 as mkt_ecm_unclosed_channel_cost
      ,0 as mkt_ecm_unclosed_channel_cash_cost
    from
      redcdm_dev.dws_ads_industry_product_account_1d_di
    where
      dtm = '{{ds_nodash}}'
    union all
    -- 收入数据，来源crm
    select
       date_key
      ,case
        when module = '内容加热' then '品合'
        else module
      end as module
      ,case
        when module in ('品合' '内容加热') then '品合'
        when module = '薯条' then '薯条'
        else product
      end as product
      ,brand_user_id as brand_account_id
      ,market_target
      ,0 as imp_cnt
      ,0 as click_cnt
      ,0 as like_cnt
      ,0 as fav_cnt
      ,0 as cmt_cnt
      ,0 as share_cnt
      ,0 as follow_cnt
      ,sum(cash_cost) as cash_cost
      ,sum(cost) as cost
      ,sum(case when market_target = '闭环电商' then cost end) as mkt_ecm_cost
      ,sum(case when market_target = '线索' then cost end) as mkt_leads_cost
      ,sum(case when market_target = '种草' then cost end) as mkt_zc_cost
      ,0 as open_sale_num
      ,sum(case when coalesce(sales_system,'') <> '渠道业务部' then cash_cost end ) as direct_cash_cost
      ,sum(case when coalesce(sales_system,'') <> '渠道业务部' then cost end) as direct_cost
      ,sum(case when coalesce(sales_system,'') = '渠道业务部' then cash_cost end) as channel_cash_cost
      ,sum(case when coalesce(sales_system,'') = '渠道业务部' then cost end) as channel_cost
      ,0 as campaign_cnt
      ,0 as unit_cnt
      ,0 as brand_campaign_cnt
      ,0 as cpc_cost_budget_rate
      ,0 as cpc_budget
      ,sum(case when market_target = '闭环电商' then cash_cost end) as mkt_ecm_cash_cost
      ,sum(case when market_target = '线索' then cash_cost end) as mkt_leads_cash_cost
      ,sum(case when market_target = '种草' then cash_cost end) as mkt_zc_cash_cost
      ,sum(case when coalesce(sales_system,'') <> '渠道业务部' and market_target = '闭环电商' then cost end) as mkt_ecm_direct_cost
      ,sum(case when coalesce(sales_system,'') <> '渠道业务部' and market_target = '线索' then cost end) as mkt_leads_direct_cost
      ,sum(case when coalesce(sales_system,'') <> '渠道业务部' and market_target = '种草' then cost end) as mkt_zc_direct_cost
      ,sum(case when coalesce(sales_system,'') <> '渠道业务部' and market_target = '闭环电商' then cash_cost end) as mkt_ecm_direct_cash_cost
      ,sum(case when coalesce(sales_system,'') <> '渠道业务部' and market_target = '线索' then cash_cost end) as mkt_leads_direct_cash_cost
      ,sum(case when coalesce(sales_system,'') <> '渠道业务部' and market_target = '种草' then cash_cost end) as mkt_zc_direct_cash_cost
      ,sum(case when coalesce(sales_system,'') = '渠道业务部' and market_target = '闭环电商' then cost end) as mkt_ecm_channel_cost
      ,sum(case when coalesce(sales_system,'') = '渠道业务部' and market_target = '线索' then cost end) as mkt_leads_channel_cost
      ,sum(case when coalesce(sales_system,'') = '渠道业务部' and market_target = '种草' then cost end) as mkt_zc_channel_cost
      ,sum(case when coalesce(sales_system,'') = '渠道业务部' and market_target = '闭环电商' then cash_cost end) as mkt_ecm_channel_cash_cost
      ,sum(case when coalesce(sales_system,'') = '渠道业务部' and market_target = '线索' then cash_cost end) as mkt_leads_channel_cash_cost
      ,sum(case when coalesce(sales_system,'') = '渠道业务部' and market_target = '种草' then cash_cost end) as mkt_zc_channel_cash_cost
       --20230508新增非闭环电商字段
      ,sum(case when market_target = '非闭环电商' then cost end) as mkt_ecm_unclosed_cost
      ,sum(case when market_target = '非闭环电商' then cash_cost end) as mkt_ecm_unclosed_cash_cost
      ,sum(case when coalesce(sales_system,'') <> '渠道业务部' and market_target = '非闭环电商' then cost end) as mkt_ecm_unclosed_direct_cost
      ,sum(case when coalesce(sales_system,'') <> '渠道业务部' and market_target = '非闭环电商' then cash_cost end) as mkt_ecm_unclosed_direct_cash_cost
      ,sum(case when coalesce(sales_system,'') = '渠道业务部' and market_target = '非闭环电商' then cost end) as mkt_ecm_unclosed_channel_cost
      ,sum(case when coalesce(sales_system,'') = '渠道业务部' and market_target = '非闭环电商' then cash_cost end) as mkt_ecm_unclosed_channel_cash_cost
    from
      reddm.dm_ads_crm_advertiser_income_wide_day a
    where
      a.dtm = '{{ds_nodash}}'
      and a.date_key <= '{{ds}}'
    group by
      1,2,3,4,5
    union all
    -- IP收入
    select 
       date_key
      ,'IP' as module
      ,'IP' as product
      ,brand_account_id
      ,'整体' as market_target
      ,0 as imp_cnt
      ,0 as click_cnt
      ,0 as like_cnt
      ,0 as fav_cnt
      ,0 as cmt_cnt
      ,0 as share_cnt
      ,0 as follow_cnt
      ,sum(income_amount) as cash_cost
      ,sum(income_amount) as cost
      ,0 as mkt_ecm_cost
      ,0 as mkt_leads_cost
      ,0 as mkt_zc_cost
      ,0 as open_sale_num
      ,sum(case when coalesce(account_brand_seller_sales_system,'') <> '渠道业务部' then income_amount end ) as direct_cash_cost
      ,sum(case when coalesce(account_brand_seller_sales_system,'') <> '渠道业务部' then income_amount end) as direct_cost
      ,sum(case when coalesce(account_brand_seller_sales_system,'') = '渠道业务部' then income_amount end) as channel_cash_cost
      ,sum(case when coalesce(account_brand_seller_sales_system,'') = '渠道业务部' then income_amount end) as channel_cost
      ,0 as campaign_cnt
      ,0 as unit_cnt
      ,0 as brand_campaign_cnt
      ,0 as cpc_cost_budget_rate
      ,0 as cpc_budget
      ,0 as mkt_ecm_cash_cost
      ,0 as mkt_leads_cash_cost
      ,0 as mkt_zc_cash_cost
      ,0 as mkt_ecm_direct_cost
      ,0 as mkt_leads_direct_cost
      ,0 as mkt_zc_direct_cost
      ,0 as mkt_ecm_direct_cash_cost
      ,0 as mkt_leads_direct_cash_cost
      ,0 as mkt_zc_direct_cash_cost
      ,0 as mkt_ecm_channel_cost
      ,0 as mkt_leads_channel_cost
      ,0 as mkt_zc_channel_cost
      ,0 as mkt_ecm_channel_cash_cost
      ,0 as mkt_leads_channel_cash_cost
      ,0 as mkt_zc_channel_cash_cost
      ,0 as mkt_ecm_unclosed_cost
      ,0 as mkt_ecm_unclosed_cash_cost
      ,0 as mkt_ecm_unclosed_direct_cost
      ,0 as mkt_ecm_unclosed_direct_cash_cost
      ,0 as mkt_ecm_unclosed_channel_cost
      ,0 as mkt_ecm_unclosed_channel_cash_cost
    from 
      reddw.dw_ads_crm_brand_stats_day a
    where
      a.dtm = '{{ds_nodash}}'
      and a.is_marketing_product = '1'
      and a.date_key <= '{{ds}}'
    group by 
      1,4
) a
group by
   date_key
  ,module
  ,product
  ,brand_account_id
  ,market_target
;
