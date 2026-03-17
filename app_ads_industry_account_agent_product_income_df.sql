with his_gmv as 
(select t1.dtm,
    module,
    product,
    marketing_target,
    brand_account_id,
    advertiser_id,
    sum(rgmv) as rgmv,
    0 as mini_rgmv
  from 
  (select dtm,
    advertiser_id,
    creativity_id,
    module,
    product,
    brand_account_id,
    marketing_target,
    click_rgmv_7d as rgmv
  from redcdm.dm_ads_rtb_creativity_1d_di
  where dtm>='20230101' and dtm<='20230731'and coalesce(click_rgmv_7d,0)>0
  )t1 
  -- left join 
  -- (select  
  --   creativity_id,
   
  -- from redcdm.dim_ads_creativity_core_df
  -- where dtm='{{ds_nodash}}'
  -- )t2
  -- on t1.creativity_id=t2.creativity_id
  group by 1,2,3,4,5,6
)
insert overwrite table redapp.app_ads_industry_account_agent_product_income_df   partition( dtm = '{{ds_nodash}}')
select
  date_key,
  stat_month,
  module,
  product,
  marketing_target_type,
  is_marketing_product,
  brand_account_id,
  agent_user_id,
  group_code,
  group_name,
  brand_company_code,
  brand_company_name,
  brand_user_name,
  agent_user_name,
  first_ad_industry_code,
  first_ad_industry_name,
  second_ad_industry_code,
  second_ad_industry_name,
  agent_company_code,
  agent_company_name,
  gp,
  origin_agent_type,
  agent_type,
  cash_income_amt,
  credit_income_amt,
  ti_cash_income_amt,
  leads_cash_income_amt,
  ecm_closed_cash_income_amt,
  ecm_unclosed_cash_income_amt,
  is_month_account,
  is_agent_month_account,
  account_bcoo_cash_income_amt_1m,
  account_ads_cash_income_amt_1m,
  agent_bcoo_cash_income_amt_1m,
  agent_ads_cash_income_amt_1m,
  rgmv,
  marketing_target
from 
(select
  t1.date_key,
  t1.stat_month,
  t1.module,
  t1.product,
  t1.marketing_target_type,
  t1.is_marketing_product,
  t1.brand_user_id as brand_account_id,
  t1.agent_user_id,
  account.group_code,
  account.group_name,
  t1.brand_company_code,
  t1.brand_company_name,
  t1.brand_user_name,
  t1.agent_user_name,
  null as first_ad_industry_code,
  t1.first_ad_industry_name,
  null as second_ad_industry_code,
  t1.second_ad_industry_name,
  t1.agent_company_code,
  t1.agent_company_name,
  case when t2.agnet_name is not null then t2.gp else t1.agent_company_name end as gp,
  t3.agent_type as origin_agent_type,
 case when t1.module ='品合' then '蒲公英渠道'
    when t3.agent_type in ('SME服务商','直签','内广','家居渠道') then t3.agent_type 
    when t1.agent_company_name is null or t1.agent_company_name = '' then '直签'
    when t1.second_ad_industry_name in ('汽车厂商','汽车经销商') and agent_company_name is not null then '汽车代理'
    when t3.agent_type in ('汽车代理','汽车行代','汽车服务商') and t1.second_ad_industry_name not in ('汽车厂商','汽车经销商') then '汽车代理'    
    when t3.agent_type in ('整合孵化-品牌渠道','整合孵化-效果渠道','ISP渠道') and t1.second_ad_industry_name not in ('汽车厂商','汽车经销商') then '整合代理-陈辰' 
    when t3.agent_type in ('整合孵化-蒲公英渠道') and t1.second_ad_industry_name not in ('汽车厂商','汽车经销商') then '整合代理-当归'
    when t3.agent_type in ('效果渠道','品牌渠道','房产代理商','区域渠道','海外渠道','跨境渠道','国际4A','本土-蔡琰组','本土-伊达组') and t1.second_ad_industry_name not in ('汽车厂商','汽车经销商') then t3.agent_type 
    when t3.agent_type is not null then t3.agent_type else '其他'
        end as agent_type,
  t1.cash_income_amt,
  t1.credit_income_amt,
  t1.ti_cash_income_amt,
  t1.leads_cash_income_amt,
  t1.ecm_closed_cash_income_amt,
  t1.ecm_unclosed_cash_income_amt,
  case
    when t4.bcoo_cash_income_amt > 0
    and t4.ads_cash_income_amt > 0 then 1
    else 0
  end as is_month_account,
  case
    when t5.bcoo_cash_income_amt > 0
    and t5.ads_cash_income_amt > 0 then 1
    else 0
  end as is_agent_month_account,
  t4.bcoo_cash_income_amt as account_bcoo_cash_income_amt_1m,
  t4.ads_cash_income_amt  as account_ads_cash_income_amt_1m,
  t5.bcoo_cash_income_amt as agent_bcoo_cash_income_amt_1m,
  t5.ads_cash_income_amt as agent_ads_cash_income_amt_1m,
  rgmv,
  t1.marketing_target
from
  (select coalesce(t1.date_key, gmv.date_key) as date_key,
      coalesce(t1.stat_month, gmv.stat_month) as stat_month,
      coalesce(t1.module, gmv.module) as module,
      coalesce(t1.product, gmv.product) as product,
      coalesce(t1.marketing_target_type, gmv.marketing_target_type) as marketing_target_type,
      coalesce(t1.marketing_target, gmv.marketing_target) as marketing_target,
      coalesce(t1.brand_user_id, gmv.brand_account_id) as brand_user_id,
      coalesce(t1.agent_user_id, gmv.agent_user_id) as agent_user_id,
      coalesce(t1.brand_company_code, gmv.company_code) as  brand_company_code,
      coalesce(t1.brand_company_name, gmv.company_name) as brand_company_name,
      coalesce(t1.brand_user_name, gmv.brand_user_name) as brand_user_name,
      coalesce(t1.agent_user_name, gmv.agent_user_name) as agent_user_name,
      
      coalesce(t1.first_ad_industry_name, gmv.first_ad_industry_name) as first_ad_industry_name,
      
      coalesce(t1.second_ad_industry_name, gmv.second_ad_industry_name) as second_ad_industry_name,
      coalesce(t1.agent_company_code, gmv.agent_company_code) as agent_company_code,
      coalesce(t1.agent_company_name, gmv.agent_company_name) as agent_company_name,
      coalesce(t1.is_marketing_product, gmv.is_marketing_product) as is_marketing_product,
      cash_income_amt,
      credit_income_amt,
      ti_cash_income_amt,
      leads_cash_income_amt,
      ecm_closed_cash_income_amt,
      ecm_unclosed_cash_income_amt,
      rgmv
  from (
    select
      date_key,
      substring(date_key, 1, 7) as stat_month,
      case
        when module = '内容加热' then '品合'
        else module
      end as module,
      case
        when module in ('品合' ,'内容加热') then '品合'
        when module = '薯条' then '薯条'
        else product
      end as product,
      market_target as marketing_target_type,
      null as marketing_target,
      brand_user_id,
      agent_user_id,
      -- group_code,
      -- group_name,
      company_code as brand_company_code,
      company_name as brand_company_name,
      brand_user_name,
      agent_user_name,
      --first_ad_industry_code,
      first_ad_industry_name,
      --second_ad_industry_code,
      second_ad_industry_name,
      agent_company_code,
      agent_company_name,
      coalesce(is_marketing_product, '0') as is_marketing_product,
      sum(cash_cost) as cash_income_amt,
      sum(actual_credit_cost) as credit_income_amt,
      sum(
        case
          when market_target = '种草' then cash_cost
          else 0
        end
      ) as ti_cash_income_amt,
      sum(
        case
          when market_target = '线索' then cash_cost
          else 0
        end
      ) as leads_cash_income_amt,
      sum(
        case
          when market_target = '闭环电商' then cash_cost
          else 0
        end
      ) as ecm_closed_cash_income_amt,
      sum(
        case
          when market_target = '非闭环电商' then cash_cost
          else 0
        end
      ) as ecm_unclosed_cash_income_amt
    from
      reddm.dm_ads_crm_advertiser_income_wide_day
    where
      dtm = '{{ds_nodash}}'
       and date_key<='{{ds}}'
       and module not in ('薯条','效果') --薯条不算渠道收入
    group by
      date_key,
      substring(date_key, 1, 7),
      case
        when module = '内容加热' then '品合'
        else module
      end ,
      case
        when module in ('品合' ,'内容加热') then '品合'
        when module = '薯条' then '薯条'
        else product
      end,
      market_target,
      marketing_target,
      brand_user_id,
      agent_user_id,
      -- group_code,
      -- group_name,
      company_code,
      company_name,
      brand_user_name,
      agent_user_name,
      --first_ad_industry_code,
      first_ad_industry_name,
     -- second_ad_industry_code,
      second_ad_industry_name,
      agent_company_code,
      agent_company_name,
      is_marketing_product
  union all  
  --效果收入区分营销目标
      select
      date_key,
      substring(date_key, 1, 7) as stat_month,
       module,
      case
        when module in ('品合' '内容加热') then '品合'
        when module = '薯条' then '薯条'
        when product='火焰话题'  then '品牌其他'
        when product='信息流' then '竞价-信息流'
        when product='搜索' then '竞价-搜索'
        when product='视频内流' then '竞价-视频内流'
        else product
      end as product,
      marketing_target_type,
      marketing_target_id as marketing_target,
      t1.brand_user_id,
      b.agent_user_id,
      b.brand_company_code as company_code,
      b.brand_company_name as  company_name,
      b.brand_user_name,
      b.agent_user_name,
      t2.first_ad_industry_name,
      t2.second_ad_industry_name,
      b.agent_company_code,
      b.agent_company_name,
      coalesce(is_marketing_product, '0') as is_marketing_product,
      sum(cash_income_amt) as cash_income_amt,
      sum(credit_income_amt) as credit_income_amt,
      sum(
        case
          when marketing_target_type = '种草' then cash_income_amt
          else 0
        end
      ) as ti_cash_income_amt,
      sum(
        case
          when marketing_target_type = '线索' then cash_income_amt
          else 0
        end
      ) as leads_cash_income_amt,
      sum(
        case
          when marketing_target_type = '闭环电商' then cash_income_amt
          else 0
        end
      ) as ecm_closed_cash_income_amt,
      sum(
        case
          when marketing_target_type = '非闭环电商' then cash_income_amt
          else 0
        end
      ) as ecm_unclosed_cash_income_amt
    from 
    (select   date_key,
        module,
        product,
        marketing_target_type,
        marketing_target_id,
        optimize_target_id,
        advertiser_id,
        virtual_seller_id,
        brand_user_id,
        agent_user_id,
        income_amt,
        cash_income_amt,
        credit_income_amt,
        actual_income_amt,
        virtual_seller_name,
        group_code,
        group_name,
        group_abbreviation,
        brand_company_code,
        brand_company_name,
        brand_user_name,
        agent_user_name,
        first_ad_industry_code,
        first_ad_industry_name,
        second_ad_industry_code,
        second_ad_industry_name,
        company_credit_code,
        agent_company_code,
        agent_company_name,
        is_marketing_product,
        client_name,
        coupon_income_amt
    from
     redcdm.dws_ads_advertiser_product_income_detail_df
    where
      dtm = '{{ds_nodash}}'
       and date_key<='{{ds}}'
       and module  in ('效果') --薯条不算渠道收入
    )t1 
    left join  reddw.dw_ads_crm_advertiser_day b on b.dtm='{{ds_nodash}}' and t1.advertiser_id=b.rtb_advertiser_id
    LEFT JOIN (
    SELECT
      user_id,
      first_ad_industry_name,
      second_ad_industry_name
    FROM
      redods.ods_ads_crm_ads_brand brand
    WHERE
      dtm = '{{ds_nodash}}'
      group by 1,2,3
    ) t2 on t1.brand_user_id = t2.user_id
    group by
      date_key,
      substring(date_key, 1, 7),
      module,
      case
        when module in ('品合' '内容加热') then '品合'
        when module = '薯条' then '薯条'
        when product='火焰话题'  then '品牌其他'
        when product='信息流' then '竞价-信息流'
        when product='搜索' then '竞价-搜索'
        when product='视频内流' then '竞价-视频内流'
        else product
      end,
      marketing_target_type,
      marketing_target_id,
      brand_user_id,
      agent_user_id,
      b.agent_user_id,
      b.brand_company_code ,
      b.brand_company_name ,
      b.brand_user_name,
      b.agent_user_name,
      t2.first_ad_industry_name,
      t2.second_ad_industry_name,
      b.agent_company_code,
      b.agent_company_name,
      is_marketing_product
  ) t1
  full outer join 
  (select f_getdate(a.dtm) as date_key,
    substring(f_getdate(a.dtm), 1, 7) as stat_month,
    module,
    case when product ='视频内流' then '竞价-视频内流' 
    when product='信息流' then '竞价-信息流' 
    when product ='搜索' then '竞价-搜索' 
    when module='薯条' then '薯条' else product end as product,
    case
      when a.marketing_target in (3, 8, 14,15) then '闭环电商'
      when a.marketing_target in (13) then '非闭环电商'
      when a.marketing_target in (2, 5, 9) then '线索'
    else '种草' end as marketing_target_type,
    a.marketing_target,
    '0' as is_marketing_product,
    a.brand_account_id,
    b.agent_user_id,
    b.brand_company_code as company_code,
    b.brand_company_name as  company_name,
    b.brand_user_name,
    b.agent_user_name,
    t2.first_ad_industry_name,
    t2.second_ad_industry_name,
    b.agent_company_code,
    b.agent_company_name,
    sum(coalesce(rgmv,0)+coalesce(mini_rgmv,0)) as rgmv --click_rgmv_7d
  from 
  (select dtm,
    module,
    product,
    marketing_target,
    brand_account_id,
    advertiser_id,
    purchase_rgmv as rgmv,
    mini_purchase_rgmv as mini_rgmv
  from redcdm.dws_ads_cvr_creativity_1d_di 
  where dtm>='20230801' and coalesce(purchase_rgmv,0)+coalesce(mini_purchase_rgmv,0)>0
  union all 
  --history
  select dtm,
    module,
    product,
    marketing_target,
    brand_account_id,
    advertiser_id,
    rgmv,
    mini_rgmv
  from his_gmv
  )a
  left join reddw.dw_ads_crm_advertiser_day b on b.dtm='{{ds_nodash}}' and a.advertiser_id=b.rtb_advertiser_id
  LEFT JOIN (
  SELECT
    user_id,
    first_ad_industry_name,
    second_ad_industry_name
  FROM
    redods.ods_ads_crm_ads_brand brand
  WHERE
    dtm = '{{ds_nodash}}'
    group by 1,2,3
  ) t2 on a.brand_account_id = t2.user_id
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
  )gmv 
  on gmv.brand_account_id = t1.brand_user_id
    and gmv.agent_user_id = t1.agent_user_id
    and gmv.is_marketing_product = t1.is_marketing_product
    and gmv.product = t1.product
    and gmv.module = t1.module
    and gmv.date_key = t1.date_key
    and gmv.marketing_target_type = t1.marketing_target_type
    and gmv.marketing_target = t1.marketing_target
  )t1
  left join (
    select
      agnet_name,
      gp
    from
      reddim.dim_ads_agent_list_lengte_2201_month
    where
      dtm = '20220124'
    group by
      1,
      2
  ) t2 on t1.agent_company_name = t2.agnet_name
  left join (
    select
      agnet_name,
      max(agent_type) as agent_type --只有agnet_name='广州鼎承文化传媒科技有限公司'有重复，兜底处理下已和bi对齐
    from
      reddim.dim_ads_agent_list_lengte_2201_month
    where
      dtm = '20220124' and agent_type <>'品合任务代理'
    group by
      1
  ) t3 on t3.agnet_name = t1.agent_company_name
  left join --企业号自然月通投
  (
    select
      substring(date_key, 1, 7) as stat_month,
      brand_user_id,
      sum(
        case
          when module in ('品合','内容加热') then cash_cost
          else 0
        end
      ) as bcoo_cash_income_amt,
      sum(
        case
          when module in ('效果', '品牌') then cash_cost
          else 0
        end
      ) as ads_cash_income_amt
    from
      reddm.dm_ads_crm_advertiser_income_wide_day
    where
      dtm = '{{ds_nodash}}'
       and date_key<='{{ds}}'
    group by
      substring(date_key, 1, 7),
      brand_user_id
  ) t4 on t1.stat_month = t4.stat_month
  and t1.brand_user_id = t4.brand_user_id
  left join --企业号*代理商自然月通投
  (
    select
      substring(date_key, 1, 7) as stat_month,
      brand_user_id,
      case when t2.agnet_name is not null then t2.gp else t1.agent_company_name end as gp,
      sum(
        case
          when module in ('品合','内容加热') then cash_cost
          else 0
        end
      ) as bcoo_cash_income_amt,
      sum(
        case
          when module in ('效果', '品牌') then cash_cost
          else 0
        end
      ) as ads_cash_income_amt
    from
      reddm.dm_ads_crm_advertiser_income_wide_day t1
    left join (
    select
      agnet_name,
      gp
    from
      reddim.dim_ads_agent_list_lengte_2201_month
    where
      dtm = '20220124'
    group by
      1,
      2
  ) t2 on t1.agent_user_name = t2.agnet_name
    where
      t1.dtm = '{{ds_nodash}}'
      and t1.date_key<='{{ds}}'
    group by
      substring(date_key, 1, 7),
      brand_user_id,
      case when t2.agnet_name is not null then t2.gp else t1.agent_company_name end
  ) t5 on t1.stat_month = t5.stat_month
  and t1.brand_user_id = t5.brand_user_id
  and if(t2.agnet_name is not null,t2.gp,t1.agent_company_name) = t5.gp
  left join 
  (select brand_account_id,
    group_code,
    group_name
  from redcdm.dim_ads_industry_account_df 
  where dtm='{{ds_nodash}}'
  )account 
  on account.brand_account_id = t1.brand_user_id
  )detail 
  where agent_type<> '直签'
  
