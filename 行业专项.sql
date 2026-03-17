drop table
  if exists temp.temp_app_ads_industry_account_income_df_{{ds_nodash}}_001;

create table
  temp.temp_app_ads_industry_account_income_df_{{ds_nodash}}_001 as
select   date_key,
  t1.brand_account_id,
  t1.module,
  t1.product,
  brand_user_name,
  company_code,
  company_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  direct_sales_dept1_name,
  direct_sales_dept2_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_tag_code,
  brand_tag_name,
  brand_group_tag_code,
  brand_group_tag_name,
  first_industry_name,
  second_industry_name,
  t1.market_target,
  cpc_operator_code,
  cpc_operator_name,
  cpc_operator_dept1_name,
  cpc_operator_dept2_name,
  cpc_operator_dept3_name,
  cpc_operator_dept4_name,
  cpc_operator_dept5_name,
  cpc_operator_dept6_name,
  direct_sales_name,
  process_track_industry_name,
  process_track_detail_name,
  case when t2.brand_account_id is not null then 1  else 0 end as is_core_account,
  cash_income_amt,
  income_amt,
  if(month(date_key) & 1 = 1,trunc(date_key,'MM'),trunc(add_months(date_key,-1),'MM')) as bimonthly_date,
  if(month(add_months(date_key,-2)) & 1 = 1,trunc(add_months(date_key,-2),'MM'),trunc(add_months(add_months(date_key,-2),-1),'MM')) as last_bimonthly_date
from 
(SELECT
      date_key,
      module,
       case when module='品牌' then '品牌' WHEN module in ('品合','内容加热') then '品合' else product end as product,
      brand_account_id,
      brand_user_name,
      company_code,
      company_name,
      track_group_id,
      track_group_name,
      track_industry_name,
      track_detail_name,
      direct_sales_dept1_name,
      direct_sales_dept2_name,
      direct_sales_dept3_name,
      direct_sales_dept4_name,
      direct_sales_dept5_name,
      direct_sales_dept6_name,
      brand_tag_code,
      brand_tag_name,
      brand_group_tag_code,
      brand_group_tag_name,
      first_industry_name,
      second_industry_name,
      market_target,
      cpc_operator_code,
      cpc_operator_name,
      cpc_operator_dept1_name,
      cpc_operator_dept2_name,
      cpc_operator_dept3_name,
      cpc_operator_dept4_name,
      cpc_operator_dept5_name,
      cpc_operator_dept6_name,
      direct_sales_name,
      CASE
        WHEN (track_industry_name is null)
        or (coalesce(direct_sales_dept2_name, '') <> '行业团队') THEN (
          CASE
            WHEN (direct_sales_dept2_name = '创作者商业化部') THEN '创作者商业化部'
            WHEN (direct_sales_dept2_name = '生态客户业务部') THEN '生态客户业务部'
            WHEN (direct_sales_dept2_name = '行业团队') THEN '行业团队其他'
            ELSE '自闭环及其他'
          END
        ) ELSE track_industry_name END AS process_track_industry_name,
      CASE when coalesce(direct_sales_dept2_name, '') <> '行业团队' then '其他'  else track_detail_name  end AS process_track_detail_name,
      sum(cash_cost) as cash_income_amt,
      sum(cost) as income_amt
    FROM
      redapp.app_ads_insight_industry_product_account_td_df
    WHERE
     dtm = '{{ds_nodash}}' and ((cash_cost<>0 and module<>'IP' ) or ( direct_sales_dept2_name in ('行业团队','生态客户业务部'))) --有投放，或行业团队生态团队整体
     and date_key>='2022-01-01'
    group by
      date_key,
      module,
     case when module='品牌' then '品牌' WHEN module in ('品合','内容加热') then '品合' else product end,
      brand_account_id,
      brand_user_name,
      company_code,
      company_name,
      track_group_id,
      track_group_name,
      track_industry_name,
      track_detail_name,
      direct_sales_dept1_name,
      direct_sales_dept2_name,
      direct_sales_dept3_name,
      direct_sales_dept4_name,
      direct_sales_dept5_name,
      direct_sales_dept6_name,
      brand_tag_code,
      brand_tag_name,
      brand_group_tag_code,
      brand_group_tag_name,
      first_industry_name,
      second_industry_name,
      market_target,
      cpc_operator_code,
      cpc_operator_name,
      direct_sales_name,
      cpc_operator_dept1_name,
      cpc_operator_dept2_name,
      cpc_operator_dept3_name,
      cpc_operator_dept4_name,
      cpc_operator_dept5_name,
      cpc_operator_dept6_name
      )t1 
      left join (
    select
      brand_account_id
    from
      redapp.app_ads_industry_core_brand_account_df
    where
      dtm = '20230901'
    group by
      brand_account_id
  ) t2 on t1.brand_account_id = t2.brand_account_id ;


drop table
  if exists temp.temp_app_ads_industry_account_income_df_{{ds_nodash}}_account;

create table
  temp.temp_app_ads_industry_account_income_df_{{ds_nodash}}_account as
select market_target,product,brand_account_id,dtm
from 
(select market_target,product
from temp.temp_app_ads_industry_account_income_df_{{ds_nodash}}_001
    WHERE date_key='{{ds}}' and module in ('效果','品牌') and (market_target<>'整体' or module='品牌')
     group by market_target,product
     )t1 
     left join 
     (select  t2.brand_account_id,
      t2.dtm
      from 
     (SELECT
      brand_account_id,
      dtm
    FROM
      redcdm.dim_ads_industry_account_df
    where
      dtm >= '20220101'
      and dtm <= '{{ds_nodash}}'
  
    group by brand_account_id,
      dtm
     )t2 
     join 
     (select brand_account_id
     from redcdm.dim_ads_industry_account_df
    where
      dtm= '{{ds_nodash}}' and coalesce(cpc_direct_sales_dept2_name, cpc_operator_dept2_name) in ('行业团队','生态客户业务部')
      )t3
      on t2.brand_account_id = t3.brand_account_id
     )t4
     on 1=1;
---底表是4份收入明细union ，4种客户打标逻辑
insert overwrite table redapp.app_ads_industry_account_income_df partition( dtm = '{{ds_nodash}}')
--全部*全部
SELECT
  date_key,
  t1.brand_account_id,
  t1.module,
  '全部' as product,
  brand_user_name,
  company_code,
  company_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  direct_sales_dept1_name,
  direct_sales_dept2_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_tag_code,
  brand_tag_name,
  brand_group_tag_code,
  brand_group_tag_name,
  first_industry_name,
  second_industry_name,
  '全部' as market_target,
  cpc_operator_code,
  cpc_operator_name,
  cpc_operator_dept1_name,
  cpc_operator_dept2_name,
  cpc_operator_dept3_name,
  cpc_operator_dept4_name,
  cpc_operator_dept5_name,
  cpc_operator_dept6_name,
  direct_sales_name,
  process_track_industry_name,
  process_track_detail_name,
  is_core_account,
  case when coalesce(bimonthly_cost,0)>0 and coalesce(last_bimonthly_cost,0)>0 then '双月投放' 
  when coalesce(bimonthly_cost,0)>0 and coalesce(last_bimonthly_cost,0)<=0 then '本双月新增客户' 
  when coalesce(bimonthly_cost,0)<=0 and coalesce(last_bimonthly_cost,0)>0 then '本双月流失客户' 
    when coalesce(bimonthly_cost,0)<=0 and coalesce(last_bimonthly_cost,0)<=0 then '未投放客户' end as active_level_type,
  case when module in ('效果', '品牌', '薯条') then cash_income_amt else 0 end,
  case when module in ('效果', '品牌', '薯条') then income_amt else 0 end,
  t1.bimonthly_date,
  null as bimonthly_cost_date,
  last_bimonthly_date,
  null as last_bimonthly_cost_date 
from temp.temp_app_ads_industry_account_income_df_{{ds_nodash}}_001 t1 
left join 
(select *
from redapp.app_ads_industry_account_active_type_df
where dtm='{{ds_nodash}}' and tag = 1
) t2 on t1.brand_account_id =t2.brand_account_id
and t1.bimonthly_date =t2.bimonthly_date
union all 
--product*market_target
--全部*全部
SELECT
  coalesce(t1.date_key,f_getdate(a2.dtm)) as date_key,
  coalesce(t1.brand_account_id,a2.brand_account_id) as brand_account_id,
  coalesce(t1.module,a2.product) as module,
  coalesce(t1.product,a2.product) as product,
  brand_user_name,
  company_code,
  company_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  direct_sales_dept1_name,
  direct_sales_dept2_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_tag_code,
  brand_tag_name,
  brand_group_tag_code,
  brand_group_tag_name,
  first_industry_name,
  second_industry_name,
  coalesce(t1.market_target,a2.market_target) as market_target,
  cpc_operator_code,
  cpc_operator_name,
  cpc_operator_dept1_name,
  cpc_operator_dept2_name,
  cpc_operator_dept3_name,
  cpc_operator_dept4_name,
  cpc_operator_dept5_name,
  cpc_operator_dept6_name,
  direct_sales_name,
  process_track_industry_name,
  process_track_detail_name,
  is_core_account,
  case when coalesce(bimonthly_cost,0)>0 and coalesce(last_bimonthly_cost,0)>0 then '双月投放' 
  when coalesce(bimonthly_cost,0)>0 and coalesce(last_bimonthly_cost,0)<=0 then '本双月新增客户' 
  when coalesce(bimonthly_cost,0)<=0 and coalesce(last_bimonthly_cost,0)>0 then '本双月流失客户' 
    when coalesce(bimonthly_cost,0)<=0 and coalesce(last_bimonthly_cost,0)<=0 then '未投放客户' end as active_level_type,
  case when module in ('效果', '品牌', '薯条') then cash_income_amt else 0 end,
  case when module in ('效果', '品牌', '薯条') then income_amt else 0 end,
  t1.bimonthly_date,
  null as bimonthly_cost_date,
  last_bimonthly_date,
  null as last_bimonthly_cost_date 
from temp.temp_app_ads_industry_account_income_df_{{ds_nodash}}_001 t1 
left join 
(select *
from redapp.app_ads_industry_account_active_type_df
where dtm='{{ds_nodash}}' and tag = 2
) t2 on t1.brand_account_id =t2.brand_account_id
and t1.bimonthly_date =t2.bimonthly_date
and t1.product =t2.product
and t1.market_target =t2.market_target
full outer join temp.temp_app_ads_industry_account_income_df_{{ds_nodash}}_account a2
on t1.product=a2.product
and t1.market_target=a2.market_target
and t1.brand_account_id=a2.brand_account_id
and t1.date_key=f_getdate(a2.dtm)

union all 
--product*market_target
--竞价*market
SELECT
  coalesce(t1.date_key,f_getdate(a2.dtm)) as date_key,
  coalesce(t1.brand_account_id,a2.brand_account_id) as brand_account_id,
  coalesce(t1.module,'效果') as module,
  '效果' as product,
  brand_user_name,
  company_code,
  company_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  direct_sales_dept1_name,
  direct_sales_dept2_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_tag_code,
  brand_tag_name,
  brand_group_tag_code,
  brand_group_tag_name,
  first_industry_name,
  second_industry_name,
  coalesce(t1.market_target,a2.market_target) as market_target,
  cpc_operator_code,
  cpc_operator_name,
  cpc_operator_dept1_name,
  cpc_operator_dept2_name,
  cpc_operator_dept3_name,
  cpc_operator_dept4_name,
  cpc_operator_dept5_name,
  cpc_operator_dept6_name,
  direct_sales_name,
  process_track_industry_name,
  process_track_detail_name,
  is_core_account,
  case when coalesce(bimonthly_cost,0)>0 and coalesce(last_bimonthly_cost,0)>0 then '双月投放' 
  when coalesce(bimonthly_cost,0)>0 and coalesce(last_bimonthly_cost,0)<=0 then '本双月新增客户' 
  when coalesce(bimonthly_cost,0)<=0 and coalesce(last_bimonthly_cost,0)>0 then '本双月流失客户' 
    when coalesce(bimonthly_cost,0)<=0 and coalesce(last_bimonthly_cost,0)<=0 then '未投放客户' end as active_level_type,
  case when module in ('效果') then cash_income_amt else 0 end,
  case when module in ('效果') then income_amt else 0 end,
  t1.bimonthly_date,
  null as bimonthly_cost_date,
  last_bimonthly_date,
  null as last_bimonthly_cost_date 
from temp.temp_app_ads_industry_account_income_df_{{ds_nodash}}_001 t1 
left join 
(select *
from redapp.app_ads_industry_account_active_type_df
where dtm='{{ds_nodash}}' and tag = 4
) t2 on t1.brand_account_id =t2.brand_account_id
and t1.bimonthly_date =t2.bimonthly_date
and t1.market_target =t2.market_target
full outer join 
(select market_target,brand_account_id,dtm
from temp.temp_app_ads_industry_account_income_df_{{ds_nodash}}_account 
where product = '竞价-信息流'
)a2
on  t1.market_target=a2.market_target
and t1.brand_account_id=a2.brand_account_id
and t1.date_key=f_getdate(a2.dtm)
union all 

--竞价
SELECT
  date_key,
  t1.brand_account_id,
  t1.module,
  '效果' as product,
  brand_user_name,
  company_code,
  company_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  direct_sales_dept1_name,
  direct_sales_dept2_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_tag_code,
  brand_tag_name,
  brand_group_tag_code,
  brand_group_tag_name,
  first_industry_name,
  second_industry_name,
  '全部' as market_target,
  cpc_operator_code,
  cpc_operator_name,
  cpc_operator_dept1_name,
  cpc_operator_dept2_name,
  cpc_operator_dept3_name,
  cpc_operator_dept4_name,
  cpc_operator_dept5_name,
  cpc_operator_dept6_name,
  direct_sales_name,
  process_track_industry_name,
  process_track_detail_name,
  is_core_account,
  case when coalesce(bimonthly_cost,0)>0 and coalesce(last_bimonthly_cost,0)>0 then '双月投放' 
  when coalesce(bimonthly_cost,0)>0 and coalesce(last_bimonthly_cost,0)<=0 then '本双月新增客户' 
  when coalesce(bimonthly_cost,0)<=0 and coalesce(last_bimonthly_cost,0)>0 then '本双月流失客户' 
    when coalesce(bimonthly_cost,0)<=0 and coalesce(last_bimonthly_cost,0)<=0 then '未投放客户' end as active_level_type,
  case when module in ('效果') then cash_income_amt else 0 end,
  case when module in ('效果') then income_amt else 0 end,
  t1.bimonthly_date,
  null as bimonthly_cost_date,
  last_bimonthly_date,
  null as last_bimonthly_cost_date 
from temp.temp_app_ads_industry_account_income_df_{{ds_nodash}}_001 t1 
left join 
(select *
from redapp.app_ads_industry_account_active_type_df
where dtm='{{ds_nodash}}' and tag = 3
) t2 on t1.brand_account_id =t2.brand_account_id
and t1.bimonthly_date =t2.bimonthly_date

union all 
--竞价
SELECT
  coalesce(t1.date_key,f_getdate(a2.dtm)) as date_key,
  coalesce(t1.brand_account_id,a2.brand_account_id) as brand_account_id,
  coalesce(t1.module,a2.product) as module,
  coalesce(t1.product,a2.product) as product,
  brand_user_name,
  company_code,
  company_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  direct_sales_dept1_name,
  direct_sales_dept2_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_tag_code,
  brand_tag_name,
  brand_group_tag_code,
  brand_group_tag_name,
  first_industry_name,
  second_industry_name,
  '全部' as market_target,
  cpc_operator_code,
  cpc_operator_name,
  cpc_operator_dept1_name,
  cpc_operator_dept2_name,
  cpc_operator_dept3_name,
  cpc_operator_dept4_name,
  cpc_operator_dept5_name,
  cpc_operator_dept6_name,
  direct_sales_name,
  process_track_industry_name,
  process_track_detail_name,
  is_core_account,
  case when coalesce(bimonthly_cost,0)>0 and coalesce(last_bimonthly_cost,0)>0 then '双月投放' 
  when coalesce(bimonthly_cost,0)>0 and coalesce(last_bimonthly_cost,0)<=0 then '本双月新增客户' 
  when coalesce(bimonthly_cost,0)<=0 and coalesce(last_bimonthly_cost,0)>0 then '本双月流失客户' 
    when coalesce(bimonthly_cost,0)<=0 and coalesce(last_bimonthly_cost,0)<=0 then '未投放客户' end as active_level_type,
  case when module in ('效果','品牌') then cash_income_amt else 0 end,
  case when module in ('效果','品牌') then income_amt else 0 end,
  t1.bimonthly_date,
  null as bimonthly_cost_date,
  last_bimonthly_date,
  null as last_bimonthly_cost_date 
from temp.temp_app_ads_industry_account_income_df_{{ds_nodash}}_001 t1 
left join 
(select *
from redapp.app_ads_industry_account_active_type_df
where dtm='{{ds_nodash}}' and tag = 5
) t2 on t1.brand_account_id =t2.brand_account_id
and t1.bimonthly_date =t2.bimonthly_date
and t1.product =t2.product
full outer join 
(select product,brand_account_id,dtm
from temp.temp_app_ads_industry_account_income_df_{{ds_nodash}}_account 
where market_target in ('种草') or  product ='品牌'
)a2
on  t1.product=a2.product
and t1.brand_account_id=a2.brand_account_id
and t1.date_key=f_getdate(a2.dtm)

