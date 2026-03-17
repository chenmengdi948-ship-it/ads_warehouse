drop table if exists temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_001;
create table if not exists temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_001 as
select 
   a.date_key
  ,a.module
  ,a.product
  ,case 
    when department in ('行业团队','生态客户业务部','创作者商业化部') then department 
    else '自闭环及其他' 
  end as department
  ,case 
    when department in ('行业团队') and industry like '美奢-%' then if(industry rlike '奢品','奢品行业','美妆洗护行业')
    when department in ('行业团队') then industry
    when department in ('生态客户业务部','创作者商业化部') then department
    else '自闭环及其他' 
  end as industry
  ,cash_cost
  ,cost
from (
  select 
    date_key
    ,case
      when a.module in ('效果') then coalesce(b.cpc_direct_sales_dept2_name,b.cpc_operator_dept2_name,if(b.company_name is null,'创作者商业化部','未挂接'))
      when a.module in ('薯条') then coalesce(b.cpc_direct_sales_dept2_name,b.cpc_operator_dept2_name,'创作者商业化部')
      when a.module = '品牌' then brand_direct_sales_dept2_name
      when a.module in ('品合','内容加热') then coalesce(bcoo_direct_sales_dept2_name,'渠道业务部')
    end as department
    ,case
      when a.module in ('效果','薯条') then if(b.cpc_direct_sales_dept3_name = '美奢洗护行业',concat('美奢-',coalesce(cpc_direct_sales_dept4_name,'')),cpc_direct_sales_dept3_name)
      when a.module = '品牌' then if(b.brand_direct_sales_dept3_name = '美奢洗护行业',concat('美奢-',coalesce(brand_direct_sales_dept4_name,'')),brand_direct_sales_dept3_name)
      when a.module in ('品合','内容加热') then if(b.bcoo_direct_sales_dept3_name = '美奢洗护行业',concat('美奢-',coalesce(bcoo_direct_sales_dept4_name,'')),bcoo_direct_sales_dept3_name) 
    end as industry
    ,module
    ,product
    ,sum(direct_cash_cost) as cash_cost
    ,sum(direct_cost) as cost
  from 
    redcdm.dm_ads_industry_product_account_td_df a 
  left join
    redcdm.dim_ads_industry_account_df b on b.dtm = '{{ds_nodash}}' and a.brand_account_id = b.brand_account_id
  where
    a.dtm = '{{ds_nodash}}'
    and a.module <> 'IP'
  group by
    1,2,3,4,5
  union all
  select 
     date_key
    ,'渠道业务部' as department
    ,'渠道业务部' as industry
    ,module
    ,product
    ,sum(channel_cash_cost) as cash_cost
    ,sum(channel_cost) as cost
  from 
    redcdm.dm_ads_industry_product_account_td_df a 
  where
    a.dtm = '{{ds_nodash}}'
    and a.module <> 'IP'
  group by
    1,2,3,4,5
) a 
;


drop table if exists temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_002;
create table if not exists temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_002 as
select 
   a.date_key
  ,a.module
  ,case 
    when department in ('行业团队','生态客户业务部','创作者商业化部') then department 
    else '自闭环及其他' 
  end as department
  ,case 
    when department in ('行业团队') and industry like '美奢-%' then if(industry rlike '奢品','奢品行业','美妆洗护行业')
    when department in ('行业团队') then industry
    when department in ('生态客户业务部','创作者商业化部') then department
    else '自闭环及其他' 
  end as industry
  ,cash_cost
  ,cost
from (
    select
      date_key
      ,coalesce(b.cpc_direct_sales_dept2_name,b.cpc_operator_dept2_name,if(b.company_name is null,'创作者商业化部','未挂接')) as department
      ,if(b.cpc_direct_sales_dept3_name = '美奢洗护行业',concat('美奢-',coalesce(cpc_direct_sales_dept4_name,'')),cpc_direct_sales_dept3_name) as industry
      ,split(struct.tag,'-')[1] as module
      ,sum(case when split(struct.tag,'-')[2] = '现金' then struct.cost end) as cash_cost
      ,sum(case when split(struct.tag,'-')[2] = '运营' then struct.cost end) as cost
    from 
      redcdm.dm_ads_industry_product_account_td_df a
    left join 
      redcdm.dim_ads_industry_account_df b on b.dtm = '{{ds_nodash}}' and a.brand_account_id = b.brand_account_id
    lateral view explode(
      str_to_map(
        concat(
        '直客-电商-运营=',a.mkt_ecm_direct_cost
        ,'&直客-线索-运营=',a.mkt_leads_direct_cost
        ,'&直客-种草-运营=',a.mkt_zc_direct_cost
        ,'&直客-电商-现金=',a.mkt_ecm_direct_cash_cost
        ,'&直客-线索-现金=',a.mkt_leads_direct_cash_cost
        ,'&直客-种草-现金=',a.mkt_zc_direct_cash_cost
        ),'&','='
      )
    ) struct as tag,cost
    where 
      a.dtm = '{{ds_nodash}}'
    group by
      1,2,3,4
    union all 
    select
      date_key
      ,'渠道业务部' as department
      ,'渠道业务部' as industry
      ,split(struct.tag,'-')[1] as module
      ,sum(case when split(struct.tag,'-')[2] = '现金' then struct.cost end) as cash_cost
      ,sum(case when split(struct.tag,'-')[2] = '运营' then struct.cost end) as cost
    from 
      redcdm.dm_ads_industry_product_account_td_df a
    lateral view explode(
      str_to_map(
        concat(
        '&渠道-电商-运营=',a.mkt_ecm_channel_cost
        ,'&渠道-线索-运营=',a.mkt_leads_channel_cost
        ,'&渠道-种草-运营=',a.mkt_zc_channel_cost
        ,'&渠道-电商-现金=',a.mkt_ecm_channel_cash_cost
        ,'&渠道-线索-现金=',a.mkt_leads_channel_cash_cost
        ,'&渠道-种草-现金=',a.mkt_zc_channel_cash_cost
        ),'&','='
      )
    ) struct as tag,cost
    where 
      a.dtm = '{{ds_nodash}}'
    group by
      1,2,3,4
) a
;


insert overwrite table redapp.app_ads_department_module_okr_dash_td_df partition(dtm = '{{ ds_nodash }}')
select 
   a.date_key
  ,'广告整体' as module
  ,'商业部' as department
  ,'商业部' as industry
  ,sum(cash_cost) as cash_cost
  ,sum(cost) as cost
from 
  temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_001 a
where 
  a.module in ('品牌','效果','薯条')
group by 
  1
union all 
select 
   a.date_key
  ,a.module
  ,'商业部' as department
  ,'商业部' as industry
  ,sum(cash_cost) as cash_cost
  ,sum(cost) as cost
from 
  temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_001 a 
group by 
  1,2
union all 
select 
   a.date_key
  ,a.product as module
  ,'商业部' as department
  ,'商业部' as industry
  ,sum(cash_cost) as cash_cost
  ,sum(cost) as cost
from 
  temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_001 a 
where 
  a.module in ('品牌','效果')
group by 
  1,2
union all 
select 
   a.date_key
  ,a.module
  ,'商业部' as department
  ,'商业部' as industry
  ,sum(cash_cost) as cash_cost
  ,sum(cost) as cost
from 
  temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_002 a 
group by 
  1,2
union all 
select 
   a.date_key
  ,'广告整体' as module
  ,a.department
  ,a.industry
  ,sum(cash_cost) as cash_cost
  ,sum(cost) as cost
from 
  temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_001 a
where 
  a.module in ('品牌','效果','薯条')
group by 
  1,2,3,4
union all 
select 
   a.date_key
  ,a.module
  ,a.department
  ,a.industry
  ,sum(cash_cost) as cash_cost
  ,sum(cost) as cost
from 
  temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_001 a 
group by 
  1,2,3,4
union all 
select 
   a.date_key
  ,a.product as module
  ,a.department
  ,a.industry
  ,sum(cash_cost) as cash_cost
  ,sum(cost) as cost
from 
  temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_001 a 
where 
  a.module in ('品牌','效果')
group by 
  1,2,3,4
union all 
select 
   a.date_key
  ,a.module
  ,a.department
  ,a.industry
  ,sum(cash_cost) as cash_cost
  ,sum(cost) as cost
from 
  temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_002 a 
group by 
  1,2,3,4
;

