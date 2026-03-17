SET "kyuubi.spark.option.--conf spark.sql.crossJoin.enabled=true";
SET "kyuubi.spark.option.--conf spark.redExtension.allowBroadcastNestedLoopJoin=true";
spark.sql.optimizer.insertRepartitionBeforeWrite.enabled

drop table if exists temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_001;
create table if not exists temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_001 as
select 
   a.date_key
  ,a.module
  ,a.product
  ,coalesce(company_name,brand_account_id) as brand_account_id--客户数口径
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
  ,industry_1 --满风onedash需要合并美妆洗护和奢品
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
    ,case
      when a.module in ('效果','薯条') then cpc_direct_sales_dept3_name
      when a.module = '品牌' then brand_direct_sales_dept3_name
      when a.module in ('品合','内容加热') then bcoo_direct_sales_dept3_name
    end as industry_1
    ,module
    ,product
    ,a.brand_account_id
    ,b.company_name
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
    1,2,3,4,5,6,7,8
  union all
  select 
     date_key
    ,'渠道业务部' as department
    ,'渠道业务部' as industry
    ,'渠道业务部' as industry_1
    ,module
    ,product
    ,a.brand_account_id
    ,b.company_name
    ,sum(channel_cash_cost) as cash_cost
    ,sum(channel_cost) as cost
  from 
    redcdm.dm_ads_industry_product_account_td_df a 
  left join
    redcdm.dim_ads_industry_account_df b on b.dtm = '{{ds_nodash}}' and a.brand_account_id = b.brand_account_id
  where
    a.dtm = '{{ds_nodash}}'
    and a.module <> 'IP'
  group by
    1,2,3,4,5,6,7,8
) a 
;

--20230508增加非闭环电商收入
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
  ,brand_account_id
  ,cash_cost
  ,cost
from (
    select
      date_key
      ,coalesce(b.cpc_direct_sales_dept2_name,b.cpc_operator_dept2_name,if(b.company_name is null,'创作者商业化部','未挂接')) as department
      ,if(b.cpc_direct_sales_dept3_name = '美奢洗护行业',concat('美奢-',coalesce(cpc_direct_sales_dept4_name,'')),cpc_direct_sales_dept3_name) as industry
      ,split(struct.tag,'-')[1] as module
      ,a.brand_account_id
      ,sum(case when split(struct.tag,'-')[2] = '现金' then coalesce(struct.cost,0) end) as cash_cost
      ,sum(case when split(struct.tag,'-')[2] = '运营' then coalesce(struct.cost,0) end) as cost
    from 
      redcdm.dm_ads_industry_product_account_td_df a
    left join 
      redcdm.dim_ads_industry_account_df b on b.dtm = '{{ds_nodash}}' and a.brand_account_id = b.brand_account_id
    lateral view explode(
      str_to_map(
        concat(
        '&直客-电商-运营=',a.mkt_ecm_direct_cost
        ,'&直客-线索-运营=',a.mkt_leads_direct_cost
        ,'&直客-种草-运营=',a.mkt_zc_direct_cost
        ,'&直客-非闭环电商-运营=',a.mkt_ecm_unclosed_direct_cost
        ,'&直客-电商-现金=',a.mkt_ecm_direct_cash_cost
        ,'&直客-线索-现金=',a.mkt_leads_direct_cash_cost
        ,'&直客-种草-现金=',a.mkt_zc_direct_cash_cost
        ,'&直客-非闭环电商-现金=',a.mkt_ecm_unclosed_direct_cash_cost
        ),'&','='
      )
    ) struct as tag,cost
    where 
      a.dtm = '{{ds_nodash}}'
    group by
      1,2,3,4,5
    union all 
    select
      date_key
      ,'渠道业务部' as department
      ,'渠道业务部' as industry
      ,split(struct.tag,'-')[1] as module
      ,a.brand_account_id
      ,sum(case when split(struct.tag,'-')[2] = '现金' then coalesce(struct.cost,0) end) as cash_cost
      ,sum(case when split(struct.tag,'-')[2] = '运营' then coalesce(struct.cost,0) end) as cost
    from 
      redcdm.dm_ads_industry_product_account_td_df a
    lateral view explode(
      str_to_map(
        concat(
        '&渠道-电商-运营=',a.mkt_ecm_channel_cost
        ,'&渠道-线索-运营=',a.mkt_leads_channel_cost
        ,'&渠道-种草-运营=',a.mkt_zc_channel_cost
        ,'&渠道-非闭环电商-运营=',a.mkt_ecm_unclosed_channel_cost
        ,'&渠道-电商-现金=',a.mkt_ecm_channel_cash_cost
        ,'&渠道-线索-现金=',a.mkt_leads_channel_cash_cost
        ,'&渠道-种草-现金=',a.mkt_zc_channel_cash_cost
        ,'&渠道-非闭环电商-现金=',a.mkt_ecm_unclosed_channel_cash_cost
        ),'&','='
      )
    ) struct as tag,cost
    where 
      a.dtm = '{{ds_nodash}}'
    group by
      1,2,3,4,5
) a
;
--20230508新增，增加002表的二级分类（信息流，搜索和内流）写入module
drop table if exists temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_003;
create table if not exists temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_003 as
select 
   a.date_key
  ,concat(product,'-',a.module) as module
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
  ,brand_account_id
  ,cash_cost
  ,cost
from (
    select
      date_key
      ,coalesce(b.cpc_direct_sales_dept2_name,b.cpc_operator_dept2_name,if(b.company_name is null,'创作者商业化部','未挂接')) as department
      ,if(b.cpc_direct_sales_dept3_name = '美奢洗护行业',concat('美奢-',coalesce(cpc_direct_sales_dept4_name,'')),cpc_direct_sales_dept3_name) as industry
      ,split(struct.tag,'-')[1] as module
      ,case when product='竞价-搜索' then '搜索' 
        when product='竞价-信息流' then '信息流' 
        when product='竞价-视频内流' then '视频内流' end as product
      ,a.brand_account_id
      ,sum(case when split(struct.tag,'-')[2] = '现金' then coalesce(struct.cost,0) end) as cash_cost
      ,sum(case when split(struct.tag,'-')[2] = '运营' then coalesce(struct.cost,0) end) as cost
    from 
      redcdm.dm_ads_industry_product_account_td_df a
    left join 
      redcdm.dim_ads_industry_account_df b on b.dtm = '{{ds_nodash}}' and a.brand_account_id = b.brand_account_id
    lateral view explode(
      str_to_map(
        concat(
        '&直客-电商-运营=',a.mkt_ecm_direct_cost
        ,'&直客-线索-运营=',a.mkt_leads_direct_cost
        ,'&直客-种草-运营=',a.mkt_zc_direct_cost
        ,'&直客-非闭环电商-运营=',a.mkt_ecm_unclosed_direct_cost
        ,'&直客-电商-现金=',a.mkt_ecm_direct_cash_cost
        ,'&直客-线索-现金=',a.mkt_leads_direct_cash_cost
        ,'&直客-种草-现金=',a.mkt_zc_direct_cash_cost
        ,'&直客-非闭环电商-现金=',a.mkt_ecm_unclosed_direct_cash_cost
        ),'&','='
      )
    ) struct as tag,cost
    where 
      a.dtm = '{{ds_nodash}}'
      and a.product in ('竞价-搜索','竞价-信息流','竞价-视频内流')
    group by
      1,2,3,4,5,6
    union all 
    select
      date_key
      ,'渠道业务部' as department
      ,'渠道业务部' as industry
      ,split(struct.tag,'-')[1] as module
      ,case when product='竞价-搜索' then '搜索' 
        when product='竞价-信息流' then '信息流' 
        when product='竞价-视频内流' then '视频内流' end as product
      ,a.brand_account_id
      ,sum(case when split(struct.tag,'-')[2] = '现金' then coalesce(struct.cost,0) end) as cash_cost
      ,sum(case when split(struct.tag,'-')[2] = '运营' then coalesce(struct.cost,0) end) as cost
    from 
      redcdm.dm_ads_industry_product_account_td_df a
    lateral view explode(
      str_to_map(
        concat(
        '&渠道-电商-运营=',a.mkt_ecm_channel_cost
        ,'&渠道-线索-运营=',a.mkt_leads_channel_cost
        ,'&渠道-种草-运营=',a.mkt_zc_channel_cost
        ,'&渠道-非闭环电商-运营=',a.mkt_ecm_unclosed_channel_cost
        ,'&渠道-电商-现金=',a.mkt_ecm_channel_cash_cost
        ,'&渠道-线索-现金=',a.mkt_leads_channel_cash_cost
        ,'&渠道-种草-现金=',a.mkt_zc_channel_cash_cost
        ,'&渠道-非闭环电商-现金=',a.mkt_ecm_unclosed_channel_cash_cost
        ),'&','='
      )
    ) struct as tag,cost
    where 
      a.dtm = '{{ds_nodash}}'
      and a.product in ('竞价-搜索','竞价-信息流','竞价-视频内流')
    group by
      1,2,3,4,5,6
) a
;

--mtd数据--广告整体，注意避免月末间断，关联日期维表
drop table if exists temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_004;
create table temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_004 as
select
  a.date_key
  ,a.module
  ,a.department
  ,a.industry
  ,a.cash_cost
  ,a.cost
 ,a.new_ads_campany_num_mtd  --月日新增广告客户数
 ,a.ads_campany_num --日消耗客户数
 ,sum(a.new_ads_campany_num_mtd) over(partition by substring(a.date_key,1,7) order by a.date_key) as ads_campany_num_mtd ---当月广告客户数
from 
(
  select
    a.date_key
    ,a.module
    ,a.department
    ,a.industry
    ,sum(cash_cost) as cash_cost 
    ,sum(cost) as cost
    ,sum(a.ads_campany_num) as ads_campany_num 
    ,sum(a.new_ads_campany_num_mtd) as new_ads_campany_num_mtd
  from
  (
    select
       a.first_order_date_key_mtd as date_key
       ,'广告整体' as module
      ,'商业部' as department
      ,'商业部' as industry
      ,0 as cash_cost
      ,0 as cost
      ,0 as ads_campany_num
      ,count(distinct a.brand_account_id ) as new_ads_campany_num_mtd 
    from 
    (
      select
          a.date_key
          ,a.brand_account_id
          -- ,min(a.date_key) over(partition by a.client_name order by a.date_key) as first_order_date_key ---首次消费日期
          ,min(a.date_key) over(partition by a.brand_account_id,substring(a.date_key,1,7) order by a.date_key) as first_order_date_key_mtd ---月首次消费日期
      from temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_001 a 
      where cost > 0
      --and department = '行业团队'
      and module in ('品牌','效果','薯条')
      group by a.date_key
          ,a.brand_account_id
    ) a 
    group by first_order_date_key_mtd

    union all
    select
        a.date_key 
        ,'广告整体' as module
        ,'商业部' as department
        ,'商业部' as industry
        ,sum(cash_cost) as cash_cost
        ,sum(cost) as cost
        ,count(distinct case when cost > 0 then a.brand_account_id else null end) as ads_campany_num --当日有消耗客户数
        ,0 as new_ads_campany_num_mtd
    from temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_001 a 
    where a.module in ('品牌','效果','薯条')
    group by 1,2,3,4
    -- 防止月末无新增消耗客户的情况
    union all 
    select date_key
        ,'广告整体' as module
        ,'商业部' as department
        ,'商业部' as industry
        ,0 as cash_cost
        ,0 as cost
        ,0 as  ads_campany_num
        ,0 as  new_ads_campany_num_mtd 
    from reddim.dim_date 
    where date_key<='{{ds}}' and date_key>='2019-01-01'
  ) a 
  group by 1,2,3,4
) a
;

--mtd数据--广告整体-行业数据整体
drop table if exists temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_005;
create table temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_005 as
select
  a.date_key
  ,a.module
  ,a.department
  ,a.industry
  ,a.cash_cost
  ,a.cost
 ,a.new_ads_campany_num_mtd  --月日新增广告客户数
 ,a.ads_campany_num --日消耗客户数
 ,sum(a.new_ads_campany_num_mtd) over(partition by substring(a.date_key,1,7),a.department order by a.date_key) as ads_campany_num_mtd ---当月广告客户数
from 
(
  select
    a.date_key
    ,a.module
    ,a.department
    ,a.industry
    ,sum(cash_cost) as cash_cost 
    ,sum(cost) as cost
    ,sum(a.ads_campany_num) as ads_campany_num 
    ,sum(a.new_ads_campany_num_mtd) as new_ads_campany_num_mtd
  from
  (
    select
       a.first_order_date_key_mtd as date_key
       ,'广告整体' as module
      ,a.department
      ,'整体' as industry
      ,0 as cash_cost
      ,0 as cost
      ,0 as ads_campany_num
      ,count(distinct a.brand_account_id ) as new_ads_campany_num_mtd 
    from 
    (
      select
          a.date_key
          ,a.brand_account_id
          ,a.department
          -- ,min(a.date_key) over(partition by a.client_name order by a.date_key) as first_order_date_key ---首次消费日期
          ,min(a.date_key) over(partition by a.brand_account_id,substring(a.date_key,1,7),a.department order by a.date_key) as first_order_date_key_mtd ---月首次消费日期
      from temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_001 a 
      where cost > 0
      --and department = '行业团队'
      and module in ('品牌','效果','薯条')
      group by a.date_key
          ,a.brand_account_id
          ,a.department
    ) a 
    group by first_order_date_key_mtd
      ,a.department

    union all
    select
        a.date_key 
        ,'广告整体' as module
        ,department
        ,'整体' as industry
        ,sum(cash_cost) as cash_cost
        ,sum(cost) as cost
        ,count(distinct case when cost > 0 then a.brand_account_id else null end) as ads_campany_num --当日有消耗客户数
        ,0 as new_ads_campany_num_mtd
    from temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_001 a 
    where module in ('品牌','效果','薯条')
    group by 1,2,3,4
   
    union all 
    -- 防止月末无新增消耗客户的情况
    select date_dim.date_key
      ,'广告整体' as module
      ,dim.department
      ,'整体' as industry
      ,0 as cash_cost
      ,0 as cost
      ,0 as  ads_campany_num
      ,0 as  new_ads_campany_num_mtd 
    from 
    (select department
      ,1 as tag
    from temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_001
    where  module in ('品牌','效果','薯条')
    group by department
    )dim 
    join 
    (
    select date_key
      ,1 as tag
    from reddim.dim_date 
    where date_key<='{{ds}}' and date_key>='2019-01-01'
    )date_dim
    on date_dim.tag = dim.tag
  ) a 
  group by 1,2,3,4
) a
;
--mtd数据--广告整体-行业数据分部门
drop table if exists temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_006;
create table temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_006 as
select
  a.date_key
  ,a.module
  ,a.department
  ,a.industry
  ,a.cash_cost
  ,a.cost
 ,a.new_ads_campany_num_mtd  --月日新增广告客户数
 ,a.ads_campany_num --日消耗客户数
 ,sum(a.new_ads_campany_num_mtd) over(partition by substring(a.date_key,1,7),a.module,a.department,a.industry order by a.date_key) as ads_campany_num_mtd ---当月广告客户数
from 
(
  select
    a.date_key
    ,a.module
    ,a.department
    ,a.industry
    ,sum(cash_cost) as cash_cost 
    ,sum(cost) as cost
    ,sum(a.ads_campany_num) as ads_campany_num 
    ,sum(a.new_ads_campany_num_mtd) as new_ads_campany_num_mtd
  from
  (
    select
       a.first_order_date_key_mtd as date_key
       ,'广告整体' as module
      ,department
      ,industry
      ,0 as cash_cost
      ,0 as cost
      ,0 as ads_campany_num
      ,count(distinct a.brand_account_id ) as new_ads_campany_num_mtd 
    from 
    (
      select
          a.date_key
          ,department
          ,industry
          ,a.brand_account_id
          -- ,min(a.date_key) over(partition by a.client_name order by a.date_key) as first_order_date_key ---首次消费日期
          ,min(a.date_key) over(partition by a.brand_account_id,substring(a.date_key,1,7),a.industry,a.department order by a.date_key) as first_order_date_key_mtd ---月首次消费日期
      from temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_001 a 
      where cost > 0
      --and department = '行业团队'
      and module in ('品牌','效果','薯条')
      group by a.date_key
          ,a.brand_account_id
          ,a.industry
          ,a.department
    ) a 
    group by first_order_date_key_mtd
      ,department
      ,industry

    union all
    select
        a.date_key 
        ,'广告整体' as module
        ,department
        ,industry
        ,sum(cash_cost) as cash_cost
        ,sum(cost) as cost
        ,count(distinct case when cost > 0 then a.brand_account_id else null end) as ads_campany_num --当日有消耗客户数
        ,0 as new_ads_campany_num_mtd
    from temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_001 a 
    where module in ('品牌','效果','薯条')
    group by a.date_key
      ,department
      ,industry
    union all 
    -- 防止月末无新增消耗客户的情况
    select date_dim.date_key
      ,'广告整体' as module
      ,dim.department
      ,dim.industry
      ,0 as cash_cost
      ,0 as cost
      ,0 as  ads_campany_num
      ,0 as  new_ads_campany_num_mtd 
    from 
    (select department
      ,industry
      ,1 as tag
    from temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_001
    where  module in ('品牌','效果','薯条')
    group by department,
      industry
    )dim 
    join 
    (
    select date_key
      ,1 as tag
    from reddim.dim_date 
    where date_key<='{{ds}}' and date_key>='2019-01-01'
    )date_dim
    on date_dim.tag = dim.tag
    
  ) a 
    group by 1,2,3,4
  ) a
  union all 
  --美奢洗护
  select
  a.date_key
  ,a.module
  ,a.department
  ,a.industry
  ,a.cash_cost
  ,a.cost
 ,a.new_ads_campany_num_mtd  --月日新增广告客户数
 ,a.ads_campany_num --日消耗客户数
 ,sum(a.new_ads_campany_num_mtd) over(partition by substring(a.date_key,1,7) order by a.date_key) as ads_campany_num_mtd ---当月广告客户数
from 
(
  select
    a.date_key
    ,a.module
    ,a.department
    ,a.industry
    ,sum(cash_cost) as cash_cost 
    ,sum(cost) as cost
    ,sum(a.ads_campany_num) as ads_campany_num 
    ,sum(a.new_ads_campany_num_mtd) as new_ads_campany_num_mtd
  from
  (
    select
       a.first_order_date_key_mtd as date_key
       ,'广告整体' as module
      ,department
      ,industry
      ,0 as cash_cost
      ,0 as cost
      ,0 as ads_campany_num
      ,count(distinct a.brand_account_id ) as new_ads_campany_num_mtd 
    from 
    (
      select
          a.date_key
          ,department
          ,industry_1 as industry
          ,a.brand_account_id
          -- ,min(a.date_key) over(partition by a.client_name order by a.date_key) as first_order_date_key ---首次消费日期
          ,min(a.date_key) over(partition by a.brand_account_id,substring(a.date_key,1,7),a.industry_1,a.department order by a.date_key) as first_order_date_key_mtd ---月首次消费日期
      from temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_001 a 
      where cost > 0
      and department = '行业团队'
      and module in ('品牌','效果','薯条')
      and industry_1='美奢洗护行业'
      group by a.date_key
          ,a.brand_account_id
          ,a.industry_1
          ,a.department
    ) a 
    group by first_order_date_key_mtd
      ,department
      ,industry

    union all
    select
        a.date_key 
        ,'广告整体' as module
        ,department
        ,industry_1 as industry
        ,sum(cash_cost) as cash_cost
        ,sum(cost) as cost
        ,count(distinct case when cost > 0 then a.brand_account_id else null end) as ads_campany_num --当日有消耗客户数
        ,0 as new_ads_campany_num_mtd
    from temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_001 a 
    where department = '行业团队'
      and module in ('品牌','效果','薯条')
      and industry_1='美奢洗护行业'
    group by a.date_key
      ,department
      ,industry_1
    union all 
    -- 防止月末无新增消耗客户的情况
    select date_dim.date_key
      ,'广告整体' as module
      ,dim.department as department
      ,dim.industry as industry
      ,0 as cash_cost
      ,0 as cost
      ,0 as  ads_campany_num
      ,0 as  new_ads_campany_num_mtd 
    from 
    (
    --美奢洗护
    select department
      ,industry_1 as industry
      ,1 as tag
    from temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_001
    where department = '行业团队'
      and module in ('品牌','效果','薯条')
      and industry_1='美奢洗护行业'
    group by department,
      industry_1
    )dim 
    join 
    (
    select date_key
      ,1 as tag
    from reddim.dim_date 
    where date_key<='{{ds}}' and date_key>='2019-01-01'
    )date_dim
    on date_dim.tag = dim.tag
    
  ) a 
group by 1,2,3,4
) a
;
insert overwrite table redapp_dev.app_ads_department_module_okr_dash_td_df partition(dtm = '{{ ds_nodash }}')
select 
   date_key
  ,'广告整体' as module
  ,'商业部' as department
  ,'商业部' as industry
  , cash_cost
  , cost
 , new_ads_campany_num_mtd  --月日新增广告客户数
 ,ads_campany_num --日消耗客户数
 , ads_campany_num_mtd
from 
  temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_004 a

union all 
select 
   a.date_key
  ,a.module
  ,'商业部' as department
  ,'商业部' as industry
  ,sum(cash_cost) as cash_cost
  ,sum(cost) as cost
  ,0 as new_ads_campany_num_mtd  --月日新增广告客户数
  ,0 as ads_campany_num --日消耗客户数
  ,0 as ads_campany_num_mtd
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
  ,0 as new_ads_campany_num_mtd  --月日新增广告客户数
  ,0 as ads_campany_num --日消耗客户数
  ,0 as ads_campany_num_mtd
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
  ,0 as new_ads_campany_num_mtd  --月日新增广告客户数
  ,0 as ads_campany_num --日消耗客户数
  ,0 as ads_campany_num_mtd
from 
  temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_002 a 
group by 
  1,2
union all 
select 
   date_key
  ,module
  ,department
  ,industry
  , cash_cost
  , cost
 ,new_ads_campany_num_mtd  --月日新增广告客户数
 ,ads_campany_num --日消耗客户数
 ,ads_campany_num_mtd
from 
  temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_005 a

union all 
select 
   date_key
  ,module
  ,department
  ,industry
  ,cash_cost
  , cost
 , new_ads_campany_num_mtd  --月日新增广告客户数
 ,ads_campany_num --日消耗客户数
 , ads_campany_num_mtd
from 
  temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_006 a
union all 
select 
   a.date_key
  ,a.module
  ,a.department
  ,a.industry
  ,sum(cash_cost) as cash_cost
  ,sum(cost) as cost
  ,0 as new_ads_campany_num_mtd  --月日新增广告客户数
  ,0 as ads_campany_num --日消耗客户数
  ,0 as ads_campany_num_mtd
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
  ,0 as new_ads_campany_num_mtd  --月日新增广告客户数
  ,0 as ads_campany_num --日消耗客户数
  ,0 as ads_campany_num_mtd
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
  ,0 as new_ads_campany_num_mtd  --月日新增广告客户数
  ,0 as ads_campany_num --日消耗客户数
  ,0 as ads_campany_num_mtd
from 
  temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_002 a 
group by 
  1,2,3,4
union all 
--拆分信息流搜索内流的003表收入
select a.date_key
  ,a.module
  ,a.department
  ,a.industry
  ,sum(cash_cost) as cash_cost
  ,sum(cost) as cost
  ,0 as new_ads_campany_num_mtd  --月日新增广告客户数
  ,0 as ads_campany_num --日消耗客户数
  ,0 as ads_campany_num_mtd
from temp.app_ads_department_module_okr_dash_td_df_{{ds_nodash}}_003 a 
group by 
  1,2,3,4
;

