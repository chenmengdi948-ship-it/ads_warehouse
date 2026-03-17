--每日有消耗笔记绑定的spu和笔记消耗情况
drop table if exists temp.temp_app_ads_industry_spu_note_df_{{ds_nodash}}_online1;

create table
  temp.temp_app_ads_industry_spu_note_df_{{ds_nodash}}_online1 as
select date_key
  ,base_all.spu_id
  ,base_all.note_id
  ,v_seller_id
  -- ,dim.v_seller_name
  ,base_all.brand_account_id
  --,account.brand_account_name
  ,marketing_target
  , module
  ,product
  ,note_cash_cost
  ,note_cost
  ,is_note_cost
  ,cash_cost
  ,cost
  ,coalesce(a2.spu_name,a3.spu_name) as spu_name,
  coalesce(a2.brand_id,a3.brand_id) as brand_id,
  coalesce(a2.brand_name,a3.brand_name) as brand_name,
  bind_update_time,
  bind_type,
  create_time,
  bind_time,
  case when bind_type in (2,-1) then '人工' when bind_type is not null then '算法'  else '未绑定' end as bind_type_desc,
  substring(date_key,1,7) as stat_month,
  cash_income_amt,
  income_amt,
  cpc_direct_sales_dept3_name,
  is_marketing_product
from 
(select  coalesce(base.date_key,spu.date_key) as date_key
    ,coalesce(base.spu_id,spu.spu_id) as spu_id
    ,note_id
    ,v_seller_id
    --,coalesce(t1.v_seller_name,t2.v_seller_name) as  v_seller_name
    ,brand_account_id
    --,coalesce(t1.brand_account_name,t2.brand_account_name) as brand_account_name
    , marketing_target
    ,module
    ,product
    ,is_marketing_product
    ,note_cash_cost
    ,note_cost
    ,is_note_cost
    ,cash_cost
    ,cost
    ,cash_income_amt
    ,income_amt
from
  (
  select 
    coalesce(t1.date_key,t2.date_key) as date_key
    ,coalesce(t1.spu_id,t2.spu_id) as spu_id
    ,coalesce(t1.note_id , t2.note_id ) as  note_id
    ,coalesce(t1.v_seller_id,t2.v_seller_id) as  v_seller_id
    --,coalesce(t1.v_seller_name,t2.v_seller_name) as  v_seller_name
    ,coalesce(t1.brand_account_id,t2.brand_account_id) as brand_account_id
    --,coalesce(t1.brand_account_name,t2.brand_account_name) as brand_account_name
    ,coalesce(t1.marketing_target,t2.marketing_target) as marketing_target
    ,coalesce(t1.module,t2.module) as module
    ,coalesce(t1.product,t2.product) as product
    ,coalesce(t1.is_marketing_product,t2.is_marketing_product) as is_marketing_product
    ,note_cash_cost
    ,note_cost
    ,is_note_cost
    ,cash_cost
    ,cost
    ,0 as cash_income_amt
    ,0 as income_amt
  from 
    (select date_key,
      bind_type,
      coalesce(spu_id,-911) as spu_id,
      spu_name,
      t1.note_id,
      brand_id,
      brand_name,
      bind_update_time,
      create_time,
      bind_time,
      v_seller_id
      --,v_seller_name
      ,brand_account_id
      --,brand_account_name
      ,module
      ,product
      ,marketing_target
      ,note_cash_cost
      ,note_cost
      ,is_marketing_product
      ,case when rn is null or rn =1 then 1 else 0 end as is_note_cost
    from
    --有消耗笔记
      (select
          a.date_key
          ,a.note_id as note_id
          ,b.virtual_seller_id as v_seller_id
          --,b.virtual_seller_name as v_seller_name
          ,a.brand_account_id
          ,module
          ,product
          ,marketing_target
          ,'0' as is_marketing_product
          ,sum(cash_income_amt) as note_cash_cost
          ,sum(income_amt) as note_cost
          from 
            (select date_key,
              module,
              product,
              note_id,
              brand_user_id as brand_account_id,
              brand_user_name as brand_account_name,
              advertiser_id,
              -911 as marketing_target,
              cash_income_amt,
              income_amt
            from redcdm.dws_ads_note_product_income_detail_df
            where
              dtm = '{{ds_nodash}}'
              and coalesce(module,'')<>'效果' and income_amt>0
              and date_key>='2023-01-01'
            union all 
            select
              date_key
              ,module
              ,product
              ,note_id
              ,brand_user_id as brand_account_id
              ,brand_user_name as brand_account_name
              ,advertiser_id
              ,marketing_target
              ,cash_income_amt
              ,income_amt
          from redcdm.dws_ads_creativity_product_income_detail_df
          where dtm = '{{ds_nodash}}' and note_id is not null and note_id<>'' 
            and module='效果' 
            and income_amt>0
            and date_key>='2023-01-01'
          )a 
          left join
            redcdm.dim_ads_advertiser_df b on b.dtm = '{{ds_nodash}}' and a.advertiser_id = b.rtb_advertiser_id
        group by
          1,2,3,4,5,6,7,8
      )t1
      left join 
      (select spu_id,
        spu_name,
        note_id,
        brand_id,
        brand_name,
        bind_update_time,
        bind_type,
        create_time,
        bind_time,
        row_number()over(partition by note_id order by spu_id) as rn
      from ads_databank.dim_spu_note_df 
      where dtm = '{{ds_nodash}}'
      ) t2
      on t1.note_id =t2.note_id
  
  )t1 
  full outer join 
  --spu均摊收入
  (
        select
          date_key,
          note_id,
          v_seller_id,
          v_seller_name,
          brand_user_id as brand_account_id,
          --brand_user_name as brand_account_name,
          marketing_target,
          case when product ='口碑通' then '口碑通' else module end as module,
          product,
          spu_id,
          is_marketing_product,
          sum(cash_income_amt) as cash_cost,
          sum(income_amt) as cost
        from
          redcdm.dws_ads_note_spu_product_income_detail_td_df
        where dtm='{{ds_nodash}}' and date_key>='2023-01-01'
        group by date_key,
          note_id,
          v_seller_id,
          v_seller_name,
          brand_user_id ,
        -- brand_user_name ,
          marketing_target,
          case when product ='口碑通' then '口碑通' else module end,
          product,
          spu_id,
          is_marketing_product
      ) t2
      on t1.date_key=t2.date_key and t1.note_id = t2.note_id and t1.v_seller_id=t2.v_seller_id
        and t1.brand_account_id=t2.brand_account_id 
        and t1.marketing_target=t2.marketing_target
        and  t1.module=t2.module
        and t1.product=t2.product
        and t1.spu_id =t2.spu_id
        and t1.is_marketing_product=t2.is_marketing_product

  union all 
  --20240614加行业总流水 
  select date_key
    ,-911 as spu_id
    ,'-911' as note_id
    ,'-911' as v_seller_id
    ,brand_account_id as brand_account_id
    ,marketing_target as marketing_target
    ,module
    ,product
    ,is_marketing_product
    ,0 as note_cash_cost
    ,0 as note_cost
    ,0 as is_note_cost
    ,0 as cash_cost
    ,0 as cost
    ,sum(case when module = '品牌' then perf_brand_cash_income_amt else cash_income_amt  end) as cash_income_amt
    ,sum(case when module = '品牌' then perf_brand_income_amt else income_amt  end) as income_amt
  from redcdm.dm_ads_pub_product_account_detail_td_df
  where dtm='{{ds_nodash}}' and date_key>='2023-01-01' and date_key<='{{ds}}' 

  group by date_key
     
    ,brand_account_id 
    ,marketing_target
    ,module
    ,product
    ,is_marketing_product
  )base
  full outer join 
  (select dt as date_key,
    spu_id
  from ads_databank.dim_spu_df a
  left join 
  (select dt
  from redcdm.dim_ads_date_df 
  where dtm='all' and dt>='2024-01-01' and dt<='{{ds}}'
  )b 
  on 1=1
  where a.dtm='{{ds_nodash}}' --生命周期180天
  )spu 
  on base.spu_id=spu.spu_id and base.date_key = spu.date_key
)base_all
left join 
 (select spu_id,
  spu_name,
  note_id,
  brand_id,
  brand_name,
  bind_update_time,
  bind_type,
  create_time,
  bind_time
from ads_databank.dim_spu_note_df 
where dtm = '{{ds_nodash}}'
)a2 on  a2.note_id = base_all.note_id and base_all.spu_id=a2.spu_id
left join 
 (select spu_id,
  name as spu_name,
  brand_id,
  brand_name
from ads_databank.dim_spu_df
where dtm = '{{ds_nodash}}'
)a3 on  base_all.spu_id=a3.spu_id
left join  
(select brand_account_id,
  cpc_direct_sales_dept3_name
from redcdm.dim_ads_industry_account_df 
where dtm='{{ds_nodash}}'
)dept 
on dept.brand_account_id = base_all.brand_account_id
;


insert overwrite table redapp.app_insight_account_note_spu_info_td_df partition(dtm = '{{ ds_nodash }}')
select 
  a.date_key as date_key
  ,a.note_id
  ,case 
    when coalesce(d.is_brand,0) = 1 then '企业号' 
    when c.order_type_cate1_id = 1 then '定制'
    when c.order_type_cate1_id = 2 then '招募'
    when c.order_type_cate1_id = 3 then '共创'
    when c.order_type_cate1_id = 4 then '新芽'
  end as note_type 
  ,a.v_seller_id 
  ,e.virtual_seller_name as v_seller_name
  ,a.brand_account_id
  ,dim.brand_user_name as brand_account_name
  ,case when f.brand_account_id is not null then 1 else 0 end as is_spu_white_list
  ,f.update_time as white_list_time
  ,e.first_ad_industry_name as first_ad_industry_name
  ,e.second_ad_industry_name as second_ad_industry_name
  ,dim.company_name
  ,a.spu_id
  ,a.spu_name
  ,a.bind_time
  ,a.bind_update_time
  ,case when a.bind_type_desc ='人工' then 1 else 0 end as bind_status 
  ,a.brand_id
  ,a.brand_name
  ,e.agent_user_name
  ,a.marketing_target
  ,e.sales_system
  ,dim.direct_sales_code
  ,dim.direct_sales_name
  ,cpc_direct_sales_dept5_code as direct_sales_dept_code
  ,cpc_direct_sales_dept5_name as direct_sales_dept_name
  ,cpc_direct_sales_dept4_code as direct_sales_parent_dept1_code
  ,cpc_direct_sales_dept4_name as direct_sales_parent_dept1_name
  ,cpc_direct_sales_dept3_code as direct_sales_parent_dept2_code
  ,dim.cpc_direct_sales_dept3_name as direct_sales_parent_dept2_name
  ,e.channel_sales_code             
  ,e.channel_sales_name             
  ,e.channel_sales_dept_code        
  ,e.channel_sales_dept_name        
  ,e.channel_sales_parent_dept1_code
  ,e.channel_sales_parent_dept1_name
  ,e.channel_sales_parent_dept2_code
  ,e.channel_sales_parent_dept2_name
  ,a.cash_cost 
  ,a.cost
  ,e.agent_type
  ,coalesce(d.support_bind_status,1) as support_bind_status
  ,case when t2.element_id is not null then spu_type else 1 end as is_need_bind
  ,a.module
  ,a.product
  ,note_cash_cost
  ,a.note_cost
  ,a.is_note_cost
  ,bind_type_desc
  ,last_month_spu_cash_cost
  ,case when substring(date_key,1,7)=substring('{{ds}}',1,7) then process_mtd_spu_cash_cost else mtd_spu_cash_cost end as mtd_spu_cash_cost
  ,ytd_spu_cash_cost
  ,last_ytd_spu_cash_cost
  --, process_mtd_spu_cash_cost
  ,cash_income_amt
  ,income_amt
  ,mtd_spu_cash_cost as mtd_actual_spu_cash_cost,
  mtd_actual_spu_rtb_cash_cost,
  mtd_actual_spu_brand_cash_cost,
  mtd_actual_spu_bcoo_cash_cost,
  commercial_taxonomy_name1,
  commercial_taxonomy_name2,
  commercial_taxonomy_name3,
  commercial_taxonomy_name4,
  last_month_spu_dept3_cash_cost,
  case when substring(date_key,1,7)=substring('{{ds}}',1,7) then process_mtd_spu_dept3_cash_cost else mtd_spu_dept3_cash_cost end as mtd_spu_dept3_cash_cost,
  pic_url,
  a.is_marketing_product,
  brand_tag_name
from 
temp.temp_app_ads_industry_spu_note_df_{{ds_nodash}}_online1 a
left join 
  reddm.dm_soc_brand_coo_order_note_detail_day c on c.dtm = '{{ds_nodash}}' and a.note_id = c.note_id
left join (
  select 
    note_id
    ,max(is_brand) as is_brand
    ,max(case
          when (
            enabled = false -- 已删除
            or is_private = 1 -- 仅自己可见
            or note_level in (5,0,-1,-2,-3,-4,-5) -- 笔记审核状态
            or punish_level in (1,2,3) -- punish_level
            or (fine rlike '虚假图片|价值观低质|封面标题|低质画风|广告|低成本创作|虚假不实|搬运|诱导互动' and audit_time > update_time) -- 违规判断
          ) then 0
          else 1
        end
    ) as support_bind_status
  from 
    redcdm.dm_note_detail_td_df
  where
    dtm = '{{ds_nodash}}'
  group by 1 
) d on a.note_id = d.note_id
left join(
  select --crm获取组织架构
    virtual_seller_id
    ,virtual_seller_name
    ,company_name
    ,agent_company_name as agent_user_name
    ,sales_system
    ,direct_sales_code
    ,direct_sales_name
    ,direct_sales_dept_code
    ,direct_sales_dept_name
    ,direct_sales_parent_dept1_code
    ,direct_sales_parent_dept1_name
    ,direct_sales_parent_dept2_code
    ,direct_sales_parent_dept2_name
    ,channel_sales_code             
    ,channel_sales_name             
    ,channel_sales_dept_code        
    ,channel_sales_dept_name        
    ,channel_sales_parent_dept1_code
    ,channel_sales_parent_dept1_name
    ,channel_sales_parent_dept2_code
    ,channel_sales_parent_dept2_name
    ,agent_type
    ,first_ad_industry_name
    ,second_ad_industry_name
    --,brand_user_name
  from reddm.dm_ads_crm_advertiser_income_wide_day e 
  where 
    e.dtm = '{{ds_nodash}}' 
    and e.module = '效果'
  group by     
     virtual_seller_id
     ,virtual_seller_name
    ,company_name
    ,agent_company_name
    ,sales_system
    ,direct_sales_code
    ,direct_sales_name
    ,direct_sales_dept_code
    ,direct_sales_dept_name
    ,direct_sales_parent_dept1_code
    ,direct_sales_parent_dept1_name
    ,direct_sales_parent_dept2_code
    ,direct_sales_parent_dept2_name
    ,channel_sales_code             
    ,channel_sales_name             
    ,channel_sales_dept_code        
    ,channel_sales_dept_name        
    ,channel_sales_parent_dept1_code
    ,channel_sales_parent_dept1_name
    ,channel_sales_parent_dept2_code
    ,channel_sales_parent_dept2_name
    ,agent_type
    ,first_ad_industry_name
    ,second_ad_industry_name
    -- ,brand_user_name
) e on a.v_seller_id = e.virtual_seller_id 
left join (
  select 
    item as brand_account_id
    ,max(update_time) as update_time
  from 
    redods.ods_brand_account_tb_common_white_list
  where 
    dtm = '{{ds_nodash}}' 
    and type in ('spu_brand_user_id','spu_brand_user_id_new')
    and status = 1
  group by 1 
) f on a.brand_account_id = f.brand_account_id
left join 
(select  distinct element_id,
  spu_type
from redods.ods_shequ_feed_ads_tb_material_bind_spu_df 
where dtm = '{{ds_nodash}}'
 and  bind_status = 2
 and  spu_type in (2,3) 
 and del = 0
)t2 
 on a.note_id = t2.element_id
 left join 
 --企业号组织架构
 (select brand_account_id,
 brand_user_name,
  cpc_direct_sales_dept3_name,
  cpc_direct_sales_dept4_name,
  cpc_direct_sales_dept5_name,
  cpc_direct_sales_dept3_code,
  cpc_direct_sales_dept4_code,
  cpc_direct_sales_dept5_code,
  cpc_direct_sales_code as direct_sales_code,
  cpc_direct_sales_name as direct_sales_name,
  company_name,
  brand_tag_name
 from redcdm.dim_ads_industry_account_df
  where dtm='{{ds_nodash}}'  
)dim 
on dim.brand_account_id=a.brand_account_id
left join 
(select substring(date_key,1,7) as stat_month,
  substring(add_months(date_key,1),1,7) as next_month,
  spu_id,
  sum(cash_income_amt) as last_month_spu_cash_cost,
  sum(income_amt) as last_month_cost
from
  redcdm.dws_ads_note_spu_product_income_detail_td_df
where dtm='{{ds_nodash}}' and date_key>='2023-01-01'
and module in ('品牌','效果','薯条','口碑通')
group by substring(date_key,1,7),
  substring(add_months(date_key,1),1,7),
  spu_id
)spu_last_cost
on spu_last_cost.spu_id = a.spu_id and spu_last_cost.next_month = a.stat_month
--20240701增加dept3*spu粒度，原因是spu可能多广告主投放分布在不同赛道，然后投放规模不同。
left join 
(select substring(date_key,1,7) as stat_month,
  substring(add_months(date_key,1),1,7) as next_month,
  spu_id,
  cpc_direct_sales_dept3_name,
  sum(cash_income_amt) as last_month_spu_dept3_cash_cost
from
  redcdm.dws_ads_note_spu_product_income_detail_td_df t1
  left join redcdm.dim_ads_industry_account_df t2 
  on t1.brand_user_id=t2.brand_account_id and t2.dtm='{{ds_nodash}}'
where t1.dtm='{{ds_nodash}}' and t1.date_key>='2023-01-01'
and t1.module in ('品牌','效果','薯条','口碑通')
group by substring(date_key,1,7),
  substring(add_months(date_key,1),1,7),
  spu_id,
  cpc_direct_sales_dept3_name
)spudept_last_cost
on spudept_last_cost.spu_id = a.spu_id and spudept_last_cost.next_month = a.stat_month
and spudept_last_cost.cpc_direct_sales_dept3_name = a.cpc_direct_sales_dept3_name
left join 
--20240701增加dept3*spu粒度，原因是spu可能多广告主投放分布在不同赛道，然后投放规模不同。
(select substring(date_key,1,7) as stat_month,
  -- substring(add_months(date_key,1),1,7) as next_month,
  spu_id,
  cpc_direct_sales_dept3_name,
  sum(case when module in ('品牌','效果','薯条','口碑通') then cash_income_amt else 0 end) as mtd_spu_dept3_cash_cost,
  sum(case when module in ('品牌','效果','薯条','口碑通') and substring(date_key,1,7) =substring('{{ds}}',1,7) then cash_income_amt else 0 end) / day('{{ds}}') *(datediff(trunc(add_months('{{ds}}',1),'MM'),trunc('{{ds}}','MM'))) as process_mtd_spu_dept3_cash_cost
from
  redcdm.dws_ads_note_spu_product_income_detail_td_df t1
  left join redcdm.dim_ads_industry_account_df t2 
  on t1.brand_user_id=t2.brand_account_id and t2.dtm='{{ds_nodash}}'
where t1.dtm='{{ds_nodash}}' and t1.date_key>='2023-01-01'
--and module in ('品牌','效果','薯条','口碑通')
group by substring(date_key,1,7),
  spu_id,
  cpc_direct_sales_dept3_name
)spudept_month
on spudept_month.spu_id = a.spu_id and spudept_month.stat_month = a.stat_month
--统计日期当月spu流水
left join 
(select substring(date_key,1,7) as stat_month,
  -- substring(add_months(date_key,1),1,7) as next_month,
  spu_id,
  sum(case when module in ('品牌','效果','薯条','口碑通') then cash_income_amt else 0 end) as mtd_spu_cash_cost,
  sum(case when module in ('品牌','效果','薯条','口碑通') and substring(date_key,1,7) =substring('{{ds}}',1,7) then cash_income_amt else 0 end) / day('{{ds}}') *(datediff(trunc(add_months('{{ds}}',1),'MM'),trunc('{{ds}}','MM'))) as process_mtd_spu_cash_cost,
  sum(case when module in ('效果','薯条','口碑通') then cash_income_amt else 0 end) as mtd_actual_spu_rtb_cash_cost,
  sum(case when module in ('品牌') then cash_income_amt else 0 end) as mtd_actual_spu_brand_cash_cost,
  sum(case when module in ('品合') then cash_income_amt else 0 end) as mtd_actual_spu_bcoo_cash_cost
from
  redcdm.dws_ads_note_spu_product_income_detail_td_df
where dtm='{{ds_nodash}}' and date_key>='2023-01-01'
--and module in ('品牌','效果','薯条','口碑通')
group by substring(date_key,1,7),
  spu_id
)spu_month
on spu_month.spu_id = a.spu_id and spu_month.stat_month = a.stat_month
left join 
--当年和去年spu流水

(select --substring(date_key,1,4) as stat_month,
  -- substring(add_months(date_key,1),1,7) as next_month,
  spu_id,
  sum(case when substring(date_key,1,4)= substring('{{ds}}',1,4) then cash_income_amt else 0 end) as ytd_spu_cash_cost,
  sum(case when substring(date_key,1,4)= year('{{ds}}')-1 then cash_income_amt else 0 end) as last_ytd_spu_cash_cost
from
  redcdm.dws_ads_note_spu_product_income_detail_td_df
where dtm='{{ds_nodash}}' and date_key>='2023-01-01'
and module in ('品牌','效果','薯条','口碑通')
group by 
  spu_id
)spu_ytd
on spu_ytd.spu_id = a.spu_id
left join 
(select spu_id,
  commercial_taxonomy_name1,
  commercial_code2,
  commercial_taxonomy_name2,
  commercial_code3,
  commercial_taxonomy_name3,
  commercial_code4,
  commercial_taxonomy_name4,
  split(pic_url_list,';')[0] as pic_url
from  ads_databank.dim_spu_df 
where dtm='{{ds_nodash}}'
)dim 
on dim.spu_id =a.spu_id
where a.date_key<='{{ds}}'
;