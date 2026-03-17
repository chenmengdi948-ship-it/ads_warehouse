--每日有消耗笔记绑定的spu和笔记消耗情况
drop table if exists temp.temp_app_ads_industry_spu_note_df_{{ds_nodash}}_online;

create table
  temp.temp_app_ads_industry_spu_note_df_{{ds_nodash}}_online as
select date_key
  ,base.spu_id
  ,base.note_id
  ,v_seller_id
  -- ,dim.v_seller_name
  ,brand_account_id
  --,account.brand_account_name
  ,marketing_target
  , module
  ,product
  ,note_cash_cost
  ,note_cost
  ,is_note_cost
  ,cash_cost
  ,cost
  ,spu_name,
  brand_id,
  brand_name,
  bind_update_time,
  bind_type,
  create_time,
  bind_time,
  case when bind_type in (2,-1) then '人工' when bind_type is not null then '算法'  else '未绑定' end as bind_tye_desc
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
  ,note_cash_cost
  ,note_cost
  ,is_note_cost
  ,cash_cost
  ,cost
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
      1,2,3,4,5,6,7
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
    row_number()over(partition by note_id ) as rn
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
        spu_id
        
    ) t2
    on t1.date_key=t2.date_key and t1.note_id = t2.note_id and t1.v_seller_id=t2.v_seller_id
      and t1.brand_account_id=t2.brand_account_id 
      and t1.marketing_target=t2.marketing_target
      and  t1.module=t2.module
      and t1.product=t2.product
      and t1.spu_id =t2.spu_id
)base
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
)a2 on  a2.note_id = base.note_id and base.spu_id=a2.spu_id
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
  ,e.v_seller_name
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
  ,case when a.bind_tye_desc ='人工' then 1 else 0 end as bind_status 
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
  ,cpc_direct_sales_dept3_name as direct_sales_parent_dept2_name
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
  ,case when t2.element_id is not null then spu_type else 1 end as is_need_bind,
  ,a.module
  ,a.product
  ,note_cash_cost
  ,note_cost
  ,is_note_cost
  ,bind_type_desc
from 



temp.temp_app_ads_industry_spu_note_df_{{ds_nodash}}_online a
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
  company_name
 from redcdm.dim_ads_industry_account_df
  where dtm='{{ds_nodash}}'  
)dim 
on dim.brand_account_id=a.brand_account_id
;