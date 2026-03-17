--这是一个cube表
select date_key,
  base.brand_account_id,
  brand_user_name,
  company_code,
  company_name,
  module,
  first_track_industry_name ,
  case when first_track_industry_name = '创作者商业化部' then '创作者商业化部' else first_track_industry_dept_name end as first_track_industry_dept_name,
  case when first_track_industry_name = '创作者商业化部' then '创作者商业化部' else second_track_industry_dept_name end as second_track_industry_dept_name,
  case when first_track_industry_name = '创作者商业化部' then '创作者商业化部' else third_track_industry_dept_name end as third_track_industry_dept_name,
  cash_cost,
  cost,
  cast('{{ds_nodash}}' as int) as dtm
from 
(select date_key,
  brand_account_id,
  module,
  coalesce(first_track_industry_name,'行业整体') as first_track_industry_name ,
  coalesce(first_track_industry_dept_name,'整体') as first_track_industry_dept_name,
  coalesce(second_track_industry_dept_name,'整体') as second_track_industry_dept_name,
  coalesce(third_track_industry_dept_name,'整体') as third_track_industry_dept_name,
  sum(cash_income_amt) as cash_cost,
  sum(income_amt) as cost
from 
(select date_key,
  base.brand_account_id,
  brand_user_name,
  company_code,
  company_name,
  module,
  case
    when track_industry_dept_group_name in ('母婴','宠物') then '母婴宠物'
    --when first_track_industry_dept_name in ('家居','房地产','家居家装') then '家居地产'
    -- when first_track_industry_dept_name in ('互联网','交通出行','生活服务','跨境') then second_track_industry_dept_name
    when track_industry_dept_group_name is not null then track_industry_dept_group_name
    when direct_sales_dept2_name in ('创作者商业化部') then direct_sales_dept2_name
    when direct_sales_dept2_name in ('生态客户业务部') then sme_dept3_name
    else '其他'
   end as first_track_industry_dept_name ,
  case
      when track_industry_dept_group_name in ('母婴','宠物') then track_industry_dept_group_name
      --when first_track_industry_dept_name in ('家居','房地产','家居家装') then '家居地产'
      -- when first_track_industry_dept_name in ('互联网','交通出行','生活服务','跨境') then second_track_industry_dept_name
      when first_track_industry_dept_name is not null then second_track_industry_dept_name
      when direct_sales_dept2_name in ('创作者商业化部') then direct_sales_dept2_name
      when direct_sales_dept2_name in ('生态客户业务部') then sme_dept3_name
      else '其他'
   end as  second_track_industry_dept_name,
  case
      when first_track_industry_dept_name in ('母婴','宠物') then second_track_industry_dept_name
      --when first_track_industry_dept_name in ('家居','房地产','家居家装') then '家居地产'
      -- when first_track_industry_dept_name in ('互联网','交通出行','生活服务','跨境') then second_track_industry_dept_name
      when first_track_industry_dept_name is not null then split(track_detail_name,'-')[2]
      when direct_sales_dept2_name in ('创作者商业化部') then direct_sales_dept2_name
      when direct_sales_dept2_name in ('生态客户业务部') then sme_dept3_name
      else '其他'
   end as  third_track_industry_dept_name,
  case
    when  first_track_industry_name is not null then first_track_industry_dept_name --20240821互联网单列，从耐销拆出
    when direct_sales_dept2_name = '行业团队' then '行业团队其他'
    when direct_sales_dept2_name = '生态客户业务部' then sme_dept3_name
    when direct_sales_dept2_name = '创作者商业化部' then '创作者商业化部'
    else first_track_industry_name
   end as first_track_industry_name ,
   cash_income_amt
   ,income_amt
from 
  (select date_key
    ,t1.brand_account_id
    ,module
    ,cash_income_amt
    ,income_amt
    ,brand_user_name,
    company_code,
    company_name,
    -- first_ad_industry_name,
    -- second_ad_industry_name,
    sme_dept3_name,
    first_track_industry_dept_name,
    second_track_industry_dept_name,
    track_industry_dept_group_name,
    CASE
        WHEN (
          (module IN ('效果', '品合', '内容加热'))
          OR (module IS NULL)
        ) THEN coalesce(cpc_direct_sales_dept2_name,cpc_operator_dept2_name,if(company_name is null,'创作者商业化部','未挂接'))
        WHEN module in ('薯条','口碑通') then coalesce(cpc_direct_sales_dept2_name,cpc_operator_dept2_name,'创作者商业化部')
        WHEN (module IN ('品牌', 'IP')) THEN brand_direct_sales_dept2_name
      END as direct_sales_dept2_name ,
    track_detail_name,
    first_track_industry_name
  from
      (select  date_key
            ,brand_account_id
            ,module
            ,sum(case when module ='品牌' then perf_brand_cash_income_amt else cash_income_amt end) as cash_income_amt
            ,sum(case when module ='品牌' then perf_brand_income_amt else income_amt end)  as income_amt
          from 
            redcdm.dm_ads_pub_product_account_detail_td_df
          where dtm='{{ds_nodash}}'
          and date_key<='{{ds}}' and date_key>='2023-01-01'
          group by date_key
            ,brand_account_id
          ,module
      )t1

      left join 
      (select brand_account_id,
        brand_account_name as brand_user_name,
        company_code,
        company_name,
        -- first_ad_industry_name,
        -- second_ad_industry_name,
        sme_dept3_name,
        first_track_industry_dept_name,
        second_track_industry_dept_name,
        cpc_direct_sales_dept2_name ,
        cpc_operator_dept2_name ,
        track_detail_name,
        brand_direct_sales_dept2_name,
        track_industry_dept_group_name
      from redcdm.dim_ads_industry_brand_account_df
      where dtm='{{ds_nodash}}'
      )sme 
      on sme.brand_account_id = t1.brand_account_id
      left join 
      (SELECT
        first_track_industry_dept_name as first_track_name,
        first_track_industry_name
      FROM
        redapp.ods_ads_redoc2hive_onedash_track_mapping_df
      WHERE
        dtm = '{{ds_nodash}}'
      )mapp 
      on mapp.first_track_name = sme.track_industry_dept_group_name
  )base
)a 
group by
     a.date_key,
     a.brand_account_id,
     a.module,
    first_track_industry_name ,
  first_track_industry_dept_name,
  second_track_industry_dept_name,
  third_track_industry_dept_name
  grouping sets(
     (date_key,brand_account_id,module) -- 行业整体
    ,(date_key,brand_account_id,module,first_track_industry_name) -- 分行业
    ,(date_key,brand_account_id,module, first_track_industry_name ,first_track_industry_dept_name) -- 分行业、赛道
     ,(date_key,brand_account_id,module,first_track_industry_name,first_track_industry_dept_name,second_track_industry_dept_name) -- 分行业
    ,(date_key,brand_account_id,module, first_track_industry_name,first_track_industry_dept_name,second_track_industry_dept_name,third_track_industry_dept_name) -- 分行业、赛道
  ) 
)base 
left join 
(select brand_account_id,
  brand_account_name as brand_user_name,
  company_code,
  company_name,
  -- first_ad_industry_name,
  -- second_ad_industry_name,
  sme_dept3_name,
  -- first_track_industry_dept_name,
  -- second_track_industry_dept_name,
  -- cpc_direct_sales_dept2_name ,
  cpc_operator_dept2_name ,
  track_detail_name
from redcdm.dim_ads_industry_brand_account_df
where dtm='{{ds_nodash}}'
)dim 
on dim.brand_account_id =base.brand_account_id

where first_track_industry_name not in ('行业整体','其他','行业团队其他') or (first_track_industry_name in ('行业整体','其他','行业团队其他')  and first_track_industry_dept_name='整体')



