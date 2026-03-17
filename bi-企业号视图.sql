with account_module as 
--企业号去历史消耗最大的业务线
(select brand_account_id,
  `module`
from 
  (select *,
    row_number()over(partition by  brand_account_id order by cash_cost desc) as rn
  from 
  (select brand_user_id as brand_account_id,
      `module`,
      sum(cash_cost) as cash_cost
  from reddm.dm_ads_crm_advertiser_income_wide_day  
  where dtm='{{ds_nodash}}' and module in ('效果','薯条','品牌') --bi侧对于module判断不关注品合和内容加热
  group by brand_user_id,`module`
  )t1
  )t2
where rn=1
),
base as 
(select brand_account_id,
  brand_user_name,
  company_code,
  company_name,
  coalesce(sales_system,'自闭环及其他') as sales_system,
  sales_team,
  sales_team_sub,
  first_industry_name,
  second_industry_name,
  track_industry_name,
  first_cost_date,
  last_cost_date,
  is_today_cost,
  case when track_industry_name is not null then track_industry_name
    when track_industry_name is null and sales_system = '行业团队' then '行业团队其他'
    when track_industry_name is null and sales_system is not null then sales_system
    else '其他' end as process_track_industry_name,
  origin_first_industry_name,
  origin_second_industry_name,
  track_group_name,
  track_detail_name
from 
(select t1.brand_account_id,
  brand_user_name,
  case
    when module in ('效果','品合','内容加热') then coalesce(cpc_direct_sales_dept2_name,cpc_operator_dept2_name,if(company_name is null,'创作者商业化部','未挂接'))
    when module in ('薯条') then coalesce(cpc_direct_sales_dept2_name,cpc_operator_dept2_name,'创作者商业化部')
    when module = '品牌' then brand_direct_sales_dept2_name
  else coalesce(cpc_direct_sales_dept2_name,cpc_operator_dept2_name,brand_direct_sales_dept2_name,if(company_name is null,'创作者商业化部','未挂接'))
    -- when module in ('品合','内容加热') then coalesce(bcoo_direct_sales_dept2_name,'渠道业务部')
  end as sales_system,
  case when (module IN ('效果', '薯条', '品合', '内容加热') or module is null ) AND (cpc_direct_sales_dept4_name LIKE '%美妆%') AND (cpc_direct_sales_dept3_name = '美奢洗护行业') THEN '美妆洗护行业'
    WHEN (module IN ('效果', '薯条', '品合', '内容加热')or module is null ) AND (cpc_direct_sales_dept4_name = '奢品行业部门') THEN '奢品行业'
    WHEN (module IN ('效果', '薯条', '品合', '内容加热') ) THEN cpc_direct_sales_dept3_name
    WHEN (module IN ('品牌') or module is null ) AND (brand_direct_sales_dept4_name LIKE '%美妆%') AND (brand_direct_sales_dept3_name = '美奢洗护行业') THEN '美妆洗护行业'
    WHEN (module IN ('品牌') or module is null )  AND (brand_direct_sales_dept4_name = '奢品行业部门') THEN '奢品行业'
    WHEN (module IN ('品牌') ) THEN brand_direct_sales_dept3_name 
    else coalesce(cpc_direct_sales_dept3_name,brand_direct_sales_dept3_name) END AS sales_team,--direct_sales_dept3_name,
  CASE WHEN (module IN ('效果', '薯条', '品合', '内容加热') or module is null )  AND ((cpc_direct_sales_dept4_name = '奢品行业部门') OR (cpc_direct_sales_dept3_name = '本地客户业务部') ) THEN cpc_direct_sales_dept5_name
    WHEN (module IN ('效果', '薯条', '品合', '内容加热')  ) THEN cpc_direct_sales_dept4_name
    WHEN (module IN ('品牌') or module is null ) AND ( (brand_direct_sales_dept4_name = '奢品行业部门')OR (brand_direct_sales_dept3_name = '本地客户业务部')) THEN brand_direct_sales_dept5_name
    WHEN (module IN ('品牌')  ) THEN brand_direct_sales_dept4_name
  else coalesce(cpc_direct_sales_dept4_name,brand_direct_sales_dept4_name) END AS sales_team_sub,-- direct_sales_dept4_name
  first_industry_name,
  second_industry_name,
  track_industry_name,
  first_cost_date,
  last_cost_date,
  is_today_cost,
  track_industry_name,
  company_code,
  company_name,
  origin_first_industry_name,
  origin_second_industry_name,
  track_group_name,
  track_detail_name
from  
(select brand_account_id,
  brand_user_name,
  company_code,
  company_name,
  group_code,
  group_name,
  trade_type_first_name,
  trade_type_second_name,
  track_group_id,
  track_group_name,
  active_level,
  cpc_direct_sales_code,
  cpc_direct_sales_name,
  cpc_direct_sales_dept1_code,
  cpc_direct_sales_dept1_name,
  cpc_direct_sales_dept2_code,
  cpc_direct_sales_dept2_name,
  cpc_direct_sales_dept3_code,
  cpc_direct_sales_dept3_name,
  cpc_direct_sales_dept4_code,
  cpc_direct_sales_dept4_name,
  cpc_direct_sales_dept5_code,
  cpc_direct_sales_dept5_name,
  cpc_direct_sales_dept6_code,
  cpc_direct_sales_dept6_name,
  brand_direct_sales_code,
  brand_direct_sales_name,
  brand_direct_sales_dept1_code,
  brand_direct_sales_dept1_name,
  brand_direct_sales_dept2_code,
  brand_direct_sales_dept2_name,
  brand_direct_sales_dept3_code,
  brand_direct_sales_dept3_name,
  brand_direct_sales_dept4_code,
  brand_direct_sales_dept4_name,
  brand_direct_sales_dept5_code,
  brand_direct_sales_dept5_name,
  brand_direct_sales_dept6_code,
  brand_direct_sales_dept6_name,
  cpc_operator_dept1_code,
  cpc_operator_dept1_name,
  cpc_operator_dept2_code,
  cpc_operator_dept2_name,
  cpc_operator_dept3_code,
  cpc_operator_dept3_name,
  cpc_operator_dept4_code,
  cpc_operator_dept4_name,
  cpc_operator_dept5_code,
  cpc_operator_dept5_name,
  cpc_operator_dept6_code,
  cpc_operator_dept6_name,
  first_cost_date,
  last_cost_date,
  is_today_cost,
  first_industry_name,
  second_industry_name,
  crm_first_industry_code,
  crm_first_industry_name,
  crm_second_industry_code,
  crm_second_industry_name,
  origin_first_industry_code,
  origin_first_industry_name,
  origin_second_industry_code,
  origin_second_industry_name,
  track_industry_name,
  track_detail_name
from redcdm.dim_ads_industry_account_df
where dtm='{{ds_nodash}}'
 ) t1
left join account_module 
on t1.brand_account_id = account_module.brand_account_id
)detail 
) ,
--company客户近30天交易日cash_cost>0消耗最大组织架构
company as 
(select 
    company_code,
    sales_system as company_sales_system,
    sales_team as company_sales_team,
    first_industry_name as company_first_industry_name,
    second_industry_name as company_second_industry_name,
    track_industry_name as company_track_industry_name,
    process_track_industry_name as company_process_track_industry_name,
    origin_first_industry_name as company_origin_first_industry_name,
    origin_second_industry_name as company_origin_second_industry_name,
    track_group_name as company_track_group_name,
    track_detail_name as company_track_detail_name
  from (
    select
      *
      ,row_number() over(partition by company_code order by cash_cost desc,sales_system_tag asc) as rn
    from (
      select 
        company_code,
        sales_system,
        sales_team,
        sales_team_sub,
        first_industry_name,
        second_industry_name,
        track_industry_name,
        process_track_industry_name,
        origin_first_industry_name,
        origin_second_industry_name,
        track_group_name,
        track_detail_name,
        case when sales_system='行业团队' then 1 
        when sales_system='生态客户业务部' then 2 
        when sales_system='创作者商业化部' then 3 
        when sales_system='自闭环及其他'  then 4 else 5 end as sales_system_tag, --收入相同排序方式
        sum(cash_cost) as cash_cost
      from (
        select 
          *
          ,dense_rank() over(partition by company_code order by date_key desc) as drk
        from (
          select 
            t1.company_code,
            sales_system,
            sales_team,
            sales_team_sub,
            first_industry_name,
            second_industry_name,
            track_industry_name,
            process_track_industry_name,
            origin_first_industry_name,
            origin_second_industry_name,
            track_group_name,
            track_detail_name,
            date_key,
            sum(cash_cost) as cash_cost
          from
            (select  date_key,
              company_code,
              cash_cost,
              brand_user_id
            from reddm.dm_ads_crm_advertiser_income_wide_day   
            where dtm='{{ds_nodash}}'
            )t1 
            left join 
            base --企业号组织架构
            on base.brand_account_id =t1.brand_user_id
          where cash_cost>0 --有消耗
          group by 
            t1.company_code,
            sales_system,
            sales_team,
            sales_team_sub,
            first_industry_name,
            second_industry_name,
            track_industry_name,
            process_track_industry_name,
            origin_first_industry_name,
            origin_second_industry_name,
            track_group_name,
            track_detail_name,
            date_key
        ) as a 
      ) as a 
      where
        drk <= 30
      group by 
        company_code,
        sales_system,
        sales_team,
        sales_team_sub,
        first_industry_name,
        second_industry_name,
        track_industry_name,
        process_track_industry_name,
        case when sales_system='行业团队' then 1 when sales_system='生态客户业务部' then 2 else 3 end,
        origin_first_industry_name,
        origin_second_industry_name,
        track_group_name,
        track_detail_name
      union all 
    --无消耗的company

      select 
        base.company_code,
        sales_system,
        sales_team,
        sales_team_sub,
        first_industry_name,
        second_industry_name,
        track_industry_name,
        process_track_industry_name,
        origin_first_industry_name,
        origin_second_industry_name,
        track_group_name,
        track_detail_name,
        case when sales_system='行业团队' then 1 
        when sales_system='生态客户业务部' then 2 
        when sales_system='创作者商业化部' then 3 
        when sales_system='自闭环及其他'  then 4 else 5 end as sales_system_tag, --收入相同排序方式
        0 as cash_cost
      from
        base --企业号组织架构
        left join 
        (select  date_key,
          company_code,
          cash_cost,
          brand_user_id
        from reddm.dm_ads_crm_advertiser_income_wide_day   
        where dtm='{{ds_nodash}}'
        )t1 
        on base.brand_account_id =t1.brand_user_id
      where coalesce(cash_cost,0)=0 --没有消耗
      group by 
        base.company_code,
        sales_system,
        sales_team,
        sales_team_sub,
        first_industry_name,
        second_industry_name,
        track_industry_name,
        process_track_industry_name,
        origin_first_industry_name,
        origin_second_industry_name,
        track_group_name,
        track_detail_name,
        case when sales_system='行业团队' then 1 
        when sales_system='生态客户业务部' then 2 
        when sales_system='创作者商业化部' then 3 
        when sales_system='自闭环及其他'  then 4 else 5 end
    ) as a
    
  ) as a 
  where
    rn = 1 
)

insert overwrite table redapp.app_ads_insight_industry_account_df partition(dtm = '{{ ds_nodash }}') 
select brand_account_id,
  brand_user_name,
  base.company_code,
  company_name,
  coalesce(sales_system,'自闭环及其他') as sales_system,
  sales_team,
  sales_team_sub,
  first_industry_name,
  second_industry_name,
  track_industry_name,
  first_cost_date,
  last_cost_date,
  is_today_cost,
  process_track_industry_name,
  coalesce(company_sales_system,'自闭环及其他') as company_sales_system,
  company_sales_team,
  company_first_industry_name,
  company_second_industry_name,
  company_track_industry_name,
  company_process_track_industry_name,
  origin_first_industry_name,
  origin_second_industry_name,
  track_detail_name,
  track_group_name,
  company_origin_first_industry_name,
  company_origin_second_industry_name,
  company_track_detail_name,
  company_track_group_name
from base 
left join 
company 
on base.company_code = company.company_code 