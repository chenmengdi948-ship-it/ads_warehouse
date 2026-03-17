with account_info as (
    select
      t2.all_brand_group_tag_name,
     
      ,min(date_key) as first_cost_date
      ,max(case when date_key = '{{ds}}' then '0000-00-00' else date_key end) as last_cost_date
      ,max(case when date_key = '{{ds}}' then 1 else 0 end) as is_today_cost -- 当天是否消耗
      -- 品牌 B
      ,min(case when module = '品牌' then date_key end) as b_first_cost_date
      ,max(case when module = '品牌' and date_key = '{{ds}}' then '0000-00-00' else date_key end) as b_last_cost_date
      ,max(case when module = '品牌' and date_key = '{{ds}}' then 1 else 0 end) as b_is_today_cost -- 当天是否消耗
      -- 蒲公英 K
      ,min(case when module in ('品合','内容加热')  then date_key end) as k_first_cost_date
      ,max(case when module in ('品合','内容加热')  and date_key = '{{ds}}' then '0000-00-00' else date_key end) as k_last_cost_date
      ,max(case when module in ('品合','内容加热')  and date_key = '{{ds}}' then 1 else 0 end) as k_is_today_cost -- 当天是否消耗
      -- 信息流 F
      ,min(case when (module in ('薯条') or product in ('信息流','视频内流'))  then date_key end) as f_first_cost_date
      ,max(case when (module in ('薯条') or product in ('信息流','视频内流'))  and date_key = '{{ds}}' then '0000-00-00' else date_key end) as f_last_cost_date
      ,max(case when (module in ('薯条') or product in ('信息流','视频内流'))  and date_key = '{{ds}}' then 1 else 0 end) as f_is_today_cost -- 当天是否消耗
      -- 搜索 S
      ,min(case when product in ('搜索')  then date_key end) as s_first_cost_date
      ,max(case when product in ('搜索')  and date_key = '{{ds}}' then '0000-00-00' else date_key end) as s_last_cost_date
      ,max(case when product in ('搜索')  and date_key = '{{ds}}' then 1 else 0 end) as s_is_today_cost -- 当天是否消耗

      ,max(case when module = '效果' and sales_system = '渠道业务部' then direct_sales_code end) as cpc_direct_seller_code
      ,max(case when module = '效果' and sales_system = '渠道业务部' then direct_sales_name end) as cpc_direct_seller_name

      -- 美奢行业，双月客户分层
      ,max(case when module in ('品牌','效果') and date_key <= '{{ds}}' then date_key else '0000-00-00' end) as ads_last_cost_date -- 除去t-1的最后一次消耗时间
      ,min(case when module in ('品牌','效果') and date_key <= '{{ds}}' then date_key else '9999-12-31' end) as ads_first_cost_date -- 首次消耗时间20230712添加
      ,max(case when module in ('品牌','效果') and date_key < if(month('{{ds}}') & 1 = 1, trunc('{{ds}}','MM'), trunc(add_months('{{ds}}',-1),'MM')) then date_key else '0000-00-00' end) as ads_last_cost_date_bimonthly --双月初之前的最后一次消耗时
      ,max(case when module in ('品牌','效果') then if(month('{{ds}}') & 1 = 1,trunc('{{ds}}','MM'),trunc(add_months('{{ds}}',-1),'MM')) end) as ads_bimonthly_first_date -- 双月初的日期
      ,max(case when module in ('品牌','效果') and date_key <  trunc('{{ds}}','MM') then date_key else '0000-00-00' end) as ads_last_cost_date_monthly --双月初之前的最后一次消耗时
      ,max(case when module in ('品牌','效果') then trunc('{{ds}}','MM')  end) as ads_monthly_first_date -- 双月初的日期
      ,sum(case when module in ('品牌','效果') and date_key <= '{{ds}}' and date_key >='{{ds_29_days_ago}}' then cash_income_amt else 0 end)/30 as avg_cash_cost_30d
    from 
    (select *
    from  redcdm.dws_ads_advertiser_product_income_detail_df_view 
    where 
      dtm = '{{ds_nodash}}'
      and date_key <= '{{ds}}' -- 去掉预定单的未来消耗数据
      and income_amt > 0
      )t1 
    left join 
    (select brand_account_id,track_industry_name,brand_group_tag_name,brand_user_name,coalesce(brand_group_tag_name,brand_user_name) as all_brand_group_tag_name
     from redcdm.dim_ads_industry_account_df 
     where dtm = '{{ ds_nodash }}'
     )t2 
    on t1.brand_user_id=t2.brand_account_id
    group by 1
    
),
-- 客户双月分层逻辑
account_active_level as (
  select
    all_brand_group_tag_name,
    
    case
      -- 本双月是否有消耗
      when ads_last_cost_date between ads_bimonthly_first_date and '{{ds}}' 
        then
          case
            -- 本双月之前没有消耗
            when coalesce(ads_last_cost_date_bimonthly,'0000-00-00') = '0000-00-00' then '双月新客(潜客激活)' 
            -- 本双月前停投是否超过180d(上双月末 - 本双月前的最后一次消耗时间)
            when datediff(date_sub(ads_bimonthly_first_date,1),ads_last_cost_date_bimonthly) between 0 and 180 then '双月持续活跃老客' 
            else '双月沉睡激活老客' 
          end
      else
        case
          -- 本双月前停投是否超过180d(上双月末 - 本双月前的最后一次消耗时间)
          when datediff(date_sub(ads_bimonthly_first_date,1),ads_last_cost_date_bimonthly) >= 180 then '双月持续沉睡老客'
          else 
            case
              -- 本双月前距离昨日停投是否超过180d(昨天 - 本双月前的最后一次消耗时间)
              when datediff('{{ds}}',ads_last_cost_date_bimonthly) >= 180 then '双月新增沉睡客户'
              else '双月沉睡风险客户'
            end
        end
    end as brand_active_level,
    case
      -- 本双月是否有消耗
      when ads_last_cost_date between ads_monthly_first_date and '{{ds}}' 
        then
          case
            -- 本双月之前没有消耗
            when coalesce(ads_last_cost_date_monthly,'0000-00-00') = '0000-00-00' then '双月新客(潜客激活)' 
            -- 本双月前停投是否超过180d(上双月末 - 本双月前的最后一次消耗时间)
            when datediff(date_sub(ads_monthly_first_date,1),ads_last_cost_date_monthly) between 0 and 180 then '双月持续活跃老客' 
            else '双月沉睡激活老客' 
          end
      else
        case
          -- 本双月前停投是否超过180d(上双月末 - 本双月前的最后一次消耗时间)
          when datediff(date_sub(ads_monthly_first_date,1),ads_last_cost_date_monthly) >= 180 then '双月持续沉睡老客'
          else 
            case
              -- 本双月前距离昨日停投是否超过180d(昨天 - 本双月前的最后一次消耗时间)
              when datediff('{{ds}}',ads_last_cost_date_monthly) >= 180 then '双月新增沉睡客户'
              else '双月沉睡风险客户'
            end
        end
    end as month_active_level,
    case when avg_cash_cost_30d>=30000 then 's' when avg_cash_cost_30d>=10000 then 'a' when avg_cash_cost_30d>=3000 then 'b' else 'c' end as active_cost_level
  from 
    account_info
  where
    ads_last_cost_date <> '0000-00-00' -- 历史有品牌、效果消耗的
   ) 
select t1.brand_account_id,
    t1.all_brand_group_tag_name,
    t1.brand_group_tag_name,
    t1.brand_user_name,
    track_industry_name as  first_track_industry_dept_name,
    cpc_direct_sales_dept3_name,
    brand_direct_sales_dept3_name,
    direct_sales_dept2_name,
    coalesce(brand_active_level,'投放潜客') as active_level,
    coalesce(month_active_level,'投放潜客') as month_active_level,
    coalesce(active_cost_level,'c') as active_cost_level,
    ads_last_cost_date -- 除去t-1的最后一次消耗时间
    ads_first_cost_date -- 首次消耗时间20230712添加
    ads_last_cost_date_bimonthly --双月初之前的最后一次消耗时
    ads_last_cost_date_monthly --双月初之前的最后一次消耗时
    avg_cash_cost_30d
from 
(select
  brand_account_id,
  coalesce(brand_group_tag_name, brand_user_name) as all_brand_group_tag_name,
  brand_group_tag_name,
  brand_user_name,
  track_industry_name,
  cpc_direct_sales_dept3_name,
  brand_direct_sales_dept3_name,
  coalesce(cpc_direct_sales_dept2_name,cpc_operator_dept2_name,if(company_name is null,'创作者商业化部','未挂接')) as direct_sales_dept2_name
from
  redcdm.dim_ads_industry_account_df
where
  dtm = '{{ds_nodash}}'
  and track_industry_name in ('生活服务')
  and coalesce(cpc_direct_sales_dept2_name,cpc_operator_dept2_name,if(company_name is null,'创作者商业化部','未挂接')) in ('行业团队')
  
)t1 
left join account_active_level t2 
on t1.all_brand_group_tag_name = t2.all_brand_group_tag_name
left join account_info t3 
on t1.all_brand_group_tag_name = t3.all_brand_group_tag_name




