select brand_id
    ,t1.brand_account_id
    ,t1.agent_user_id
    ,t1.agent_user_name
    ,channel_sales_name
from 
   --brand_id和brand_account_id映射关系
  (select
    brandz_id as brand_id
    ,brand_user_id as brand_account_id
    ,agent_user_id
    ,agent_user_name
  from
        (
            select
              brandz_id
              ,brand_user_id
              ,agent_user_id
              ,agent_user_name
              ,coalesce(cash_cost,0) as cash_cost
              ,row_number() over(partition by brandz_id order by cash_cost desc) as rk
            from
            (
              select
                brandz_id
                ,brand_user_id
              from redods.ods_ads_crm_crm_account_brandz_info
              -- where dtm = '{{ds_nodash}}'
              where dtm = greatest('20231009','{{ds_nodash}}')
              and state = 1
            ) a 
            left join
            (
              select
                brand_user_id as brand_account_id
                ,agent_user_id
                ,agent_user_name
                ,sum(cash_cost) as cash_cost
              from reddm.dm_ads_crm_advertiser_income_wide_day
              where dtm = greatest('20231009','{{ds_nodash}}')
              and module in ('品牌','效果','薯条')
              
              and date_key > f_getdate('{{ds}}',-30) and date_key <= '{{ds}}'  --近30日流水最大
              group by 1,2,3
            ) b
            on a.brand_user_id = b.brand_account_id
            
          
        ) a 
    where rk = 1
    )t1
  left join 
  (select date_key,agent_user_id,brand_user_id,concat_ws(',',collect_set(name)) as channel_sales_name
  from reddm.dm_ads_crm_advertiser_income_wide_day t1
  left join 
  (select red_name,
   name
  from
    (select red_name,
      concat(red_name, '(', true_name, ')') as name,
      row_number()over(partition by  red_name order by create_time desc) as rn --兜底历史脏数据56条署名重复
    from redods.ods_ads_crm_ads_crm_user 
    where dtm=greatest('20231009','{{ds_nodash}}')
    )a 
    where rn=1
  )t5 
  on t5.red_name = t1.channel_sales_name
  where t1.dtm=greatest('20231009','{{ds_nodash}}') and date_key='{{ds}}'
  group by  date_key,agent_user_id,brand_user_id
  )t2 
  on t1.brand_account_id=t2.brand_user_id and t1.agent_user_id = t2.agent_user_id
    




select brand_id
    ,t1.brand_user_id
    ,t1.agent_user_id
    ,t1.agent_user_name
    ,channel_sales_name
from 
     -- 品牌组织架构信息
  (select 
    brand_id
    ,brand_user_id
    ,agent_user_id
    ,agent_user_name
  from (
    select
       brand_id
      ,brand_user_id
      ,agent_user_id
      ,agent_user_name
      ,coalesce(cash_cost,0) as cash_cost
      ,row_number() over(partition by brand_id order by cash_cost desc) as rk
    from (
      select
        brand_id
        ,brand_account_user_id as brand_user_id
      from 
        redapp.app_ads_commercial_brand_relation_df
      where 
        dtm = greatest('{{ds_nodash}}','20230910')
    ) a 
    left join (
      select
        brand_user_id as brand_account_id
        ,agent_user_id
        ,agent_user_name
        ,sum(cash_cost) as cash_cost
      from 
        reddm.dm_ads_crm_advertiser_income_wide_day
      where 
        dtm = greatest('{{ds_nodash}}','20230910')
        and module in ('品牌','效果','薯条')
        and date_key > f_getdate('{{ds}}',-30) --近30日流水最大
        and date_key <= '{{ds}}'
      group by 1,2,3
    ) b
    on 
      a.brand_user_id = b.brand_account_id
  ) a
  where
    a.rk = 1
    )t1
  left join 
  (select date_key,agent_user_id,brand_user_id,concat_ws(',',collect_set(name)) as channel_sales_name
  from reddm.dm_ads_crm_advertiser_income_wide_day t1
  left join 
  (select red_name,
   name
  from
    (select red_name,
      concat(red_name, '(', true_name, ')') as name,
      row_number()over(partition by  red_name order by create_time desc) as rn --兜底历史脏数据56条署名重复
    from redods.ods_ads_crm_ads_crm_user 
    where dtm=greatest('20231009','{{ds_nodash}}')
    )a 
    where rn=1
  )t5 
  on t5.red_name = t1.channel_sales_name
  where t1.dtm=greatest('20231009','{{ds_nodash}}') and date_key='{{ds}}'
  group by  date_key,agent_user_id,brand_user_id
  )t2 
  on t1.brand_user_id=t2.brand_user_id and t1.agent_user_id = t2.agent_user_id
