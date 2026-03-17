-->>KOS
   select
    brand_account_id
  ,'效果' as module
  ,case when note_type='' then 'KOS' ELSE '带货笔记' END AS product
    ,sum(case when dtm='{{ ds_nodash }}' then cash_cost else 0 end) as cash_cost
    ,sum(case when dtm<='{{ ds_nodash }}' and dtm>=regexp_replace(if(month('{{ds}}') & 1 = 1,trunc('{{ds}}','MM'),trunc(add_months('{{ds}}',-1),'MM')),'-','') then cash_cost else 0 end) as cash_cost_2m
    ,sum(case when dtm<=regexp_replace(add_months('{{ds}}',-2),'-','') and dtm>=regexp_replace(if(month(add_months('{{ds}}',-2)) & 1 = 1,trunc(add_months('{{ds}}',-2),'MM'),trunc(add_months(add_months('{{ds}}',-2),-1),'MM')),'-','') then cash_cost else 0 end) as last_cash_cost_2m
    ,sum(case when dtm<=regexp_replace(add_months('{{ds}}',-12),'-','') and dtm>=regexp_replace(if(month(add_months('{{ds}}',-12)) & 1 = 1,trunc(add_months('{{ds}}',-12),'MM'),trunc(add_months(add_months('{{ds}}',-12),-1),'MM')),'-','') then cash_cost else 0 end) as last_cash_cost_2m
    from redapp.app_ads_bid_engage_cube_di
    where ((dtm<='{{ ds_nodash }}' and dtm>=regexp_replace(trunc(add_months('{{ds}}',-3),'MM'),'-','')) 
    or (dtm<=regexp_replace(add_months('{{ds}}',-12),'-','') and dtm>=regexp_replace(if(month(add_months('{{ds}}',-12)) & 1 = 1,trunc(add_months('{{ds}}',-12),'MM'),trunc(add_months(add_months('{{ds}}',-12),-1),'MM')),'-',''))
    )
    and grain=2 and note_type in ('KOS','CPS')
    group by brand_account_id,
    case when note_type='' then 'KOS' ELSE '带货笔记' END
union all
-->>直播投流

    select
    brand_account_id
  ,'效果' as module
  ,'直播投流' as product
    ,sum(case when dtm='{{ ds_nodash }}' then cash_cost else 0 end) as cash_cost
    ,sum(case when dtm<='{{ ds_nodash }}' and dtm>=regexp_replace(if(month('{{ds}}') & 1 = 1,trunc('{{ds}}','MM'),trunc(add_months('{{ds}}',-1),'MM')),'-','') then cash_cost else 0 end) as cash_cost_2m
    ,sum(case when dtm<=regexp_replace(add_months('{{ds}}',-2),'-','') and dtm>=regexp_replace(if(month(add_months('{{ds}}',-2)) & 1 = 1,trunc(add_months('{{ds}}',-2),'MM'),trunc(add_months(add_months('{{ds}}',-2),-1),'MM')),'-','') then cash_cost else 0 end) as last_cash_cost_2m
    ,sum(case when dtm<=regexp_replace(add_months('{{ds}}',-12),'-','') and dtm>=regexp_replace(if(month(add_months('{{ds}}',-12)) & 1 = 1,trunc(add_months('{{ds}}',-12),'MM'),trunc(add_months(add_months('{{ds}}',-12),-1),'MM')),'-','') then cash_cost else 0 end) as last_cash_cost_2m
    from  redst.st_ads_wide_cpc_creativity_day_inc
    where 
    ((dtm<='{{ ds_nodash }}' and dtm>=regexp_replace(trunc(add_months('{{ds}}',-3),'MM'),'-','')) 
    or (dtm<=regexp_replace(add_months('{{ds}}',-12),'-','') and dtm>=regexp_replace(if(month(add_months('{{ds}}',-12)) & 1 = 1,trunc(add_months('{{ds}}',-12),'MM'),trunc(add_months(add_months('{{ds}}',-12),-1),'MM')),'-',''))
    )
    and marketing_target=8
    and (
        is_effective = 1
        or cost > 0
      )
    group by brand_account_id


union all
--视频流加粉

    select
    brand_account_id
    ,'效果' as module
  , case when product='视频内流' then '视频流加粉' else product end as product
    ,sum(case when dtm='{{ ds_nodash }}' then cash_income_amt else 0 end) as cash_cost
    ,sum(case when dtm<='{{ ds_nodash }}' and dtm>=regexp_replace(if(month('{{ds}}') & 1 = 1,trunc('{{ds}}','MM'),trunc(add_months('{{ds}}',-1),'MM')),'-','') then cash_income_amt else 0 end) as cash_cost_2m
    ,sum(case when dtm<=regexp_replace(add_months('{{ds}}',-2),'-','') and dtm>=regexp_replace(if(month(add_months('{{ds}}',-2)) & 1 = 1,trunc(add_months('{{ds}}',-2),'MM'),trunc(add_months(add_months('{{ds}}',-2),-1),'MM')),'-','') then cash_income_amt else 0 end) as last_cash_cost_2m
    ,sum(case when dtm<=regexp_replace(add_months('{{ds}}',-12),'-','') and dtm>=regexp_replace(if(month(add_months('{{ds}}',-12)) & 1 = 1,trunc(add_months('{{ds}}',-12),'MM'),trunc(add_months(add_months('{{ds}}',-12),-1),'MM')),'-','') then cash_income_amt else 0 end) as last_cash_cost_2m
    from redapp.app_ads_industry_rtb_creativity_di
    where ((dtm<='{{ ds_nodash }}' and dtm>=regexp_replace(trunc(add_months('{{ds}}',-3),'MM'),'-','')) 
    or (dtm<=regexp_replace(add_months('{{ds}}',-12),'-','') and dtm>=regexp_replace(if(month(add_months('{{ds}}',-12)) & 1 = 1,trunc(add_months('{{ds}}',-12),'MM'),trunc(add_months(add_months('{{ds}}',-12),-1),'MM')),'-',''))
    )
    and (
    (product='视频内流'
    and marketing_target not  in(3,8,13,2,5,9) 
    ) or product ='搜索')
    group by brand_account_id
    ,case when product='视频内流' then '视频流加粉' else product end

union all
-->>外链

    select
    brand_account_id
    ,module--外链中有部分品牌
    ,tag as product
    ,sum(case when dtm='{{ ds_nodash }}' then cash_cost else 0 end) as cash_cost
    ,sum(case when dtm<='{{ ds_nodash }}' and dtm>=regexp_replace(if(month('{{ds}}') & 1 = 1,trunc('{{ds}}','MM'),trunc(add_months('{{ds}}',-1),'MM')),'-','') then cash_cost else 0 end) as cash_cost_2m
    ,sum(case when dtm<=regexp_replace(add_months('{{ds}}',-2),'-','') and dtm>=regexp_replace(if(month(add_months('{{ds}}',-2)) & 1 = 1,trunc(add_months('{{ds}}',-2),'MM'),trunc(add_months(add_months('{{ds}}',-2),-1),'MM')),'-','') then cash_cost else 0 end) as last_cash_cost_2m
    ,sum(case when dtm<=regexp_replace(add_months('{{ds}}',-12),'-','') and dtm>=regexp_replace(if(month(add_months('{{ds}}',-12)) & 1 = 1,trunc(add_months('{{ds}}',-12),'MM'),trunc(add_months(add_months('{{ds}}',-12),-1),'MM')),'-','') then cash_cost else 0 end) as last_cash_cost_2m
    
    from redapp.app_ads_industry_cid_account_di
    where ((dtm<='{{ ds_nodash }}' and dtm>=regexp_replace(trunc(add_months('{{ds}}',-3),'MM'),'-','')) 
    or (dtm<=regexp_replace(add_months('{{ds}}',-12),'-','') and dtm>=regexp_replace(if(month(add_months('{{ds}}',-12)) & 1 = 1,trunc(add_months('{{ds}}',-12),'MM'),trunc(add_months(add_months('{{ds}}',-12),-1),'MM')),'-',''))
    )
    and tag in ('外链','CID')
    group by brand_account_id
    ,module--外链中有部分品牌
    ,tag

union all


--私信通一期

    
select t1.brand_account_id
    ,'效果' as module
    ,'私信通一期' as product
     ,sum(case when t1.dtm='{{ ds_nodash }}' then cash_cost else 0 end) as cash_cost
    ,sum(case when t1.dtm<='{{ ds_nodash }}' and t1.dtm>=regexp_replace(if(month('{{ds}}') & 1 = 1,trunc('{{ds}}','MM'),trunc(add_months('{{ds}}',-1),'MM')),'-','') then cash_cost else 0 end) as cash_cost_2m
    ,sum(case when t1.dtm<=regexp_replace(add_months('{{ds}}',-2),'-','') and t1.dtm>=regexp_replace(if(month(add_months('{{ds}}',-2)) & 1 = 1,trunc(add_months('{{ds}}',-2),'MM'),trunc(add_months(add_months('{{ds}}',-2),-1),'MM')),'-','') then cash_cost else 0 end) as last_cash_cost_2m
    ,sum(case when t1.dtm<=regexp_replace(add_months('{{ds}}',-12),'-','') and t1.dtm>=regexp_replace(if(month(add_months('{{ds}}',-12)) & 1 = 1,trunc(add_months('{{ds}}',-12),'MM'),trunc(add_months(add_months('{{ds}}',-12),-1),'MM')),'-','') then cash_cost else 0 end) as last_cash_cost_2m
    
from
    (
    select 
    dtm
    ,brand_account_id
    ,sum(cash_cost) as cash_cost
    from redst.st_ads_wide_cpc_creativity_day_inc
    where dtm>='20230706'
    and is_effective = 1
    and optimize_target in (5,9,13,50)
    group by 1,2
    ) t1 
    join 
    (
    select dtm, item as brand_account_id 
    from bi_ads.sxt_brand_account_common_white_list_df
    where dtm>='20230706'
    and type = 'business_seller' 
    and status = 1
    group by 1,2
    ) t2 
    on t1.dtm = t2.dtm
    and t1.brand_account_id = t2.brand_account_id
     join 
    (
    select dtm, brand_account_id
        from bi_ads.sxt_white_list_config_di
    where dtm>='20230706'
    and   ( is_welcome > 0 or is_question > 0 or (is_reservation + is_call + is_service + is_activity + is_package + is_learn_more) > 0 )
    group by dtm, brand_account_id
    ) t3 
    on t1.dtm = t3.dtm
    and t1.brand_account_id = t3.brand_account_id
    
    group by 1
union all
--私信通二期


    select
    
    t1.brand_account_id
    ,'效果' as module
    ,'私信通二期' as product
   
    ,sum(case when t1.dtm='{{ ds_nodash }}' and  t6.first_date <= t1.datekey then cash_cost else 0 end) as cash_cost
    ,sum(case when t6.first_date <= t1.datekey and t1.dtm<='{{ ds_nodash }}' and t1.dtm>=regexp_replace(if(month('{{ds}}') & 1 = 1,trunc('{{ds}}','MM'),trunc(add_months('{{ds}}',-1),'MM')),'-','') then cash_cost else 0 end) as cash_cost_2m
    ,sum(case when t6.first_date <= t1.datekey and t1.dtm<=regexp_replace(add_months('{{ds}}',-2),'-','') and t1.dtm>=regexp_replace(if(month(add_months('{{ds}}',-2)) & 1 = 1,trunc(add_months('{{ds}}',-2),'MM'),trunc(add_months(add_months('{{ds}}',-2),-1),'MM')),'-','') then cash_cost else 0 end) as last_cash_cost_2m
    ,sum(case when t6.first_date <= t1.datekey and t1.dtm<=regexp_replace(add_months('{{ds}}',-12),'-','') and t1.dtm>=regexp_replace(if(month(add_months('{{ds}}',-12)) & 1 = 1,trunc(add_months('{{ds}}',-12),'MM'),trunc(add_months(add_months('{{ds}}',-12),-1),'MM')),'-','') then cash_cost else 0 end) as last_cash_cost_2m
    
    from
    (
    select dtm, datekey,brand_account_id, max(ad_trade_type_1st_category) as ad_trade_type_1st_category, max(ad_trade_type_2nd_category) as ad_trade_type_2nd_category,
    sum(cash_cost ) as cash_cost

    from redst.st_ads_wide_cpc_creativity_day_inc
    where dtm>='20230706'
    and is_effective = 1
    and optimize_target in (5,9,13,50) 
    group by 1,2,3
    ) t1 


     join 
    (
    select dtm, item as brand_account_id 
    from bi_ads.sxt_brand_account_common_white_list_df
    where dtm>='20230706'
    and type = 'pro_octopus_session_menu'
    and status = 1
    group by 1,2
    ) t2 
    on t1.dtm = t2.dtm
    and t1.brand_account_id = t2.brand_account_id
    left join
    (
    select a.brand_account_id, least(coalesce(first_date,'9999-01-01'),coalesce(min_csa_date,'9999-01-01')) as first_date
    from (
    select brand_account_id, first_date
    from bi_ads.sxt_csa_config_tracking_df
    where dtm =  greatest('{{ds_nodash}}', '20231023')
    group by 1,2
    ) a 
    left join 
    (
    select brand_account_id, min(datekey) as min_csa_date
    from bi_ads.sxt_csa_config_tracking_df
    where dtm= greatest('{{ds_nodash}}', '20231023')
    group by 1
    ) b 
    on a.brand_account_id = b.brand_account_id
    ) t6
    on t1.brand_account_id = t6.brand_account_id
    group by 1