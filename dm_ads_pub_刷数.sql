--set spark.sql.hive.manageFilesourcePartitions=false;
--品牌开屏和gd依赖纵向模型投放时间。

create table temp.temp_cmd_shuashu_{{ds_nodash}}_01 as 
select
  date_key,
  brand_account_id,
  module,
  product,
  marketing_target,
  optimize_target,
  market_target_type,
  '0' as is_marketing_product,
  imp_cnt,
  click_cnt,
  like_cnt,
  fav_cnt,
  cmt_cnt,
  follow_cnt,
  share_cnt,
  screenshot_cnt,
  image_save_cnt,
  0 as cash_income_amt,
  0 as income_amt,
  open_sale_num,
  0 as direct_cash_income_amt,
  0 as direct_income_amt,
  0 as channel_cash_income_amt,
  0 as channel_income_amt,
  campaign_cnt,
  unit_cnt,
  brand_campaign_cnt,
  rtb_cost_income_amt,
  rtb_budget_income_amt
from
  (
    select
      coalesce(detail.date_key_1, budget.date_key) as date_key,
      coalesce(
        detail.brand_account_id_1,
        budget.brand_account_id
      ) as brand_account_id,
      coalesce(detail.module_1, budget.module) as module,
      coalesce(detail.product_1, budget.product) as product,
      coalesce(if(detail.marketing_target_1=0,-911,detail.marketing_target_1),-911) as marketing_target,
      coalesce(if(detail.optimize_target_1=0,-911,detail.optimize_target_1), -911) as optimize_target,
      coalesce(detail.market_target_1, budget.market_target) as market_target_type,
      imp_cnt,
      click_cnt,
      like_cnt,
      fav_cnt,
      cmt_cnt,
      follow_cnt,
      share_cnt,
      screenshot_cnt,
      image_save_cnt,
      open_sale_num,
      campaign_cnt,
      unit_cnt,
      brand_campaign_cnt,
      rtb_cost_income_amt,
      rtb_budget_income_amt
    from
      (
        select
          base.*,
          coalesce(base.date_key, splash.date_key) as date_key_1,
          coalesce(base.module, splash.module) as module_1,
          coalesce(base.product, splash.product) as product_1,
          coalesce(base.brand_account_id, splash.brand_account_id) as brand_account_id_1,
          coalesce(base.market_target, splash.market_target) as market_target_1,
          coalesce(base.marketing_target, -911) as marketing_target_1,
          coalesce(base.optimize_target, -911) as optimize_target_1,
          open_sale_num
        from
          (
            select
              f_getdate(a.dtm) as date_key,
              a.module,
              a.product,
              a.brand_account_id,
              marketing_target,
              optimize_target,
              case
                when a.marketing_target in (3, 8,14,15) then '闭环电商'
                when a.marketing_target in (13) then '非闭环电商'
                when a.marketing_target in (2, 5, 9) then '线索'
                when a.marketing_target not in (3, 8, 2, 5, 9, 13,14,15) and module in ('效果') then '种草'
              when module in ('品牌','薯条') then '整体' else null end  as market_target,
              sum(imp_cnt) as imp_cnt,
              sum(case when a.product='视频内流' then coalesce(true_view_cnt,0) else click_cnt end) as click_cnt,
              -- sum(like_cnt) as like_cnt,
              -- sum(fav_cnt) as fav_cnt,
              -- sum(cmt_cnt) as cmt_cnt,
              -- sum(share_cnt) as share_cnt,
              -- sum(follow_cnt) as follow_cnt,
              -- sum(screenshot_cnt) as screenshot_cnt,
              -- sum(image_save_cnt) as image_save_cnt,
              0 as like_cnt,
              0 as fav_cnt,
              0 as cmt_cnt,
              0 as share_cnt,
              0 as follow_cnt,
              0 as screenshot_cnt,
              0 as image_save_cnt,
              count(distinct campaign_id) as campaign_cnt,
              count(distinct unit_id) as unit_cnt,
              max(coalesce(c.campaign_cnt, 0)) as brand_campaign_cnt
            from
              (select *
              from redcdm.dws_ads_log_creativity_1d_di
              where  dtm >= '20231101'
              and module in ('效果', '薯条', '品牌')
              and (coalesce(is_own_ads,0)=0 or is_own_ads=-1)
              ) a
              left join (
                select
                  creativity_id,
                  product,
                  dtm
                from
                  redcdm.dim_ads_brand_creativity_df
                where dtm >= '20231101'
                  and ((product = '信息流GD'
                  and case when n_reach_num > 0 then substr(launch_start_time,1,10) <= '{{ds}}' and substr(launch_end_time,1,10) >= '{{ds}}'
                    else substr(launch_date,1,10) = '{{ds}}'
                    end = true) 
                    or (product ='开屏' and substr(launch_date,1,10) = '{{ds}}')
                )
                group by 1,2,3
              ) b on a.creativity_id = b.creativity_id
              and a.product = b.product
              and a.dtm = b.dtm
              left join (
                -- select
                --   advertiser_id as brand_account_id
                --   ,count(distinct campaign_id) as campaign_cnt
                -- from 
                --   redst.st_ads_brand_creativity_loc_metrics_day_inc
                -- where
                --   dtm = '{{ds_nodash}}'
                -- group by
                --   1
                --中间层切换
                select t1.dtm,
                  t1.brand_account_id,
                  count(distinct t1.campaign_id) as campaign_cnt
                from
                  (
                    select dtm,
                      creativity_id,brand_account_id,campaign_id
                    from
                      redcdm.dim_ads_brand_creativity_df
                    where
                      dtm >= '20231101'
                      and (
                        (
                          product = '信息流GD'
                          and case
                            when n_reach_num > 0 then substr(launch_start_time, 1, 10) <= '{{ds}}'
                            and substr(launch_end_time, 1, 10) >= '{{ds}}'
                            else substr(launch_date, 1, 10) = '{{ds}}'
                          end = true
                        )
                        or (
                          product in ('开屏', '搜索第三位', '品牌专区')
                          and substr(launch_date, 1, 10) = '{{ds}}'
                        )
                        and campaign_id <> 0
                      )
                  ) t1
                  join (
                    select
                      creativity_id,
                      dtm
                    from
                      redcdm.dws_ads_log_creativity_1d_di
                    where
                      dtm >= '20231101'
                      and module in ('品牌')
                      
                  ) a on t1.creativity_id = a.creativity_id and  t1.dtm = a.dtm
                group by
                  1  ,2
              ) c on c.brand_account_id = a.brand_account_id and c.dtm=a.dtm
            where
             
            (
                (a.is_effective = 1 and a.module='效果')
                or a.module = '薯条'
                or (
                  a.module = '品牌'
                  and a.product in ('火焰话题', '品牌专区', '搜索第三位')
                )
                or (
                  a.module = '品牌'
                  and a.product in ('信息流GD', '开屏') --兜底逻辑后续纵向模型产出后调整
                  and b.creativity_id is not null
                )
                
              )
              
            group by  f_getdate(a.dtm),
              a.module,
              a.product,
              a.brand_account_id,
              marketing_target,
              optimize_target,
             case
                when a.marketing_target in (3, 8,14,15) then '闭环电商'
                when a.marketing_target in (13) then '非闭环电商'
                when a.marketing_target in (2, 5, 9) then '线索'
                when a.marketing_target not in (3, 8, 2, 5, 9, 13,14,15) and module in ('效果') then '种草'
              when module in ('品牌','薯条') then '整体' else null end
          ) base
          full outer join -- 开屏售卖轮次-品牌中间层
          (
            select
              f_getdate(dtm) as date_key,
              '品牌' as module,
              '开屏' as product,
              brand_account_id,
              '整体' as market_target,
              sum(cast(open_sale_num as double)) as open_sale_num
            from
              redcdm.dim_ads_brand_creativity_df a
            where
              a.dtm >=  '20231101'
              and open_sale_num > 0
              and is_internal = 0
            group by
              f_getdate(dtm),brand_account_id
          ) splash on splash.brand_account_id = base.brand_account_id
          and splash.module = base.module
          and splash.product = base.product
          and splash.market_target = base.market_target
          and splash.date_key = base.date_key
      ) detail
      full outer join --预算新模型
      (
        select
          f_getdate(dtm) as date_key,
          module,
          product,
          brand_account_id,
          '整体' as market_target,
          --ads_purpose字段就是marketing_type对应中文描述
          sum(cost_special_campaign) as rtb_cost_income_amt,
          sum(min_budget) as rtb_budget_income_amt
        from
          redcdm.dm_ads_rtb_budget_1d_di a
        where
          dtm >=  '20231101'
          and granularity = '分场域'
          and groups = 3
        group by 1,2,
          3,
          4,
          5
      ) budget on budget.brand_account_id = detail.brand_account_id_1
      and budget.module = detail.module_1
      and budget.product = detail.product_1
      and budget.market_target = detail.market_target_1
      and budget.date_key = detail.date_key_1
    union all
    -- 品合-社区流量
    select
      f_getdate(dtm) as date_key,
      report_brand_user_id as brand_account_id,
      '品合' as module,
      '品合' as product,
      -911 as marketing_target,
      -911 as optimize_target,
      '整体' as market_target,
      sum(imp_cnt) as imp_cnt,
      sum(click_cnt) as click_cnt,
      sum(like_cnt) as like_cnt,
      sum(fav_cnt) as fav_cnt,
      sum(cmt_cnt) as cmt_cnt,
      sum(follow_cnt) as follow_cnt,
      sum(share_cnt) as share_cnt,
      sum(screenshot_cnt) as screenshot_cnt,
      sum(image_save_cnt) as image_save_cnt,
      0 as open_sale_num,
      0 as campaign_cnt,
      0 as unit_cnt,
      0 as brand_campaign_cnt,
      0 as rtb_cost_income_amt,
      0 as rtb_budget_income_amt
    from
      redcdm.dws_ads_bcoo_note_engagement_di
    where dtm >= '20231101'
    group by
      1,2
  ) log_cvr;

insert overwrite table redcdm.dm_ads_pub_product_account_detail_td_df  partition(dtm = '{{ ds_nodash }}') 
select date_key,
  brand_account_id,
  module,
  product,
  marketing_target,
  optimize_target,
  market_target_type,
  is_marketing_product,
  sum(imp_cnt) as imp_cnt,
  sum(click_cnt) as click_cnt,
  sum(like_cnt) as like_cnt,
  sum(fav_cnt) as fav_cnt,
  sum(cmt_cnt) as cmt_cnt,
  sum(follow_cnt) as follow_cnt,
  sum(share_cnt) as share_cnt,
  sum(screenshot_cnt) as screenshot_cnt,
  sum(image_save_cnt) as image_save_cnt,
  sum(cash_income_amt) as cash_income_amt,
  sum(income_amt) as income_amt,
  sum(open_sale_num) as open_sale_num,
  sum(direct_cash_income_amt) as direct_cash_income_amt,
  sum(direct_income_amt ) as direct_income_amt,
  sum(channel_cash_income_amt ) as channel_cash_income_amt,
  sum(channel_income_amt) as channel_income_amt,
  sum(campaign_cnt) as campaign_cnt,
  sum(unit_cnt) as unit_cnt,
  sum(brand_campaign_cnt) as brand_campaign_cnt,
  sum(rtb_cost_income_amt) as rtb_cost_income_amt,
  sum(rtb_budget_income_amt) as rtb_budget_income_amt
from 
(select date_key,
  brand_account_id,
  module,
  product,
  marketing_target,
  optimize_target,
  market_target_type,
  is_marketing_product,
  imp_cnt,
  click_cnt,
  like_cnt,
  fav_cnt,
  cmt_cnt,
  follow_cnt,
  share_cnt,
  screenshot_cnt,
  image_save_cnt,
  cash_income_amt,
  income_amt,
  open_sale_num,
  direct_cash_income_amt,
  direct_income_amt,
  channel_cash_income_amt,
  channel_income_amt,
  campaign_cnt,
  unit_cnt,
  brand_campaign_cnt,
  rtb_cost_income_amt,
  rtb_budget_income_amt
from temp.temp_cmd_shuashu_{{ds_nodash}}_01
union all
--收入中间层
select
  date_key,
  brand_user_id as brand_account_id,
  module,
  product,
  if(marketing_target_id='',-911,marketing_target_id) as marketing_target,
  if(optimize_target_id='',-911,optimize_target_id)  as optimize_target,
  coalesce(if(marketing_target_type='',null,marketing_target_type), '整体') as market_target_type,
  coalesce(is_marketing_product, '0') as is_marketing_product,
  0 as imp_cnt,
  0 as click_cnt,
  0 as like_cnt,
  0 as fav_cnt,
  0 as cmt_cnt,
  0 as follow_cnt,
  0 as share_cnt,
  0 as screenshot_cnt,
  0 as image_save_cnt,
  sum(cash_income_amt) as cash_income_amt,
  sum(income_amt) income_amt,
  0 as open_sale_num,
  sum(
    case
      when coalesce(sales_system, '') <> '渠道业务部' then cash_income_amt
    end
  ) as direct_cash_cost,
  sum(
    case
      when coalesce(sales_system, '') <> '渠道业务部' then income_amt
    end
  ) as direct_cost,
  sum(
    case
      when coalesce(sales_system, '') = '渠道业务部' then cash_income_amt
    end
  ) as channel_cash_cost,
  sum(
    case
      when coalesce(sales_system, '') = '渠道业务部' then income_amt
    end
  ) as channel_cost,
  0 as campaign_cnt,
  0 as unit_cnt,
  0 as brand_campaign_cnt,
  0 as rtb_cost_income_amt,
  0 as rtb_budget_income_amt
from
  redcdm.dws_ads_advertiser_product_income_detail_df_view a
where
  a.dtm = '{{ds_nodash}}'
  and a.date_key <= '{{ds}}'
group by
  1,2,3,4,5,6,7,8
union all
--前一日全量流量数据
select
  date_key,
  brand_account_id,
  module,
  product,
  marketing_target,
  optimize_target,
  market_target_type,
  '0' as is_marketing_product,
  sum(imp_cnt) as imp_cnt,
  sum(click_cnt) as click_cnt,
  sum(like_cnt) as like_cnt,
  sum(fav_cnt) as fav_cnt,
  sum(cmt_cnt) as cmt_cnt,
  sum(follow_cnt) as follow_cnt,
  sum(share_cnt) as share_cnt,
  sum(screenshot_cnt) as screenshot_cnt,
  sum(image_save_cnt) as image_save_cnt,--互动7日回刷
  0 as cash_income_amt,
  0 as income_amt,
  sum(open_sale_num) as open_sale_num,
  0 as direct_cash_cost,
  0 as direct_cost,
  0 as channel_cash_cost,
  0 as channel_cost,
  sum(campaign_cnt) as campaign_cnt,
  sum(unit_cnt) as unit_cnt,
  max(brand_campaign_cnt) as brand_campaign_cnt,
  sum(rtb_cost_income_amt) as  rtb_cost_income_amt,
  sum(rtb_budget_income_amt) as rtb_budget_income_amt
from
  redcdm.dm_ads_pub_product_account_detail_td_df
where
  dtm = '{{yesterday_ds_nodash}}'
  and date_key<='2023-10-31'
group by 1,2,3,4,5,6,7,8

  )detail
  group by 1,2,3,4,5,6,7,8



  --20240704
  --20240704
  create table temp.temp_cmd_shuashu_{{ds_nodash}}_04 as 
select
  date_key,
  brand_account_id,
  module,
  product,
  marketing_target,
  optimize_target,
  market_target_type,
  '0' as is_marketing_product,
  imp_cnt,
  click_cnt,
  like_cnt,
  fav_cnt,
  cmt_cnt,
  follow_cnt,
  share_cnt,
  screenshot_cnt,
  image_save_cnt,
  0 as cash_income_amt,
  0 as income_amt,
  open_sale_num,
  0 as direct_cash_income_amt,
  0 as direct_income_amt,
  0 as channel_cash_income_amt,
  0 as channel_income_amt,
  campaign_cnt,
  unit_cnt,
  brand_campaign_cnt,
  rtb_cost_income_amt,
  rtb_budget_income_amt
from
  (
    select
      coalesce(detail.date_key_1, budget.date_key) as date_key,
      coalesce(
        detail.brand_account_id_1,
        budget.brand_account_id
      ) as brand_account_id,
      coalesce(detail.module_1, budget.module) as module,
      coalesce(detail.product_1, budget.product) as product,
      coalesce(if(detail.marketing_target_1=0,0,detail.marketing_target_1),-911) as marketing_target,--20231219之前优化目标为0的会归为-911.1220修改回0，点击量
      coalesce(if(detail.optimize_target_1=0,0,detail.optimize_target_1), -911) as optimize_target,
      coalesce(detail.market_target_1, budget.market_target) as market_target_type,
      imp_cnt,
      click_cnt,
      like_cnt,
      fav_cnt,
      cmt_cnt,
      follow_cnt,
      share_cnt,
      screenshot_cnt,
      image_save_cnt,
      open_sale_num,
      campaign_cnt,
      unit_cnt,
      brand_campaign_cnt,
      rtb_cost_income_amt,
      rtb_budget_income_amt
    from
      (
        select
          base.*,
          coalesce(base.date_key, splash.date_key) as date_key_1,
          coalesce(base.module, splash.module) as module_1,
          coalesce(base.product, splash.product) as product_1,
          coalesce(base.brand_account_id, splash.brand_account_id) as brand_account_id_1,
          coalesce(base.market_target, splash.market_target) as market_target_1,
          coalesce(base.marketing_target, -911) as marketing_target_1,
          coalesce(base.optimize_target, -911) as optimize_target_1,
          open_sale_num
        from
          (
            select
              f_getdate(a.dtm) as date_key,
              a.module,
              a.product,
              a.brand_account_id,
              marketing_target,
              optimize_target,
              case
                when a.marketing_target in (3, 8,14,15) then '闭环电商'
                when a.marketing_target in (13) then '非闭环电商'
                when a.marketing_target in (2, 5, 9) then '线索'
                 when a.marketing_target in (16) then '平台UG'
                when a.marketing_target not in (3, 8, 2, 5, 9, 13,14,15,16) and module in ('效果') then '种草'
              when module in ('品牌','薯条') then '整体' else null end  as market_target,
              sum(process_imp_cnt) as imp_cnt,
              sum(case when a.product='视频内流' then coalesce(true_view_cnt,0) else process_click_cnt end) as click_cnt,
              -- sum(like_cnt) as like_cnt,
              -- sum(fav_cnt) as fav_cnt,
              -- sum(cmt_cnt) as cmt_cnt,
              -- sum(share_cnt) as share_cnt,
              -- sum(follow_cnt) as follow_cnt,
              -- sum(screenshot_cnt) as screenshot_cnt,
              -- sum(image_save_cnt) as image_save_cnt,
              0 as like_cnt,
              0 as fav_cnt,
              0 as cmt_cnt,
              0 as share_cnt,
              0 as follow_cnt,
              0 as screenshot_cnt,
              0 as image_save_cnt,
              count(distinct campaign_id) as campaign_cnt,
              count(distinct unit_id) as unit_cnt,
              max(coalesce(c.campaign_cnt, 0)) as brand_campaign_cnt
            from
              (SELECT campaign_id,
                  unit_id,
                  module,
                  product,
                  creativity_id,
                  brand_account_id,
                  marketing_target,
                  optimize_target,
                  click_cnt,
                  imp_cnt,
                  unique_click_cnt,
                  unique_imp_cnt,
                  is_effective,
                  is_own_ads,
                  true_view_cnt,
                  dtm,
                  case when module in ('效果') then  unique_imp_cnt else imp_cnt end as process_imp_cnt,--竞价使用去重曝光数（按照track_id统计）
                  case when module in ('效果') then  unique_click_cnt else click_cnt end as process_click_cnt --竞价使用去重点击数（按照track_id统计）
              from redcdm.dws_ads_log_creativity_1d_di
              where  dtm >= '20240130' 
              and module in ('品牌')
              and (coalesce(is_own_ads,0)=0 or is_own_ads=-1)
              ) a
              left join (
                select
                  creativity_id,
                  product,
                  dtm
                from
                  redcdm.dim_ads_brand_creativity_df
                where dtm >= '20240130'
                  and ((product = '信息流GD'
                  and case when n_reach_num > 0 then substr(launch_start_time,1,10) <= f_getdate(dtm) and substr(launch_end_time,1,10) >= f_getdate(dtm)
                    else substr(launch_date,1,10) = f_getdate(dtm)
                    end = true) 
                    or (product ='开屏' and substr(launch_date,1,10) = f_getdate(dtm))
                )
                group by 1,2,3
              ) b on a.creativity_id = b.creativity_id
              and a.product = b.product
              and a.dtm = b.dtm
              left join (
                -- select
                --   advertiser_id as brand_account_id
                --   ,count(distinct campaign_id) as campaign_cnt
                -- from 
                --   redst.st_ads_brand_creativity_loc_metrics_day_inc
                -- where
                --   dtm = '{{ds_nodash}}'
                -- group by
                --   1
                --中间层切换
                select
                  t1.brand_account_id,
                  count(distinct t1.campaign_id) as campaign_cnt
                from
                  (
                    select
                      creativity_id,brand_account_id,campaign_id
                    from
                      redcdm.dim_ads_brand_creativity_df
                    where
                      dtm = '{{ds_nodash}}'
                      and (
                        (
                          product = '信息流GD'
                          and case
                            when n_reach_num > 0 then substr(launch_start_time, 1, 10) <= '{{ds}}'
                            and substr(launch_end_time, 1, 10) >= '{{ds}}'
                            else substr(launch_date, 1, 10) = '{{ds}}'
                          end = true
                        )
                        or (
                          product in ('开屏', '搜索第三位', '品牌专区')
                          and substr(launch_date, 1, 10) = '{{ds}}'
                        )
                        and campaign_id <> 0
                      )
                  ) t1
                  join (
                    select
                      creativity_id
                    from
                      redcdm.dws_ads_log_creativity_1d_di
                    where
                      dtm = '{{ds_nodash}}'
                      and module in ('品牌')
                      
                  ) a on t1.creativity_id = a.creativity_id
                group by
                  1  
              ) c on c.brand_account_id = a.brand_account_id
            where
             
            (
                (a.is_effective = 1 and a.module='效果')
                or a.module = '薯条'
                or (
                  a.module = '品牌'
                  and a.product in ('火焰话题', '品牌专区', '搜索第三位')
                )
                or (
                  a.module = '品牌'
                  and a.product in ('信息流GD', '开屏') --兜底逻辑后续纵向模型产出后调整
                  and b.creativity_id is not null
                )
                
              )
              
            group by f_getdate(a.dtm) ,
              a.module,
              a.product,
              a.brand_account_id,
              marketing_target,
              optimize_target,
             case
                when a.marketing_target in (3, 8,14,15) then '闭环电商'
                when a.marketing_target in (13) then '非闭环电商'
                when a.marketing_target in (2, 5, 9) then '线索'
                when a.marketing_target in (16) then '平台UG'
                when a.marketing_target not in (3, 8, 2, 5, 9, 13,14,15,16) and module in ('效果') then '种草'
              when module in ('品牌','薯条') then '整体' else null end
          ) base
          full outer join -- 开屏售卖轮次-品牌中间层
          (
            select
              '{{ds}}' as date_key,
              '品牌' as module,
              '开屏' as product,
              brand_account_id,
              '整体' as market_target,
              sum(cast(open_sale_num as double)) as open_sale_num
            from
              redcdm.dim_ads_brand_creativity_df a
            where
              a.dtm = '{{ds_nodash}}'
              and cast(open_sale_num as double) > 0
              and is_own_ads=0
            group by
              4
          ) splash on splash.brand_account_id = base.brand_account_id
          and splash.module = base.module
          and splash.product = base.product
          and splash.market_target = base.market_target
      ) detail
      full outer join --预算新模型
      (
        select
          '{{ds}}' as date_key,
          module,
          product,
          brand_account_id,
          '整体' as market_target,
          --ads_purpose字段就是marketing_type对应中文描述
          sum(cost_special_campaign) as rtb_cost_income_amt,
          sum(min_budget) as rtb_budget_income_amt
        from
          redcdm.dm_ads_rtb_budget_1d_di a
        where
          dtm = '{{ds_nodash}}'
          and granularity = '分场域'
          and groups = 3
        group by 2,
          3,
          4,
          5
      ) budget on budget.brand_account_id = detail.brand_account_id
      and budget.module = detail.module
      and budget.product = detail.product
      and budget.market_target = detail.market_target
    
  ) log_cvr
