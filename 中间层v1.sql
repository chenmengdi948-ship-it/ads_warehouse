--set spark.sql.hive.manageFilesourcePartitions=false;
--中间层验数中品牌-开屏和信息流gd不一致，依赖纵向模型搭建，先使用兜底逻辑，后续替换。
with
  feed_gd_spalsh as (
    select
      creativity_id,
      '信息流GD' as product
    from
      (
        select
          b.id as creativity_id,
          b.unique_id,
          c.advertiser_id as brand_account_id,
          a.id as unit_id,
          a.campaign_id,
          from_unixtime(floor(a.launch_date / 1000 + 28800)) as launch_date,
          from_unixtime(floor(a.create_time / 1000 + 28800)) as create_time,
          from_unixtime(floor(a.modify_time / 1000 + 28800)) as modify_time,
          from_unixtime(floor(a.reserve_time / 1000 + 28800)) as reserve_time,
          case
            when a.across_day_info = ''
            or a.across_day_info is null then 0
            else cast(
              get_json_object(a.across_day_info, '$.nreach') as int
            )
          end as n_reach_num,
          b.creativity_relation_type as related_creativity_type,
          b.related_creativity_id,
          from_unixtime(floor(b.start_time / 1000 + 28800)) as creativity_start_time,
          from_unixtime(floor(b.expire_time / 1000 + 28800)) as creativity_expire_time,
          from_unixtime(floor(a.launch_start_time / 1000 + 28800)) as unit_launch_start_time,
          from_unixtime(floor(a.launch_end_time / 1000 + 28800)) as unit_launch_end_time
        from
          redods.ods_shequ_feed_ads_t_ads_cpd_unit a
          join redcdm.dwd_ads_rtb_creativity_df b on a.id = b.unit_id
          and b.dtm = '{{ds_nodash}}'
          and b.material_type not in (100, 101, 102) -- 普通、互动、特色
          join redods.ods_shequ_feed_ads_t_ads_campaign c on a.campaign_id = c.id
          and c.dtm = '{{ds_nodash}}'
        where
          a.dtm = '{{ds_nodash}}'
      ) a
      left outer join (
        select
          id
        from
          redods.ods_shequ_feed_ads_t_ads_cpd_unit
        where
          dtm = greatest('{{ds_nodash}}', '20220513')
          and case
            when across_day_info <> ''
            and cast(get_json_object(across_day_info, '$.nreach') as int) > 0 then substr(
              from_unixtime(floor(launch_start_time / 1000 + 28800)),
              1,
              10
            ) <= '{{ds}}'
            and substr(
              from_unixtime(floor(launch_end_time / 1000 + 28800)),
              1,
              10
            ) >= '{{ds}}'
            else substr(
              from_unixtime(floor(launch_date / 1000 + 28800)),
              1,
              10
            ) = '{{ds}}'
          end = true
      ) b on b.id = a.unit_id
    where
      (
        (
          n_reach_num > 0
          and related_creativity_type = 1
          and substr(creativity_start_time, 1, 10) <= '{{ds}}'
          and substr(creativity_expire_time, 1, 10) >= '{{ds}}'
        )
        or n_reach_num <= 0
      )
      and b.id is not null  
    group by
      1
    union all
    select
      creativity_id,
      '开屏' as product
    from
      redcdm.dim_ads_creativity_core_df
    where
      dtm = '{{ds_nodash}}'
      and product = '开屏'
      and substr(launch_date, 1, 10) = '{{ds}}'
    group by
      creativity_id,
      product
  ),
log_cvr_di as --流量转化中间层
(select
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
              '{{ds}}' as date_key,
              a.module,
              a.product,
              a.brand_account_id,
              marketing_target,
              optimize_target,
              case
                when a.marketing_target in (3, 8) then '闭环电商'
                when a.marketing_target in (13) then '非闭环电商'
                when a.marketing_target in (2, 5, 9) then '线索'
                when a.marketing_target not in (3, 8, 2, 5, 9, 13) and module in ('效果') then '种草'
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
              from redcdm.dws_ads_log_creativity_cube_1d_di
              where  dtm = '{{ds_nodash}}'
              and module in ('效果', '薯条', '品牌')
              and cube_type = '创意' 
              and coalesce(is_own_ads,0)=0
              ) a
              left join feed_gd_spalsh b on a.creativity_id = b.creativity_id
              and a.product = b.product
              left join (
                select
                  brand_account_id,
                  count(distinct campaign_id) as campaign_cnt
                from
                  redcdm.dws_ads_log_creativity_cube_1d_di --流量转化中间层
                where
                  dtm = '{{ds_nodash}}'
                  and module in ('品牌')
                  and product in ('火焰话题', '品牌专区', '搜索第三位', '信息流GD', '开屏')
                  and cube_type = '创意' 
                group by
                  brand_account_id
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
              
            group by
              a.module,
              a.product,
              a.brand_account_id,
              marketing_target,
              optimize_target,
             case
                when a.marketing_target in (3, 8) then '闭环电商'
                when a.marketing_target in (13) then '非闭环电商'
                when a.marketing_target in (2, 5, 9) then '线索'
                when a.marketing_target not in (3, 8, 2, 5, 9, 13) and module in ('效果') then '种草'
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
              and open_sale_num > 0
              and is_internal = 0
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
    union all
    -- 品合-社区流量
    select
      '{{ds}}' as date_key,
      report_brand_user_id as brand_account_id,
      '品合' as module,
      '品合' as product,
      -911 as marketing_target,
      -911 as optimize_target,
      '整体' as market_target,
      sum(b.imp_num) as imp_cnt,
      sum(b.click_num) as click_cnt,
      sum(b.like_num) as like_cnt,
      sum(b.fav_num) as fav_cnt,
      sum(b.cmt_num) as cmt_cnt,
      sum(b.follow_from_discovery_num) as follow_cnt,
      sum(b.share_num) as share_cnt,
      0 as screenshot_cnt,
      0 as image_save_cnt,
      0 as open_sale_num,
      0 as campaign_cnt,
      0 as unit_cnt,
      0 as brand_campaign_cnt,
      0 as rtb_cost_income_amt,
      0 as rtb_budget_income_amt
    from
      redcdm.dwd_ads_bcoo_ord_note_df a
      left join reddm.dm_soc_discovery_engagement_new_day_inc b on b.dtm = '{{ds_nodash}}'
      and a.note_id = b.discovery_id
    where
      a.dtm = '{{ds_nodash}}'
    group by
      2
  ) log_cvr
)
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
from log_cvr_di
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
  case when product ='竞价-搜索' then '搜索' 
  when product ='竞价-信息流' then '信息流' 
  when product ='竞价-视频内流' then '视频内流' 
  when product ='效果-其他' then '效果其他' 
  when product ='竞价-CPM' then '竞价CPM' else product end as product,
  -911  as marketing_target,
  -911 as optimize_target,
  case when module ='效果' then market_target else coalesce(market_target,'整体') end as market_target_type,
  '0' as is_marketing_product,
  sum(imp_cnt) as imp_cnt,
  sum(click_cnt) as click_cnt,
  sum(like_cnt) as like_cnt,
  sum(fav_cnt) as fav_cnt,
  sum(cmt_cnt) as cmt_cnt,
  sum(follow_cnt) as follow_cnt,
  sum(share_cnt) as share_cnt,
  0 as screenshot_cnt,
  0 as image_save_cnt,
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
  sum(cpc_cost_budget_rate) as  rtb_cost_income_amt,
  sum(cpc_budget) as rtb_budget_income_amt
 
from
  redcdm.dm_ads_industry_product_account_td_df
where
  dtm = '{{yesterday_ds_nodash}}'
group by 1,2,3,4,5,6,7,8
  )detail
  group by 1,2,3,4,5,6,7,8















--------------------
insert overwrite table redcdm_dev.dm_ads_industry_product_account_td_df partition(dtm = '{{ ds_nodash }}')
select 
   date_key
  ,module
  ,product
  ,brand_account_id
  ,sum(imp_cnt) as imp_cnt
  ,sum(click_cnt) as click_cnt
  ,sum(like_cnt) as like_cnt
  ,sum(fav_cnt) as fav_cnt
  ,sum(cmt_cnt) as cmt_cnt
  ,sum(share_cnt) as share_cnt
  ,sum(follow_cnt) as follow_cnt
  ,sum(cash_cost) as cash_cost
  ,sum(cost) as cost
  ,sum(mkt_ecm_cost) as mkt_ecm_cost
  ,sum(mkt_leads_cost) as mkt_leads_cost
  ,sum(mkt_zc_cost) as mkt_zc_cost
  ,sum(open_sale_num) as open_sale_num
  ,sum(direct_cash_cost) as direct_cash_cost
  ,sum(direct_cost) as direct_cost
  ,sum(channel_cash_cost) as channel_cash_cost
  ,sum(channel_cost) as channel_cost
  ,sum(campaign_cnt) as campaign_cnt
  ,sum(unit_cnt) as unit_cnt
  ,sum(brand_campaign_cnt) as brand_campaign_cnt
  ,sum(cpc_cost_budget_rate) as cpc_cost_budget_rate
  ,sum(cpc_budget) as cpc_budget
  ,sum(mkt_ecm_cash_cost) as mkt_ecm_cash_cost
  ,sum(mkt_leads_cash_cost) as mkt_leads_cash_cost
  ,sum(mkt_zc_cash_cost) as mkt_zc_cash_cost
  ,sum(mkt_ecm_direct_cost) as mkt_ecm_direct_cost
  ,sum(mkt_leads_direct_cost) as mkt_leads_direct_cost
  ,sum(mkt_zc_direct_cost) as mkt_zc_direct_cost
  ,sum(mkt_ecm_direct_cash_cost) as mkt_ecm_direct_cash_cost
  ,sum(mkt_leads_direct_cash_cost) as mkt_leads_direct_cash_cost
  ,sum(mkt_zc_direct_cash_cost) as mkt_zc_direct_cash_cost
  ,sum(mkt_ecm_channel_cost) as mkt_ecm_channel_cost
  ,sum(mkt_leads_channel_cost) as mkt_leads_channel_cost
  ,sum(mkt_zc_channel_cost) as mkt_zc_channel_cost
  ,sum(mkt_ecm_channel_cash_cost) as mkt_ecm_channel_cash_cost
  ,sum(mkt_leads_channel_cash_cost) as mkt_leads_channel_cash_cost
  ,sum(mkt_zc_channel_cash_cost) as mkt_zc_channel_cash_cost
  ,sum(mkt_ecm_unclosed_cost) as mkt_ecm_unclosed_cost
  ,sum(mkt_ecm_unclosed_cash_cost) as mkt_ecm_unclosed_cash_cost
  ,sum(mkt_ecm_unclosed_direct_cost) as mkt_ecm_unclosed_direct_cost
  ,sum(mkt_ecm_unclosed_direct_cash_cost) as mkt_ecm_unclosed_direct_cash_cost
  ,sum(mkt_ecm_unclosed_channel_cost) as mkt_ecm_unclosed_channel_cost
  ,sum(mkt_ecm_unclosed_channel_cash_cost) as mkt_ecm_unclosed_channel_cash_cost
  ,market_target
  ,is_marketing_product
from ( 
    
    select
       date_key
      ,module
      ,case
        when module in ('品合' '内容加热') then '品合'
        when module = '薯条' then '薯条'
        when product='火焰话题'  then '品牌其他'
        when product='信息流' then '竞价-信息流'
        when product='搜索' then '竞价-搜索'
        when product='视频内流' then '竞价-视频内流'
        else product
      end as product
      ,brand_account_id
      ,coalesce(market_target_type,'整体') as market_target
      ,coalesce(is_marketing_product,'0') as is_marketing_product
      ,sum(imp_cnt) as imp_cnt
      ,sum(click_cnt) as click_cnt
      ,sum(like_cnt) as like_cnt
      ,sum(fav_cnt) as fav_cnt
      ,sum(cmt_cnt) as cmt_cnt
      ,sum(share_cnt) as share_cnt
      ,sum(follow_cnt) as follow_cnt
      ,sum(cash_income_amt) as cash_cost
      ,sum(income_amt) as cost
      ,sum(case when market_target_type = '闭环电商' then income_amt end) as mkt_ecm_cost
      ,sum(case when market_target_type = '线索' then income_amt end) as mkt_leads_cost
      ,sum(case when market_target_type = '种草' then income_amt end) as mkt_zc_cost
      ,sum(open_sale_num) as open_sale_num
      ,sum(direct_cash_income_amt  ) as direct_cash_cost
      ,sum(direct_income_amt ) as direct_cost
      ,sum(channel_cash_income_amt ) as channel_cash_cost
      ,sum(channel_income_amt) as channel_cost
      ,sum(campaign_cnt) as campaign_cnt
      ,sum(unit_cnt) as unit_cnt
      ,sum(brand_campaign_cnt) as brand_campaign_cnt
      ,sum(rtb_cost_income_amt) as cpc_cost_budget_rate
      ,sum(rtb_budget_income_amt) as cpc_budget
      ,sum(case when market_target_type = '闭环电商' then cash_income_amt end) as mkt_ecm_cash_cost
      ,sum(case when market_target_type = '线索' then cash_income_amt end) as mkt_leads_cash_cost
      ,sum(case when market_target_type = '种草' then cash_income_amt end) as mkt_zc_cash_cost
      ,sum(case when  market_target_type = '闭环电商' then direct_income_amt end) as mkt_ecm_direct_cost
      ,sum(case when  market_target_type = '线索' then direct_income_amt end) as mkt_leads_direct_cost
      ,sum(case when  market_target_type = '种草' then direct_income_amt end) as mkt_zc_direct_cost
      ,sum(case when  market_target_type = '闭环电商' then direct_cash_income_amt end) as mkt_ecm_direct_cash_cost
      ,sum(case when  market_target_type = '线索' then direct_cash_income_amt end) as mkt_leads_direct_cash_cost
      ,sum(case when  market_target_type = '种草' then direct_cash_income_amt end) as mkt_zc_direct_cash_cost
      ,sum(case when  market_target_type = '闭环电商' then channel_income_amt end) as mkt_ecm_channel_cost
      ,sum(case when  market_target_type = '线索' then channel_income_amt end) as mkt_leads_channel_cost
      ,sum(case when  market_target_type = '种草' then channel_income_amt end) as mkt_zc_channel_cost
      ,sum(case when  market_target_type = '闭环电商' then channel_cash_income_amt end) as mkt_ecm_channel_cash_cost
      ,sum(case when  market_target_type = '线索' then channel_cash_income_amt end) as mkt_leads_channel_cash_cost
      ,sum(case when  market_target_type = '种草' then channel_cash_income_amt end) as mkt_zc_channel_cash_cost
       --20230508新增非闭环电商字段
      ,sum(case when market_target_type = '非闭环电商' then income_amt end) as mkt_ecm_unclosed_cost
      ,sum(case when market_target_type = '非闭环电商' then cash_income_amt end) as mkt_ecm_unclosed_cash_cost
      ,sum(case when market_target_type = '非闭环电商' then direct_income_amt end) as mkt_ecm_unclosed_direct_cost
      ,sum(case when market_target_type = '非闭环电商' then direct_cash_income_amt end) as mkt_ecm_unclosed_direct_cash_cost
      ,sum(case when market_target_type = '非闭环电商' then channel_income_amt end) as mkt_ecm_unclosed_channel_cost
      ,sum(case when market_target_type = '非闭环电商' then channel_cash_income_amt end) as mkt_ecm_unclosed_channel_cash_cost
    from
      redcdm.dm_ads_pub_product_account_detail_td_df a
    where
      a.dtm = '{{ds_nodash}}'
      
    group by
      1,2,3,4,5,6
    
) a
group by
   date_key
  ,module
  ,product
  ,brand_account_id
  ,market_target
  ,is_marketing_product
;











-----------spu
--set spark.sql.hive.manageFilesourcePartitions=false;
--中间层验数中品牌-开屏和信息流gd不一致，依赖纵向模型搭建，先使用兜底逻辑，后续替换。
with
  feed_gd_spalsh as (
    select
      creativity_id,
      '信息流GD' as product
    from
      (
        select
          b.id as creativity_id,
          b.unique_id,
          c.advertiser_id as brand_account_id,
          a.id as unit_id,
          a.campaign_id,
          from_unixtime(floor(a.launch_date / 1000 + 28800)) as launch_date,
          from_unixtime(floor(a.create_time / 1000 + 28800)) as create_time,
          from_unixtime(floor(a.modify_time / 1000 + 28800)) as modify_time,
          from_unixtime(floor(a.reserve_time / 1000 + 28800)) as reserve_time,
          case
            when a.across_day_info = ''
            or a.across_day_info is null then 0
            else cast(
              get_json_object(a.across_day_info, '$.nreach') as int
            )
          end as n_reach_num,
          b.creativity_relation_type as related_creativity_type,
          b.related_creativity_id,
          from_unixtime(floor(b.start_time / 1000 + 28800)) as creativity_start_time,
          from_unixtime(floor(b.expire_time / 1000 + 28800)) as creativity_expire_time,
          from_unixtime(floor(a.launch_start_time / 1000 + 28800)) as unit_launch_start_time,
          from_unixtime(floor(a.launch_end_time / 1000 + 28800)) as unit_launch_end_time
        from
          redods.ods_shequ_feed_ads_t_ads_cpd_unit a
          join redcdm.dwd_ads_rtb_creativity_df b on a.id = b.unit_id
          and b.dtm = '{{ds_nodash}}'
          and b.material_type not in (100, 101, 102) -- 普通、互动、特色
          join redods.ods_shequ_feed_ads_t_ads_campaign c on a.campaign_id = c.id
          and c.dtm = '{{ds_nodash}}'
        where
          a.dtm = '{{ds_nodash}}'
      ) a
      left outer join (
        select
          id
        from
          redods.ods_shequ_feed_ads_t_ads_cpd_unit
        where
          dtm = greatest('{{ds_nodash}}', '20220513')
          and case
            when across_day_info <> ''
            and cast(get_json_object(across_day_info, '$.nreach') as int) > 0 then substr(
              from_unixtime(floor(launch_start_time / 1000 + 28800)),
              1,
              10
            ) <= '{{ds}}'
            and substr(
              from_unixtime(floor(launch_end_time / 1000 + 28800)),
              1,
              10
            ) >= '{{ds}}'
            else substr(
              from_unixtime(floor(launch_date / 1000 + 28800)),
              1,
              10
            ) = '{{ds}}'
          end = true
      ) b on b.id = a.unit_id
    where
      (
        (
          n_reach_num > 0
          and related_creativity_type = 1
          and substr(creativity_start_time, 1, 10) <= '{{ds}}'
          and substr(creativity_expire_time, 1, 10) >= '{{ds}}'
        )
        or n_reach_num <= 0
      )
      and b.id is not null  
    group by
      1
    union all
    select
      creativity_id,
      '开屏' as product
    from
      redcdm.dim_ads_creativity_core_df
    where
      dtm = '{{ds_nodash}}'
      and product = '开屏'
      and substr(launch_date, 1, 10) = '{{ds}}'
    group by
      creativity_id,
      product
  ),
log_info as  (
select


      '{{ds}}' as date_key
      ,module
      ,case 
        when a.product = '信息流' then '竞价-信息流' 
        when a.product = '搜索' then '竞价-搜索' 
        when a.product = '视频内流' then '竞价-视频内流'
        when a.product='火焰话题' then '品牌其他'
        when a.module='薯条' then '薯条'
      end as product
      ,a.brand_account_id
      ,a.ads_material_id as note_id
      ,a.creativity_id
      ,case when  a.marketing_target in (3, 8) then '闭环电商广告'
            when a.marketing_target in (13) then '非闭环电商广告'
            when a.marketing_target in (2, 5, 9) then '线索广告'
            when  a.marketing_target  not in (2,5,8,9,3,13) then '种草广告' else '其他' end as marketing_target
      ,sum(a.imp_cnt) as imp_cnt
      ,sum(a.click_cnt) as click_cnt
      ,sum(a.like_cnt) as like_cnt
      ,sum(a.fav_cnt) as fav_cnt
      ,sum(a.cmt_cnt) as cmt_cnt
      ,sum(a.share_cnt) as share_cnt
      ,sum(a.follow_cnt) as follow_cnt   
    from 
    (select *
    from redcdm.dm_ads_creativity_cube_1d_di 
    where  dtm = '{{ds_nodash}}'
    and module in ('效果', '薯条', '品牌')
    and cube_type = 'creativity' 
    and coalesce(is_own_ads,0)=0
    ) a
    left join feed_gd_spalsh b on a.creativity_id = b.creativity_id
    and a.product = b.product
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
    group by 
      1,2,3,4,5,6,7
    union all 
    -- 品合
    select 
      '{{ds}}' as date_key
      ,'品合' as module
      ,'品合' as product
      ,report_brand_user_id as brand_account_id
      ,a.note_id
      ,'' as creativity_id
      ,'整体' as marketing_target
      ,sum(b.ads_imp_num) as imp_cnt
      ,sum(b.ads_click_num) as click_cnt
      ,sum(b.ads_like_num) as like_cnt
      ,sum(b.ads_fav_num) as fav_cnt
      ,sum(b.ads_cmt_num) as cmt_cnt
      ,sum(b.ads_share_num) as share_cnt
      ,sum(b.ads_follow_num) as follow_cnt
    from 
      redcdm.dwd_ads_bcoo_ord_note_df a
    left join 
      redapp.app_ads_note_engagement_1d_di b 
    on 
      b.dtm = '{{ds_nodash}}'
      and a.note_id = b.note_id
    where
      a.dtm = '{{ds_nodash}}'
    group by 
      1,2,3,4,5,6,7
    
  ),
  all_cost as (
    select date_key,
      creativity_id,
      brand_account_id,
      t1.note_id,
      engage_spu_id,
      spu_id,
      module,
      product,
      marketing_target,
      sum(cash_cost) as cash_cost,
      sum(imp_cnt) as imp_cnt,
      sum(click_cnt) as click_cnt,
      sum(like_cnt) as like_cnt,
      sum(fav_cnt) as fav_cnt,
      sum(cmt_cnt) as cmt_cnt,
      sum(share_cnt) as share_cnt,
      sum(follow_cnt) as follow_cnt
    from
      (
        select '{{ds}}' as date_key,
          virtual_object_id as creativity_id,
          brand_account_id,
          note_id,
          module,
          case when product='发现feed' then '竞价-信息流' 
            when product='搜索feed' then '竞价-搜索'
            when product='视频内流' then '竞价-视频内流' 
            when module = '薯条' then '薯条' when module ='品合' then '品合' else product end as product,
          case when module = '效果'
            then
              case
                when marketing_target in (3, 8) then '闭环电商广告'
                when marketing_target in (13) then '非闭环电商广告'
                when marketing_target in (2, 5, 9) then '线索广告'
                else '种草广告'
              end
          else '其他'
          end as marketing_target,
          sum(cash_income_amt) as cash_cost,
          0 as imp_cnt,
          0 as click_cnt,
          0 as like_cnt,
          0 as fav_cnt,
          0 as cmt_cnt,
          0 as share_cnt,
          0 as follow_cnt
        from
          redcdm.dws_ads_creativity_order_share_income_nd_df
        where
          dtm =  greatest('{{ds_nodash}}', '20230625')
          and date_key = '{{ds}}'
        group by
          virtual_object_id,
          brand_account_id,
          note_id,
          module,
          case when product='发现feed' then '竞价-信息流' 
            when product='搜索feed' then '竞价-搜索'
            when product='视频内流' then '竞价-视频内流' 
            when module = '薯条' then '薯条' when module ='品合' then '品合' else product end,
          case when module = '效果'
            then
              case
                when marketing_target in (3, 8) then '闭环电商广告'
                when marketing_target in (13) then '非闭环电商广告'
                when marketing_target in (2, 5, 9) then '线索广告'
                else '种草广告'
              end
          else '其他'
          end 
        union all 
        --流量
        select date_key,
          creativity_id,
          brand_account_id,
          note_id,
          module,
          product,
          marketing_target,
          0 as cash_cost,
          imp_cnt,
          click_cnt,
          like_cnt,
          fav_cnt,
          cmt_cnt,
          share_cnt,
          follow_cnt
        from log_info
      ) t1
      left join (--互动看算法+人工
        select
          note_id,
          spu_id as engage_spu_id
        from
          ads_databank.dim_spu_note_df
        where
          dtm = greatest('{{ds_nodash}}', '20230625')
          --and bind_type = 2
        group by 1,2
      ) spu_note_engage on spu_note_engage.note_id = t1.note_id
      left join (--收入只看人工绑定
        select
          note_id,
          spu_id
        from
          ads_databank.dim_spu_note_df
        where
          dtm = greatest('{{ds_nodash}}', '20230625')
          and bind_type = 2
        group by 1,2
      ) spu_note on spu_note.note_id = t1.note_id and spu_note.spu_id = spu_note_engage.engage_spu_id
    group by
      date_key,
      creativity_id,
      brand_account_id,
      t1.note_id,
      spu_id,
      engage_spu_id,
      module,
      product,
      marketing_target
  ),
spu_cost_log as (
    select
      spu_id,
      module,
      product,
      marketing_target,
      brand_account_id,
      sum(cash_cost) as cash_cost,
      sum(imp_cnt) as imp_cnt,
      sum(click_cnt) as click_cnt,
      sum(like_cnt) as like_cnt,
      sum(fav_cnt) as fav_cnt,
      sum(cmt_cnt) as cmt_cnt,
      sum(share_cnt) as share_cnt,
      sum(follow_cnt) as follow_cnt
    from
      (
        select
          creativity_id,
          brand_account_id,
          note_id,
          spu_id,
          module,
          product,
          marketing_target,
          cash_cost,
          imp_cnt,
          click_cnt,
          like_cnt,
          fav_cnt,
          cmt_cnt,
          share_cnt,
          follow_cnt
        from
          all_cost
        where
          spu_id is not null --笔记人工绑定spu
        union all 
        --互动算法绑定
        select
          creativity_id,
          brand_account_id,
          note_id,
          engage_spu_id as spu_id,
          module,
          product,
          marketing_target,
          0 as cash_cost,
          imp_cnt,
          click_cnt,
          like_cnt,
          fav_cnt,
          cmt_cnt,
          share_cnt,
          follow_cnt
        from
          all_cost
        where
          spu_id is null and engage_spu_id is not null --算法绑定spu
        union all
        select
          creativity_id,
          brand_account_id,
          note_id,
          ele_note.spu_id,
          module,
          product,
          marketing_target,
          cash_cost,
          imp_cnt,
          click_cnt,
          like_cnt,
          fav_cnt,
          cmt_cnt,
          share_cnt,
          follow_cnt
        from
          (
            select
              date_key,
              creativity_id,
              brand_account_id,
              note_id,
              engage_spu_id,
              spu_id,
              module,
              product,
              marketing_target,
              cash_cost,
              0 as imp_cnt,
              0 as click_cnt,
              0 as like_cnt,
              0 as fav_cnt,
              0 as cmt_cnt,
              0 as share_cnt,
              0 as follow_cnt
            from
              all_cost
            where
              spu_id is null --未人工或算法绑定spu
            
          ) t1
          left join (
            select
              a.element_id,
              a.main_spu_id as spu_id
            from
              redods.ods_shequ_feed_ads_tb_material_bind_spu_df a
            where
              a.dtm=greatest('20230718','{{ds_nodash}}')
              and a.bind_status = 2
              and a.del = 0
            group by
              1,
              2
          ) ele_note on ele_note.element_id = t1.creativity_id
        where
          ele_note.spu_id is not null --element人工绑定spu
        union all
        select
          creativity_id,
          brand_account_id,
          note_id,
          ele_note.spu_id,
          module,
          product,
          marketing_target,
          cash_cost,
          imp_cnt,
          click_cnt,
          like_cnt,
          fav_cnt,
          cmt_cnt,
          share_cnt,
          follow_cnt
        from
          (
            select
              date_key,
              creativity_id,
              brand_account_id,
              note_id,
              engage_spu_id,
              spu_id,
              module,
              product,
              marketing_target,
              0 as cash_cost,
              imp_cnt,
              click_cnt,
              like_cnt,
              fav_cnt,
              cmt_cnt,
              share_cnt,
              follow_cnt
            from
              all_cost
            where
              engage_spu_id is null --未人工或算法绑定spu
            
          ) t1
          left join (
            select
              a.element_id,
              a.main_spu_id as spu_id
            from
              redods.ods_shequ_feed_ads_tb_material_bind_spu_df a
            where
              a.dtm =greatest('20230718','{{ds_nodash}}')
              and a.del = 0
            group by
              1,
              2
          ) ele_note on ele_note.element_id = t1.creativity_id
          where ele_note.spu_id is not null --element人工绑定spu
      ) info
    group by
      spu_id,
      brand_account_id,
      module,
      product,
      marketing_target
  )
insert overwrite table redcdm_dev.dm_ads_spu_account_detail_1d_di   partition( dtm = '{{ds_nodash}}')
select  detail.date_key,
  detail.spu_id,
  detail.brand_account_id,
  detail.module,
  case when detail.product='发现feed' then '竞价-信息流' 
    when detail.product='搜索feed' then '竞价-搜索'
    when detail.product='视频内流' then '竞价-视频内流' 
    when detail.product='蒲公英' then '品合' else detail.product end as product ,
  detail.marketing_target,
  tt1.spu_name,
  tt1.brand_id,
  tt1.brand_name,
  tt1.commercial_taxonomy_name1,
  tt1.commercial_code2,
  tt1.commercial_taxonomy_name2,
  tt1.commercial_code3,
  tt1.commercial_taxonomy_name3,
  tt1.commercial_code4,
  tt1.commercial_taxonomy_name4,
  spu_account.brand_account_name,
  spu_account.operator_code,
  spu_account.operator_name ,
  spu_account.direct_sales_code,
  spu_account.direct_sales_name ,
  spu_account.direct_sales_dept1_name,
  spu_account.direct_sales_dept2_name ,
  spu_account.direct_sales_dept3_name,
  sum(imp_num) as imp_num,
  sum(click_num) as click_num,
  sum(like_num) as like_num,
  sum(fav_num) as fav_num,
  sum(read_feed_num) as read_feed_num,
  sum(share_num) as share_num,
  sum(cash_cost) as cash_cost,
  sum(bind_cash_cost) as bind_cash_cost
from 
( 
select '{{ds}}' as date_key,
  module,
  product,
  marketing_target,
  cost.spu_id,
  cost.brand_account_id,
  sum(imp_num) as imp_num,
  sum(click_num) as click_num,
  sum(like_num) as like_num,
  sum(fav_num) as fav_num,
  sum(read_feed_num) as read_feed_num,
  sum(share_num) as share_num,
  sum(cash_cost) as cash_cost,
  sum(bind_cash_cost) as bind_cash_cost
from
   (
    select ----绑定spu的流水分摊 
      spu_id,
      brand_user_id as brand_account_id,
      case when module = '蒲公英' then '品合' 
      when module =  '竞价' then '效果' 
      when module =  '品牌广告' then '品牌' else module end as module,
      case when module = '薯条' then '薯条' when module = '蒲公英' then '品合' else product end as product,
      case when marketing_target is null or marketing_target='' then '其他' else marketing_target end as marketing_target,
      cash_cost,
      case when bind_type=2 then cash_cost else 0 end as bind_cash_cost,
      0 as imp_num,
      0 as click_num,
      0 as like_num,
      0 as fav_num,
      0 as read_feed_num,
      0 as share_num
    from
      redcdm.dm_ads_note_spu_engage_cost_avg_1d_di
    where
      dtm = '{{ds_nodash}}'
      and spu_id is not null
    union all --人工绑定spu的流水加和以及流量数据
    select
      spu_id,
      brand_account_id,
      module,
      case when module = '薯条' then '薯条' when module ='品合' then '品合' else product end as product,
      case when marketing_target is null or marketing_target='' then '其他' else marketing_target end as marketing_target,
      0 as cash_cost,
      0 as bind_cash_cost,
      sum(imp_cnt) as imp_num,
      sum(click_cnt) as click_num,
      sum(like_cnt) as like_num,
      sum( fav_cnt) as fav_num,
      null as read_feed_num,
      sum(share_cnt) as share_num
    from spu_cost_log
    group by spu_id,
      brand_account_id,
      module,
      case when module = '薯条' then '薯条' when module ='品合' then '品合' else product end,
      case when marketing_target is null or marketing_target='' then '其他' else marketing_target end
  ) cost 
  
  group by  module,
    product,
    marketing_target,
    cost.spu_id,
    cost.brand_account_id
)detail
left join (
    select
      spu_id,
      brand_id,
      brand_name,
      name as spu_name,
      commercial_taxonomy_name1,
      commercial_code2,
      commercial_taxonomy_name2,
      commercial_code3,
      commercial_taxonomy_name3,
      commercial_code4,
      commercial_taxonomy_name4
    from
      ads_databank.dim_spu_df
    where
      dtm = '{{ds_nodash}}'
  ) tt1 on tt1.spu_id = detail.spu_id
left join 
  (select brand_account_id,
      direct_sales_dept1_name,
      direct_sales_dept2_name,
      direct_sales_dept3_name,
      direct_sales_name,
      operator_name,
      operator_code ,
      direct_sales_code,
      brand_user_name as brand_account_name
  from redapp.app_ads_insight_industry_account_df
  where dtm= greatest('20230718','{{ds_nodash}}')
  )spu_account 
  on spu_account.brand_account_id=detail.brand_account_id
group by detail.date_key,
  detail.spu_id,
  detail.brand_account_id,
  detail.module,
  case when detail.product='发现feed' then '竞价-信息流' 
    when detail.product='搜索feed' then '竞价-搜索'
    when detail.product='视频内流' then '竞价-视频内流' 
    when detail.product='蒲公英' then '品合' else detail.product end,
  detail.marketing_target,
  tt1.spu_name,
  tt1.brand_id,
  tt1.brand_name,
  tt1.commercial_taxonomy_name1,
  tt1.commercial_code2,
  tt1.commercial_taxonomy_name2,
  tt1.commercial_code3,
  tt1.commercial_taxonomy_name3,
  tt1.commercial_code4,
  tt1.commercial_taxonomy_name4,
  spu_account.brand_account_name,
  spu_account.operator_code,
  spu_account.operator_name ,
  spu_account.direct_sales_code,
  spu_account.direct_sales_name ,
  spu_account.direct_sales_dept1_name,
  spu_account.direct_sales_dept2_name ,
  spu_account.direct_sales_dept3_name












  ----------dm_ads_pub xin

  --set spark.sql.hive.manageFilesourcePartitions=false;
--中间层验数中品牌-开屏和信息流gd不一致，依赖纵向模型搭建，先使用兜底逻辑，后续替换。
with
  feed_gd_spalsh as (
    select
      creativity_id,
      '信息流GD' as product
    from
      (
        select
          b.id as creativity_id,
          b.unique_id,
          c.advertiser_id as brand_account_id,
          a.id as unit_id,
          a.campaign_id,
          from_unixtime(floor(a.launch_date / 1000 + 28800)) as launch_date,
          from_unixtime(floor(a.create_time / 1000 + 28800)) as create_time,
          from_unixtime(floor(a.modify_time / 1000 + 28800)) as modify_time,
          from_unixtime(floor(a.reserve_time / 1000 + 28800)) as reserve_time,
          case
            when a.across_day_info = ''
            or a.across_day_info is null then 0
            else cast(
              get_json_object(a.across_day_info, '$.nreach') as int
            )
          end as n_reach_num,
          b.creativity_relation_type as related_creativity_type,
          b.related_creativity_id,
          from_unixtime(floor(b.start_time / 1000 + 28800)) as creativity_start_time,
          from_unixtime(floor(b.expire_time / 1000 + 28800)) as creativity_expire_time,
          from_unixtime(floor(a.launch_start_time / 1000 + 28800)) as unit_launch_start_time,
          from_unixtime(floor(a.launch_end_time / 1000 + 28800)) as unit_launch_end_time
        from
          redods.ods_shequ_feed_ads_t_ads_cpd_unit a
          join redcdm.dwd_ads_rtb_creativity_df b on a.id = b.unit_id
          and b.dtm = '{{ds_nodash}}'
          and b.material_type not in (100, 101, 102) -- 普通、互动、特色
          join redods.ods_shequ_feed_ads_t_ads_campaign c on a.campaign_id = c.id
          and c.dtm = '{{ds_nodash}}'
        where
          a.dtm = '{{ds_nodash}}'
      ) a
      left outer join (
        select
          id
        from
          redods.ods_shequ_feed_ads_t_ads_cpd_unit
        where
          dtm = greatest('{{ds_nodash}}', '20220513')
          and case
            when across_day_info <> ''
            and cast(get_json_object(across_day_info, '$.nreach') as int) > 0 then substr(
              from_unixtime(floor(launch_start_time / 1000 + 28800)),
              1,
              10
            ) <= '{{ds}}'
            and substr(
              from_unixtime(floor(launch_end_time / 1000 + 28800)),
              1,
              10
            ) >= '{{ds}}'
            else substr(
              from_unixtime(floor(launch_date / 1000 + 28800)),
              1,
              10
            ) = '{{ds}}'
          end = true
      ) b on b.id = a.unit_id
    where
      (
        (
          n_reach_num > 0
          and related_creativity_type = 1
          and substr(creativity_start_time, 1, 10) <= '{{ds}}'
          and substr(creativity_expire_time, 1, 10) >= '{{ds}}'
        )
        or n_reach_num <= 0
      )
      and b.id is not null  
    group by
      1
    union all
    select
      creativity_id,
      '开屏' as product
    from
      redcdm.dim_ads_creativity_core_df
    where
      dtm = '{{ds_nodash}}'
      and product = '开屏'
      and substr(launch_date, 1, 10) = '{{ds}}'
    group by
      creativity_id,
      product
  ),
log_cvr_di as --流量转化中间层
(select
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
  rtb_budget_income_amt,
  add_cart_cnt,
  mini_add_cart_cnt,
  instant_buy_cnt,
  mini_instant_buy_cnt,
  purchase_order_num,
  mini_purchase_order_num,
  ecm_unclosed_purchase_order_num,
  deal_order_num,
  mini_deal_order_num,
  leads_cnt,
  valid_leads_cnt,
  goods_view_cnt,
  mini_goods_view_cnt,
  ecm_unclosed_goods_view_cnt,
  rgmv,
  mini_rgmv,
  ecm_unclosed_rgmv,
  purchase_rgmv,
  mini_purchase_rgmv,
  ecm_unclosed_purchase_rgmv,
  enter_seller_cnt,
  live_watch_duration,
  live_watch_cnt,
  live_watch_num,
  live_valid_watch_cnt,
  live_valid_watch_num,
  live_rgmv,
  live_dgmv,
  live_order_num,
  mini_enter_seller_cnt
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
      coalesce(detail.marketing_target_1, -911) as marketing_target,
      coalesce(detail.optimize_target_1, -911) as optimize_target,
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
      rtb_budget_income_amt,
      add_cart_cnt,
      mini_add_cart_cnt,
      instant_buy_cnt,
      mini_instant_buy_cnt,
      purchase_order_num,
      mini_purchase_order_num,
      ecm_unclosed_purchase_order_num,
      deal_order_num,
      mini_deal_order_num,
      leads_cnt,
      valid_leads_cnt,
      goods_view_cnt,
      mini_goods_view_cnt,
      ecm_unclosed_goods_view_cnt,
      rgmv,
      mini_rgmv,
      ecm_unclosed_rgmv,
      purchase_rgmv,
      mini_purchase_rgmv,
      ecm_unclosed_purchase_rgmv,
      enter_seller_cnt,
      live_watch_duration,
      live_watch_cnt,
      live_watch_num,
      live_valid_watch_cnt,
      live_valid_watch_num,
      live_rgmv,
      live_dgmv,
      live_order_num,
      --live_order_user_num,
      mini_enter_seller_cnt
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
              '{{ds}}' as date_key,
              a.module,
              a.product,
              a.brand_account_id,
              marketing_target,
              optimize_target,
              a.market_target_type as market_target,
              sum(imp_cnt) as imp_cnt,
              sum(click_cnt) as click_cnt,
              sum(like_cnt) as like_cnt,
              sum(fav_cnt) as fav_cnt,
              sum(cmt_cnt) as cmt_cnt,
              sum(share_cnt) as share_cnt,
              sum(follow_cnt) as follow_cnt,
              sum(screenshot_cnt) as screenshot_cnt,
              sum(image_save_cnt) as image_save_cnt,
              sum(add_cart_cnt) as add_cart_cnt,
              sum(mini_add_cart_cnt) as mini_add_cart_cnt,
              sum(instant_buy_cnt) as instant_buy_cnt,
              sum(mini_instant_buy_cnt) as mini_instant_buy_cnt,
              sum(purchase_order_num) as purchase_order_num,
              sum(mini_purchase_order_num) as mini_purchase_order_num,
              sum(ecm_unclosed_purchase_order_num) as ecm_unclosed_purchase_order_num,
              sum(deal_order_num) as deal_order_num,
              sum(mini_deal_order_num) as mini_deal_order_num,
              sum(leads_cnt) as leads_cnt,
              sum(valid_leads_cnt) as valid_leads_cnt,
              sum(goods_view_cnt) as goods_view_cnt,
              sum(mini_goods_view_cnt) as mini_goods_view_cnt,
              sum(ecm_unclosed_goods_view_cnt) as ecm_unclosed_goods_view_cnt,
              sum(rgmv) as rgmv,
              sum(mini_rgmv) as mini_rgmv,
              sum(ecm_unclosed_rgmv) as ecm_unclosed_rgmv,
              sum(purchase_rgmv) as purchase_rgmv,
              sum(mini_purchase_rgmv) as mini_purchase_rgmv,
              sum(ecm_unclosed_purchase_rgmv) as ecm_unclosed_purchase_rgmv,
              sum(enter_seller_cnt) as enter_seller_cnt,
              sum(live_watch_duration) as live_watch_duration,
              sum(live_watch_cnt) as live_watch_cnt,
              sum(live_watch_num) as live_watch_num,
              sum(live_valid_watch_cnt) as live_valid_watch_cnt,
              sum(live_valid_watch_num) as live_valid_watch_num,
              sum(live_rgmv) as live_rgmv,
              sum(live_dgmv) as live_dgmv,
              sum(live_order_num) as live_order_num,
              --,sum(live_order_user_num) as live_order_user_num
              sum(mini_enter_seller_cnt) as mini_enter_seller_cnt,
              count(distinct campaign_id) as campaign_cnt,
              count(distinct unit_id) as unit_cnt,
              max(coalesce(c.campaign_cnt, 0)) as brand_campaign_cnt
            from
              (select *
              from redcdm.dm_ads_creativity_cube_1d_di 
              where  dtm = '{{ds_nodash}}'
              and module in ('效果', '薯条', '品牌')
              and cube_type = 'creativity' 
              and coalesce(is_own_ads,0)=0
              ) a
              left join feed_gd_spalsh b on a.creativity_id = b.creativity_id
              and a.product = b.product
              left join (
                select
                  brand_account_id,
                  count(distinct campaign_id) as campaign_cnt
                from
                  redcdm.dm_ads_creativity_cube_1d_di --流量转化中间层
                where
                  dtm = '{{ds_nodash}}'
                  and module in ('品牌')
                  and product in ('火焰话题', '品牌专区', '搜索第三位', '信息流GD', '开屏')
                group by
                  brand_account_id
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
              
            group by
              a.module,
              a.product,
              a.brand_account_id,
              marketing_target,
              optimize_target,
              market_target_type
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
              and open_sale_num > 0
              and is_internal = 0
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
    union all
    -- 品合-社区流量
    select
      '{{ds}}' as date_key,
      report_brand_user_id as brand_account_id,
      '品合' as module,
      '品合' as product,
      -911 as marketing_target,
      -911 as optimize_target,
      '整体' as market_target,
      sum(b.imp_num) as imp_cnt,
      sum(b.click_num) as click_cnt,
      sum(b.like_num) as like_cnt,
      sum(b.fav_num) as fav_cnt,
      sum(b.cmt_num) as cmt_cnt,
      sum(b.follow_from_discovery_num) as follow_cnt,
      sum(b.share_num) as share_cnt,
      0 as screenshot_cnt,
      0 as image_save_cnt,
      0 as open_sale_num,
      0 as campaign_cnt,
      0 as unit_cnt,
      0 as brand_campaign_cnt,
      0 as rtb_cost_income_amt,
      0 as rtb_budget_income_amt,
      0 as add_cart_cnt,
      0 as mini_add_cart_cnt,
      0 as instant_buy_cnt,
      0 as mini_instant_buy_cnt,
      0 as purchase_order_num,
      0 as mini_purchase_order_num,
      0 as ecm_unclosed_purchase_order_num,
      0 as deal_order_num,
      0 as mini_deal_order_num,
      0 as leads_cnt,
      0 as valid_leads_cnt,
      0 as goods_view_cnt,
      0 as mini_goods_view_cnt,
      0 as ecm_unclosed_goods_view_cnt,
      0 as rgmv,
      0 as mini_rgmv,
      0 as ecm_unclosed_rgmv,
      0 as purchase_rgmv,
      0 as mini_purchase_rgmv,
      0 as ecm_unclosed_purchase_rgmv,
      0 as enter_seller_cnt,
      0 as live_watch_duration,
      0 as live_watch_cnt,
      0 as live_watch_num,
      0 as live_valid_watch_cnt,
      0 as live_valid_watch_num,
      0 as live_rgmv,
      0 as live_dgmv,
      0 as live_order_num,
      --live_order_user_num,
      0 as mini_enter_seller_cnt
    from
      redcdm.dwd_ads_bcoo_ord_note_df a
      left join reddm.dm_soc_discovery_engagement_new_day_inc b on b.dtm = '{{ds_nodash}}'
      and a.note_id = b.discovery_id
    where
      a.dtm = '{{ds_nodash}}'
    group by
      2
  ) log_cvr
)
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
  sum(rtb_budget_income_amt) as rtb_budget_income_amt,
  sum(add_cart_cnt) as add_cart_cnt,
  sum(mini_add_cart_cnt) as mini_add_cart_cnt,
  sum(instant_buy_cnt) as instant_buy_cnt,
  sum(mini_instant_buy_cnt) as mini_instant_buy_cnt,
  sum(purchase_order_num) as purchase_order_num,
  sum(mini_purchase_order_num) as mini_purchase_order_num,
  sum(ecm_unclosed_purchase_order_num) as ecm_unclosed_purchase_order_num,
  sum(deal_order_num) as deal_order_num,
  sum(mini_deal_order_num) as mini_deal_order_num,
  sum(leads_cnt) as leads_cnt,
  sum(valid_leads_cnt) as valid_leads_cnt,
  sum(goods_view_cnt) as goods_view_cnt,
  sum(mini_goods_view_cnt) as mini_goods_view_cnt,
  sum(ecm_unclosed_goods_view_cnt) as ecm_unclosed_goods_view_cnt,
  sum(rgmv) as rgmv,
  sum(mini_rgmv) as mini_rgmv,
  sum(ecm_unclosed_rgmv) as ecm_unclosed_rgmv,
  sum(purchase_rgmv) as purchase_rgmv,
  sum(mini_purchase_rgmv) as mini_purchase_rgmv,
  sum(ecm_unclosed_purchase_rgmv) as ecm_unclosed_purchase_rgmv,
  sum(enter_seller_cnt) as enter_seller_cnt,
  sum(live_watch_duration) as live_watch_duration,
  sum(live_watch_cnt) as live_watch_cnt,
  sum(live_watch_num) as live_watch_num,
  sum(live_valid_watch_cnt) as live_valid_watch_cnt,
  sum(live_valid_watch_num) as live_valid_watch_num,
  sum(live_rgmv) as live_rgmv,
  sum(live_dgmv) as live_dgmv,
  sum(live_order_num) as live_order_num,
  sum(mini_enter_seller_cnt) as mini_enter_seller_cnt
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
  rtb_budget_income_amt,
  add_cart_cnt,
  mini_add_cart_cnt,
  instant_buy_cnt,
  mini_instant_buy_cnt,
  purchase_order_num,
  mini_purchase_order_num,
  ecm_unclosed_purchase_order_num,
  deal_order_num,
  mini_deal_order_num,
  leads_cnt,
  valid_leads_cnt,
  goods_view_cnt,
  mini_goods_view_cnt,
  ecm_unclosed_goods_view_cnt,
  rgmv,
  mini_rgmv,
  ecm_unclosed_rgmv,
  purchase_rgmv,
  mini_purchase_rgmv,
  ecm_unclosed_purchase_rgmv,
  enter_seller_cnt,
  live_watch_duration,
  live_watch_cnt,
  live_watch_num,
  live_valid_watch_cnt,
  live_valid_watch_num,
  live_rgmv,
  live_dgmv,
  live_order_num,
  mini_enter_seller_cnt
from log_cvr_di
union all
--收入中间层
select
  date_key,
  brand_user_id as brand_account_id,
  module,
  product,
  if(marketing_target_id='',null,marketing_target_id) as marketing_target,
  optimize_target_id as optimize_target,
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
  0 as rtb_budget_income_amt,
  0 as add_cart_cnt,
  0 as mini_add_cart_cnt,
  0 as instant_buy_cnt,
  0 as mini_instant_buy_cnt,
  0 as purchase_order_num,
  0 as mini_purchase_order_num,
  0 as ecm_unclosed_purchase_order_num,
  0 as deal_order_num,
  0 as mini_deal_order_num,
  0 as leads_cnt,
  0 as valid_leads_cnt,
  0 as goods_view_cnt,
  0 as mini_goods_view_cnt,
  0 as ecm_unclosed_goods_view_cnt,
  0 as rgmv,
  0 as mini_rgmv,
  0 as ecm_unclosed_rgmv,
  0 as purchase_rgmv,
  0 as mini_purchase_rgmv,
  0 as ecm_unclosed_purchase_rgmv,
  0 as enter_seller_cnt,
  0 as live_watch_duration,
  0 as live_watch_cnt,
  0 as live_watch_num,
  0 as live_valid_watch_cnt,
  0 as live_valid_watch_num,
  0 as live_rgmv,
  0 as live_dgmv,
  0 as live_order_num,
  --live_order_user_num,
  0 as mini_enter_seller_cnt
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
  sum(image_save_cnt) as image_save_cnt,
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
  sum(rtb_budget_income_amt) as rtb_budget_income_amt,
  sum(add_cart_cnt) as add_cart_cnt,
  sum(mini_add_cart_cnt) as mini_add_cart_cnt,
  sum(instant_buy_cnt) as instant_buy_cnt,
  sum(mini_instant_buy_cnt) as mini_instant_buy_cnt,
  sum(purchase_order_num) as purchase_order_num,
  sum(mini_purchase_order_num) as mini_purchase_order_num,
  sum(ecm_unclosed_purchase_order_num) as ecm_unclosed_purchase_order_num,
  sum(deal_order_num) as deal_order_num,
  sum(mini_deal_order_num) as mini_deal_order_num,
  sum(leads_cnt) as leads_cnt,
  sum(valid_leads_cnt) as valid_leads_cnt,
  sum(goods_view_cnt) as goods_view_cnt,
  sum(mini_goods_view_cnt) as mini_goods_view_cnt,
  sum(ecm_unclosed_goods_view_cnt) as ecm_unclosed_goods_view_cnt,
  sum(rgmv) as rgmv,
  sum(mini_rgmv) as mini_rgmv,
  sum(ecm_unclosed_rgmv) as ecm_unclosed_rgmv,
  sum(purchase_rgmv) as purchase_rgmv,
  sum(mini_purchase_rgmv) as mini_purchase_rgmv,
  sum(ecm_unclosed_purchase_rgmv) as ecm_unclosed_purchase_rgmv,
  sum(enter_seller_cnt) as enter_seller_cnt,
  sum(live_watch_duration) as live_watch_duration,
  sum(live_watch_cnt) as live_watch_cnt,
  sum(live_watch_num) as live_watch_num,
  sum(live_valid_watch_cnt) as live_valid_watch_cnt,
  sum(live_valid_watch_num) as live_valid_watch_num,
  sum(live_rgmv) as live_rgmv,
  sum(live_dgmv) as live_dgmv,
  sum(live_order_num) as live_order_num,
  sum(mini_enter_seller_cnt) as mini_enter_seller_cnt
from
  redcdm.dm_ads_pub_product_account_detail_td_df
where
  dtm = '{{yesterday_ds_nodash}}'
group by 1,2,3,4,5,6,7,8
  )detail
  group by 1,2,3,4,5,6,7,8