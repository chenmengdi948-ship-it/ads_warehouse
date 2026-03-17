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
              case when module ='效果广告' then '效果' when module ='品牌广告' then '品牌' else module end as  module,
              case
                when module = '薯条' then '薯条'
                when a.product = '信息流效果' then '竞价-信息流' 
                when a.product = '搜索效果' then '竞价-搜索' 
                when a.product = '视频内流' then '竞价-视频内流'
                when a.product ='火焰话题' then '品牌其他' else a.product
              end as product,
              a.brand_account_id,
              marketing_target,
              optimize_target,
              case
                when a.marketing_target in (3, 8) then '闭环电商'
                when a.marketing_target in (13) then '非闭环电商'
                when a.marketing_target in (2, 5, 9) then '线索'
                when a.marketing_target not in (3, 8, 2, 5, 9, 13) then '种草'
              end as market_target,
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
              and module in ('效果广告', '薯条', '品牌广告')
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
                  and module in ('品牌广告')
                  and product in ('火焰话题', '品牌专区', '搜索第三位', '信息流GD', '开屏')
                group by
                  brand_account_id
              ) c on c.brand_account_id = a.brand_account_id
            where
             
            (
                (a.is_effective = 1 and a.module='效果广告')
                or a.module = '薯条'
                or (
                  a.module = '品牌广告'
                  and a.product in ('火焰话题', '品牌专区', '搜索第三位')
                )
                or (
                  a.module = '品牌广告'
                  and a.product in ('信息流GD', '开屏') --兜底逻辑后续纵向模型产出后调整
                  and b.creativity_id is not null
                )
                
              )
              
            group by
              module,
              case
                when module = '薯条' then '薯条'
                when a.product = '信息流效果' then '竞价-信息流' 
                when a.product = '搜索效果' then '竞价-搜索' 
                when a.product = '视频内流' then '竞价-视频内流'
                when a.product ='火焰话题' then '品牌其他' else a.product
              end,
              a.brand_account_id,
              marketing_target,
              optimize_target,
              case
                when a.marketing_target in (3, 8) then '闭环电商'
                when a.marketing_target in (13) then '非闭环电商'
                when a.marketing_target in (2, 5, 9) then '线索'
                when a.marketing_target not in (3, 8, 2, 5, 9, 13) then '种草'
              end
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
          '效果' as module,
          case
            when a.module = '发现feed' then '竞价-信息流'
            when a.module = '搜索feed' then '竞价-搜索'
            when a.module = '视频内流' then '竞价-视频内流'
          end as product,
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
        group by
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
      reddm.dm_soc_brand_coo_order_note_detail_day a
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
  case
    when module = '内容加热' then '品合'
    else module
  end as module,
  case
    when module in ('品合' '内容加热') then '品合'
    when module = '薯条' then '薯条'
    else product
  end as product,
  -911 as marketing_target,
  -911 as optimize_target,
  coalesce(market_target, '整体') as market_target,
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
  -911 as marketing_target,
  -911 as optimize_target,
  market_target,
  '0' as is_marketing_product,
  imp_cnt,
  click_cnt,
  like_cnt,
  fav_cnt,
  cmt_cnt,
  follow_cnt,
  share_cnt,
  0 as screenshot_cnt,
  0 as image_save_cnt,
  0 as cash_income_amt,
  0 as income_amt,
  open_sale_num,
  0 as direct_cash_cost,
  0 as direct_cost,
  0 as channel_cash_cost,
  0 as channel_cost,
  campaign_cnt,
  unit_cnt,
  brand_campaign_cnt,
  cpc_cost_budget_rate rtb_cost_income_amt,
  cpc_budget as rtb_budget_income_amt,
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
  redcdm.dm_ads_industry_product_account_td_df
where
  dtm = '{{yesterday_ds_nodash}}'
  )detail