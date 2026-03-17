
insert overwrite table redcdm.dws_ads_industry_product_account_1d_di partition(dtm = '{{ ds_nodash }}')
select
  date_key
  ,module
  ,product
  ,brand_account_id
  ,sum(a.imp_cnt) as imp_cnt
  ,sum(a.click_cnt) as click_cnt
  ,sum(a.like_cnt) as like_cnt
  ,sum(a.fav_cnt) as fav_cnt
  ,sum(a.cmt_cnt) as cmt_cnt
  ,sum(a.share_cnt) as share_cnt
  ,sum(a.follow_cnt) as follow_cnt
  ,sum(a.open_sale_num) as open_sale_num
  ,sum(a.campaign_cnt) as campaign_cnt
  ,sum(a.unit_cnt) as unit_cnt
  ,sum(a.brand_campaign_cnt) as brand_campaign_cnt
  ,sum(a.cpc_cost_budget_rate) as cpc_cost_budget_rate
  ,sum(a.cpc_budget) as cpc_budget
  ,market_target
from (
  -- 品牌广告
  select
    '{{ds}}' as date_key
    ,'品牌' as module
    ,case 
      when a.ads_container = 'app_open' then '开屏' 
      when a.ads_container = 'search_third' then '搜索第三位' 
      when a.ads_container = 'feed' then '信息流GD' 
      when a.ads_container = 'search_brand_area' then '品牌专区' 
      -- when a.ads_container = 'fire_topic' then '火焰话题' 
      else '品牌其他' 
    end as product
    ,a.advertiser_id as brand_account_id
    ,'整体' as market_target
    ,sum(a.imp_num) as imp_cnt
    ,sum(a.click_num) as click_cnt
    ,sum(a.like_num) as like_cnt
    ,sum(a.fav_num) as fav_cnt
    ,sum(a.cmt_num) as cmt_cnt
    ,sum(a.share_num) as share_cnt
    ,sum(a.follow_num) as follow_cnt
    ,0 as open_sale_num
    ,count(distinct campaign_id) as campaign_cnt
    ,count(distinct unit_id) as unit_cnt
    ,max(coalesce(b.campaign_cnt,0)) as brand_campaign_cnt
    ,0 as cpc_cost_budget_rate
    ,0 as cpc_budget
    
  from 
    redst.st_ads_brand_creativity_loc_metrics_day_inc a
  left join (
    select
       advertiser_id as brand_account_id
      ,count(distinct campaign_id) as campaign_cnt
    from 
      redst.st_ads_brand_creativity_loc_metrics_day_inc
    where
      dtm = '{{ds_nodash}}'
    group by
      1
  ) b on a.advertiser_id = b.brand_account_id
  join 
    reddw.dw_ads_account_day c on c.dtm = '{{ds_nodash}}' and a.advertiser_id = c.user_id and coalesce(c.company_name,'') <> 'offlineMockCompanyName' --剔除内广
  where
    a.dtm = '{{ds_nodash}}'
  group by 
    3,4
union all 
  -- 效果
  select 
    '{{ds}}' as date_key
    ,'效果' as module
    ,case 
      when a.module = '发现feed' then '竞价-信息流' 
      when a.module = '搜索feed' then '竞价-搜索' 
      when a.module = '视频内流' then '竞价-视频内流'
    end as product
    ,a.brand_account_id
    ,case when a.marketing_target in (3,8) then '闭环电商'
     when a.marketing_target in (13) then '非闭环电商'
     when a.marketing_target in (2,5,9) then '线索'
     when a.marketing_target not in (3,8,2,5,9,13) then '种草'
     end as market_target
    ,sum(a.imp_num) as imp_cnt
    ,sum(a.click_num) as click_cnt
    ,sum(a.like_num) as like_cnt
    ,sum(a.fav_num) as fav_cnt
    ,sum(a.comment_num) as cmt_cnt
    ,sum(a.share_num) as share_cnt
    ,sum(a.follow_num) as follow_cnt
    ,0 as open_sale_num
    ,count(distinct campaign_id) as campaign_cnt
    ,count(distinct unit_id) as unit_cnt
    ,0 as brand_campaign_cnt
    ,0 as cpc_cost_budget_rate
    ,0 as cpc_budget
    
  from 
    reddw.dw_ads_wide_cpc_creativity_base_day_inc a
  where
    a.dtm = '{{ds_nodash}}'
    and a.is_effective = 1
  group by 
    3,4,5
  -- 迪奥：薯条流量暂时没下游，保onedash时效，先下线。后续替换成新公共层
  -- -- 薯条
  union all 
  select 
    '{{ds}}' as date_key
    ,'薯条' as module
    ,case when module='薯条' then '薯条' else product end  as product
    ,chips_user_id as brand_account_id
    ,case when module in '整体' as market_target
    ,sum(imp_cnt) as imp_cnt
    ,sum(click_cnt) as click_cnt
    ,sum(like_cnt) as like_cnt
    ,sum(fav_cnt) as fav_cnt
    ,sum(cmt_cnt) as cmt_cnt
    ,sum(share_cnt) as share_cnt
    ,sum(follow_cnt) as follow_cnt
  
    ,0 as open_sale_num
    ,0 as campaign_cnt
    ,0 as unit_cnt
    ,0 as brand_campaign_cnt
    ,0 as cpc_cost_budget_rate
    ,0 as cpc_budget
  from 
    redcdm.dm_ads_creativity_cube_1d_di  a 
  where
    dtm = '{{ds_nodash}}'
    and module in ('效果','品牌','薯条')
  group by 
    4
  
  union all 
  -- 开屏售卖轮次-品牌中间层
  select
    '{{ds}}' as date_key
    ,'品牌' as module
    ,'开屏' as product
    ,brand_account_id
    ,'整体' as market_target
    ,0 as imp_cnt
    ,0 as click_cnt
    ,0 as like_cnt
    ,0 as fav_cnt
    ,0 as cmt_cnt
    ,0 as share_cnt
    ,0 as follow_cnt
   
    ,sum(
      case
        when unit_type = 'CPM' then ads_num * 1000
        when unit_type = '天/轮' then ads_num * 10000000
        when unit_type = '天/半轮' then ads_num * 5000000
        else ads_num end
    ) / 10000000  as open_sale_num
    ,0 as campaign_cnt
    ,0 as unit_cnt
    ,0 as brand_campaign_cnt
    ,0 as cpc_cost_budget_rate
    ,0 as cpc_budget
    
  from
    reddw.dw_ads_crm_brand_stats_day a 
  join 
    reddw.dw_ads_account_day b on b.dtm = '{{ds_nodash}}' and a.brand_account_id = b.user_id and coalesce(b.company_name,'') <> 'offlineMockCompanyName' --剔除内广
  where
    a.dtm = '{{ds_nodash}}'
    and a.product_name = '开屏'
    and coalesce(a.company_name,'') <> 'offlineMockCompanyName'
    and case when a.launch_start_date >= '2021-01-01' then a.settle_date else a.launch_start_date end = '{{ds}}'
  group by
    4
  
  union all 
  -- 品合-社区流量
  select 
    '{{ds}}' as date_key
    ,'品合' as module
    ,'品合' as product
    ,report_brand_user_id as brand_account_id
    ,'整体' as market_target
    ,sum(b.imp_num) as imp_cnt
    ,sum(b.click_num) as click_cnt
    ,sum(b.like_num) as like_cnt
    ,sum(b.fav_num) as fav_cnt
    ,sum(b.cmt_num) as cmt_cnt
    ,sum(b.share_num) as share_cnt
    ,sum(b.follow_from_discovery_num) as follow_cnt
    
    ,0 as open_sale_num
    ,0 as campaign_cnt
    ,0 as unit_cnt
    ,0 as brand_campaign_cnt
    ,0 as cpc_cost_budget_rate
    ,0 as cpc_budget
  from 
    reddm.dm_soc_brand_coo_order_note_detail_day a 
  left join 
    reddm.dm_soc_discovery_engagement_new_day_inc b 
  on 
    b.dtm = '{{ds_nodash}}'
    and a.note_id = b.discovery_id
  where 
    a.dtm = '{{ds_nodash}}'
  group by 
    4
  union all 
--预算新模型
  select
    '{{ds}}' as date_key
    ,'效果' as module
    ,case 
        when a.module = '发现feed' then '竞价-信息流' 
        when a.module = '搜索feed' then '竞价-搜索' 
        when a.module = '视频内流' then '竞价-视频内流'
    end as product
    ,brand_account_id
    ,case when ads_purpose in ('商品销量','直播推广') then '闭环电商' 
      when ads_purpose in ('13','行业商品推广' ) then '非闭环电商' 
      when ads_purpose in ('销售线索收集','私信营销','9','线索广告') then '线索' 
      when ads_purpose not in ('商品销量','直播推广','9','13','线索广告','行业商品推广' ,'销售线索收集','私信营销') then '种草' end as market_target --ads_purpose字段就是marketing_type对应中文描述
    ,0 as imp_cnt
    ,0 as click_cnt
    ,0 as like_cnt
    ,0 as fav_cnt
    ,0 as cmt_cnt
    ,0 as share_cnt
    ,0 as follow_cnt
    
    ,0 as open_sale_num
    ,0 as campaign_cnt
    ,0 as unit_cnt
    ,0 as brand_campaign_cnt
    ,sum(cost_special_campaign) as cpc_cost_budget_rate
    ,sum(min_budget) as cpc_budget  
  from 
    redcdm.dm_ads_rtb_budget_1d_di a
  where
    dtm = '{{ds_nodash}}'
    and granularity = '分场域'
    and groups = 3
  group by 
    3,4,5
) a
group by 
  date_key
  ,module
  ,product
  ,brand_account_id
  ,market_target
;