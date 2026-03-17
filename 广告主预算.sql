--redcdm.dwd_ads_rtb_campaign_advertiser_budget_hf
--计划分场域预算
with cost as 

(select
  case
    when placement = 1 then '信息流'
    when placement = 2 then '搜索'
    when placement = 4 then '全站智投'
    when placement = 7 then '视频内流'
  end as product,
  campaign_id,
  
  advertiser_id,
  case
    when marketing_target in (3, 8, 14,15) then '闭环电商'
    when marketing_target = 13 then '非闭环电商'
    when marketing_target in (2, 5, 9) then '线索'
    when marketing_target not in (3, 8, 2, 5, 9, 13, 14,15) then '种草'
  end as market_target_type,
   '{{ds_nodash}}' as dtm,
  sum(if(dtm = '{{ds_nodash}}', fee, 0)) / 100.0 as cost,
  sum(if(dtm = '{{ds_1_days_ago_nodash}}', fee, 0)) / 100.0 as ystd_cost
from
  redods.ad_full_log
where
  (dtm='{{ds_nodash}}' or dtm='{{ds_1_days_ago_nodash}}')
  and hh <= '24'
  and conversion_type = 'ENGAGEMENT'
  and (
    spam_level is null
    or spam_level = 0
  )
  and placement in (1, 2, 4, 7)
  and coalesce(fee, 0) > 0
group by
  1,
  2,
  3,
  4
  ),
--只看有消耗的
base as
(select '计划' as tag,
  hh,
  t1.campaign_id,
  advertiser_id,
  0 as advertiser_budget,
  campaign_day_budget/100 as campaign_budget,
  biz_product_type,
  marketing_target,
  case
              when marketing_target in(3,8,14,15) then '闭环电商'
              when marketing_target in(13)  then '非闭环电商'
              when marketing_target in(2,5,9) then '线索' else '种草' 
             end as market_target,
 search_unit_budget_ratio,
  video_feed_unit_budget_ratio,
  budget_extension_field,
  case when biz_product_type='全站智投' then campaign_day_budget/100*search_unit_budget_ratio else 0 end as search_campaign_budget,
   case when biz_product_type='全站智投' then campaign_day_budget/100*video_feed_unit_budget_ratio else 0 end as video_campaign_budget,
   case when biz_product_type='全站智投' then campaign_day_budget/100*(1-search_unit_budget_ratio-video_feed_unit_budget_ratio) else 0 end  as explore_campaign_budget,
   explore_cost,
   video_cost,
   search_cost,
   cost
from 
(select  campaign_id,
  sum(if(product='信息流',cost,0)) as explore_cost,
  sum(if(product='视频内流',cost,0)) as video_cost,
  sum(if(product='搜索',cost,0)) as search_cost,
  sum(cost) as cost
from cost
group by campaign_id
)cost
join
(select '计划' as tag,
  hh,
  id as campaign_id,
  advertiser_id,
  0 as advertiser_budget,
  case when limit_day_budget=0 then 9999999999.99 when limit_day_budget=1 then campaign_day_budget end as campaign_day_budget,
  marketing_target,
  case when placement=1 then '信息流' when  placement=2 then '搜索' when  placement=7 then '视频内流' when placement=4 then '全站智投'  else placement end as biz_product_type,
  get_json_object(budget_extension_field, '$.search_unit_budget_ratio') as search_unit_budget_ratio,
  get_json_object(budget_extension_field, '$.video_feed_unit_budget_ratio')  as video_feed_unit_budget_ratio,
  budget_extension_field
from redods.ods_shequ_feed_ads_t_ads_rtb_campaign_hf
where dtm='{{ds_nodash}}' and hh=(select max(hh) from redods.ods_shequ_feed_ads_t_ads_rtb_campaign_hf where dtm='{{ds_nodash}}')  and placement in (1,2,4,7)
and state=1 --and enable=1
)t1
on cost.campaign_id = t1.campaign_id
) ,
ystd_base as
(select '计划' as tag,
  hh,
  t1.campaign_id,
  advertiser_id,
  0 as advertiser_budget,
  ystd_campaign_day_budget/100 as ystd_campaign_budget,
  ystd_cost
from 
(select  campaign_id,
  sum(ystd_cost) as ystd_cost
from cost
group by campaign_id
)cost
join
(select '计划' as tag,
  hh,
  id as campaign_id,
  advertiser_id,
  0 as advertiser_budget,
  case when limit_day_budget=0 then 9999999999.99 when limit_day_budget=1 then campaign_day_budget end as ystd_campaign_day_budget
from redods.ods_shequ_feed_ads_t_ads_rtb_campaign_hf
where dtm='{{ds_1_days_ago_nodash}}' and hh=(select max(hh) from redods.ods_shequ_feed_ads_t_ads_rtb_campaign_hf where dtm='{{ds_nodash}}')  and placement in (1,2,4,7)
and state=1 --and enable=1
)t1
on cost.campaign_id = t1.campaign_id
) ,
adv as 
(select tag,
t1.advertiser_id,
t1.hh,
 product,
market_target,
campaign_id,
advertiser_budget,
campaign_budget,
 total_balance,
 ystd_advertiser_budget,
 ystd_total_balance
from 
(select 3 as tag,
id as advertiser_id,
hh,
'' as product,
'' as market_target,
'' as campaign_id,
sum(if(dtm = '{{ds_nodash}}',if(limit_day_budget=0,9999999999.99,account_day_budget),0))/100 as advertiser_budget,
sum(if(dtm = '{{ds_1_days_ago_nodash}}',if(limit_day_budget=0,9999999999.99,account_day_budget),0))/100 as ystd_advertiser_budget,
0 as campaign_budget
from redods.ods_shequ_feed_ads_t_advertiser_hf 
where (dtm='{{ds_nodash}}' or dtm='{{ds_1_days_ago_nodash}}' ) and hh=(select max(hh) from redods.ods_shequ_feed_ads_t_advertiser_hf where dtm='{{ds_nodash}}') 
group by 1,2,3
)t1
left join
(select advertiser_id,
  hh,
  sum(total_balance) as total_balance,
  sum(ystd_total_balance) as ystd_total_balance
from 
(  --账户小时余额
  select
    subject_id as virtual_seller_id,
    hh,
    sum(if(dtm = '{{ds_1_days_ago_nodash}}',cast(available_balance as double),0)) as ystd_total_balance,
    sum(if(dtm = '{{ds_nodash}}',cast(available_balance as double),0)) as total_balance
  from
    redods.ods_gondar_base_account_hf
  where
     (dtm='{{ds_nodash}}' or dtm='{{ds_1_days_ago_nodash}}' )
    and hh <= '{{ts[11:13]}}'
    and account_type='CASH' --有多种类型余额，如现金和授信，crm展示现金余额
  group by
    1,
    2
    )t1 
 join 
  (select virtual_seller_id,
  rtb_advertiser_id as advertiser_id
  from reddw.dw_ads_crm_advertiser_day
  where dtm = '{{ds_1_days_ago_nodash}}' and rtb_advertiser_id <> 0
  ) t0
  on t1.virtual_seller_id=t0.virtual_seller_id
  group by advertiser_id,hh
  )t2 
  on t1.advertiser_id=t2.advertiser_id
  and t1.hh=t2.hh
),
moudle_adv_target as 
(select tag,
  advertiser_id,
  product,
  market_target,
  hh,
  campaign_id,
  sum(campaign_budget) as campaign_budget,
  sum(campaign_module_budget) as campaign_module_budget,
  sum(cost) as cost
from 
(select 
1 as tag,
advertiser_id,
'搜索' as product,
market_target,
hh,
'' as campaign_id,
sum(campaign_budget) as campaign_budget,
sum(search_campaign_budget) as campaign_module_budget,
sum(search_cost) as cost
from base
where biz_product_type='全站智投'
group by 1,2,3,4,5
union all 
select 
1 as tag,
advertiser_id,
'视频内流' as product,
market_target,
hh,
'' as campaign_id,
sum(campaign_budget) as campaign_budget,
sum(video_campaign_budget) as campaign_module_budget,
sum(video_cost) as cost
from base
where biz_product_type='全站智投'
group by 1,2,3,4,5
union all 
select 
1 as tag,
advertiser_id,
'信息流' as product,
market_target,
hh,
'' as campaign_id,
sum(campaign_budget) as campaign_budget,
sum(explore_campaign_budget) as campaign_module_budget,
sum(explore_cost) as cost
from base
where biz_product_type='全站智投'
group by 1,2,3,4,5
union all
select 
1 as tag,
advertiser_id,
biz_product_type as product,
market_target,
hh,
'' as campaign_id,
sum(campaign_budget) as campaign_budget,
sum(campaign_budget) as campaign_module_budget,
sum(cost) as cost
from base
where biz_product_type<>'全站智投'
group by 1,2,3,4,5
)t1
group by 1,2,3,4,5,6
)
insert overwrite table redcdm.dwd_ads_rtb_campaign_advertiser_budget_hf partition (dtm, hh)
--广告主*分场域*营销目标
select t1.tag,
  t1.advertiser_id,
  t1.product,
  t1.market_target,
  t1.campaign_id,
  t1.campaign_module_budget,
  t1.campaign_budget,
  adv.advertiser_budget as advertiser_budget,
  least(t1.campaign_module_budget,advertiser_budget,t1.cost+total_balance) as min_budget,
  0 as ystd_advertiser_budget,
  '{{ds_nodash}}' as dtm,
  t1.hh
from
  (
    --广告主*分场域*营销目标
  select  tag,
    advertiser_id,
    product,
    market_target,
    campaign_id,
    hh,
    campaign_budget,
    campaign_module_budget,
    cost
  from moudle_adv_target
  union all 
  --广告主*分场域
  select 2 as tag,
    advertiser_id,
    product,
    '' as market_target,
    campaign_id, --是''
    hh,
    sum(campaign_budget) as campaign_budget,
    sum(campaign_module_budget) as campaign_module_budget,
    sum(cost) as cost
  from moudle_adv_target
  group by 1,2,3,4,5,6
  union all 
  --计划粒度
  select 4 as tag,
  advertiser_id as advertiser_id,
  '' as product,
  market_target,
  campaign_id,
  hh,
  sum(campaign_budget) as campaign_budget,
  sum(campaign_budget) as campaign_module_budget, --两个一样
  sum(cost) as cost
  from base
  group by 1,2,3,4,5,6
  )t1 
  left join 
  adv 
  on adv.advertiser_id=t1.advertiser_id
  and adv.hh=t1.hh
union all 
select adv.tag,
  adv.advertiser_id,
  adv.product,
  adv.market_target,
  adv.campaign_id,
  a1.campaign_module_budget,
  a1.campaign_budget,
  least(a1.campaign_module_budget,adv.advertiser_budget,a1.cost+adv.total_balance) as advertiser_budget,
  least(a1.campaign_module_budget,adv.advertiser_budget,a1.cost+adv.total_balance) as min_budget,
  least(a2.ystd_campaign_module_budget,adv.ystd_advertiser_budget,a2.ystd_cost+adv.ystd_total_balance) as ystd_advertiser_budget,
  '{{ds_nodash}}' as dtm,
  adv.hh
from adv 
left join 
  (select
    advertiser_id as advertiser_id,
    hh,
    sum(campaign_budget) as campaign_budget,
    sum(campaign_budget) as campaign_module_budget, --两个一样
    sum(cost) as cost
  from base
  group by 1,2
  )a1 
  on a1.advertiser_id=adv.advertiser_id and a1.hh=adv.hh
left join 
  (select
    advertiser_id as advertiser_id,
    hh,
    sum(ystd_campaign_budget) as ystd_campaign_budget,
    sum(ystd_campaign_budget) as ystd_campaign_module_budget, --两个一样
    sum(ystd_cost) as ystd_cost
  from ystd_base
  group by 1,2
  )a2
  on a2.advertiser_id=adv.advertiser_id and a2.hh=adv.hh
