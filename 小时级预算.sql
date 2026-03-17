--redcdm.dwd_ads_rtb_campaign_advertiser_budget_hi
--计划分场域预算
with base as
(select tag,
  hh,
  campaign_id,
  advertiser_id,
  0 as advertiser_budget,
  campaign_day_budget/100 as campaign_budget,
  biz_product_type,
  marketing_target,
  case
              when marketing_target in(3,8,14) then '闭环电商'
              when marketing_target in(13)  then '非闭环电商'
              when marketing_target in(2,5,9) then '线索' else '种草' 
             end as market_target,
 search_unit_budget_ratio,
  video_feed_unit_budget_ratio,
  budget_extension_field,
  case when biz_product_type='全站智投' then campaign_day_budget/100*search_unit_budget_ratio else 0 end as search_campaign_budget,
   case when biz_product_type='全站智投' then campaign_day_budget/100*video_feed_unit_budget_ratio else 0 end as video_campaign_budget,
   case when biz_product_type='全站智投' then campaign_day_budget/100*(1-search_unit_budget_ratio-video_feed_unit_budget_ratio) else 0 end  as explore_campaign_budget
from 
(select '计划' as tag,
  hh,
  id as campaign_id,
  advertiser_id,
  0 as advertiser_budget,
  campaign_day_budget,
  marketing_target,
  case when placement=1 then '信息流' when  placement=2 then '搜索' when  placement=7 then '视频内流' when placement=4 then '全站智投'  else placement end as biz_product_type,
  get_json_object(budget_extension_field, '$.search_unit_budget_ratio') as search_unit_budget_ratio,
  get_json_object(budget_extension_field, '$.video_feed_unit_budget_ratio')  as video_feed_unit_budget_ratio,
  budget_extension_field,
  hh
from redods.ods_shequ_feed_ads_t_ads_rtb_campaign_hf
where dtm='{{ds_nodash}}' and hh=(select max(hh) from redods.ods_shequ_feed_ads_t_ads_rtb_campaign_hf where dtm='{{ds_nodash}}') and campaign_day_budget>0 and placement in (1,2,4,7)
)t1
) ,
adv as 
(select 4 as tag,
id as advertiser_id,
hh,
'' as product,
'' as market_target,
'' as campaign_id,
sum(account_day_budget)/100 as advertiser_budget,
0 as campaign_budget
from redods.ods_shequ_feed_ads_t_advertiser_hf 
where dtm='{{ds_nodash}}' and hh=(select max(hh) from redods.ods_shequ_feed_ads_t_advertiser_hf where dtm='{{ds_nodash}}') and account_day_budget>0
group by 1,2,3
),
moudle_adv_target as 
(select 
1 as tag,
advertiser_id,
'搜索' as product,
market_target,
hh,
'' as campaign_id,
sum(campaign_budget) as campaign_budget,
sum(search_campaign_budget) as campaign_module_budget
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
sum(video_campaign_budget) as campaign_module_budget
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
sum(explore_campaign_budget) as campaign_module_budget
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
sum(campaign_budget) as campaign_module_budget
from base
where biz_product_type<>'全站智投'
group by 1,2,3,4,5
)
insert overwrite table redcdm.dwd_ads_rtb_campaign_advertiser_budget_hi partition (dtm, hh)
--广告主*分场域*营销目标
select t1.tag,
  t1.advertiser_id,
  t1.product,
  t1.market_target,
  t1.campaign_id,
  adv.advertiser_budget as advertiser_budget,
  t1.campaign_module_budget,
  t1.campaign_budget,
  least(t1.campaign_module_budget,advertiser_budget) as min_budget,
  '{{ds_nodash}}' as dtm,
  hh
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
  campaign_module_budget
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
  sum(campaign_module_budget) as campaign_module_budget
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
sum(campaign_budget) as campaign_module_budget --两个一样
from base
group by 1,2,3,4,5,6
)t1 
left join 
adv 
on adv.advertiser_id=t1.advertiser_id
union all 
select tag,
  advertiser_id,
  product,
  market_target,
  campaign_id,
  advertiser_budget,
  0 as campaign_module_budget,
  0 as campaign_budget,
  advertiser_budget as min_budget,
  '{{ds_nodash}}' as dtm,
  hh
from adv
