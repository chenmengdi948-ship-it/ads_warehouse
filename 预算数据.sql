select
  case when groups=1 then '场域/营销目的/企业号' when groups=3 then '场域/企业号' end as tag,
  '{{ds}}' as date_key,
  module,
  product,
  brand_account_id,
  case
    when ads_purpose in ('商品销量', '直播推广') then '闭环电商'
    when ads_purpose in ('行业商品推广') then '非闭环电商'
    when ads_purpose in ('销售线索收集', '私信营销', '私信营销', '销售线索收集', '线索广告') then '线索'
    else '种草'
  end as market_target,
  case
    when ads_purpose = '老计划' then 0
    when ads_purpose = '应用推广' then 1
    when ads_purpose = '销售线索收集' then 2
    when ads_purpose = '商品销量' then 3
    when ads_purpose = '笔记种草' then 4
    when ads_purpose = '私信营销' then 5
    when ads_purpose = '品牌知名度' then 6
    when ads_purpose = '品牌意向' then 7
    when ads_purpose = '直播推广' then 8
    when ads_purpose in ('私信营销', '销售线索收集', '线索广告') then 9
    when ads_purpose = '抢占关键词' then 10
    when ads_purpose = '抢占人群' then 11
    when ads_purpose = '加粉' then 12
    when ads_purpose = '行业商品推广' then 13
    else cast(ads_purpose as bigint)
  end as marketing_target, --营销目的
  sum(cost_special_campaign) as rtb_cost_income_amt,
  sum(min_budget) as rtb_budget_income_amt
from
  redcdm.dm_ads_rtb_budget_1d_di a
where
  dtm = '{{ds_nodash}}'
  and granularity = '分场域'
  and groups in (1, 3) --1-分场域营销目的，3-分场域
group by
  1,
  2,
  3,
  4,
  5,6,7
union all 
select
 '企业号' as tag,
  '{{ds}}' as date_key,
  module,
  product,
  brand_account_id,
  '整体' as market_target,
 null as marketing_target, --营销目的
  sum(cost_special_campaign) as rtb_cost_income_amt,
  sum(min_budget) as rtb_budget_income_amt
from
  redcdm.dm_ads_rtb_budget_1d_di a
where
  dtm = '{{ds_nodash}}'
  and granularity = '广告主粒度'
 -- and groups in (1, 3) --1-分场域营销目的，3-分场域
group by
  1,
  2,
  3,
  4,
  5