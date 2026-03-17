insert overwrite table redapp.app_ads_industry_ecm_user_di partition(dtm) 
select process_track_industry_name,
  fans_level,
  ti_cash_cost_180d_type,
  account_ecm_type,
  shop_user_type,
  groups,
  sum(buyer_cnt) as buyer_cnt,
  sum(new_platform_user_cnt) as new_platform_user_cnt,
  sum(new_seller_user_cnt) as new_seller_user_cnt,
  sum(total_buyer_cnt) as total_buyer_cnt,
  sum(total_new_platform_user_cnt) as total_new_platform_user_cnt,
  sum(total_new_seller_user_cnt) as total_new_seller_user_cnt,
  sum(ds_buyer_cnt) as ds_buyer_cnt,
  sum(ds_new_platform_user_cnt) as ds_new_platform_user_cnt,
  sum(ds_new_seller_user_cnt) as ds_new_seller_user_cnt,
  dtm
from 
(
select
 
  coalesce(process_track_industry_name,'整体') as process_track_industry_name,
  coalesce(fans_level,'整体') as fans_level,
  coalesce(ti_cash_cost_180d_type,'整体') as ti_cash_cost_180d_type,
  coalesce(account_ecm_type,'整体') as account_ecm_type,
  coalesce(shop_user_type,'整体') as shop_user_type,
  grouping__id as groups,
  count(distinct if(rgmv > 0, a.user_id, null)) as buyer_cnt,
  count(
    distinct if(
      rgmv > 0
      and is_platform_new_buy = 1,
      a.user_id,
      null
    )
  ) as new_platform_user_cnt,
  count(
    distinct if(
      rgmv > 0
      and is_seller_new_buy = 1,
      concat(a.user_id, a.brand_account_id),
      null
    )
  ) as new_seller_user_cnt,
  0 as total_buyer_cnt,
  0 as total_new_platform_user_cnt,
 0 as total_new_seller_user_cnt,
  0 as ds_buyer_cnt,
  0 as ds_new_platform_user_cnt,
  0 as ds_new_seller_user_cnt,
   a.dtm
from
  (
    select
      dtm,
      brand_account_id,
      user_id,
      sum(rgmv) as rgmv,
      is_platform_new_buy,
      is_seller_new_buy,
      substring(f_getdate(dtm), 1, 7) as stat_month
    from
      redapp.app_ads_cvr_ecm_closed_creativity_user_di
    where
      dtm between f_getdate('{{ds_nodash}}', -31)
      and '{{ds_nodash}}'
      and marketing_target in (3, 8, 14, 15)
      and rgmv > 0
    group by
      dtm,
      brand_account_id,
      user_id,
      --rgmv,
      is_platform_new_buy,
      is_seller_new_buy,
      substring(f_getdate(dtm), 1, 7)
  ) a
  join redapp.app_ads_industry_account_ecm_type_df w1 on a.stat_month = w1.stat_month
  and a.brand_account_id = w1.brand_account_id
  and w1.dtm = '{{ds_nodash}}'
group by
  a.dtm,
  process_track_industry_name,
  fans_level,
  ti_cash_cost_180d_type,
  account_ecm_type,
  shop_user_type
grouping sets((a.dtm),
 (a.dtm, shop_user_type),
 (a.dtm,process_track_industry_name),
 (a.dtm,process_track_industry_name, shop_user_type),
 (a.dtm,fans_level),
 (a.dtm, fans_level,shop_user_type),
 (a.dtm,account_ecm_type),
 (a.dtm, account_ecm_type,shop_user_type),
 (a.dtm,account_ecm_type,ti_cash_cost_180d_type),
 (a.dtm, account_ecm_type,ti_cash_cost_180d_type,shop_user_type),
 (a.dtm,process_track_industry_name, account_ecm_type),
 (a.dtm, process_track_industry_name, account_ecm_type,shop_user_type),
 (a.dtm,process_track_industry_name, account_ecm_type,ti_cash_cost_180d_type),
 (a.dtm, process_track_industry_name, account_ecm_type,ti_cash_cost_180d_type,shop_user_type)
 )




union all
select

  '整体' as process_track_industry_name,
  '整体' as fans_level,
  '整体' as ti_cash_cost_180d_type,
  '整体' as account_ecm_type,

  coalesce(shop_user_type,'整体') as shop_user_type,
  grouping__id as groups,
  0 as buyer_cnt,
  0 as new_platform_user_cnt,
  0 as new_seller_user_cnt,
  count(distinct if(deal_gmv>0, a.user_id, null)) as total_buyer_cnt,
  count(distinct if(deal_gmv>0 and user_tag_platform='新客', a.user_id, null)) as total_new_platform_user_cnt,
  count(distinct if(deal_gmv>0 and user_tag_seller = '新客', concat(a.user_id, a.seller_id), null)) as total_new_seller_user_cnt,
  0 as ds_buyer_cnt,
  0 as ds_new_platform_user_cnt,
  0 as ds_new_seller_user_cnt,
  
   a.dtm
from
(select dtm,
      a.seller_id,
      user_id,
      deal_gmv,
      user_tag_platform,
      user_tag_seller ,
       stat_month,
       coalesce(shop_user_type,'其他') as shop_user_type
from  (
    select
      dtm,
      seller_id,
      user_id,
      sum(deal_gmv) as deal_gmv,
      user_tag_platform,
      user_tag_seller ,
      substring(f_getdate(dtm), 1, 7) as stat_month
    from
      redapp.app_ads_trd_user_seller_account_detail_df
    where
      dtm between f_getdate('{{ds_nodash}}', -31)
      and '{{ds_nodash}}'
      and deal_gmv>0
    group by
      dtm,
      seller_id,
      user_id,
      user_tag_platform,
      user_tag_seller ,
      substring(f_getdate(dtm), 1, 7)
  ) a
  left join 
  (select seller_id,coalesce(shop_user_type,'其他') as shop_user_type
  from redapp.app_ads_trd_seller_cvr_detail_1d_di 
  where dtm='{{ds_nodash}}'
  group by seller_id,shop_user_type
  )seller 
  on seller.seller_id = a.seller_id
  )a
group by
  a.dtm,
  shop_user_type
grouping sets((a.dtm),
 (a.dtm, shop_user_type)
 )
union all
select
 coalesce(process_track_industry_name,'整体') as process_track_industry_name,
  coalesce(fans_level,'整体') as fans_level,
  coalesce(ti_cash_cost_180d_type,'整体') as ti_cash_cost_180d_type,
  coalesce(account_ecm_type,'整体') as account_ecm_type,
  coalesce(shop_user_type,'整体') as shop_user_type,
  grouping__id as groups,
  0 as buyer_cnt,
  0 as new_platform_user_cnt,
  0 as new_seller_user_cnt,
  0 as total_buyer_cnt,
  0 as total_new_platform_user_cnt,
  0 as total_new_seller_user_cnt,
  count(distinct if(deal_gmv>0, a.user_id, null)) as ds_buyer_cnt,
  count(distinct if(deal_gmv>0 and user_tag_platform='新客', a.user_id, null)) as ds_new_platform_user_cnt,
  count(distinct if(deal_gmv>0 and user_tag_seller = '新客', concat(a.user_id, a.seller_id), null)) as ds_new_seller_user_cnt,
   a.dtm
from
(select  a.dtm,
      a.seller_id,
      user_id,
      deal_gmv,
      user_tag_platform,
      user_tag_seller ,
       a.stat_month,
       process_track_industry_name,
      fans_level,
      ti_cash_cost_180d_type,
      account_ecm_type,
       coalesce(shop_user_type,'其他') as shop_user_type
from  (
    select
      dtm,
      seller_id,
      brand_account_id,
      user_id,
      sum(deal_gmv) as deal_gmv,
      user_tag_platform,
      user_tag_seller ,
      substring(f_getdate(dtm), 1, 7) as stat_month
    from
      redapp.app_ads_trd_user_seller_account_detail_df
    where
      dtm between f_getdate('{{ds_nodash}}', -31)
      and '{{ds_nodash}}'
      and deal_gmv>0
    group by
      dtm,
      seller_id,
      brand_account_id,
      user_id,
      user_tag_platform,
      user_tag_seller ,
      substring(f_getdate(dtm), 1, 7)
  ) a
  join 
 redapp.app_ads_industry_account_ecm_type_df w1 on a.stat_month = w1.stat_month
  and a.brand_account_id = w1.brand_account_id
  and w1.dtm = '{{ds_nodash}}'
  where ecm_closed_cash_cost>0 --有闭环电商投放
  )a
group by
  a.dtm,
  process_track_industry_name,
  fans_level,
  ti_cash_cost_180d_type,
  account_ecm_type,
  shop_user_type
grouping sets((a.dtm),
 --(a.dtm, shop_user_type),
 (a.dtm,process_track_industry_name),
 (a.dtm,process_track_industry_name, shop_user_type),
 (a.dtm,fans_level),
 (a.dtm, fans_level,shop_user_type),
 (a.dtm,account_ecm_type),
 (a.dtm, account_ecm_type,shop_user_type),
 (a.dtm,account_ecm_type,ti_cash_cost_180d_type),
 (a.dtm, account_ecm_type,ti_cash_cost_180d_type,shop_user_type),
 (a.dtm,process_track_industry_name, account_ecm_type),
 (a.dtm, process_track_industry_name, account_ecm_type,shop_user_type),
 (a.dtm,process_track_industry_name, account_ecm_type,ti_cash_cost_180d_type),
 (a.dtm, process_track_industry_name, account_ecm_type,ti_cash_cost_180d_type,shop_user_type)
 )
)info 
group by process_track_industry_name,
  fans_level,
  ti_cash_cost_180d_type,
  account_ecm_type,
  shop_user_type,
  groups,
  dtm
