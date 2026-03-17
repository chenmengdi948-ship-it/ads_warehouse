select *
from 
(select seller_id,
seller_user_id,
--carrier_user_id,
is_seller_user,
 
 sum(case when carrier_page_name='直播' then deal_gmv else 0 end) as live_dgmv,
 sum(case when carrier_page_name='直播' and channel1='K播' then deal_gmv else 0 end) as k_live_dgmv,
 sum(case when carrier_page_name='直播' and channel1='店铺' then deal_gmv else 0 end) as s_live_dgmv,
 sum(case when carrier_page_name='直播' and channel1='其他' then deal_gmv else 0 end) as other_live_dgmv,
 sum(deal_gmv) as dgmv
from reddm.dm_trd_user_channel_goods_indicators_lv1_day_inc 
where dtm='{{ds_nodash}}' and deal_gmv>0
group by  seller_id,
seller_user_id,
--carrier_user_id,
is_seller_user
)t1 
left join 
(select seller_id,
  shopname,
 
  user_id,
  user_name,
  main_category_name,
  industry,
  first_category_name,
  second_category_name,
  third_category_name
from reddw.dw_trd_seller_base_metrics_day
where dtm='{{ds_nodash}}'
)t2 
on t1.seller_id = t2.seller_id

select *
from 
(select brand_account_id,brand_account_name,
sum(case when marketing_target in (8,14) then coalesce(live_dgmv,live_rgmv) else 0 end) as ads_live_dgmv,
sum(case when marketing_target in (3,15) then click_rgmv_7d else 0 end) as ads_sx_live_dgmv,
sum(case when marketing_target in (8,14) then coalesce(live_dgmv,live_rgmv)  when marketing_target in (3,15) then click_rgmv_7d else 0 end) as live_rgmv
from redcdm.dm_ads_industry_product_advertiser_td_df
where dtm='{{ds_nodash}}'
group by brand_account_id,brand_account_name
)t1 
left join 
(select user_id as seller_user_id,
    seller_id,
    relation_type
  from redcdm.dim_pro_soc_user_relation_df
  WHERE
    dtm = '{{ds_nodash}}'
    and seller_id<>'UNKNOWN'
    and is_valid=1
    and bind_type in (1,2)
  )account 
  on t1.brand_account_id =account.seller_user_id 
  