insert  overwrite table redapp.app_ads_industry_ecm_seller_metrics_nd_di partition (dtm='{{ds_nodash}}')
select t1.seller_id,
seller_gmv_level,
deal_gmv,
--deal_gmv_1d, --对比
 -- pro_user_id,
 deal_gmv_30d,
  order_num_1d as order_num,
  goods_total_1d as goods_num,
  mini_deal_gmv,
  seller_live_dgmv,
   k_live_dgmv,
  goods_note_dgmv,
  
  message_dgmv,
  search_dgmv,
  coalesce(deal_gmv,0)-coalesce(seller_live_dgmv,0)-coalesce(k_live_dgmv,0)-coalesce(goods_note_dgmv,0)-coalesce(message_dgmv,0)-coalesce(search_dgmv,0) as other_dgmv,
  
  online_spu_num,
  online_spu_num_30d,
  online_spu_num_7d,
  sale_spu_cnt_1d,
  sale_spu_cnt_30d,
  sale_spu_cnt_7d,
  
 
  
  online_spu_num_mtd,
  sale_spu_cnt_mtd,
  live_num,
  live_duration,
  
  
  live_impression_cnt,
  live_click_cnt,
  live_view_user_num,
  live_view_cnt,
  live_view_duration,
  goods_detail_impression_cnt,
  goods_detail_impression_user_num,
  store_dgmv,
  store_order_num,
  store_buy_cnt,
  store_buy_user_num,
  store_spu_num,
  store_goods_num,
  store_add_cart_cnt,
  store_add_cart_user_num,
  store_buy_now_cnt,
  store_buy_now_user_num,
  carry_distributor_cnt_1d,
  carry_distributor_cnt_30d,
  carry_distributor_cnt_7d,
  valid_contract_1d,
  valid_contract_30d,
  valid_contract_7d,
  valid_bk_contract_1d,
  valid_bk_contract_30d,
  valid_bk_contract_7d,
  klive_cnt_1d,
  create_valid_contract_cnt_1d,
  create_valid_bk_contract_cnt_1d,

  carry_distributor_cnt_1m,
  valid_contract_cnt_1m,
  valid_bk_contract_cnt_1m,
    klive_dgmv_1d,
    null as live_all_dgmv_1d,
  dgmv_1d
from 
(select seller_id,
  pro_user_id,
  online_spu_num,
  online_spu_num_30d,
  online_spu_num_7d,
  sale_spu_cnt_1d,
  sale_spu_cnt_30d,
  sale_spu_cnt_7d,
  deal_gmv_1d,
  seller_gmv_level,
  order_num_1d,
  goods_total_1d,
  deal_gmv_30d,
  online_spu_num_mtd,
  sale_spu_cnt_mtd
from redcdm.dm_seller_metrics_nd_df
where dtm='{{ds_nodash}}'
)t1 
left join 
--小程序gmv 
(SELECT
  seller_id,
  sum(dgmv) as mini_deal_gmv
FROM
  reddw.dw_trd_o2o_order_package_day_inc
WHERE
  dtm = '{{ds_nodash}}'
  and is_valid=1 
  and channel_type=202
group by 1
)t2 
on t1.seller_id = t2.seller_id
left join 
(select seller_id,
 
  sum(carry_distributor_cnt_1d) as carry_distributor_cnt_1d,
  sum(carry_distributor_cnt_30d) as carry_distributor_cnt_30d,
  sum(carry_distributor_cnt_7d) as carry_distributor_cnt_7d,
  sum(valid_contract_1d) as valid_contract_1d,
  sum(valid_contract_30d) as valid_contract_30d,
  sum(valid_contract_7d) as valid_contract_7d,
  sum(valid_bk_contract_1d) as valid_bk_contract_1d,
  sum(valid_bk_contract_30d) as valid_bk_contract_30d,
  sum(valid_bk_contract_7d) as valid_bk_contract_7d,
  sum(klive_cnt_1d) as klive_cnt_1d,
  sum(create_valid_contract_cnt_1d) as create_valid_contract_cnt_1d,
  sum(create_valid_bk_contract_cnt_1d) as create_valid_bk_contract_cnt_1d,
  sum(klive_dgmv_1d) as klive_dgmv_1d,
  sum(dgmv_1d) as dgmv_1d,
  sum(carry_distributor_cnt_1m) as carry_distributor_cnt_1m,
  sum(valid_contract_cnt_1m) as valid_contract_cnt_1m,
  sum(valid_bk_contract_cnt_1m) as valid_bk_contract_cnt_1m
from redcdm.dm_ads_ecm_seller_distributor_metrics_di 
where dtm='{{ds_nodash}}'
group by 1
)t3
on t1.seller_id = t3.seller_id
left join 
--分流量类型gmv
(select seller_id,
  sum(deal_gmv) as deal_gmv,
  sum(if(channel_group = '直播' and channel = 'K播', deal_gmv,0)) as k_live_dgmv,
  sum(if(channel_group = '直播' and channel = '店播', deal_gmv,0)) as seller_live_dgmv,
  sum(if(channel_group = '笔记', deal_gmv,0)) as goods_note_dgmv,
  sum(if(channel_group = '搜索', deal_gmv,0)) as search_dgmv,
  sum(if(channel_group = '私信群聊', deal_gmv,0)) as message_dgmv
from redapp.app_ads_ecm_seller_account_detail_di
where dtm='{{ds_nodash}}'
group by 1
)t4 
on t4.seller_id = t1.seller_id
left join 
--店播
(select seller_id,
  --anchor_id,
  live_duration,
  live_impression_cnt,
  live_click_cnt,
  live_view_user_num,
  live_view_cnt,
  live_view_duration,
  goods_detail_impression_cnt,
  goods_detail_impression_user_num,
  store_dgmv,
  store_order_num,
  store_buy_cnt,
  store_buy_user_num,
  store_spu_num,
  store_goods_num,
  store_add_cart_cnt,
  store_add_cart_user_num,
  store_buy_now_cnt,
  store_buy_now_user_num,
  live_num
from redcdm.dm_ads_ecm_live_seller_metrics_2d_di 
where dtm='{{ds_nodash}}'
)t5
on t5.seller_id=t1.seller_id
