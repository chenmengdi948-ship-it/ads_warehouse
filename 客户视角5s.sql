insert OVERWRITE TABLE redcdm.dm_ads_ecm_live_seller_metrics_2d_di PARTITION (dtm)
select dur.seller_id,
  dur.anchor_id,
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
  dur.live_num,
  dur.dtm
from 
(select a.seller_id,a.user_id as anchor_id,dtm,sum(live_duration) as live_duration ,
count(1) as live_num
from 
  (
  --主播账号与商家id关系
  -- select 
  --     user_id -- 主子账号ID，relation_type=1时是主账号，relation_type=2时是子账号
  --     ,seller_id -- 商家seller_id
  -- from redcdm.dim_pro_soc_user_relation_df
  -- where dtm = '{{ds_nodash}}'
  -- and relation_type in (1)
  -- and is_recommend = 1
  -- and is_valid=1
  -- and seller_id <> 'UNKNOWN'
  -- group by 1,2
  select 
      pro_user_id as user_id -- 主子账号ID，relation_type=1时是主账号，relation_type=2时是子账号
      ,seller_id -- 商家seller_id
  from redcdm.dim_seller_base_df
  where dtm = '{{ds_nodash}}'
  ) a 
  join 
   (select dtm,
        live_id,
        anchor_seller_id,
        live_duration,
        anchor_id
    
    from
    redapp.app_live_traffic_engage_deal_backfill_2d_di
    where
    dtm >= f_getdate('{{ds_nodash}}', -50)
    and dtm <= '{{ds_nodash}}'
    and inside_goods_live_type='店播'
    )t1 
    on a.user_id = t1.anchor_id
    -- left join 
    -- (select anchor_id,seller_id
    -- from redcdm.dim_live_anchor_df 
    -- where dtm='{{ds_nodash}}'
    -- )t3 
    -- on t3.anchor_id = t1.anchor_id
group by 1,2,3
)dur 
left join 
(select
  --t1.anchor_id,
  t1.dtm,
  a.seller_id,
  count(distinct t1.live_id) as live_num,
  sum(live_impression_cnt) as live_impression_cnt,
  sum(live_click_cnt) as live_click_cnt,
  count(distinct case when live_view_cnt>0 then user_id else null end) as live_view_user_num,
  sum(live_view_cnt) as live_view_cnt,
  sum(live_view_duration) as live_view_duration,
  count(distinct case when is_seller_live=1 and goods_view_num > 0 then user_id end) as goods_detail_impression_cnt,
  sum(case when is_seller_live=1 then goods_view_num end)  as goods_detail_impression_user_num,
  sum(case when is_seller_live=1 then dgmv end) as store_dgmv,
  --store_order_num,
  sum(case when is_seller_live=1 then buy_cnt end) as store_buy_cnt,
  count(distinct case when is_seller_live=1 and goods_total > 0 then user_id end) as store_buy_user_num,
  --store_spu_num,
  sum(case when is_seller_live=1 then goods_total end) as store_goods_num,
  count(distinct case when is_seller_live=1 and add_cart_num > 0 then user_id end) as store_add_cart_user_num,
  sum(case when is_seller_live=1 then add_cart_num end) as store_add_cart_cnt,
  sum(case when is_seller_live=1 then instant_buy_num end) as store_buy_now_cnt,
  count(distinct case when is_seller_live=1 and instant_buy_num > 0 then user_id end) as store_buy_now_user_num,
  count(distinct if(goods_total > 0, order_id, null)) as store_order_num,
  count(distinct if(goods_total > 0, spu_id, null)) as store_spu_num
from
   (select
      pro_user_id as anchor_id -- 主子账号ID，relation_type=1时是主账号，relation_type=2时是子账号
      ,seller_id -- 商家seller_id
  from redcdm.dim_seller_base_df
  where dtm = '{{ds_nodash}}'
  ) a 
  join 
   (select dtm,
        live_id,
        anchor_seller_id,
        live_duration,
        anchor_id
    from
    redapp.app_live_traffic_engage_deal_backfill_2d_di
    where
    dtm >= f_getdate('{{ds_nodash}}', -50)
    and dtm <= '{{ds_nodash}}'
    and inside_goods_live_type='店播'
    )t1
    on a.anchor_id = t1.anchor_id
    
 join
 (select carrier_page_id as  live_id,
  user_id,
  spu_id,
  order_id,
  is_seller_live,
  dtm,
  sum(impression_num) as impression_num, -- 商卡曝光
  sum(click_num) as click_num, -- 商卡点击
  sum(buy_num) as buy_cnt,
  sum(goods_view_num) as goods_view_num, --商详曝光
  sum(agmv) as agmv,
  sum(goods_total) as goods_total,
  sum(instant_buy_num) as instant_buy_num, --立购
  sum(add_cart_num) as add_cart_num, --加车
  sum(deal_gmv) as dgmv,
  0 as live_impression_cnt,
  0 as live_click_cnt,
  -- live_view_user_num,
  0 as live_view_cnt,
  0 as live_view_duration
from reddm.dm_trd_user_channel_goods_indicators_lv1_day_inc
where
  dtm >= f_getdate('{{ds_nodash}}', -50)
  and dtm <= '{{ds_nodash}}'
  and carrier_page_name = '直播'
  and spam_level = 1
  --and is_seller_live = 1 --店播
  group  by 1,2,3,4,5 ,6
union all 
select live_id,
  user_id,
  null as spu_id,
  null as order_id,
  1 as is_seller_live,
  dtm,
  0 as impression_num, -- 商卡曝光
  0 as click_num, -- 商卡点击
  0 as buy_cnt,
  0 as goods_view_num, --商详曝光
  0 as agmv,
  0 as goods_total,
  0 as instant_buy_num, --立购
  0 as add_cart_num, --加车
  0 as dgmv,
  live_impression_cnt,
  live_click_cnt,
  -- live_view_user_num,
  live_view_cnt,
  live_view_duration
from  redcdm.dm_live_consume_live_user_traffic_engage_deal_1d_di  
where dtm >= f_getdate('{{ds_nodash}}', -50)
  and dtm <= '{{ds_nodash}}'
 )t2 
 on t1.live_id = t2.live_id and t2.dtm>=t1.dtm and t2.dtm<=f_getdate(t1.dtm, 1)
--  left join 
--  (select anchor_id,seller_id
--  from redcdm.dim_live_anchor_df 
--  where dtm='{{ds_nodash}}'
--  )t3 
--  on t3.anchor_id = t1.anchor_id
 group by 1,2
 )t1 

on dur.seller_id = t1.seller_id and dur.dtm=t1.dtm 