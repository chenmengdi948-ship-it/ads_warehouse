select
  t1.goods_id,
  channel_group1,
  channel1,
  goods_live_anchor_type,
  traffic_type_group,
  new_carrier_name_group,
  new_carrier_name,
  channel_group_name,
  carrier_page_name,
  carrier_page_id,
  seller_id,
  seller_user_id,
  carrier_user_id,
  traffic_type,
  spu_id,
  new_item_id,
  spam_level,
  entrance_channel_group,
  entrance_channel,
  channel_group_name_v2,
  add_wishlist_num,
  add_cart_num,
  instant_buy_num,
  goods_view_num,
  goods_total,
  buy_num,
  rgmv,
  pgmv,
  agmv,
  gmv,
  deal_gmv,
  impression_num,
  click_num,
  add_cart_agmv,
  instant_buy_agmv,
  user_real_pay_amt,
  seller_real_income_amt
from
  (
    SELECT
      goods_id,
      channel_group1,
      channel1,
      goods_live_anchor_type,
      traffic_type_group,
      new_carrier_name_group,
      new_carrier_name,
      channel_group_name,
      carrier_page_name,
      carrier_page_id,
      seller_id,
      seller_user_id,
      carrier_user_id,
      spu_id,
      new_item_id,
      spam_level,
      entrance_channel_group,
      entrance_channel,
      channel_group_name_v2,
      traffic_type,
      sum(add_wishlist_num) as add_wishlist_num,
      sum(add_cart_num) as add_cart_num,
      sum(instant_buy_num) as instant_buy_num,
      sum(goods_view_num) as goods_view_num,
      sum(goods_total) as goods_total,
      sum(buy_num) as buy_num,
      sum(rgmv) as rgmv,
      sum(pgmv) as pgmv,
      sum(agmv) as agmv,
      sum(gmv) as gmv,
      sum(deal_gmv) as deal_gmv,
      sum(impression_num) as impression_num,
      sum(click_num) as click_num,
      --sum() as traffic_type,
      -- sum(channel_group_name_v2) as channel_group_name_v2,
      sum(add_cart_agmv) as add_cart_agmv,
      sum(instant_buy_agmv) as instant_buy_agmv,
      sum(user_real_pay_amt) as user_real_pay_amt,
      sum(seller_real_income_amt) as seller_real_income_amt
    FROM
      reddm.dm_trd_user_channel_goods_indicators_lv1_day_inc
    WHERE
      dtm = '{{ds_nodash}}'
    group by
      goods_id,
      channel_group1,
      channel1,
      goods_live_anchor_type,
      traffic_type_group,
      new_carrier_name_group,
      new_carrier_name,
      channel_group_name,
      carrier_page_name,
      carrier_page_id,
      seller_id,
      seller_user_id,
      carrier_user_id,
      spu_id,
      new_item_id,
      spam_level,
      entrance_channel_group,
      entrance_channel,
      channel_group_name_v2,
      traffic_type,
  ) t1
  join (
    select
      goods_id
    from
      ads_databank.dim_spu_goods_base_category_df
    where
      dtm = '{{ds_nodash}}'
      and shop_type = 1
    group by
      1
  ) t2 on t1.goods_id = t2.goods_id


















  ------------------------
  SET
  "kyuubi.spark.option.--conf spark.sql.crossJoin.enabled=true";

SET
  "kyuubi.spark.option.--conf spark.redExtension.allowBroadcastNestedLoopJoin=true";

select
  t3.spu_id,
  t3.date_key,
  coalesce(ti_cash_income_amt, 0) as ti_cash_income_amt,
  sum(ti_cash_income_amt) over(
    partition by t3.spu_id
    order by
      cast(t3.date_key as date) asc rows between 29 PRECEDING
      and 0 FOLLOWING
  ) as ti_cash_income_amt_30d,
  sum(ti_cash_income_amt) over (
    partition by t3.spu_id,
    stat_month
    order by
      cast(t3.date_key as date) asc rows between unbounded PRECEDING
      and current row
  ) as ti_cash_income_amt_1m
from
  (
    select
      spu_id,
      dt as date_key,
      substring(dt, 1, 7) as stat_month
    from
      (
        select
          spu_id
        from
          redcdm.dws_ads_note_spu_product_income_detail_td_df
        where
          dtm = '{{ds_nodash}}'
          and date_key >= '2022-12-01'
          and marketing_target_msg = '种草'
        group by
          spu_id
      ) t1
      left join (
        select
          dt
        from
          redcdm.dim_ads_date_df
        where
          dtm = 'all'
          and dt >= '2022-12-01'
          and dt <= '{{ds}}'
      ) t2 on 1 = 1
  ) t3
  left join (
    select
      date_key,
      spu_id,
      sum(cash_income_amt) as ti_cash_income_amt
    from
      redcdm.dws_ads_note_spu_product_income_detail_td_df
    where
      dtm = '{{ds_nodash}}'
      and date_key >= '2022-12-01'
      and marketing_target_msg = '种草'
    group by
      date_key,
      spu_id
  ) t4 on t3.date_key = t4.date_key
  and t3.spu_id = t4.spu_id