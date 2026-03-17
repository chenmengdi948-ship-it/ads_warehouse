drop table
  if exists temp.temp_app_ads_indsutry_spu_mapping_metrics_detail_df_{{ds_nodash}};

create table
  temp.temp_app_ads_indsutry_spu_mapping_metrics_detail_df_{{ds_nodash}}
select date_key,
  spu_id,
  marketing_target,
  module,
  product,
  optimize_target,
  sum(total_amount) as total_amount,
  sum(purchase_rgmv) as purchase_rgmv,
  sum(imp_cnt) as imp_cnt,
  sum(income_amt) as income_amt,
  sum(cash_income_amt) as cash_income_amt
from 
(select
  date_key,
  spu_id,
  marketing_target,
  module,
  product,
  optimize_target,
  sum(total_amount) as total_amount,
  sum(purchase_rgmv) as purchase_rgmv,
  sum(imp_cnt) as imp_cnt,
  0 as income_amt,
  0 as cash_income_amt
from
  (
    select
      f_getdate(a.dtm) as date_key,
      if(
        coalesce(a.goods_id, '') <> '',
        a.goods_id,
        b.goods_id
      ) as goods_id,
      marketing_target,
      module,
      product,
      optimize_target,
      sum(total_amount) as total_amount,
      sum(
        coalesce(purchase_rgmv, 0) + coalesce(mini_purchase_rgmv, 0)
      ) as purchase_rgmv,
      sum(unique_imp_cnt) as imp_cnt
    from
      redcdm.dm_ads_rtb_creativity_1d_di as a
      left join (
        select
          discovery_id,
          max(goods_id) as goods_id
        from
          reddw.dw_soc_discovery_delta_7_day
        where
          dtm = '{{ds_nodash}}'
        group by
          1
      ) as b on a.note_id = b.discovery_id
    where
      a.dtm between '20230101' and '{{ds_nodash}}' -- and a.marketing_target in (3,15,8,14)
      and (
        a.total_amount > 0
        or is_effective = 1
      )
    group by
      f_getdate(dtm),
      if(
        coalesce(a.goods_id, '') <> '',
        a.goods_id,
        b.goods_id
      ),
      marketing_target,
      module,
      product,
      optimize_target
  ) t1
  join (
    select
      spu_id,
      goods_id
    from
      ads_databank.dim_spu_goods_base_category_df
    where
      dtm = '{{ds_nodash}}'
      and shop_type = 1
    group by
      1,
      2
  ) t2 on t1.goods_id = t2.goods_id
group by
  date_key,
  spu_id,
  marketing_target,
  module,
  product,
  optimize_target
union all
SELECT
  date_key,
  spu_id,
  marketing_target,
  module,
  product,
  optimize_target,
  0 as total_amount,
  0 as purchase_rgmv,
  0 as imp_cnt,
  sum(income_amt) as income_amt,
  sum(cash_income_amt) as cash_income_amt
FROM
  redcdm.dws_ads_note_spu_product_income_detail_td_df
WHERE
  dtm = '{{ds_nodash}}'
group by
  date_key,
  spu_id,
  marketing_target,
  module,
  product,
  optimize_target
  )base 
group by date_key,
  spu_id,
  marketing_target,
  module,
  product,
  optimize_target;
insert overwrite table redapp.app_ads_indsutry_spu_mapping_metrics_detail_df  partition( dtm ='{{ds_nodash}}')
select t1.date_key,
  t1.spu_id,
  spu_name,
  brand_id,
  commercial_level,
  commercial_code,
  commercial_name,
  commercial_code1,
  commercial_taxonomy_name1,
  commercial_code2,
  commercial_taxonomy_name2,
  commercial_code3,
  commercial_taxonomy_name3,
  commercial_code4,
  commercial_taxonomy_name4,
  brand_name,
  spu_type,
  mapping_first_industry_name,
  mapping_second_industry_name,
  is_brand_ka_spu,
  is_ads_ka_spu,
  is_trd_mapping_spu,
  ti_cash_income_amt,
  ti_cash_income_amt_30d,
  ti_cash_income_amt_1m,
  spu_deal_gmv_30d,
  spu_deal_gmv_1m,
  marketing_target,
  module,
  product,
  optimize_target,
  total_amount,
  purchase_rgmv,
  imp_cnt,
  income_amt,
  cash_income_amt,
  deal_gmv,
  k_live_dgmv,
  b_live_dgmv,
  note_dgmv,
  gq_deal_gmv,
  jihe_deal_gmv,
  ti_deal_gmv,
  origin_deal_gmv,
  goods_view_num,
  instant_buy_num,
  add_cart_num,
  goods_total,
  buy_num,
  buy_goods_num,
  buy_spu_num
from 
(SELECT
  date_key,
  spu_id,
  spu_name,
  brand_id,
  commercial_level,
  commercial_code,
  commercial_name,
  commercial_code1,
  commercial_taxonomy_name1,
  commercial_code2,
  commercial_taxonomy_name2,
  commercial_code3,
  commercial_taxonomy_name3,
  commercial_code4,
  commercial_taxonomy_name4,
  brand_name,
  spu_type,
  mapping_first_industry_name,
  mapping_second_industry_name,
  is_brand_ka_spu,
  is_ads_ka_spu,
  is_trd_mapping_spu,
  ti_cash_income_amt,
  ti_cash_income_amt_30d,
  ti_cash_income_amt_1m,
  spu_deal_gmv_30d,
  spu_deal_gmv_1m
FROM
  redapp.app_ads_industry_spu_mapping_label_df
WHERE
  dtm = '{{ds_nodash}}'
)t1
left join 
(select date_key,
  spu_id,
  marketing_target,
  module,
  product,
  optimize_target,
  total_amount,
  purchase_rgmv,
  imp_cnt,
  income_amt,
  cash_income_amt,
  0 as deal_gmv,
  0 as   k_live_dgmv,
  0 as   b_live_dgmv,
  0 as   note_dgmv,
  0 as   gq_deal_gmv,
  0 as   jihe_deal_gmv,
  0 as   ti_deal_gmv,
  0 as   origin_deal_gmv,
  0 as   goods_view_num,
  0 as   instant_buy_num,
  0 as   add_cart_num,
  0 as   goods_total,
  0 as   buy_num,
  0 as   buy_goods_num,
  0 as   buy_spu_num
from 
  temp.temp_app_ads_indsutry_spu_mapping_metrics_detail_df_{{ds_nodash}}
union all 
--交易
SELECT f_getdate(dtm) as date_key,
  spu_id,
  -911 as  marketing_target,
  '交易' as module,
  '交易' as product,
  -911 as optimize_target,
  0 as total_amount,
  0 as purchase_rgmv,
  0 as imp_cnt,
  0 as income_amt,
  0 as cash_income_amt,
  deal_gmv,
  k_live_dgmv,
  b_live_dgmv,
  note_dgmv,
  gq_deal_gmv,
  jihe_deal_gmv,
  ti_deal_gmv,
  origin_deal_gmv,
  goods_view_num,
  instant_buy_num,
  add_cart_num,
  goods_total,
  buy_num,
  buy_goods_num,
  buy_spu_num
  -- spu_deal_gmv_30d,
  -- spu_deal_gmv_1m
FROM
  redapp.app_ads_indsutry_spu_trd_detail_di
WHERE
  dtm >= '20240101' and dtm<='{{ds_nodash}}'
  )t2 on t1.date_key=t2.date_key and t1.spu_id = t2.spu_id




























--------------------cube表
SELECT
  date_key,

  commercial_taxonomy_name1,

  case when ti_cash_income_amt_30d>=1000000 then '月流水百万以上'
  when ti_cash_income_amt_30d>=500000 then '月流水五十万以上'
  when ti_cash_income_amt_30d>=100000 then '月流水十万以上'
  when ti_cash_income_amt_30d>=10000 then '月流水一万以上'
  when ti_cash_income_amt_30d>0 then '月流水一万以下',
  count(distinct spu_id) as spu_cnt,
  count(distinct case when is_trd_mapping_spu = 1 then spu_id else null end) as trd_mapping_spu_cnt,
  count(distinct case when deal_gmv > 0 then spu_id else null end) as deal_spu_cnt,
  count(distinct case when deal_gmv > 1000000 then spu_id else null end) as big_deal_spu_cnt,
  sum(deal_gmv) as deal_gmv,
  sum(k_live_dgmv) as k_live_dgmv,
  sum(b_live_dgmv) as b_live_dgmv,
  sum(note_dgmv) as note_dgmv,
  sum(gq_deal_gmv) as gq_deal_gmv,
  sum(jihe_deal_gmv) as jihe_deal_gmv,
  sum(ti_deal_gmv) as ti_deal_gmv,
  sum(origin_deal_gmv) as origin_deal_gmv,
 
  marketing_target,
  module,
  product,
  optimize_target,
  total_amount,
  purchase_rgmv,
  imp_cnt,
  income_amt,
  cash_income_amt,
  
FROM
  redapp.app_ads_indsutry_spu_mapping_metrics_detail_df
WHERE
  dtm = '20240718'
  and is_ads_ka_spu=1
