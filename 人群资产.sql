---临时表：temp.app_ads_industry_spu_aips_user_{{ds_nodas}}, temp.app_ads_industry_spu_aips_user_{{ds_nodash}}_date, chip_spu_zhongshou_iti_track_step2, chip_spu_zhongshou_iti_track_step3, 
 
drop table if exists temp.app_ads_industry_spu_aips_user_{{ds_nodas}};
create table temp.app_ads_industry_spu_aips_user_{{ds_nodash}} as
 
SELECT
  dtm,
  spu_id,
  user_id,
  ---电商类
  sum(goods_view_num) as goods_view_num,
  sum(add_cart_num) as add_cart_num,
  sum(instant_buy_num) as instant_buy_num,
  sum(add_wishlist_num) as add_wishlist_num,
  sum(goods_click_num) as goods_click_num,
  sum(goods_impression_num) as goods_impression_num,
  ---阅读类
  sum(read_feed_num) as read_feed_num,
  ---互动类
  sum(engagement_num) as engagement_num
FROM hive_prod.redcdm.dws_ads_spu_user_ti_1d_di
WHERE dtm between f_getdate('{{ds_nodash}}',-59) and '{{ds_nodash}}'
GROUP BY 1, 2, 3;
 
SET "kyuubi.spark.option.--conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=8000";
SET "kyuubi.spark.option.--conf spark.hadoop.fs.s3a.multipart.copy.threshold=10737418240";
drop table if exists temp.app_ads_industry_spu_aips_user_{{ds_nodash}}_date;
create table temp.app_ads_industry_spu_aips_user_{{ds_nodash}}_date as
SELECT
  t1.spu_id,
  t1.user_id,
  interest_level2,
  interest_level_map,
  interest_level_map['I'] as first_i_date,
  interest_level_map['TI'] as first_ti_date,
t2.user_id as new_user_id
from 
    hive_prod.redcdm.dws_ads_spu_user_ti_nd_di t1---SPU范围，能匹配上APIS的 --redapp.app_ads_spu_user_ti_trans_1d_di算新增
    left  join 
    (select spu_id,user_id
      from redapp.app_ads_spu_user_ti_trans_1d_di 
      where dtm = '{{ds_nodash}}' and new_interest_level in ('I','TI')
      )t2 on t1.spu_id=t2.spu_id and t1.user_id =t2.user_id
where 
    t1.dtm = '{{ds_nodash}}'
    and t1.interest_level2 in ('I', 'TI');
    -- and user_id = '5558543a62a60c2792092b8d'
    -- and spu_id = '1564398'
 
SET "kyuubi.spark.option.--conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=8000";
SET "kyuubi.spark.option.--conf spark.hadoop.fs.s3a.multipart.copy.threshold=10737418240";
drop table if exists temp.app_ads_industry_spu_aips_user_{{ds_nodas}}_step2;
create table temp.app_ads_industry_spu_aips_user_{{ds_nodash}}_step2 as
 
SELECT
    t1.spu_id,
    t1.user_id,
    t1.interest_level2,
    t1.first_i_date,
    t1.first_ti_date,
    t1.first_date,
    t2.dtm,
    sum(ec_num) as ec_num,
    ---阅读类
    sum(read_feed_num) as read_feed_num,
    ---互动类
    sum(engagement_num) as engagement_num
FROM(
  SELECT
    z1.spu_id,
    user_id,
    z1.interest_level2,
    first_i_date,
    first_ti_date,
    case
        when interest_level2 = 'I' then first_i_date
        when interest_level2 = 'TI' then first_ti_date
      end as first_date
    FROM temp.app_ads_industry_spu_aips_user_{{ds_nodash}}_date z1
) t1
LEFT JOIN(
  SELECT
      dtm,
      z1.spu_id,
      z1.user_id,
      ---电商类
      sum(goods_view_num) + sum(add_cart_num) + sum(instant_buy_num) + sum(add_wishlist_num) + sum(goods_click_num) as ec_num,
      sum(goods_view_num) as goods_view_num,
      sum(add_cart_num) as add_cart_num,
      sum(instant_buy_num) as instant_buy_num,
      sum(add_wishlist_num) as add_wishlist_num,
      sum(goods_click_num) as goods_click_num,
      sum(goods_impression_num) as goods_impression_num,
      ---阅读类
      sum(read_feed_num) as read_feed_num,
      ---互动类
      sum(engagement_num) as engagement_num
    FROM temp.app_ads_industry_spu_aips_user_{{ds_nodash}} z1
    GROUP BY 1, 2, 3
) t2 on t1.spu_id = t2.spu_id and t1.user_id = t2.user_id and t1.first_date >= t2.dtm
GROUP BY 1, 2, 3, 4, 5, 6, 7;
 
 
drop table if exists temp.app_ads_industry_spu_aips_user_{{ds_nodas}}_step3;
create table temp.app_ads_industry_spu_aips_user_{{ds_nodash}}_step3 as
SELECT
    spu_id,
    user_id,
    interest_level2,
    first_i_date,
    first_ti_date,
    first_date,
    sum(ec_num) as ec_num,
    sum(read_feed_num) as read_feed_num,
    sum(engagement_num) as engagement_num
FROM(
  SELECT
    spu_id,
    user_id,
    interest_level2,
    first_i_date,
    first_ti_date,
    first_date,
    date_add(first_date, -30) as min_date,
    dtm,
    ec_num,
    read_feed_num,
    engagement_num
  FROM temp.app_ads_industry_spu_aips_user_{{ds_nodash}}_step2
  WHERE 
  -- spu_id = '74855' and user_id = '5a5ad1fce8ac2b59e3ef2fbf'
    date(dtm) >= date_add(first_date, -30)
) t
GROUP BY 1, 2, 3, 4, 5, 6;
 
SELECT
        spu_id,
        count(distinct user_id) as asset_cnt,--种草人群资产
        count(distinct (if(interest_level2 = 'TI', user_id, null))) as ti_asset_cnt,--xx
        count(distinct (if(ec_num>0, user_id, null))) as ec_asset_cnt, --电商行为人群资产量
        count(distinct (if(read_feed_num>0, user_id, null))) as read_asset_cnt,--阅读行为人群资产量
        count(distinct (if(engagement_num>0, user_id, null))) as eng_asset_cnt--互动行为人群资产量
        count(distinct new_user_id) as new_asset_cnt,--种草人群资产
        count(distinct (if(ec_num>0, new_user_id, null))) as new_ec_asset_cnt, --电商行为人群资产量
        count(distinct (if(read_feed_num>0, new_user_id, null))) as new_read_asset_cnt,--阅读行为人群资产量
        count(distinct (if(engagement_num>0, new_user_id, null))) as new_eng_asset_cnt--互动行为人群资产量
    FROM(
      SELECT
          spu_id,
          user_id,
          interest_level2,
          first_i_date,
          first_ti_date,
          first_date,
          new_user_id,
          coalesce(ec_num, 0) as ec_num,
          coalesce(read_feed_num, 0) as read_feed_num,
          coalesce(engagement_num, 0) as engagement_num
      FROM temp.app_ads_industry_spu_aips_user_{{ds_nodash}}_step3
    ) p1
    GROUP BY 1


    ---广告引导的种草人群资产量()
SELECT 
  t1.spu_id,
  count(distinct t2.user_id) as ad_asset_cnt ---广告引导的种草人群资产
FROM(
  SELECT
    spu_id,
    user_id
  from 
      hive_prod.redcdm.dws_ads_spu_user_ti_nd_di ---SPU范围，能匹配上APIS的
  where 
      dtm = '{{ds_nodash}}'
      and interest_level2 in ('I', 'TI')
  GROUP BY 1, 2
) t1
---在广告触点过有过点击
LEFT JOIN(
  SELECT
    spu_id,
    user_id
  FROM redapp.app_ads_spu_user_touch_1d_di   redcdm.dwd_ads_spu_user_ti_cvr_hi
  WHERE dtm between '20250726' and '{{ds_nodash}}' 
    and click_cnt > 0
  GROUP BY 1, 2
) t2 on t1.spu_id = t2.spu_id and t1.user_id = t2.user_id
GROUP BY 1