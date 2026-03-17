 SET "kyuubi.spark.option.--conf spark.sql.crossJoin.enabled=true";
SET "kyuubi.spark.option.--conf spark.redExtension.allowBroadcastNestedLoopJoin=true";
drop table if exists temp.temp_dm_ads_rtb_note_spu_td_df_note_{{ds_nodash}};

create table
  temp.temp_dm_ads_rtb_note_spu_td_df_note_{{ds_nodash}} as
select dt as date_key,
    spu_id,
      note_id,
      user_id,
      --type as note_type,
      publish_time,
      note_name,
      note_type,
      first_image_url,
      nickname,
      oss_image
from 
(select dt
from redcdm.dim_ads_date_df
where dtm='all' and  dt<= '{{ds}}' and dt>=f_getdate('{{ds}}', -7)
)t1 
left join 
(select t2.spu_id,
  t2.note_id,
  t1.user_id,
  --type as note_type,
  t1.publish_time,
  t1.note_name,
  t1.note_type,
  first_image_url,
  nickname,
  oss_image
 from 
 
 (
      select
        spu_id,
        note_id
      from
        ads_databank.dim_spu_note_df
      where
        dtm = max_dtm('ads_databank.dim_spu_note_df')
      group by
        1,
        2
    ) t2
    left join
    (
      select discovery_id as note_id,
        user_id,
        --type as note_type,
        publish_time,
        title as note_name,
        concat_ws(',',if (is_brand=1,'企业号笔记',null),if(is_bind=1 ,'蒲公英笔记',null),if(is_cps_note = 1 ,'购物笔记',null)) as note_type
      from reddw.dw_soc_discovery_delta_7_day
      where
        dtm = max_dtm('reddw.dw_soc_discovery_delta_7_day')
        -- and (
        --   is_brand = 1
        --   or is_bind = 1
        --   or is_cps_note = 1
        -- )
        --and substring(publish_time, 1, 10) >= '2022-07-01'
    ) t1 on t1.note_id = t2.note_id
    left join 
    (select discovery_id as note_id,
     first_file_id,
     concat('http://sns-img-qn.xhscdn.com/',first_file_id,'?imageView2/2/w/540/format/jpg/q/75') as first_image_url
    from reddw.dw_soc_discovery_content_day
    where dtm='{{ds_nodash}}'
    )t4
    on t2.note_id = t4.note_id
    left join 
    (select user_id,
      nickname,
      concat('https://img.xiaohongshu.com/avatar/',oss_image) as oss_image
    from reddw.dw_user_basic_day 
    where dtm = greatest('{{ds_nodash}}', '20230601')
    group by user_id,
      nickname,
      concat('https://img.xiaohongshu.com/avatar/',oss_image) 
    )t6 
    on t6.user_id = t1.user_id
    )t2 on 1=1
;

insert overwrite table redcdm.dm_ads_pub_spu_note_1d_di   partition( dtm )
SELECT
  t2.date_key,
  base.module,
  case when base.product='信息流' then '竞价-信息流' when base.product='搜索' then '竞价-搜索' 
  when base.product='视频内流' then '竞价-视频内流' else base.product end as product,
  base.marketing_target,
  t2.spu_id,
  t2.note_id,
  t2.user_id as user_id,
  t2.nickname as user_name,
  oss_image,
  note_type,
  publish_time,
  note_name,
  first_image_url,
  -- brand_id,
  -- brand_name,
  spu_name,
  note_income_amt,
  imp_cnt,
  click_cnt,
  income_amt,
  click_rgmv_7d,
  total_purchase_order_num,
  ecm_income_amt,
  engage_cnt,
  note_imp_cnt,
  note_click_cnt,
  note_engage_cnt,
  replace(t2.date_key,'-','') as dtm
from
(select date_key,
    spu_id,
      note_id,
      user_id,
      --type as note_type,
      publish_time,
      note_name,
      note_type,
      first_image_url,
      nickname,
      oss_image
from
        temp.temp_dm_ads_rtb_note_spu_td_df_note_{{ds_nodash}} 
        )t2
    left join 
    (
        
    select 
      
      date_key,
      module,
      product,
      marketing_target,
      spu_id,
      note_id,
      sum(avg_income_amt) as note_income_amt,
      sum(imp_cnt) as imp_cnt,
      sum(click_cnt) as click_cnt,
      sum(income_amt) as income_amt,
      sum(click_rgmv_7d) as click_rgmv_7d,
      sum(total_purchase_order_num) as total_purchase_order_num,
      sum(ecm_income_amt) as ecm_income_amt,
      sum(engage_cnt) as engage_cnt,
      0 as note_imp_cnt,
      0 as note_click_cnt,
      0 as note_engage_cnt
    from
      redcdm.dm_ads_rtb_creativity_spu_td_df
    where dtm='{{ds_nodash}}' and  date_key <= '{{ds}}' and date_key>=f_getdate('{{ds}}', -7)
    group by note_id,
      spu_id,
      module,
      product,
      marketing_target,
      date_key
    union all
    select
      date_key as date_key,
      '整体' as module,
      '整体' as product,
      '整体' as marketing_target,
      t2.spu_id,
     t1.note_id,
      0 as note_income_amt,
      0 as imp_cnt,
      0 as click_cnt,
      0 as income_amt,
      0 as click_rgmv_7d,
      0 as total_purchase_order_num,
      0 as ecm_income_amt,
      0 as engage_cnt,
      coalesce(imp_num,0) as note_imp_cnt,
      coalesce(click_num,0) as note_click_cnt,
      coalesce(engage_num,0) as note_engage_cnt
    from
      (
        select
          discovery_id as note_id,
          substring(publish_time, 1, 10) as dt
        from
          reddw.dw_soc_discovery_delta_7_day
        where
          dtm = max_dtm('reddw.dw_soc_discovery_delta_7_day')
          and (
            is_brand = 1
            or is_bind = 1
            or is_cps_note = 1
          )
          --and substring(publish_time, 1, 10) >= '2022-07-01'
      ) t1
      join (
        select
          spu_id,
          note_id
        from
          ads_databank.dim_spu_note_df
        where
          dtm = max_dtm('ads_databank.dim_spu_note_df')
        group by
          1,
          2
      ) t2 on t1.note_id = t2.note_id

      join (
      SELECT f_getdate(dtm) as date_key,
        note_id,
        read_feed_num,
        imp_num,
        click_num,
        like_num,
        fav_num,
        cmt_num,
        share_num,
        follow_num,
        engage_num,
        ads_imp_num,
        ads_click_num,
        ads_like_num,
        ads_fav_num,
        ads_cmt_num,
        ads_share_num,
        ads_follow_num,
        ads_engage_num,
        origin_imp_num,
        origin_click_num,
        origin_like_num,
        origin_fav_num,
        origin_cmt_num,
        origin_share_num,
        origin_follow_num,
        origin_engage_num,
        video_views,
        full_views,
        true_views,
        ads_full_views,
        imp_search_num,
        ads_imp_search_num,
        origin_imp_search_num,
        click_search_num,
        ads_click_search_num,
        origin_click_search_num
      FROM
        redapp.app_ads_note_engagement_1d_di
      WHERE
        dtm <= '{{ds_nodash}}' and dtm>=f_getdate('{{ds_nodash}}', -7)
      ) t3 on t2.note_id = t3.note_id
  ) base
on base.note_id = t2.note_id and base.spu_id=t2.spu_id and base.date_key=t2.date_key
  left join 
    (select
    spu_id,
    brand_id,
    brand_name,
    name as spu_name,
    commercial_taxonomy_name1,
    commercial_code2,
    commercial_taxonomy_name2,
    commercial_code3,
    commercial_taxonomy_name3,
    commercial_code4,
    commercial_taxonomy_name4,
    split(pic_url_list,';')[0] as pic_url
  from
    ads_databank.dim_spu_df
  where
    dtm = greatest('{{ds_nodash}}', '20231205')
  group by 1,2,3,4,5,6,7,8,9,10,11,12
  )spu on spu.spu_id = base.spu_id