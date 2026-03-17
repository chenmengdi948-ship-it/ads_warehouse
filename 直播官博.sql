with rtb as 
(select
  module,
  product,
  t1.user_id,
  t1.creativity_id,
  campaign_id,
  brand_account_id,
  t1.live_id,
  t1.anchor_id,
  live_start_time,
  live_end_time,
  live_duration,
  imp_cnt,
  click_cnt,
  live_impression_cnt,
  live_click_cnt,
  view_duration,
  view_cnt,
  valid_view_cnt,
  share_cnt,
  follow_cnt,
  like_cnt,
  comment_cnt,
  optimize_target,
  marketing_target,
  marketing_target as launch_type,
  t1.is_fans as is_fans,
  case when substring(create_time,1,10)<f_getdate(t1.dtm) then '老粉' when substring(create_time,1,10)=f_getdate(t1.dtm) then '新粉' else '非粉' end as fans_type,
  is_new_customer,
  is_live_goods,
 live_order_num_7d,
   live_order_num_1d,
  live_dgmv_7d,
  live_dgmv_1d,
  live_rgmv_7d,
  live_rgmv_1d,
  store_goods_impression_cnt as goods_impression_cnt,
  store_goods_click_cnt as goods_click_cnt,
  store_buy_now_cnt as buy_cnt,
  anchor_name
from
  (
    select
      user_id,
      creativity_id,
      campaign_id,
      live_id,
      is_fans,
      fans_type,
      is_new_customer,
      dtm,
      sum(ads_imp_num) as imp_cnt, -- 广告侧曝光
      sum(ads_click_num) as click_cnt, -- 广告侧点击
      sum(live_impression_cnt) as live_impression_cnt, -- 直播入口曝光
      sum(live_click_cnt) as live_click_cnt, -- 直播入口点击
      sum(ads_view_duration) as view_duration,
      sum(ads_view_pv) as view_cnt,
      sum(ads_valid_view_pv) as valid_view_cnt,
      sum(ads_live_share_num) as share_cnt,
      sum(ads_live_follow_num) as follow_cnt,
      sum(ads_live_like_num) as like_cnt,
      sum(ads_live_cmt_num) as comment_cnt,
      sum(ads_order_num_7d) as live_order_num_7d,
      sum(ads_order_num_1d) as live_order_num_1d,
      sum(ads_dgmv_7d) as live_dgmv_7d,
      sum(ads_dgmv_1d) as live_dgmv_1d,
      sum(ads_rgmv_7d) as live_rgmv_7d,
      sum(ads_rgmv_1d) as live_rgmv_1d,
      --sum(cps_goods_impression_cnt)  cps_goods_impression_cnt,-- k播商卡曝光PV 
      sum(store_goods_impression_cnt) as store_goods_impression_cnt, -- 店播商卡曝光PV
      --sum(cps_goods_click_cnt)  cps_goods_click_cnt,-- k播商卡曝光PV 
      sum(store_goods_click_cnt) as store_goods_click_cnt, -- 店播商卡曝光PV
      --sum(cps_buy_now_cnt) cps_buy_now_cnt  -- K播立购pv
      sum(store_buy_now_cnt) as store_buy_now_cnt -- 店播立购pv
    from
      redcdm.dm_ads_live_user_metrics_1d_di 
     where dtm='{{ds_nodash}}' 
     and is_ads=1
    group by user_id,
      creativity_id,
      campaign_id,
      live_id,
      is_fans,
      fans_type,
      is_new_customer,
      dtm
  ) t1
  left join (
    select
      creativity_id,
      optimize_target,
      marketing_target,
      brand_account_id,
      product,
      module
    from
      redcdm.dim_ads_creativity_core_df
    where
      dtm = greatest('{{ds_nodash}}', '20230901')
    
  ) t2 on t1.creativity_id = t2.creativity_id
  left join (
    select
      live_id,
      is_live_goods,
      anchor_id,
      live_start_time,
      live_end_time,
      live_duration
    from
      redcdm.dim_live_base_df
    where
      dtm = greatest('{{ds_nodash}}', '20230901')
  ) t3 on t3.live_id = t1.live_id
  left join (
    select
      user_id,
      target_user_id,
      1 as is_fans,
      create_time
    from
      reddw.dw_soc_follow_record_day
    where
      dtm = greatest('{{ds_nodash}}', '20230901')
      and coalesce(follow_type, '') != 'fake'
      and coalesce(follow_source, '') != 'fake_user_migrate'
      and spam_disabled = false --去除spam行为
      and coalesce(enabled, true) --未取消
  ) t4 on t4.user_id = t1.user_id
  and t4.target_user_id = t3.anchor_id
  left join -- 主播的维度属性
(
  select anchor_id,nickname as anchor_name
  from redcdm.dim_live_anchor_df 
  where dtm = greatest('{{ds_nodash}}', '20230901')
) t6 on t1.anchor_id = t6.anchor_id 
  ),
brand as 
(select
  module,
  product,
  t1.user_id,
  t1.creativity_id,
  campaign_id,
  brand_account_id,
  t1.live_id,
  t1.anchor_id,
  live_start_time,
  live_end_time,
  live_duration,
  imp_cnt,
  click_cnt,
  live_impression_cnt,
  live_click_cnt,
  view_duration,
  view_cnt,
  null as valid_view_cnt,
  share_cnt,
  follow_cnt,
  like_cnt,
  comment_cnt,
  optimize_target,
  marketing_target,
  launch_type,
  coalesce(is_fans,0) as is_fans,
 case when substring(create_time,1,10)<f_getdate(dtm) then '老粉' when substring(create_time,1,10)=f_getdate(dtm) then '新粉' else '非粉' end as fans_type,
 case 
        when replace(t7.buy_first_dt,'-','')=t1.dtm then '店铺新客'
        when replace(t7.buy_first_dt,'-','')<t1.dtm then '店铺老客'
        else '潜在客户' 
    end as is_new_customer,
  is_live_goods,
  null as live_order_num_7d,
  null as live_order_num_1d,
  null as live_dgmv_7d,
  null as live_dgmv_1d,
  null as live_rgmv_7d,
  null as live_rgmv_1d,
  null as  goods_impression_cnt,
  null as  goods_click_cnt,
  null as  buy_cnt,
 anchor_name
from
  (
    select
      '品牌' as module,
      product,
      user_id,
      creativity_id,
      campaign_id,
      brand_account_id,
      live_id,
      anchor_id,
      live_start_time,
      live_end_time,
      dtm,
      sum(live_duration) as live_duration,
      sum(imp_num) as imp_cnt,
      sum(click_num) as click_cnt,
      sum(live_impression_cnt) as live_impression_cnt,
      sum(live_click_cnt) as live_click_cnt,
      sum(view_duration) as view_duration,
      sum(view_cnt) as view_cnt,
      sum(share_cnt) as share_cnt,
      sum(follow_cnt) as follow_cnt,
      sum(like_cnt) as like_cnt,
      sum(comment_cnt) as comment_cnt
    from
      redcdm.dwd_ads_brand_live_log_1d_di
    where
      dtm = '{{ds_nodash}}'
    group by
      product,
      user_id,
      creativity_id,
      campaign_id,
      brand_account_id,
      live_id,
      anchor_id,
      live_start_time,
      live_end_time,
      dtm
  ) t1
  left join (
    select
      creativity_id,
      optimize_target,
      marketing_target
    from
      redcdm.dim_ads_creativity_core_df
    where
      dtm = greatest('{{ds_nodash}}', '20230901')
    group by
      1,
      2,
      3
  ) t2 on t1.creativity_id = t2.creativity_id
  left join (
    select
      creativity_id,
      launch_type
    from
      redcdm.dim_ads_brand_creativity_df
    where
      dtm = greatest('{{ds_nodash}}', '20230901')
  ) t3 on t1.creativity_id = t3.creativity_id
  left join (
    select
      user_id,
      target_user_id,
      1 as is_fans,
      create_time
    from
      reddw.dw_soc_follow_record_day
    where
      dtm = greatest('{{ds_nodash}}', '20230901')
      and coalesce(follow_type, '') != 'fake'
      and coalesce(follow_source, '') != 'fake_user_migrate'
      and spam_disabled = false --去除spam行为
      and coalesce(enabled, true) --未取消
  ) t4 on t4.user_id = t1.user_id
  and t4.target_user_id = t1.anchor_id
  left join (
    select
      live_id,
      is_live_goods
    from
      redcdm.dim_live_base_df
    where
      dtm = greatest('{{ds_nodash}}', '20230901')
  ) t5 on t5.live_id = t1.live_id
left join -- 主播的维度属性
(
  select anchor_id,seller_id,nickname as anchor_name
  from redcdm.dim_live_anchor_df 
  where dtm = greatest('{{ds_nodash}}', '20230901')
) t6 on t1.anchor_id = t6.anchor_id 
left join (
    select
      user_id,seller_id,min(to_date(create_time)) as buy_first_dt
    from reddw.dw_trd_order_package_goods_day
    where dtm = greatest('{{ds_nodash}}', '20230901')
    and is_valid=1
    group by 1,2
) t7 on t7.user_id = t1.user_id and t7.seller_id=t6.seller_id
  ),
trd as 
(select
  '' as module,
  '' as product,
  t1.user_id,
  '' as creativity_id,
  '' as campaign_id,
  t1.anchor_id as brand_account_id,
  live_id,
  t1.anchor_id,
  live_start_time,
  live_end_time,
  live_duration,
  live_impression_cnt as imp_cnt,
  live_click_cnt as click_cnt,
  live_impression_cnt,
  live_click_cnt,
  live_view_duration as view_duration,
  live_view_cnt as view_cnt,
  case
    when live_view_duration > 5 then live_view_cnt
    else 0
  end as valid_view_cnt,
  live_share_cnt as share_cnt,
  user_live_follow_cnt as follow_cnt,
  live_like_cnt as like_cnt,
  live_comment_cnt as comment_cnt,
  '' as optimize_target,
  '' as marketing_target,
  '' as launch_type,
  t1.is_fans,
  case
    when substring(t4.create_time, 1, 10) < f_getdate(dtm) then '老粉'
    when substring(t4.create_time, 1, 10) = f_getdate(dtm) then '新粉'
    else '非粉'
  end as fans_type,
  case
    when replace(t7.buy_first_dt, '-', '') = t1.dtm then '店铺新客'
    when replace(t7.buy_first_dt, '-', '') < t1.dtm then '店铺老客'
    else '潜在客户'
  end as is_new_customer,
  is_live_goods,
  goods_num as live_order_num_7d,
  goods_num as live_order_num_1d,
  rgmv as live_dgmv_7d,
  rgmv as live_dgmv_1d,
  dgmv as live_rgmv_7d,
  dgmv as live_rgmv_1d,
  goods_impression_cnt,
  goods_click_cnt,
  buy_cnt,
  '整体流量' as log_type,
  anchor_name
from
  (
    select
      *
    from
      redcdm.dm_live_consume_live_user_traffic_engage_deal_1d_di
    where
      dtm = '{{ds_nodash}}'
  ) t1
  left join (
    select
      user_id,
      target_user_id,
      1 as is_fans,
      create_time
    from
      reddw.dw_soc_follow_record_day
    where
      dtm = greatest('{{ds_nodash}}', '20230901')
      and coalesce(follow_type, '') != 'fake'
      and coalesce(follow_source, '') != 'fake_user_migrate'
      and spam_disabled = false --去除spam行为
      and coalesce(enabled, true) --未取消
  ) t4 on t4.user_id = t1.user_id
  and t4.target_user_id = t1.anchor_id
  left join -- 主播的维度属性
  (
    select
      anchor_id,
      seller_id,
      nickname as anchor_name
    from
      redcdm.dim_live_anchor_df
    where
      dtm = greatest('{{ds_nodash}}', '20230901')
  ) t6 on t1.anchor_id = t6.anchor_id
  left join (
    select
      user_id,
      seller_id,
      min(to_date(create_time)) as buy_first_dt
    from
      reddw.dw_trd_order_package_goods_day
    where
      dtm = greatest('{{ds_nodash}}', '20230901')
      and is_valid = 1
    group by
      1,
      2
  ) t7 on t7.user_id = t1.user_id
  and t7.seller_id = t6.seller_id
  )
  insert
  overwrite table redapp.app_ads_industry_live_creativity_user_di partition (dtm='{{ds_nodash}}')
  select 
    user_id,
    creativity_id,
    live_id,
    '广告流量' as log_type,
    module,
    product,
    campaign_id,
    brand_account_id,
    anchor_id,
    live_start_time,
    live_end_time,
    live_duration,
    imp_cnt,
    click_cnt,
    live_impression_cnt,
    live_click_cnt,
    view_duration,
    view_cnt,
    valid_view_cnt,
    share_cnt,
    follow_cnt,
    like_cnt,
    comment_cnt,
    optimize_target,
    marketing_target,
    launch_type,
    is_fans,
    fans_type,
    is_new_customer,
    is_live_goods,
    live_order_num_7d,
    live_order_num_1d,
    live_dgmv_7d,
    live_dgmv_1d,
    live_rgmv_7d,
    live_rgmv_1d,
    goods_impression_cnt,
    goods_click_cnt,
    buy_cnt,
    anchor_name
  from rtb 
  union all 
  select 
    user_id,
    creativity_id,
    live_id,
    '广告流量' as log_type,
    module,
    product,
    campaign_id,
    brand_account_id,
    anchor_id,
    live_start_time,
    live_end_time,
    live_duration,
    imp_cnt,
    click_cnt,
    live_impression_cnt,
    live_click_cnt,
    view_duration,
    view_cnt,
    valid_view_cnt,
    share_cnt,
    follow_cnt,
    like_cnt,
    comment_cnt,
    optimize_target,
    marketing_target,
    launch_type,
    is_fans,
    fans_type,
    is_new_customer,
    is_live_goods,
    live_order_num_7d,
    live_order_num_1d,
    live_dgmv_7d,
    live_dgmv_1d,
    live_rgmv_7d,
    live_rgmv_1d,
    goods_impression_cnt,
    goods_click_cnt,
    buy_cnt,
    anchor_name
  from brand 
  union all 
  select 
    user_id,
    creativity_id,
    live_id,
    '整体流量' as log_type,
    module,
    product,
    campaign_id,
    brand_account_id,
    anchor_id,
    live_start_time,
    live_end_time,
    live_duration,
    imp_cnt,
    click_cnt,
    live_impression_cnt,
    live_click_cnt,
    view_duration,
    view_cnt,
    valid_view_cnt,
    share_cnt,
    follow_cnt,
    like_cnt,
    comment_cnt,
    optimize_target,
    marketing_target,
    launch_type,
    is_fans,
    fans_type,
    is_new_customer,
    is_live_goods,
    live_order_num_7d,
    live_order_num_1d,
    live_dgmv_7d,
    live_dgmv_1d,
    live_rgmv_7d,
    live_rgmv_1d,
    goods_impression_cnt,
    goods_click_cnt,
    buy_cnt,
    anchor_name
  from trd
