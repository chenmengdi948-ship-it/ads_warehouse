 `platform` bigint COMMENT '投放平台', 
`cost_creativity_num` bigint COMMENT '在投创意数', 
  `new_cost_creativity_num` bigint COMMENT '新投创意数', 
  `new_cost_campaign_num` bigint COMMENT '新投计划数首次有消耗', 
  `campaign_num` bigint COMMENT '总计划数', 

  `create_campaign_num` bigint COMMENT '新建计划数', 
  `create_cost_campaign_num` bigint COMMENT '新建且有消耗计划数', 
  `effecient_campaign_num` bigint COMMENT '在投计划数', 
`rtb_note_num` bigint COMMENT '在投笔记数（子账户粒度,汇总看户均）', 
  `rtb_cost_note_num` bigint COMMENT '有消耗笔记数（子账户粒度,汇总看户均）', 
  `new_cost_note_num` bigint COMMENT '新投笔记数，笔记首次有消耗', 
`launch_finish_campaign_num` bigint COMMENT '冷启动计划数', 
`min_advertiser_budget_income_amt` double COMMENT '广告主预算', 
  `advertiser_cost_income_amt` double COMMENT '广告主消耗', 

  `msg_income_user_cnt` bigint COMMENT '私信进线人数$$用户点击创意后24小时内,进入到商家私信对话的用户数', 


  `msg_open_user_cnt` bigint COMMENT '私信开口人数$$用户点击创意后24小时内,进入到商家私信对话且对商家手动发私信消息的用户数', 

  `msg_leads_user_cnt` bigint COMMENT '私信留资人数', 

  `valid_leads_num` bigint COMMENT '有效表单量', 
  `msg_first_15s_reply_cnt` bigint COMMENT '15s首响率-分子$$用户第一条消息发出后商家15s内回复', 
  `msg_first_15s_cnt` bigint COMMENT '15s首响率-分母', 
  `msg_driving_open_wait_duration` bigint COMMENT '回复间隔时长$$单位秒 用户每次手动发出消息(不包含快捷菜单)后收到商家回复间隔时长', 
  `msg_driving_open_30s_cnt` bigint COMMENT '30s内回复次数', 
  `msg_driving_open_60s_cnt` bigint COMMENT '60s内回复次数', 
  `msg_driving_open_effi_cnt` bigint COMMENT '私信回复效率分母', 



  `live_reserve_cnt` bigint COMMENT '预告组件点击次数$$笔记左下角组件按钮点击次数', 
  `live_subscribe_user_cnt` bigint COMMENT '直播预约人数$$按钮点击后提示成功预约人数', 
  `live_comment_cnt` bigint COMMENT '直播间评论次数', 
  `live_watch_num` bigint COMMENT '直播间观看次数', 
  `live_effective_shutdown_num` bigint COMMENT '直播间有效观看次数', 
  `live_effective_shutdown_distinct_eventid_num` bigint COMMENT '直播间有效观看人次', 
  `live_watch_duration` double COMMENT '直播间观看时长$$单位分', 
  `live_watch_distinct_eventid_num` bigint COMMENT '直播间观看人次', 


  `live_follow_num` bigint COMMENT '直播间新增粉丝量', 
  `enter_seller_cnt_7d` bigint COMMENT '进店访问量', 
  `goods_view_cnt_7d` bigint COMMENT '商品访客量', 
  `add_cart_cnt_7d` bigint COMMENT '商品加购量', 

  `search_component_click_cnt` bigint COMMENT '搜索组件点击量'














































--聚光转化
select f_getdate(cast(attri_date as string)) as date_key,
    '效果' as module,
    t1.product,
    t2.brand_account_id,
    t1.advertiser_id,
    t2.v_seller_id,
    t1.optimize_target,
    t1.marketing_target,
    t1.platform,
    t1.promotion_target,
    imp_num as imp_num,
    click_num as click_num,
    fav_num as like_num,
    collect_num as fav_num,
    cmt_num as cmt_num,
    follow_num as follow_num,
    share_num as share_num,
    reserve_pv as live_reserve_cnt,
    live_subscribe_cnt as live_subscribe_user_cnt,
    live_comment_num as live_comment_cnt,
    live_watch_num as live_watch_num,
    live_effective_shutdown_num as live_effective_shutdown_num,
    live_effective_shutdown_distinct_eventid_num as live_effective_shutdown_distinct_eventid_num,
    live_watch_duration as live_watch_duration,
    live_watch_distinct_eventid_num as live_watch_distinct_eventid_num,
    live_total_order_num as live_order_num,
    live_rgmv as live_gmv,
    all_follow_num as live_follow_num,
    goods_view_pv as enter_seller_cnt,
    seller_view_pv as goods_view_cnt,
    add_cart as add_cart_cnt,
    total_order as purchase_order_num_7d,
    rgmv as purchase_order_gmv_7d,
    success_order as deal_order_num_7d,
    purchase_order_gmv_7d as deal_order_gmv_7d,
    search_component_click_num as search_component_click_cnt,
  
    attri_date as dtm
from redapp.app_aurora_creativity_aggr_di t1 
left join redcdm.dim_ads_creativity_core_df t2
on t1.creativity_id = t2.creativity_id and t2.dtm='{{ds_nodash}}'
WHERE
  t1.dtm<='{{ds_nodash}}' and t1.dtm>='{{ds_7_days_ago_nodash}}'
  and t1.attri_date<='{{ds_nodash}}' and t1.attri_date>='{{ds_7_days_ago_nodash}}'
  ;
SELECT
  date_key,
  module,
  product,
  brand_account_id,
  advertiser_id,
  v_seller_id,
  optimize_target,
  marketing_target,
  platform,
  promotion_target,
  cost_creativity_cnt,
  cost_campaign_cnt,
  new_cost_creativity_cnt,
  new_cost_campaign_cnt,
  campaign_num,
  cost_campaign_num,
  create_campaign_num,
  create_cost_campaign_num,
  effecient_campaign_num,
  rtb_cost_campaign_num,
  new_cost_num,
  launch_finish_campaign_num,
  invest_duration_7d,
  cost_days,
  total_note_num,
  new_note_num,
  rtb_note_num,
  rtb_cost_note_num,
  imp_num,
  click_num,
  like_num,
  fav_num,
  cmt_num,
  share_num,
  follow_num,
  live_reserve_cnt,
  live_subscribe_user_cnt,
  live_comment_cnt,
  live_watch_num,
  live_effective_shutdown_num,
  live_effective_shutdown_distinct_eventid_num,
  live_watch_duration,
  live_watch_distinct_eventid_num,
  live_order_num,
  live_gmv,
  live_follow_num,
  enter_seller_cnt,
  goods_view_cnt,
  add_cart_cnt,
  purchase_order_num_7d,
  purchase_order_gmv_7d,
  deal_order_num_7d,
  deal_order_gmv_7d,
  search_component_click_cnt,
  conversion_cnt,
  cash_income_amt,
  income_amt，
  t1.dtm
FROM
  --缺少乘风数据
  (select f_getdate(attri_date) as date_key,
    '效果' as module,
    t1.product,
    t2.brand_account_id,
    t1.advertiser_id,
    t2.v_seller_id,
    t1.optimize_target,
    t1.marketing_target,
    t1.platform,
    t1.promotion_target,
    imp_num as imp_num,
    click_num as click_num,
    fav_num as like_num,
    collect_num as fav_num,
    cmt_num as cmt_num,
    follow_num as follow_num,
    share_num as share_num,
    reserve_pv as live_reserve_cnt,
    live_subscribe_cnt as live_subscribe_user_cnt,
    live_comment_num as live_comment_cnt,
    live_watch_num as live_watch_num,
    live_effective_shutdown_num as live_effective_shutdown_num,
    live_effective_shutdown_distinct_eventid_num as live_effective_shutdown_distinct_eventid_num,
    live_watch_duration as live_watch_duration,
    live_watch_distinct_eventid_num as live_watch_distinct_eventid_num,
    live_total_order_num as live_order_num,
    live_rgmv as live_gmv,
    all_follow_num as live_follow_num,
    goods_view_pv as enter_seller_cnt,
    seller_view_pv as goods_view_cnt,
    add_cart as add_cart_cnt,
    total_order as purchase_order_num_7d,
    rgmv as purchase_order_gmv_7d,
    success_order as deal_order_num_7d,
    purchase_order_gmv_7d as deal_order_gmv_7d,
    search_component_click_num as search_component_click_cnt,
    out_click_total_order_7d as ecm_unclosed_purchase_order_num_7d,
    out_click_rgmv_7d as ecm_unclosed_purchase_order_gmv_7d，
    attri_date as dtm
from redapp.app_aurora_creativity_aggr_di t1 
left join redcdm.dim_ads_creativity_core_df t2
on t1.creativity_id = t2.creativity_id and t2.dtm='{{ds_nodash}}'
WHERE
  dtm<='{{ds_nodash}}' and dtm>='{{ds_7_days_ago_nodash}}'
  and attri_date<='{{ds_nodash}}' and attri_date>='{{ds_7_days_ago_nodash}}'
  )t1 








----------------------结果数据-----------------------------

--收入

drop table if exists temp.temp_dm_ad_pub_advertiser_product_metrics_detail_df_{{ds_nodash}}_01;
create table temp.temp_dm_ad_pub_advertiser_product_metrics_detail_df_{{ds_nodash}}_01
select date_key,
  base.brand_account_id,
  module,
  product,
  marketing_target,
  optimize_target,
  market_target,
  is_marketing_product,
  virtual_seller_id as v_seller_id,
  agent_user_id,
  ads_note_type,
  promotion_target,
  platform,
  advertiser_id,
  cash_cost,
  cost
from
(
  --非效果收入
  SELECT
    date_key,
    case when module in ('品合','内容加热') then '品合' when product in ('口碑通','口碑通v2.0') then '口碑通' else module end as module,
    product,
    is_marketing_product,
    market_target,
    virtual_seller_id,
    brand_user_id as brand_account_id,
    agent_user_id,
    -911 as optimize_target,
    -911 as marketing_target,
    '非竞价' as ads_note_type,
    -911 as promotion_target,
    -911 as platform,
    advertiser_id,
    sum(cost) as cost,
    sum(cash_cost) as cash_cost
  FROM
    reddm.dm_ads_crm_advertiser_income_wide_day
  WHERE
    dtm = '{{ds_nodash}}' and module <>'效果' --效果需要到更细粒度
  group by date_key,
    case when module in ('品合','内容加热') then '品合' when product in ('口碑通','口碑通v2.0') then '口碑通' else module end ,
    product,
    is_marketing_product,
    market_target,
    virtual_seller_id,
    brand_user_id,
    agent_user_id,
    advertiser_id
  union all
        --效果收入区分营销目标
  select
    date_key,
    '效果' as module,
    t1.product,
    coalesce(is_marketing_product, '0') as is_marketing_product,
    dim.market_target,
    b.virtual_seller_id,
    t1.brand_user_id as brand_account_id,
    b.agent_user_id,
    t1.optimize_target,
    t1.marketing_target,
    note_ads_type as ads_note_type,
    promotion_target,
    bb.platform,
    bb.advertiser_id,
    sum(total_amount) as cost,
    sum(cash_income_amt) as cash_cost     
  from
    (
      SELECT
        date_key,
        creativity_id,
        cash_amount,
        coalesce(cash_amount, 0) + coalesce(credit_amount, 0) as cash_income_amt,
        return_amount,
        total_amount,
        credit_amount as credit_income_amt,
        coupon_amount,
        brand_account_id as brand_user_id,
        --market_target ,
        marketing_target ,
        '0' as is_marketing_product,
        '效果' as module,
        optimize_target,
        product,
        advertiser_id
      FROM
        redcdm.dws_ads_rtb_log_creativity_income_1d_df
      WHERE
        dtm = '{{ds_nodash}}'
        and date_key <= '{{ds}}'
    ) t1
    LEFT JOIN redcdm.dim_ads_creativity_core_extend_df bb on t1.creativity_id = bb.creativity_id
    and bb.dtm = '{{ds_nodash}}'
    left join redcdm.dim_ads_advertiser_df b on b.dtm = '{{ds_nodash}}'
    and t1.advertiser_id = b.rtb_advertiser_id
    left join redcdm.dim_ads_social_note_base_info_df c 
    on c.dtm='{{ds_nodash}}' and c.note_id =bb.ads_material_id
    left join 
    (select
      dim_value,
      dim_value_name ,
      value_type as market_target
    FROM
      redcdm.dim_ads_industry_dimension_code_df
    WHERE
      dtm = 'all' and dimension_code = 'marketing_target'
    )dim --枚举值维表
    on t1.marketing_target = dim.dim_value
    group by
      t1.date_key,
      t1.product,
      dim.market_target ,
      t1.marketing_target ,
      b.virtual_seller_id,
      t1.brand_user_id,
      b.agent_user_id,
      is_marketing_product,
      t1.optimize_target,
      note_ads_type,
      promotion_target,
      bb.platform,
      bb.advertiser_id
    )base 
    ;

--所有产品线展点消转化
drop table if exists temp.temp_dm_ad_pub_advertiser_product_metrics_detail_df_{{ds_nodash}}_02;
create table temp.temp_dm_ad_pub_advertiser_product_metrics_detail_df_{{ds_nodash}}_02
SELECT
  f_getdate(a.dtm) as date_key,
  module,
  product,
  advertiser_id,
  v_seller_id,
  b.market_target,
  note_ads_type,
  a.brand_account_id,
  cast(marketing_target as bigint) as marketing_target,
  cast(optimize_target as bigint) as optimize_target,
  cast(platform as bigint) as platform,
  cast(promotion_target as bigint) as promotion_target,
  '0' as is_marketing_product,
  agent_user_id,
  coalesce(sum(imp_num),0) as imp_num  --'曝光量'
  ,coalesce(sum(click_num),0) as click_num  --'点击量'
  ,coalesce(sum(like_num),0) as like_num  --'点赞量'
  ,coalesce(sum(fav_num),0) as fav_num  --'收藏量'
  ,coalesce(sum(cmt_num),0) as cmt_num  --'评论量'
  ,coalesce(sum(follow_num),0) as follow_num  --'关注量'
  ,coalesce(sum(share_num),0) as share_num  --'分享量'
  ,coalesce(sum(engagement_num),0) as engagement_num
  -- ,coalesce(sum(income_amt),0) as income_amt  --'运营消耗$$单位元'
  -- ,coalesce(sum(cash_income_amt),0) as cash_income_amt  --'现金消耗$$单位元'
  ,coalesce(sum(conversion_cnt),0) as conversion_cnt
  ,coalesce(sum(msg_income_cnt),0) as msg_income_cnt  --'进线-进线次数'
  ,coalesce(sum(msg_income_user_cnt),0) as msg_income_user_cnt  --'进线-人数'
  ,coalesce(sum(msg_open_msg_cnt),0) as msg_open_msg_cnt  --'开口-私信条数'
  ,coalesce(sum(msg_open_cnt),0) as msg_open_cnt  --'开口-开口次数'
  ,coalesce(sum(msg_open_user_cnt),0) as msg_open_user_cnt  --'开口-人数'
  ,coalesce(sum(msg_leads_num),0) as msg_leads_num  --'私信留资量'
  ,coalesce(sum(msg_leads_user_cnt),0) as msg_leads_user_cnt  --'私信留资人数'
  ,coalesce(sum(leads_submit_num),0) as leads_submit_num  --'表单提交量'
  ,coalesce(sum(valid_leads_num),0) as valid_leads_num  --'有效表单量'
  ,coalesce(sum(msg_first_15s_reply_cnt),0) as msg_first_15s_reply_cnt  --'15s首响率-分子$$用户第一条消息发出后商家15s内回复'
  ,coalesce(sum(msg_first_15s_cnt),0) as msg_first_15s_cnt  --'15s首响率-分母'
  ,coalesce(sum(msg_driving_open_wait_duration),0) as msg_driving_open_wait_duration  --'回复间隔时长'
  ,coalesce(sum(msg_driving_open_30s_cnt),0) as msg_driving_open_30s_cnt  --'30s内回复次数'
  ,coalesce(sum(msg_driving_open_60s_cnt),0) as msg_driving_open_60s_cnt  --'60s内回复次数'
  ,coalesce(sum(msg_driving_open_effi_cnt),0) as msg_driving_open_effi_cnt  --'私信回复效率分母'
  ,coalesce(sum(friend_add_cnt),0) as friend_add_cnt  --'企微添加次数'
  ,coalesce(sum(friend_add_success_cnt),0) as friend_add_success_cnt  --'企微添加成功次数'
  ,coalesce(sum(friend_chat_open_cnt),0) as friend_chat_open_cnt  --'企微开口次数'
  ,coalesce(sum(live_reserve_cnt),0) as live_reserve_cnt  --'预告组件点击次数'
  ,coalesce(sum(live_subscribe_user_cnt),0) as live_subscribe_user_cnt  --'直播预约人数'
  ,coalesce(sum(live_comment_cnt),0) as live_comment_cnt  --'直播间评论次数'
  ,coalesce(sum(live_watch_num),0) as live_watch_num  --'直播间观看次数'
  ,coalesce(sum(live_effective_shutdown_num),0) as live_effective_shutdown_num  --'直播间有效观看次数'
  ,coalesce(sum(live_effective_shutdown_distinct_eventid_num),0) as live_effective_shutdown_distinct_eventid_num  --'直播间有效观看人次'
  ,coalesce(sum(live_watch_duration),0) as live_watch_duration  --'直播间观看时长$$单位分'
  ,coalesce(sum(live_watch_distinct_eventid_num),0) as live_watch_distinct_eventid_num  --'直播间观看人次'
  ,coalesce(sum(live_order_num),0) as live_order_num  --'直播间支付订单量'
  ,coalesce(sum(live_gmv),0) as live_gmv  --'直播间支付gmv$$单位元'
  ,coalesce(sum(live_follow_num),0) as live_follow_num  --'直播间新增粉丝量'
  ,coalesce(sum(enter_seller_cnt_7d),0) as enter_seller_cnt_7d  --'进店访问量'
  ,coalesce(sum(goods_view_cnt_7d),0) as goods_view_cnt_7d  --'商品访客量'
  ,coalesce(sum(add_cart_cnt_7d),0) as add_cart_cnt_7d  --'商品加购量'
  ,coalesce(sum(purchase_order_num_7d),0) as purchase_order_num_7d  --'下单订单量$$7日归因'
  ,coalesce(sum(purchase_order_gmv_7d),0) as purchase_order_gmv_7d  --'下单gmv$$7日归因 单位元'
  ,coalesce(sum(deal_order_num_7d),0) as deal_order_num_7d  --'支付订单量$$7日归因'
  ,coalesce(sum(deal_order_gmv_7d),0) as deal_order_gmv_7d  --'支付订单gmv$$7日归因 单位元'
  ,coalesce(sum(search_component_click_cnt),0) as search_component_click_cnt  --'搜索组件点击量'
  ,coalesce(sum(ecm_unclosed_purchase_order_num_7d),0) as ecm_unclosed_purchase_order_num_7d  --'非闭环电商下单订单量$$7日归因'
  ,coalesce(sum(ecm_unclosed_purchase_order_gmv_7d),0) as ecm_unclosed_purchase_order_gmv_7d  --'非闭环电商下单gmv$$7日归因 单位元'
  ,coalesce(sum(ecm_unclosed_goods_view_cnt_7d),0) as ecm_unclosed_goods_view_cnt_7d
FROM
  redcdm.dm_ad_rtb_creativity_1d_di a
  left join 
  (select
    dim_value,
    dim_value_name ,
    value_type as market_target
  FROM
    redcdm.dim_ads_industry_dimension_code_df
  WHERE
    dtm = 'all' and dimension_code = 'marketing_target'
  )b --枚举值维表
  on a.marketing_target = b.dim_value
  left join redcdm.dim_ads_social_note_base_info_df c 
  on c.dtm='{{ds_nodash}}' and c.note_id =a.ads_material_id
WHERE
  a.dtm<='{{ds_nodash}}'
  and a.module in ('效果')
 group by f_getdate(a.dtm) ,
  module,
  product,
  advertiser_id,
  v_seller_id,
  b.market_target,
  note_ads_type,
  a.brand_account_id,
  cast(marketing_target as bigint) ,
  cast(optimize_target as bigint) ,
  cast(platform as bigint) ,
  cast(promotion_target as bigint),
  --'0' as is_marketing_product,
  agent_user_id

union all 
SELECT
  date_key,
  module,
  product,
  -911 as advertiser_id,
  '-911' as v_seller_id,
  market_target_type as market_target,
  '非竞价' as ads_note_type,
  brand_account_id,
  -911 as marketing_target,
  -911 as optimize_target,
  -911 as platform,
  -911 as promotion_target,
  '0' as is_marketing_product,
  '-911' as agent_user_id,
  coalesce(sum(imp_cnt),0) as imp_num  --'曝光量'
  ,coalesce(sum(click_cnt),0) as click_num  --'点击量'
  ,coalesce(sum(like_cnt),0) as like_num  --'点赞量'
  ,coalesce(sum(fav_cnt),0) as fav_num  --'收藏量'
  ,coalesce(sum(cmt_cnt),0) as cmt_num  --'评论量'
  ,coalesce(sum(follow_cnt),0) as follow_num  --'关注量'
  ,coalesce(sum(share_cnt),0) as share_num  --'分享量'
  ,coalesce(sum(coalesce(like_cnt,0)+coalesce(share_cnt,0)+coalesce(fav_cnt,0)+coalesce(follow_cnt,0)+coalesce(cmt_cnt,0)),0) as engagement_num
  ,0 as conversion_cnt,
  0 as msg_income_cnt,
  0 as msg_income_user_cnt,
  0 as msg_open_msg_cnt,
  0 as msg_open_cnt,
  0 as msg_open_user_cnt,
  0 as msg_leads_num,
  0 as msg_leads_user_cnt,
  0 as leads_submit_num,
  0 as valid_leads_num,
  0 as msg_first_15s_reply_cnt,
  0 as msg_first_15s_cnt,
  0 as msg_driving_open_wait_duration,
  0 as msg_driving_open_30s_cnt,
  0 as msg_driving_open_60s_cnt,
  0 as msg_driving_open_effi_cnt,
  0 as friend_add_cnt,
  0 as friend_add_success_cnt,
  0 as friend_chat_open_cnt,
  0 as live_reserve_cnt,
  0 as live_subscribe_user_cnt,
  0 as live_comment_cnt,
  0 as live_watch_num,
  0 as live_effective_shutdown_num,
  0 as live_effective_shutdown_distinct_eventid_num,
  0 as live_watch_duration,
  0 as live_watch_distinct_eventid_num,
  0 as live_order_num,
  0 as live_gmv,
  0 as live_follow_num,
  0 as enter_seller_cnt_7d,
  0 as goods_view_cnt_7d,
  0 as add_cart_cnt_7d,
  0 as purchase_order_num_7d,
  0 as purchase_order_gmv_7d,
  0 as deal_order_num_7d,
  0 as deal_order_gmv_7d,
  0 as search_component_click_cnt,
  0 as ecm_unclosed_purchase_order_num_7d,
  0 as ecm_unclosed_purchase_order_gmv_7d,
  0 as ecm_unclosed_goods_view_cnt_7d
 
FROM
 redcdm.dm_ads_pub_product_account_detail_td_df
WHERE
  dtm = '{{ds_nodash}}' and (module<>'效果')
group by date_key,
  module,
  product,
  brand_account_id,
  --v_seller_id,
  market_target_type,
  optimize_target;


insert overwrite table redcdm.dm_ad_pub_advertiser_product_metrics_detail_df partition(dtm = '{{ ds_nodash }}')
SELECT
  date_key,
  module,
  product,
  brand_account_id,
  v_seller_id,
  optimize_target,
  market_target,
  marketing_target,
  platform,
  ads_note_type,
  promotion_target,
  is_marketing_product,
  agent_user_id,
  advertiser_id,
  imp_num,
  click_num,
  like_num,
  fav_num,
  cmt_num,
  follow_num,
  share_num,
  engagement_num,
  income_amt,
  cash_income_amt,
  perf_cash_income_amt,
  perf_income_amt,
  cost_creativity_num,
  new_cost_creativity_num,
  new_cost_campaign_num,
  campaign_num,
  cost_campaign_num,
  create_campaign_num,
  create_cost_campaign_num,
  effecient_campaign_num,
  invest_duration,
  total_note_num,
  new_note_num,
  rtb_note_num,
  rtb_cost_note_num,
  new_cost_note_num,
  market_product_cost_income_amt,
  market_product_budget_income_amt,
  market_cost_income_amt,
  market_budget_income_amt,
  rtb_cost_income_amt,
  rtb_budget_income_amt,
  min_advertiser_budget_income_amt,
  advertiser_cost_income_amt,
  cash_balance,
  cash_balance_and_cost,
  launch_finish_campaign_num,
  deal_gmv,
  conversion_cnt,
  msg_income_cnt,
  msg_income_user_cnt,
  msg_open_msg_cnt,
  msg_open_cnt,
  msg_open_user_cnt,
  msg_leads_num,
  msg_leads_user_cnt,
  leads_submit_num,
  valid_leads_num,
  msg_first_15s_reply_cnt,
  msg_first_15s_cnt,
  msg_driving_open_wait_duration,
  msg_driving_open_30s_cnt,
  msg_driving_open_60s_cnt,
  msg_driving_open_effi_cnt,
  friend_add_cnt,
  friend_add_success_cnt,
  friend_chat_open_cnt,
  live_reserve_cnt,
  live_subscribe_user_cnt,
  live_comment_cnt,
  live_watch_num,
  live_effective_shutdown_num,
  live_effective_shutdown_distinct_eventid_num,
  live_watch_duration,
  live_watch_distinct_eventid_num,
  live_order_num,
  live_gmv,
  live_follow_num,
  enter_seller_cnt_7d,
  goods_view_cnt_7d,
  add_cart_cnt_7d,
  purchase_order_num_7d,
  purchase_order_gmv_7d,
  deal_order_num_7d,
  deal_order_gmv_7d,
  search_component_click_cnt,
  ecm_unclosed_purchase_order_num_7d,
  ecm_unclosed_purchase_order_gmv_7d,
  ecm_unclosed_goods_view_cnt_7d
FROM
(SELECT
  date_key,
  module,
  product,
  brand_account_id,
  v_seller_id,
  optimize_target,
  market_target,
  marketing_target,
  platform,
  ads_note_type,
  promotion_target,
  is_marketing_product,
  agent_user_id,
  advertiser_id,
0 as imp_num,
0 as click_num,
0 as like_num,
0 as fav_num,
0 as cmt_num,
0 as follow_num,
0 as share_num,
0 as engagement_num,
cost as income_amt,
cash_cost as cash_income_amt,
cash_cost as perf_cash_income_amt,
cost as perf_income_amt,
0 as cost_creativity_num,
0 as new_cost_creativity_num,
0 as new_cost_campaign_num,
0 as campaign_num,
0 as cost_campaign_num,
0 as create_campaign_num,
0 as create_cost_campaign_num,
0 as effecient_campaign_num,
0 as invest_duration,
0 as total_note_num,
0 as new_note_num,
0 as rtb_note_num,
0 as rtb_cost_note_num,
0 as new_cost_note_num,
0 as market_product_cost_income_amt,
0 as market_product_budget_income_amt,
0 as market_cost_income_amt,
0 as market_budget_income_amt,
0 as rtb_cost_income_amt,
0 as rtb_budget_income_amt,
0 as min_advertiser_budget_income_amt,
0 as advertiser_cost_income_amt,
0 as cash_balance,
0 as cash_balance_and_cost,
0 as launch_finish_campaign_num,
0 as deal_gmv,
0 as conversion_cnt,
0 as msg_income_cnt,
0 as msg_income_user_cnt,
0 as msg_open_msg_cnt,
0 as msg_open_cnt,
0 as msg_open_user_cnt,
0 as msg_leads_num,
0 as msg_leads_user_cnt,
0 as leads_submit_num,
0 as valid_leads_num,
0 as msg_first_15s_reply_cnt,
0 as msg_first_15s_cnt,
0 as msg_driving_open_wait_duration,
0 as msg_driving_open_30s_cnt,
0 as msg_driving_open_60s_cnt,
0 as msg_driving_open_effi_cnt,
0 as friend_add_cnt,
0 as friend_add_success_cnt,
0 as friend_chat_open_cnt,
0 as live_reserve_cnt,
0 as live_subscribe_user_cnt,
0 as live_comment_cnt,
0 as live_watch_num,
0 as live_effective_shutdown_num,
0 as live_effective_shutdown_distinct_eventid_num,
0 as live_watch_duration,
0 as live_watch_distinct_eventid_num,
0 as live_order_num,
0 as live_gmv,
0 as live_follow_num,
0 as enter_seller_cnt_7d,
0 as goods_view_cnt_7d,
0 as add_cart_cnt_7d,
0 as purchase_order_num_7d,
0 as purchase_order_gmv_7d,
0 as deal_order_num_7d,
0 as deal_order_gmv_7d,
0 as search_component_click_cnt,
0 as ecm_unclosed_purchase_order_num_7d,
0 as ecm_unclosed_purchase_order_gmv_7d,
0 as ecm_unclosed_goods_view_cnt_7d
FROM
  temp.temp_dm_ad_pub_advertiser_product_metrics_detail_df_{{ds_nodash}}_01 
union all 
SELECT
  date_key,
  module,
  product,
  brand_account_id,
  v_seller_id,
  optimize_target,
  market_target,
  marketing_target,
  platform,
  ads_note_type,
  promotion_target,
  is_marketing_product,
  agent_user_id,
  advertiser_id,
  imp_num,
  click_num,
  like_num,
  fav_num,
  cmt_num,
  follow_num,
  share_num,
  engagement_num,
0 as income_amt,
0 as cash_income_amt,
0 as perf_cash_income_amt,
0 as perf_income_amt,
0 as cost_creativity_num,
0 as new_cost_creativity_num,
0 as new_cost_campaign_num,
0 as campaign_num,
0 as cost_campaign_num,
0 as create_campaign_num,
0 as create_cost_campaign_num,
0 as effecient_campaign_num,
0 as invest_duration,
0 as total_note_num,
0 as new_note_num,
0 as rtb_note_num,
0 as rtb_cost_note_num,
0 as new_cost_note_num,
0 as market_product_cost_income_amt,
0 as market_product_budget_income_amt,
0 as market_cost_income_amt,
0 as market_budget_income_amt,
0 as rtb_cost_income_amt,
0 as rtb_budget_income_amt,
0 as min_advertiser_budget_income_amt,
0 as advertiser_cost_income_amt,
0 as cash_balance,
0 as cash_balance_and_cost,
0 as launch_finish_campaign_num,
0 as deal_gmv,
  conversion_cnt,
  msg_income_cnt,
  msg_income_user_cnt,
  msg_open_msg_cnt,
  msg_open_cnt,
  msg_open_user_cnt,
  msg_leads_num,
  msg_leads_user_cnt,
  leads_submit_num,
  valid_leads_num,
  msg_first_15s_reply_cnt,
  msg_first_15s_cnt,
  msg_driving_open_wait_duration,
  msg_driving_open_30s_cnt,
  msg_driving_open_60s_cnt,
  msg_driving_open_effi_cnt,
  friend_add_cnt,
  friend_add_success_cnt,
  friend_chat_open_cnt,
  live_reserve_cnt,
  live_subscribe_user_cnt,
  live_comment_cnt,
  live_watch_num,
  live_effective_shutdown_num,
  live_effective_shutdown_distinct_eventid_num,
  live_watch_duration,
  live_watch_distinct_eventid_num,
  live_order_num,
  live_gmv,
  live_follow_num,
  enter_seller_cnt_7d,
  goods_view_cnt_7d,
  add_cart_cnt_7d,
  purchase_order_num_7d,
  purchase_order_gmv_7d,
  deal_order_num_7d,
  deal_order_gmv_7d,
  search_component_click_cnt,
  ecm_unclosed_purchase_order_num_7d,
  ecm_unclosed_purchase_order_gmv_7d,
  ecm_unclosed_goods_view_cnt_7d
FROM
  temp.temp_dm_ad_pub_advertiser_product_metrics_detail_df_{{ds_nodash}}_02
union all 
--预算
SELECT
   date_key,
'效果' as module,
  product,
  brand_account_id,
  null as v_seller_id,
  -911 as optimize_target,
  market_target,
  -911 as marketing_target,
  -911 as platform,
  '非竞价' as ads_note_type,
  -911 as promotion_target,
  '0' as is_marketing_product,
  null as agent_user_id,
  advertiser_id,
0 as imp_num,
0 as click_num,
0 as like_num,
0 as fav_num,
0 as cmt_num,
0 as follow_num,
0 as share_num,
0 as engagement_num,
0 as income_amt,
0 as cash_income_amt,
0 as perf_cash_income_amt,
0 as perf_income_amt,
0 as cost_creativity_num,
0 as new_cost_creativity_num,
0 as new_cost_campaign_num,
0 as campaign_num,
0 as cost_campaign_num,
0 as create_campaign_num,
0 as create_cost_campaign_num,
0 as effecient_campaign_num,
0 as invest_duration,
0 as total_note_num,
0 as new_note_num,
0 as rtb_note_num,
0 as rtb_cost_note_num,
0 as new_cost_note_num,
  market_product_cost_income_amt,
  market_product_budget_income_amt,
  market_cost_income_amt,
  market_budget_income_amt,
  rtb_cost_income_amt,
  rtb_budget_income_amt,
  min_advertiser_budget_income_amt,
  advertiser_cost_income_amt,
  cash_balance,
  cash_balance_and_cost,
  launch_finish_campaign_num,
  deal_gmv,
0 as conversion_cnt,
0 as msg_income_cnt,
0 as msg_income_user_cnt,
0 as msg_open_msg_cnt,
0 as msg_open_cnt,
0 as msg_open_user_cnt,
0 as msg_leads_num,
0 as msg_leads_user_cnt,
0 as leads_submit_num,
0 as valid_leads_num,
0 as msg_first_15s_reply_cnt,
0 as msg_first_15s_cnt,
0 as msg_driving_open_wait_duration,
0 as msg_driving_open_30s_cnt,
0 as msg_driving_open_60s_cnt,
0 as msg_driving_open_effi_cnt,
0 as friend_add_cnt,
0 as friend_add_success_cnt,
0 as friend_chat_open_cnt,
0 as live_reserve_cnt,
0 as live_subscribe_user_cnt,
0 as live_comment_cnt,
0 as live_watch_num,
0 as live_effective_shutdown_num,
0 as live_effective_shutdown_distinct_eventid_num,
0 as live_watch_duration,
0 as live_watch_distinct_eventid_num,
0 as live_order_num,
0 as live_gmv,
0 as live_follow_num,
0 as enter_seller_cnt_7d,
0 as goods_view_cnt_7d,
0 as add_cart_cnt_7d,
0 as purchase_order_num_7d,
0 as purchase_order_gmv_7d,
0 as deal_order_num_7d,
0 as deal_order_gmv_7d,
0 as search_component_click_cnt,
0 as ecm_unclosed_purchase_order_num_7d,
0 as ecm_unclosed_purchase_order_gmv_7d,
0 as ecm_unclosed_goods_view_cnt_7d
FROM
  redcdm.dws_ad_rtb_advertiser_budget_metrics_df
WHERE
  dtm = '{{ds_nodash}}'
union all 
--基建
SELECT
  date_key,
'效果' as module,
  '整体' as product,
  brand_account_id,
  null as v_seller_id,
  -911 as optimize_target,
  '整体' as market_target,
  -911 as marketing_target,
  -911 as platform,
  '非竞价' as ads_note_type,
  -911 as promotion_target,
  '0' as is_marketing_product,
  null as agent_user_id,
  advertiser_id,
0 as imp_num,
0 as click_num,
0 as like_num,
0 as fav_num,
0 as cmt_num,
0 as follow_num,
0 as share_num,
0 as engagement_num,
0 as income_amt,
0 as cash_income_amt,
0 as perf_cash_income_amt,
0 as perf_income_amt,
  cost_creativity_num,
  new_cost_creativity_num,
  new_cost_campaign_num,
  campaign_num,
  cost_campaign_num,
  create_campaign_num,
  create_cost_campaign_num,
  effecient_campaign_num,
  invest_duration,
  total_note_num,
  new_note_num,
  rtb_note_num,
  rtb_cost_note_num,
  new_cost_note_num,
0 as market_product_cost_income_amt,
0 as market_product_budget_income_amt,
0 as market_cost_income_amt,
0 as market_budget_income_amt,
0 as rtb_cost_income_amt,
0 as rtb_budget_income_amt,
0 as min_advertiser_budget_income_amt,
0 as advertiser_cost_income_amt,
0 as cash_balance,
0 as cash_balance_and_cost,
0 as launch_finish_campaign_num,
0 as deal_gmv,
0 as conversion_cnt,
0 as msg_income_cnt,
0 as msg_income_user_cnt,
0 as msg_open_msg_cnt,
0 as msg_open_cnt,
0 as msg_open_user_cnt,
0 as msg_leads_num,
0 as msg_leads_user_cnt,
0 as leads_submit_num,
0 as valid_leads_num,
0 as msg_first_15s_reply_cnt,
0 as msg_first_15s_cnt,
0 as msg_driving_open_wait_duration,
0 as msg_driving_open_30s_cnt,
0 as msg_driving_open_60s_cnt,
0 as msg_driving_open_effi_cnt,
0 as friend_add_cnt,
0 as friend_add_success_cnt,
0 as friend_chat_open_cnt,
0 as live_reserve_cnt,
0 as live_subscribe_user_cnt,
0 as live_comment_cnt,
0 as live_watch_num,
0 as live_effective_shutdown_num,
0 as live_effective_shutdown_distinct_eventid_num,
0 as live_watch_duration,
0 as live_watch_distinct_eventid_num,
0 as live_order_num,
0 as live_gmv,
0 as live_follow_num,
0 as enter_seller_cnt_7d,
0 as goods_view_cnt_7d,
0 as add_cart_cnt_7d,
0 as purchase_order_num_7d,
0 as purchase_order_gmv_7d,
0 as deal_order_num_7d,
0 as deal_order_gmv_7d,
0 as search_component_click_cnt,
0 as ecm_unclosed_purchase_order_num_7d,
0 as ecm_unclosed_purchase_order_gmv_7d,
0 as ecm_unclosed_goods_view_cnt_7d
FROM
  redcdm.dws_ad_rtb_advertiser_supply_metrics_di
WHERE
  dtm >= '20230101' and dtm<='{{ds_nodash}}'
)base 
group by date_key,
  module,
  product,
  brand_account_id,
  v_seller_id,
  optimize_target,
  market_target,
  marketing_target,
  platform,
  ads_note_type,
  promotion_target,
  is_marketing_product,
  agent_user_id,
  advertiser_id

