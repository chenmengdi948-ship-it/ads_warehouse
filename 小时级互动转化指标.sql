drop table if exists temp.dm_ads_rtb_creativity_product_hi_01{{ds_nodash}}_{{ts[11:13]}};
create table temp.dm_ads_rtb_creativity_product_hi_01{{ds_nodash}}_{{ts[11:13]}}
select
  coalesce(t1.creativity_id, t2.creativity_id) as creativity_id,
  coalesce(t1.product, t2.product) as product,
  coalesce(t1.campaign_id, t2.campaign_id) as campaign_id,
  coalesce(t1.unit_id, t2.unit_id) as unit_id,
  coalesce(t1.advertiser_id, t2.advertiser_id) as advertiser_id,
  coalesce(t1.marketing_target,t2.marketing_target) as marketing_target,
  coalesce(t1.optimize_target, t2.optimize_target) as optimize_target,
  coalesce(t1.market_target_type, t2.market_target_type) as market_target_type,
  ystd_imp_cnt,
  ystd_click_cnt,
  ystd_cost,
  imp_cnt,
  click_cnt,
  cost,
  like_cnt,
  comment_cnt,
  share_cnt,
  follow_cnt,
  collect_cnt,
  save_cnt,
  screenshot_cnt,
  engage_cnt,
  add_cart_cnt,
  buy_now_cnt,
  goods_view_cnt,
  seller_view_cnt,
  rgmv,
  leads_cnt,
  valid_leads_cnt,
  leads_success_cnt,
  leads_success_valid_cnt,
  msg_num,
  msg_open_num,
  msg_driven_open_num,
  live_24h_click_rgmv,
  live_24h_click_effective_shutdown_num,
  all_24h_click_rgmv,
  out_click_goods_view_pv_7d,
  out_click_rgmv_7d,
  out_click_rgmv_15d,
  out_click_rgmv_30d,
  total_order_num,
  presale_order_gmv_7d,
  purchase_order_gmv_7d,
  search_after_read_num,
  ystd_like_cnt,
  ystd_comment_cnt,
  ystd_share_cnt,
  ystd_follow_cnt,
  ystd_collect_cnt,
  ystd_save_cnt,
  ystd_screenshot_cnt,
  ystd_engage_cnt,
  ystd_add_cart_cnt,
  ystd_buy_now_cnt,
  ystd_goods_view_cnt,
  ystd_seller_view_cnt,
  ystd_rgmv,
  ystd_leads_cnt,
  ystd_valid_leads_cnt,
  ystd_leads_success_cnt,
  ystd_leads_success_valid_cnt,
  ystd_msg_num,
  ystd_msg_open_num,
  ystd_msg_driven_open_num,
  ystd_live_24h_click_rgmv,
  ystd_live_24h_click_effective_shutdown_num,
  ystd_all_24h_click_rgmv,
  ystd_out_click_goods_view_pv_7d,
  ystd_out_click_rgmv_7d,
  ystd_out_click_rgmv_15d,
  ystd_out_click_rgmv_30d,
  ystd_total_order_num,
  ystd_presale_order_gmv_7d,
  ystd_purchase_order_gmv_7d,
  ystd_search_after_read_num,
  case when coalesce(t1.hh, t2.hh) = '23' then null else substring(from_unixtime(unix_timestamp(concat('{{ds}}',' ',coalesce(t1.hh, t2.hh),':00:00'))+3600),12,2) end as after_hh,
  '{{ds_nodash}}' as dtm,
  coalesce(t1.hh, t2.hh) as hh
from
  (
    select
      creativity_id,
      product,
      campaign_id,
      unit_id,
      advertiser_id,
      marketing_target,
      optimize_target,
      case
        when marketing_target in (3, 8, 14, 15) then '闭环电商'
        when marketing_target = 13 then '非闭环电商'
        when marketing_target in (2, 5, 9) then '线索'
        when marketing_target not in (3, 8, 2, 5, 9, 13, 14, 15) then '种草'
      end as market_target_type,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then like_cnt
          else 0
        end
      ) as ystd_like_cnt,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then comment_cnt
          else 0
        end
      ) as ystd_comment_cnt,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then share_cnt
          else 0
        end
      ) as ystd_share_cnt,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then follow_cnt
          else 0
        end
      ) as ystd_follow_cnt,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then collect_cnt
          else 0
        end
      ) as ystd_collect_cnt,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then save_cnt
          else 0
        end
      ) as ystd_save_cnt,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then screenshot_cnt
          else 0
        end
      ) as ystd_screenshot_cnt,
      sum(case
          when dtm = '{{ds_1_days_ago_nodash}}' then coalesce(like_cnt,0)+coalesce(comment_cnt,0)+coalesce(follow_cnt,0)+coalesce(share_cnt,0)+coalesce(collect_cnt,0) else 0 end) as ystd_engage_cnt,
      sum(case
          when dtm = '{{ds_nodash}}' then coalesce(like_cnt,0)+coalesce(comment_cnt,0)+coalesce(follow_cnt,0)+coalesce(share_cnt,0)+coalesce(collect_cnt,0) else 0 end ) as engage_cnt,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then add_cart_cnt
          else 0
        end
      ) as ystd_add_cart_cnt,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then buy_now_cnt
          else 0
        end
      ) as ystd_buy_now_cnt,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then goods_view_cnt
          else 0
        end
      ) as ystd_goods_view_cnt,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then seller_view_cnt
          else 0
        end
      ) as ystd_seller_view_cnt,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then rgmv
          else 0
        end
      ) as ystd_rgmv,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then leads_cnt
          else 0
        end
      ) as ystd_leads_cnt,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then valid_leads_cnt
          else 0
        end
      ) as ystd_valid_leads_cnt,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then leads_success_cnt
          else 0
        end
      ) as ystd_leads_success_cnt,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then leads_success_valid_cnt
          else 0
        end
      ) as ystd_leads_success_valid_cnt,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then msg_num
          else 0
        end
      ) as ystd_msg_num,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then msg_open_num
          else 0
        end
      ) as ystd_msg_open_num,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then msg_driven_open_num
          else 0
        end
      ) as ystd_msg_driven_open_num,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then live_24h_click_rgmv
          else 0
        end
      ) as ystd_live_24h_click_rgmv,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then live_24h_click_effective_shutdown_num
          else 0
        end
      ) as ystd_live_24h_click_effective_shutdown_num,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then all_24h_click_rgmv
          else 0
        end
      ) as ystd_all_24h_click_rgmv,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then out_click_goods_view_pv_7d
          else 0
        end
      ) as ystd_out_click_goods_view_pv_7d,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then out_click_rgmv_7d
          else 0
        end
      ) as ystd_out_click_rgmv_7d,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then out_click_rgmv_15d
          else 0
        end
      ) as ystd_out_click_rgmv_15d,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then out_click_rgmv_30d
          else 0
        end
      ) as ystd_out_click_rgmv_30d,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then total_order_num
          else 0
        end
      ) as ystd_total_order_num,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then presale_order_gmv_7d
          else 0
        end
      ) as ystd_presale_order_gmv_7d,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then purchase_order_gmv_7d
          else 0
        end
      ) as ystd_purchase_order_gmv_7d,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then search_after_read_num
          else 0
        end
      ) as ystd_search_after_read_num,
      sum(
        case
          when dtm = '{{ds_nodash}}' then like_cnt
          else 0
        end
      ) as like_cnt,
      sum(
        case
          when dtm = '{{ds_nodash}}' then comment_cnt
          else 0
        end
      ) as comment_cnt,
      sum(
        case
          when dtm = '{{ds_nodash}}' then share_cnt
          else 0
        end
      ) as share_cnt,
      sum(
        case
          when dtm = '{{ds_nodash}}' then follow_cnt
          else 0
        end
      ) as follow_cnt,
      sum(
        case
          when dtm = '{{ds_nodash}}' then collect_cnt
          else 0
        end
      ) as collect_cnt,
      sum(
        case
          when dtm = '{{ds_nodash}}' then save_cnt
          else 0
        end
      ) as save_cnt,
      sum(
        case
          when dtm = '{{ds_nodash}}' then screenshot_cnt
          else 0
        end
      ) as screenshot_cnt,
      sum(
        case
          when dtm = '{{ds_nodash}}' then add_cart_cnt
          else 0
        end
      ) as add_cart_cnt,
      sum(
        case
          when dtm = '{{ds_nodash}}' then buy_now_cnt
          else 0
        end
      ) as buy_now_cnt,
      sum(
        case
          when dtm = '{{ds_nodash}}' then goods_view_cnt
          else 0
        end
      ) as goods_view_cnt,
      sum(
        case
          when dtm = '{{ds_nodash}}' then seller_view_cnt
          else 0
        end
      ) as seller_view_cnt,
      sum(
        case
          when dtm = '{{ds_nodash}}' then rgmv
          else 0
        end
      ) as rgmv,
      sum(
        case
          when dtm = '{{ds_nodash}}' then leads_cnt
          else 0
        end
      ) as leads_cnt,
      sum(
        case
          when dtm = '{{ds_nodash}}' then valid_leads_cnt
          else 0
        end
      ) as valid_leads_cnt,
      sum(
        case
          when dtm = '{{ds_nodash}}' then leads_success_cnt
          else 0
        end
      ) as leads_success_cnt,
      sum(
        case
          when dtm = '{{ds_nodash}}' then leads_success_valid_cnt
          else 0
        end
      ) as leads_success_valid_cnt,
      sum(
        case
          when dtm = '{{ds_nodash}}' then msg_num
          else 0
        end
      ) as msg_num,
      sum(
        case
          when dtm = '{{ds_nodash}}' then msg_open_num
          else 0
        end
      ) as msg_open_num,
      sum(
        case
          when dtm = '{{ds_nodash}}' then msg_driven_open_num
          else 0
        end
      ) as msg_driven_open_num,
      sum(
        case
          when dtm = '{{ds_nodash}}' then live_24h_click_rgmv
          else 0
        end
      ) as live_24h_click_rgmv,
      sum(
        case
          when dtm = '{{ds_nodash}}' then live_24h_click_effective_shutdown_num
          else 0
        end
      ) as live_24h_click_effective_shutdown_num,
      sum(
        case
          when dtm = '{{ds_nodash}}' then all_24h_click_rgmv
          else 0
        end
      ) as all_24h_click_rgmv,
      sum(
        case
          when dtm = '{{ds_nodash}}' then out_click_goods_view_pv_7d
          else 0
        end
      ) as out_click_goods_view_pv_7d,
      sum(
        case
          when dtm = '{{ds_nodash}}' then out_click_rgmv_7d
          else 0
        end
      ) as out_click_rgmv_7d,
      sum(
        case
          when dtm = '{{ds_nodash}}' then out_click_rgmv_15d
          else 0
        end
      ) as out_click_rgmv_15d,
      sum(
        case
          when dtm = '{{ds_nodash}}' then out_click_rgmv_30d
          else 0
        end
      ) as out_click_rgmv_30d,
      sum(
        case
          when dtm = '{{ds_nodash}}' then total_order_num
          else 0
        end
      ) as total_order_num,
      sum(
        case
          when dtm = '{{ds_nodash}}' then presale_order_gmv_7d
          else 0
        end
      ) as presale_order_gmv_7d,
      sum(
        case
          when dtm = '{{ds_nodash}}' then purchase_order_gmv_7d
          else 0
        end
      ) as purchase_order_gmv_7d,
      sum(
        case
          when dtm = '{{ds_nodash}}' then search_after_read_num
          else 0
        end
      ) as search_after_read_num,
      '{{ds_nodash}}' as dtm,
      hh
    from
      kafka.kafka_qcsh4_rlm1_dws_ads_cvr_rtb_creativity_cube_1d_di_main
    where
      dtm <= '{{ds_nodash}}'
      and dtm >= '{{ds_1_days_ago_nodash}}'
      and hh <= '24'
      and cube_name = 'creativity'
      and advertiser_id <> 0
    group by
      creativity_id,
      product,
      campaign_id,
      unit_id,
      advertiser_id,
      marketing_target,
      optimize_target,
      case
        when marketing_target in (3, 8, 14, 15) then '闭环电商'
        when marketing_target = 13 then '非闭环电商'
        when marketing_target in (2, 5, 9) then '线索'
        when marketing_target not in (3, 8, 2, 5, 9, 13, 14, 15) then '种草'
      end,
      hh
  ) t1
  full outer join (
    select
      creativity_id,
      product,
      campaign_id,
      unit_id,
      advertiser_id,
      marketing_target,
      optimize_target,
      case
        when marketing_target in (3, 8, 14, 15) then '闭环电商'
        when marketing_target = 13 then '非闭环电商'
        when marketing_target in (2, 5, 9) then '线索'
        when marketing_target not in (3, 8, 2, 5, 9, 13, 14, 15) then '种草'
      end as market_target_type,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then imp_cnt
          else 0
        end
      ) as ystd_imp_cnt,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then click_cnt
          else 0
        end
      ) as ystd_click_cnt,
      sum(
        case
          when dtm = '{{ds_1_days_ago_nodash}}' then cost
          else 0
        end
      ) as ystd_cost,
      sum(
        case
          when dtm = '{{ds_nodash}}' then imp_cnt
          else 0
        end
      ) as imp_cnt,
      sum(
        case
          when dtm = '{{ds_nodash}}' then click_cnt
          else 0
        end
      ) as click_cnt,
      sum(
        case
          when dtm = '{{ds_nodash}}' then cost
          else 0
        end
      ) as cost,
      '{{ds_nodash}}' as dtm,
      hh
    from
      kafka.kafka_qcsh4_rlm1_dw_ads_log_creativity_cube_hh_rt_main
    where
      dtm <= '{{ds_nodash}}'
      and dtm >= '{{ds_1_days_ago_nodash}}'
      and hh <= '24'
      and cube_name = 'creativity'
      and advertiser_id <> 0
    group by
      creativity_id,
      product,
      campaign_id,
      unit_id,
      advertiser_id,
      marketing_target,
      optimize_target,
      case
        when marketing_target in (3, 8, 14, 15) then '闭环电商'
        when marketing_target = 13 then '非闭环电商'
        when marketing_target in (2, 5, 9) then '线索'
        when marketing_target not in (3, 8, 2, 5, 9, 13, 14, 15) then '种草'
      end,
      hh
  ) t2 on t1.creativity_id = t2.creativity_id
  and t1.hh = t2.hh
;
insert
  overwrite table redcdm.dm_ads_rtb_creativity_product_hi partition (dtm, hh)
select 
  coalesce(t1.creativity_id, t2.creativity_id) as creativity_id,
  coalesce(t1.product, t2.product) as product,
  coalesce(t1.campaign_id, t2.campaign_id) as campaign_id,
  coalesce(t1.unit_id, t2.unit_id) as unit_id,
  coalesce(t1.advertiser_id, t2.advertiser_id) as advertiser_id,
  coalesce(t1.marketing_target,t2.marketing_target) as marketing_target,
  coalesce(t1.optimize_target, t2.optimize_target) as optimize_target,
  coalesce(t1.market_target_type, t2.market_target_type) as market_target_type,
  ystd_imp_cnt,
  ystd_click_cnt,
  ystd_cost,
  imp_cnt_before as before_imp_cnt,
  click_cnt_before as before_click_cnt,
  cost_before as before_cost,
  imp_cnt,
  click_cnt,
  cost,
  like_cnt,
  comment_cnt,
  share_cnt,
  follow_cnt,
  collect_cnt,
  save_cnt,
  screenshot_cnt,
  engage_cnt,
  add_cart_cnt,
  buy_now_cnt,
  goods_view_cnt,
  seller_view_cnt,
  rgmv,
  leads_cnt,
  valid_leads_cnt,
  leads_success_cnt,
  leads_success_valid_cnt,
  msg_num,
  msg_open_num,
  msg_driven_open_num,
  live_24h_click_rgmv,
  live_24h_click_effective_shutdown_num,
  all_24h_click_rgmv,
  out_click_goods_view_pv_7d,
  out_click_rgmv_7d,
  out_click_rgmv_15d,
  out_click_rgmv_30d,
  total_order_num,
  presale_order_gmv_7d,
  purchase_order_gmv_7d,
  search_after_read_num,
  ystd_like_cnt,
  ystd_comment_cnt,
  ystd_share_cnt,
  ystd_follow_cnt,
  ystd_collect_cnt,
  ystd_save_cnt,
  ystd_screenshot_cnt,
  ystd_engage_cnt,
  ystd_add_cart_cnt,
  ystd_buy_now_cnt,
  ystd_goods_view_cnt,
  ystd_seller_view_cnt,
  ystd_rgmv,
  ystd_leads_cnt,
  ystd_valid_leads_cnt,
  ystd_leads_success_cnt,
  ystd_leads_success_valid_cnt,
  ystd_msg_num,
  ystd_msg_open_num,
  ystd_msg_driven_open_num,
  ystd_live_24h_click_rgmv,
  ystd_live_24h_click_effective_shutdown_num,
  ystd_all_24h_click_rgmv,
  ystd_out_click_goods_view_pv_7d,
  ystd_out_click_rgmv_7d,
  ystd_out_click_rgmv_15d,
  ystd_out_click_rgmv_30d,
  ystd_total_order_num,
  ystd_presale_order_gmv_7d,
  ystd_purchase_order_gmv_7d,
  ystd_search_after_read_num,
  
  like_cnt_before,
  comment_cnt_before,
  share_cnt_before,
  follow_cnt_before,
  collect_cnt_before,
  save_cnt_before,
  screenshot_cnt_before,
  engage_cnt_before,
  add_cart_cnt_before,
  buy_now_cnt_before,
  goods_view_cnt_before,
  seller_view_cnt_before,
  rgmv_before,
  leads_cnt_before,
  valid_leads_cnt_before,
  leads_success_cnt_before,
  leads_success_valid_cnt_before,
  msg_num_before,
  msg_open_num_before,
  msg_driven_open_num_before,
  live_24h_click_rgmv_before,
  live_24h_click_effective_shutdown_num_before,
  all_24h_click_rgmv_before,
  out_click_goods_view_pv_7d_before,
  out_click_rgmv_7d_before,
  out_click_rgmv_15d_before,
  out_click_rgmv_30d_before,
  total_order_num_before,
  presale_order_gmv_7d_before,
  purchase_order_gmv_7d_before,
  search_after_read_num_before,
  '{{ds_nodash}}' as dtm,
  coalesce(t1.hh, t2.after_hh) as hh
from temp.dm_ads_rtb_creativity_product_hi_01{{ds_nodash}}_{{ts[11:13]}} t1
full outer join 
(select  creativity_id,
  product,
  campaign_id,
  unit_id,
  advertiser_id,
  marketing_target,
  optimize_target,
  market_target_type,
  imp_cnt as imp_cnt_before,
click_cnt as click_cnt_before,
cost as cost_before,
like_cnt as like_cnt_before,
comment_cnt as comment_cnt_before,
share_cnt as share_cnt_before,
follow_cnt as follow_cnt_before,
collect_cnt as collect_cnt_before,
save_cnt as save_cnt_before,
screenshot_cnt as screenshot_cnt_before,
engage_cnt as engage_cnt_before,
add_cart_cnt as add_cart_cnt_before,
buy_now_cnt as buy_now_cnt_before,
goods_view_cnt as goods_view_cnt_before,
seller_view_cnt as seller_view_cnt_before,
rgmv as rgmv_before,
leads_cnt as leads_cnt_before,
valid_leads_cnt as valid_leads_cnt_before,
leads_success_cnt as leads_success_cnt_before,
leads_success_valid_cnt as leads_success_valid_cnt_before,
msg_num as msg_num_before,
msg_open_num as msg_open_num_before,
msg_driven_open_num as msg_driven_open_num_before,
live_24h_click_rgmv as live_24h_click_rgmv_before,
live_24h_click_effective_shutdown_num as live_24h_click_effective_shutdown_num_before,
all_24h_click_rgmv as all_24h_click_rgmv_before,
out_click_goods_view_pv_7d as out_click_goods_view_pv_7d_before,
out_click_rgmv_7d as out_click_rgmv_7d_before,
out_click_rgmv_15d as out_click_rgmv_15d_before,
out_click_rgmv_30d as out_click_rgmv_30d_before,
total_order_num as total_order_num_before,
presale_order_gmv_7d as presale_order_gmv_7d_before,
purchase_order_gmv_7d as purchase_order_gmv_7d_before,
search_after_read_num as search_after_read_num_before,
  after_hh
from temp.dm_ads_rtb_creativity_product_hi_01{{ds_nodash}}_{{ts[11:13]}} 
where after_hh is not null 
)t2
on t1.hh=t2.after_hh and t1.creativity_id = t2.creativity_id 