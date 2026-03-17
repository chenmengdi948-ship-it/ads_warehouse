select *
from 
(select
  --`date_key`,
  `attri_date`,
  `group_id`,
  `group_name`,
  `creativity_id`,
  `creativity_name`,
  `unit_id`,
  `unit_name`,
  `campaign_id`,
  `campaign_name`,
  `advertiser_id`,
  `advertiser_name`,
  `v_seller_id`,
  a.`brand_account_id`,
  b.brand_user_name as `brand_account_name`,
  `ads_uuid`,
  a.`note_id`,
  `title`,
  `note_imageurl`,
  `create_time`,
  `note_level`,
  a.`spu_id`,
  a.`spu_name`,
  `fee`,
  `imp_num`,
  `click_num`,
  `fav_num`,
  `cmt_num`,
  `share_num`,
  `follow_num`,
  `collect_num`,
  `engage_num`,
  `action_button_click_num`,
  `screenshot_num`,
  `save_num`,
  `reserve_pv`,
  `live_subscribe_cnt`,
  `live_24h_click_watch_num`,
  `live_avg_view_time`,
  `live_24h_click_follow_num`,
  `live_24h_click_effective_shutdown_num`,
  `live_cmt_num`,
  `search_component_click_num`,
  `search_after_read_avg_num`,
  `search_after_read_num`,
  `new_i_people_num`,
  `new_ti_people_num`,
  `seller_view_pv`,
  `goods_view_pv`,
  `add_cart`,
  `purchase_order_num_1d`,
  `purchase_order_gmv_1d`,
  `deal_order_num_1d`,
  `deal_order_gmv_1d`,
  `presale_order_num_7d`,
  `presale_order_gmv_7d`,
  `total_order`,
  `purchase_order_gmv_7d`,
  `success_order`,
  `rgmv`,
  `purchase_order_num_30d`,
  `purchase_order_gmv_30d`,
  `live_24h_click_total_order_num`,
  `live_24h_click_rgmv`,
  `new_seller_goods_view_cnt`,
  `new_seller_purchase_order_num_7d`,
  `new_seller_purchase_order_gmv_7d`,
  `leads_success`,
  `landing_page_pv`,
  `landing_form_imp_num`,
  `valid_leads_num`,
  `phone_call_cnt`,
  `phone_call_succ_cnt`,
  `wechat_copy_cnt`,
  `wechat_copy_succ_cnt`,
  `identity_certi_cnt`,
  `commodity_buy_cnt`,
  `message_user_cnt`,
  `message_cnt`,
  `message_open_cnt`,
  `message_avg_time`,
  `msg_3min_reply_cnt`,
  `msg_1st_45s_reply_cnt`,
  `msg_1d_cnt`,
  `message_driving_open_cnt`,
  `msg_leads_num`,
  `ext_leads_succ_num`,
  `comment_component_click_num`,
  `poi_shop_page_pv`,
  `poi_navigation_click_num`,
  `event_app_open_cnt`,
  `event_app_enter_store_cnt`,
  `event_app_engagement_cnt`,
  `event_app_payment_cnt`,
  `friend_add_cnt`,
  `friend_add_success_cnt`,
  `chat_open_cnt`,
  `marketing_target`,
  `outside_seller_pv`,
  `jd_active_user_num`,
  `parent_creativity_id`,
  `parent_unit_id`,
  `parent_campaign_id`,
  `parent_advertiser_id`,
  `msg_1st_cnt`,
  `msg_1st_reply_time`,
  `live_24h_click_watch_distinct_eventid_num`,
  `all_24h_click_follow_num`,
  `live_24h_click_cmt_num`,
  `live_24h_click_watch_duration`,
  `field`,
  `tb_task_fee`,
  `tb_task_click_num`,
  `tb_note_trans_ratio`,
  `jd_task_fee`,
  `jd_task_click_num`,
  `jd_note_trans_ratio`,
  `v_seller_name`,
  a.`brand_id`,
  a.`brand_name`,
  `dtm`,
commercial_name,
  commercial_taxonomy_name1,
  commercial_code2,
  commercial_taxonomy_name2,
  commercial_code3,
  commercial_taxonomy_name3,
  commercial_code4,
  commercial_taxonomy_name4,
  commercial_level,
   company_code
  ,company_name
  ,track_group_name
  ,direct_sales_name
  , direct_sales_dept1_name
  ,direct_sales_dept2_name
  , direct_sales_dept3_name
  , direct_sales_dept4_name
  , direct_sales_dept5_name
  , direct_sales_dept6_name
  ,track_detail_name
  ,track_industry_name
  ,cpc_operator_name
  ,cpc_operator_dept2_name
  ,cpc_operator_dept3_name
  ,cpc_operator_dept4_name
  ,cpc_operator_dept6_name
  ,cpc_operator_dept5_name
  ,agent_user_id,
  agent_user_name
  ,channel_sales_name
  ,channel_operator_name
from
  `redapp`.`app_ads_aurora_creativity_user_group_metrics_1d_di` a
left join  
(select virtual_seller_id,
agent_user_id,
agent_user_name,
  brand_account_id
  ,brand_account_name as brand_user_name
  ,company_code
  ,company_name
  ,track_group_name
  ,cpc_direct_sales_name as direct_sales_name
  ,cpc_direct_sales_dept1_name as direct_sales_dept1_name
  ,coalesce(cpc_direct_sales_dept2_name, cpc_operator_dept2_name, if (company_name is null, '创作者商业化部', '未挂接') )  as direct_sales_dept2_name
  ,cpc_direct_sales_dept3_name as direct_sales_dept3_name
  ,cpc_direct_sales_dept4_name as direct_sales_dept4_name
  ,cpc_direct_sales_dept5_name as direct_sales_dept5_name
  ,cpc_direct_sales_dept6_name as direct_sales_dept6_name 
  ,track_detail_name
  ,track_industry_name
  ,cpc_operator_code
  ,cpc_operator_name
  ,cpc_operator_dept2_name
  ,cpc_operator_dept3_name
  ,cpc_operator_dept4_name
  ,cpc_operator_dept6_name
  ,cpc_operator_dept5_name
  ,channel_sales_name
  ,channel_operator_name
from 
redcdm.dim_ads_advertiser_df
where  dtm='{{ds_nodash}}'
) b 
on a.v_seller_id = b.virtual_seller_id

left join 
(select spu_id,
  spu_name,
  note_id,
  brand_id,
  brand_name,
  commercial_name,
  commercial_taxonomy_name1,
  commercial_code2,
  commercial_taxonomy_name2,
  commercial_code3,
  commercial_taxonomy_name3,
  commercial_code4,
  commercial_taxonomy_name4,
  commercial_level
from ads_databank.dim_spu_note_df
where  dtm='{{ds_nodash}}' and bind_type = 2

)d 
on d.note_id = a.note_id
where a.dtm>='20240101' and a.dtm<='{{ds_nodash}}'
)all
