
    -- ************************************************
    -- Author: chenmengdi
    -- CreateTime:2023-12-20T17:34:54+08:00
    -- Update: Task Update Description
    -- ************************************************
 ---241211加小红星小红盟
 drop table if exists temp.temp_dm_ads_pub_spu_product_cvr_cost_td_df_{{ds_nodash}}_online;

create table
  temp.temp_dm_ads_pub_spu_product_cvr_cost_td_df_{{ds_nodash}}_online as
 select date_key,
  report_brand_user_id as brand_account_id,
  case when module='' then '整体' else module end as module,
  case when product='' then '整体'  
  	when product='信息流' then '竞价-信息流' when product='搜索' then '竞价-搜索' 
    when product='视频内流' then '竞价-视频内流' else product end as product,
  if(sanfang.marketing_target=-999999,0 ,sanfang.marketing_target) as marketing_target ,
  case
   when marketing_target_type in ('闭环电商','非闭环电商','线索','种草') then concat(marketing_target_type,'广告')
   when sanfang.module in ('效果') then marketing_target_type
   when sanfang.module in ('品牌', '薯条', '品合','口碑通') then '整体'
   else null
  end as market_target,
  optimize_target,
  spu_id,

  sum(taobao_click_user_num) as taobao_click_user_num,
  sum(taobao_ads_cash_income_amt) as taobao_ads_cash_income_amt,
  sum(taobao_ads_income_amt) as  taobao_ads_income_amt,
  sum(taobao_third_active_user_num) as taobao_third_active_user_num,
  sum(taobao_ad_click_user_num) as taobao_ad_click_user_num,
  sum(taobao_ad_ads_cash_income_amt) as taobao_ad_ads_cash_income_amt,
  sum(taobao_ad_ads_income_amt) as  taobao_ad_ads_income_amt,
  sum(taobao_ad_third_active_user_num) as taobao_ad_third_active_user_num
 from  
	(select date_key,
	  report_brand_user_id,
	  module,
	  product,
	  marketing_target,
	  optimize_target,
		note_id,

		sum(case when third_platform='TAOBAO' AND attribution_type = '1' THEN click_num ELSE 0 END) as taobao_click_num,
		sum(case when third_platform='TAOBAO' AND attribution_type = '1' THEN click_user_num ELSE 0 END) as taobao_click_user_num,
		sum(case when third_platform='TAOBAO' AND attribution_type = '1' THEN engagement_num ELSE 0 END) as taobao_engagement_num,
		sum(case when third_platform='TAOBAO' AND attribution_type = '1' THEN engagement_user_num ELSE 0 END) as taobao_engagement_user_num,
		sum(case when third_platform='TAOBAO' AND attribution_type = '1' THEN imp_num ELSE 0 END) as taobao_imp_num,
		sum(case when third_platform='TAOBAO' AND attribution_type = '1' THEN imp_user_num ELSE 0 END) as taobao_imp_user_num,
		sum(case when third_platform='TAOBAO' AND attribution_type = '1' THEN ads_income_amt ELSE 0 END) as taobao_ads_income_amt,
		sum(case when third_platform='TAOBAO' AND attribution_type = '1' THEN ads_cash_income_amt ELSE 0 END) as taobao_ads_cash_income_amt,
		sum(case when third_platform='TAOBAO' AND attribution_type = '1' THEN third_active_user_num/ (cast(trans_ratio as int) / 100)  ELSE 0 END) as taobao_third_active_user_num,--站外行为

		sum(case when third_platform='JINGDONG' AND attribution_type = '1' THEN click_num ELSE 0 END) as jingdong_click_num,
		sum(case when third_platform='JINGDONG' AND attribution_type = '1' THEN click_user_num ELSE 0 END) as jingdong_click_user_num,
		sum(case when third_platform='JINGDONG' AND attribution_type = '1' THEN engagement_num ELSE 0 END) as jingdong_engagement_num,
		sum(case when third_platform='JINGDONG' AND attribution_type = '1' THEN engagement_user_num ELSE 0 END) as jingdong_engagement_user_num,
		sum(case when third_platform='JINGDONG' AND attribution_type = '1'  THEN imp_num ELSE 0 END) as jingdong_imp_num,
		sum(case when third_platform='JINGDONG' AND attribution_type = '1' THEN imp_user_num ELSE 0 END) as jingdong_imp_user_num,
		sum(case when third_platform='JINGDONG' AND attribution_type = '1' THEN ads_income_amt ELSE 0 END) as jingdong_ads_income_amt,
		sum(case when third_platform='JINGDONG' AND attribution_type = '1' THEN ads_cash_income_amt ELSE 0 END) as jingdong_ads_cash_income_amt,
		sum(case when third_platform='JINGDONG' AND attribution_type = '1' THEN third_active_user_num/ (cast(trans_ratio as int) / 100)  ELSE 0 END) as jingdong_third_active_user_num,--站外行为



		sum(case when third_platform='TAOBAO' AND attribution_type <> '1' THEN click_num ELSE 0 END) as taobao_ad_click_num,
		sum(case when third_platform='TAOBAO' AND attribution_type <> '1' THEN click_user_num ELSE 0 END) as taobao_ad_click_user_num,
		sum(case when third_platform='TAOBAO' AND attribution_type <> '1' THEN engagement_num ELSE 0 END) as taobao_ad_engagement_num,
		sum(case when third_platform='TAOBAO' AND attribution_type <> '1' THEN engagement_user_num ELSE 0 END) as taobao_ad_engagement_user_num,
		sum(case when third_platform='TAOBAO' AND attribution_type <> '1' THEN imp_num ELSE 0 END) as taobao_ad_imp_num,
		sum(case when third_platform='TAOBAO' AND attribution_type <> '1' THEN imp_user_num ELSE 0 END) as taobao_ad_imp_user_num,
		sum(case when third_platform='TAOBAO' AND attribution_type <> '1' THEN ads_income_amt ELSE 0 END) as taobao_ad_ads_income_amt,
		sum(case when third_platform='TAOBAO' AND attribution_type <> '1' THEN ads_cash_income_amt ELSE 0 END) as taobao_ad_ads_cash_income_amt,
		sum(case when third_platform='TAOBAO' AND attribution_type <> '1' THEN third_active_user_num/ (cast(trans_ratio as int) / 100)  ELSE 0 END) as taobao_ad_third_active_user_num,--站外行为

		sum(case when third_platform='JINGDONG' AND attribution_type <> '1' THEN click_num ELSE 0 END) as jingdong_ad_click_num,
		sum(case when third_platform='JINGDONG' AND attribution_type <> '1' THEN click_user_num ELSE 0 END) as jingdong_ad_click_user_num,
		sum(case when third_platform='JINGDONG' AND attribution_type <> '1' THEN engagement_num ELSE 0 END) as jingdong_ad_engagement_num,
		sum(case when third_platform='JINGDONG' AND attribution_type <> '1' THEN engagement_user_num ELSE 0 END) as jingdong_ad_engagement_user_num,
		sum(case when third_platform='JINGDONG' AND attribution_type <> '1'  THEN imp_num ELSE 0 END) as jingdong_ad_imp_num,
		sum(case when third_platform='JINGDONG' AND attribution_type <> '1' THEN imp_user_num ELSE 0 END) as jingdong_ad_imp_user_num,
		sum(case when third_platform='JINGDONG' AND attribution_type <> '1' THEN ads_income_amt ELSE 0 END) as jingdong_ad_ads_income_amt,
		sum(case when third_platform='JINGDONG' AND attribution_type <> '1' THEN ads_cash_income_amt ELSE 0 END) as jingdong_ad_ads_cash_income_amt,
		sum(case when third_platform='JINGDONG' AND attribution_type <> '1' THEN third_active_user_num/ (cast(trans_ratio as int) / 100)  ELSE 0 END) as jingdong_ad_third_active_user_num,--站外行为


		sum(case when third_platform='TAOBAO' and attribution_type='1' then read_num ELSE 0 END) as taobao_read_num,
		sum(case when third_platform='TAOBAO' and attribution_type='1' then read_user_num ELSE 0 END) as taobao_read_user_num,


		--京东
		sum(case when third_platform='JINGDONG' and attribution_type='1' then read_num ELSE 0 END) as jingdong_read_num,
		sum(case when third_platform='JINGDONG' and attribution_type='1' then read_user_num ELSE 0 END) as jingdong_read_user_num
	from -- bi_ads.app_ads_bcoo_third_brief_attribution_metrics_1d_df a
	redcdm.dws_ads_bcoo_third_note_attribution_metrics_1d_df a
	where 1=1
	and dtm =max_dtm('redcdm.dws_ads_bcoo_third_note_attribution_metrics_1d_df')--单分区内有全量数据
	and date_key>='2023-01-01' and 
	(--该部分不要轻易变动
	    (third_platform = 'TAOBAO' AND attribution_period = '15')  --淘宝15天归因
	    or (third_platform = 'JINGDONG'  AND attribution_period = '30'
	  --and attribution_scope = '1'
	  ) --京东 attribution_scope 1 正常归因 2 归因到类目(目前主要外透场景用) 备注：京东 30天归因，对内暂不使用归因到类目
	    or (third_platform not in ('TAOBAO','JINGDONG') AND attribution_period = '30')  --剩下的30天归因逻辑
	) --归因周期淘宝15天 其他30天
	--and attribution_type = 1 -- 1 1 2 广告流量
	--and module in ('效果') --效果 薯条 品牌 品合 口碑通
	and third_platform in ('TAOBAO','JINGDONG')  --TAOBAO 淘宝 JINGDONG 京东 WEIPINHUI 唯品会
	-- and date_key>='2024-08-01' and date_key<='2024-09-28' 
	group by  date_key,
	  report_brand_user_id,
	  module,
	  product,
	  marketing_target,
	  optimize_target,
	  note_id
	)sanfang 
	left join 
	(select
	  dim_value,
	  dim_value_name ,
	  value_type as marketing_target_type
	FROM
	  redcdm.dim_ads_industry_dimension_code_df
	WHERE
	  dtm = 'all' and dimension_code = 'marketing_target'
	)b --枚举值维表
	on sanfang.marketing_target = b.dim_value
	join 
	(select spu_id,note_id
	from ads_databank.dim_spu_note_df 
	where dtm=max_dtm('ads_databank.dim_spu_note_df' ) 
	and bind_type=2
	group by 1,2
	)t2 
    on sanfang.note_id = t2.note_id
group by 1,2,3,4,5,6,7,8
  ;
insert overwrite table redcdm.dm_ads_pub_spu_product_cvr_cost_td_df  partition( dtm = '{{ds_nodash}}')
select t.date_key,
  case when  product='口碑通' then '口碑通' else module end as module,
  product,
  marketing_target,
  t.spu_id,
  spu.brand_id as brand_id,
  spu.brand_name as brand_name,
  spu.spu_name as spu_name,
  spu.commercial_taxonomy_name1,
  spu.commercial_code2,
  spu.commercial_taxonomy_name2,
  spu.commercial_code3,
  spu.commercial_taxonomy_name3,
  spu.commercial_code4,
  spu.commercial_taxonomy_name4,
  
  coalesce(spu_account.brand_account_id,t.brand_account_id) as brand_account_id,
  account.brand_account_name,
  account.operator_code,
  account.operator_name,
  account.direct_sales_code,
  account.direct_sales_name,
  account.direct_sales_dept1_name,
  account.direct_sales_dept2_name,
  account.direct_sales_dept3_name,
  imp_num,
  click_num,
  like_num,
  fav_num,
  read_feed_num,
  share_num,
  query_num,
  note_screenshot_num,
  ti_user_num,
  ti.ti_level,
  imp_note_num,
  bind_note_num,
  softad_imp_note_num,
  pos_imp_note_num,
  new_note_num,
  cash_cost,
  bind_cash_cost,
  imp_emotional_note_num,
  brand_ti_user_num,
  account.first_industry_name,
  account.second_industry_name,
  cost_note_num,
  bind_cost_note_num,
  bind_splash_creativity_num,
  bind_splash_cash_cost,
  coalesce(spu_account.agent_user_id,t.agent_user_id) as agent_user_id,
  coalesce(spu_account.agent_user_name,t.agent_user_name) as agent_user_name,
  coalesce(spu_account.channel_sales_name,t.channel_sales_name) as channel_sales_name,
  spu.pic_url,
  rgmv,
  coalesce(spu_account.channel_operator_name,t.channel_operator_name) as channel_operator_name,
  i_user_num,
  a_user_num,
  s_user_num,
  planner_name,
  cost,
  bind_cost,
  is_cspu,
  campaign_cnt,
  note_cnt,
  income_campaign_cnt,
  income_note_cnt,
  ads_imp_cnt,
   ads_click_cnt,
   ads_income_amt,
  click_rgmv_7d,
  total_purchase_order_num,
  ecm_income_amt,
  cost_special_campaign,
  min_campaign_budget,
   pgc_new_nore_num,
  pgc_note_num_6m,
  marketing_target_id,
  p_user_num,
  optimize_target,
  deal_gmv,
  ytd_ads_cash_cost,  
  ads_cash_cost_365d,  
  ads_cash_cost_30d,
  first_cost_date,
  first_note_create_date,
  purchase_rgmv,
  purchase_order_num,
  enter_seller_cnt ,
  goods_view_cnt,
  add_cart_cnt,
  deal_order_num,
  deal_order_gmv,
    new_note_cnt,
  
  imp_note_cnt,
  query_cnt,
  soc_imp_num,
  ads_imp_num,
  soc_click_num,
  ads_click_num,
  origin_click_num,
  soc_read_feed_num,
  soc_engage_num,
  ads_engage_num,
  origin_engage_num,
  origin_imp_num,
  ads_like_num,
  ads_cmt_num,
  ads_share_num,
  ads_fav_num,
  ads_follow_num,
  soc_like_num,
  soc_cmt_num,
  soc_fav_num,
  soc_share_num,
  soc_follow_num,
  account.company_code,
  account.company_name,
  account.first_ad_industry_name,
  account.second_ad_industry_name,
  ads_engage_cnt,
  spu_account.first_group_name,
  spu_account.second_group_name,
  spu_account.third_group_name,
  spu_account.brand_channel_sales_name,
(
from 
from 
(select coalesce(a1.date_key, t1.date_key) as date_key,
      coalesce(a1.module, t1.module) as module,
      coalesce(a1.product, t1.product) as product,
      coalesce(a1.marketing_target, t1.marketing_target) as marketing_target,
      coalesce(a1.marketing_target_id, t1.marketing_target_id) as marketing_target_id,
      coalesce(a1.spu_id, t1.spu_id) as spu_id,
      coalesce(a1.optimize_target, t1.optimize_target) as optimize_target,
  -- brand_id,
  -- brand_name,
  -- spu_name,
  -- commercial_taxonomy_name1,
  -- commercial_code2,
  -- commercial_taxonomy_name2,
  -- commercial_code3,
  -- commercial_taxonomy_name3,
  -- commercial_code4,
  -- commercial_taxonomy_name4,
  coalesce(a1.brand_account_id,t1.brand_account_id) as brand_account_id,
  -- brand_account_name,
  -- operator_code,
  -- operator_name,
  -- direct_sales_code,
  -- direct_sales_name,
  -- direct_sales_dept1_name,
  -- direct_sales_dept2_name,
  -- direct_sales_dept3_name,
  imp_num,
  click_num,
  like_num,
  fav_num,
  read_feed_num,
  share_num,
  query_num,
  note_screenshot_num,
  ti_user_num,
  ti_level,
  imp_note_num,
  bind_note_num,
  softad_imp_note_num,
  pos_imp_note_num,
  a1.new_note_num,
  cash_cost,
  bind_cash_cost,
  imp_emotional_note_num,
  brand_ti_user_num,
  -- first_industry_name,
  -- second_industry_name,
  cost_note_num,
  bind_cost_note_num,
  bind_splash_creativity_num,
  bind_splash_cash_cost,
  coalesce(a1.agent_user_id,t1.agent_user_id) as agent_user_id,
  coalesce(a1.agent_user_name,t1.agent_user_name) as agent_user_name,
  coalesce(a1.channel_sales_name,t1.channel_sales_name) as channel_sales_name,
  -- pic_url,
  rgmv,
  coalesce(a1.channel_operator_name,t1.channel_operator_name) as channel_operator_name,
  i_user_num,
  a_user_num,
  s_user_num,
  -- planner_name,
  cost,
  bind_cost,
  coalesce(a1.is_cspu,t1.is_cspu) as is_cspu,
  campaign_cnt,
  note_cnt,
  income_campaign_cnt,
  income_note_cnt,
  imp_cnt as ads_imp_cnt,
  click_cnt as ads_click_cnt,
  income_amt as ads_income_amt,
  click_rgmv_7d,
  total_purchase_order_num,
  ecm_income_amt,
  cost_special_campaign,
  min_campaign_budget,
  t1.new_note_num as pgc_new_nore_num,
  p_user_num,
  deal_gmv,
  purchase_rgmv,
  purchase_order_num,
  enter_seller_cnt ,
  goods_view_cnt,
  add_cart_cnt,
  deal_order_num,
  deal_order_gmv,
    new_note_cnt,
  
  imp_note_cnt,
  query_cnt,
  soc_imp_num,
  ads_imp_num,
  soc_click_num,
  ads_click_num,
  origin_click_num,
  soc_read_feed_num,
  soc_engage_num,
  ads_engage_num,
  origin_engage_num,
  origin_imp_num,
  ads_like_num,
  ads_cmt_num,
  ads_share_num,
  ads_fav_num,
  ads_follow_num,
  soc_like_num,
  soc_cmt_num,
  soc_fav_num,
  soc_share_num,
  soc_follow_num,
  engage_cnt as ads_engage_cnt
  --t1.note_num_6m as pgc_note_num_6m
from 
(--人群种草
select coalesce(t1.date_key,t2.date_key) as date_key,
  coalesce(t1.module,'整体') as module,
  coalesce(t1.product,'整体') as product,
  coalesce(t1.marketing_target,'整体') as marketing_target,
  coalesce(t1.spu_id,t2.spu_id) as spu_id,
  t1.brand_account_id,

  imp_num,
  click_num,
  like_num,
  fav_num,
  read_feed_num,
  share_num,
  query_num,
  note_screenshot_num,
  ti_user_num,
  ti_level,
  imp_note_num,
  bind_note_num,
  softad_imp_note_num,
  pos_imp_note_num,
  new_note_num,
  cash_cost,
  bind_cash_cost,
  imp_emotional_note_num,
  brand_ti_user_num,
  -- first_industry_name,
  -- second_industry_name,
  cost_note_num,
  bind_cost_note_num,
  bind_splash_creativity_num,
  bind_splash_cash_cost,
  agent_user_id,
  agent_user_name,
  channel_sales_name,
  -- pic_url,
  rgmv,
  channel_operator_name,
  i_user_num,
  a_user_num,
  s_user_num,
  -- planner_name,
  cost,
  bind_cost,
  is_cspu,
  -911 as marketing_target_id,
  -911 as optimize_target,
  p_user_num,
  deal_gmv,
  new_note_cnt,
  
  imp_note_cnt,
  query_cnt,
  soc_imp_num,
  ads_imp_num,
  soc_click_num,
  ads_click_num,
  origin_click_num,
  soc_read_feed_num,
  soc_engage_num,
  ads_engage_num,
  origin_engage_num,
  origin_imp_num,
  ads_like_num,
  ads_cmt_num,
  ads_share_num,
  ads_fav_num,
  ads_follow_num,
  soc_like_num,
  soc_cmt_num,
  soc_fav_num,
  soc_share_num,
  soc_follow_num
from redcdm.dm_ads_pub_spu_cvr_cost_1d_di t1 
full outer join 
(select   date_key,
	spu_id,
	new_note_cnt AS new_note_cnt,
  
  imp_note_cnt   AS imp_note_cnt,
  query_cnt   AS query_cnt,
  imp_num   AS soc_imp_num,
  ads_imp_num   AS ads_imp_num,
  click_num   AS soc_click_num,
  ads_click_num   AS ads_click_num,
  origin_click_num   AS origin_click_num,
  read_feed_num   AS soc_read_feed_num,
  engage_num   AS soc_engage_num,
  ads_engage_num   AS ads_engage_num,
  origin_engage_num   AS origin_engage_num,
  origin_imp_num   AS origin_imp_num,
  ads_like_num   AS ads_like_num,
  ads_cmt_num   AS ads_cmt_num,
  ads_share_num   AS ads_share_num,
  ads_fav_num   AS ads_fav_num,
  ads_follow_num   AS ads_follow_num,
  like_num   AS soc_like_num,
  cmt_num   AS soc_cmt_num,
  fav_num   AS soc_fav_num,
  share_num   AS soc_share_num,
  follow_num   AS soc_follow_num
from  redcdm.dws_ad_spu_soc_engagement_df
where dtm='{{ds_nodash}}'
)t2 on t1.date_key = t2.date_key and t1.spu_id = t2.spu_id
where t1.dtm>='20220901' and t1.dtm<='{{ds_nodash}}'
and t1.ti_level<>'其他'
union all 
--收入
select date_key,
  module,
  product,
  marketing_target,
  spu_id,

  brand_account_id,

  0 as imp_num,
  0 as click_num,
  0 as like_num,
  0 as fav_num,
  0 as read_feed_num,
  0 as share_num,
  0 as query_num,
  0 as note_screenshot_num,
  0 as ti_user_num,
  '其他' as ti_level,
  0 as imp_note_num,
  0 as bind_note_num,
  0 as softad_imp_note_num,
  0 as pos_imp_note_num,
  0 as new_note_num,
  avg_cash_income_amt as cash_cost,
  avg_bind_cash_income_amt as bind_cash_cost,
  0 as imp_emotional_note_num,
  0 as brand_ti_user_num,
  -- 0 as first_industry_name,
  -- second_industry_name,
  0 as cost_note_num,
  0 as bind_cost_note_num,
  0 as bind_splash_creativity_num,
  avg_bind_splash_cash_income_amt as bind_splash_cash_cost,
  agent_user_id,
  agent_user_name,
  channel_sales_name,
  --pic_url,
  0 as rgmv,
  channel_operator_name,
  0 as i_user_num,
  0 as a_user_num,
  0 as s_user_num,
  -- planner_name,
  avg_income_amt as cost,
  avg_bind_income_amt as bind_cost,
  is_cspu,
  marketing_target_id,
  optimize_target,
  0 as  p_user_num,
  0 as deal_gmv,

  ---------新增
  0 AS new_note_cnt,
  
  0 AS imp_note_cnt,
  0 AS query_cnt,
  0 AS soc_imp_num,
  0 AS ads_imp_num,
  0 AS soc_click_num,
  0 AS ads_click_num,
  0 AS origin_click_num,
  0 AS soc_read_feed_num,
  0 AS soc_engage_num,
  0 AS ads_engage_num,
  0 AS origin_engage_num,
  0 AS origin_imp_num,
  0 AS ads_like_num,
  0 AS ads_cmt_num,
  0 AS ads_share_num,
  0 AS ads_fav_num,
  0 AS ads_follow_num,
  0 AS soc_like_num,
  0 AS soc_cmt_num,
  0 AS soc_fav_num,
  0 AS soc_share_num,
  0 AS soc_follow_num
from redcdm.dm_ads_pub_spu_product_td_df
where dtm='{{ds_nodash}}'
and data_type = 2
)a1 
full outer join 
(select date_key,
  module,
  product,
  marketing_target,
  spu_id,
  brand_account_id,
  agent_user_id,
  agent_user_name,
  channel_sales_name,
  is_cspu,
  campaign_cnt,
  note_cnt,
  income_campaign_cnt,
  income_note_cnt,
  imp_cnt,
  click_cnt,
  income_amt,
  click_rgmv_7d,
  total_purchase_order_num,
  ecm_income_amt,
  cost_special_campaign,
  min_campaign_budget,
  new_note_num,
  channel_operator_name,
  marketing_target_id,
  optimize_target,
    purchase_rgmv,
  purchase_order_num,
 enter_seller_cnt ,
 goods_view_cnt,
add_cart_cnt,
deal_order_num,
 deal_order_gmv,
 engage_cnt
from
redcdm.dm_ads_pub_spu_product_td_df
where dtm='{{ds_nodash}}'
and data_type = 1
  )t1 
on a1.date_key = t1.date_key
      and a1.spu_id = t1.spu_id
      and a1.module = t1.module
      and a1.product = t1.product
      and a1.marketing_target = t1.marketing_target
      and a1.marketing_target_id = t1.marketing_target_id
      and a1.optimize_target=t1.optimize_target
)info
---------------20241211加小红星----------------------------------------
full outer join 
temp.temp_dm_ads_pub_spu_product_cvr_cost_td_df_{{ds_nodash}}_online spu 
on spu.date_key = info.date_key
  and spu.spu_id = info.spu_id
  and spu.module = info.module
  and a1.product = info.product
  and spu.market_target = info.marketing_target
  and spu.marketing_target = info.marketing_target_id
  and spu.optimize_target=info.optimize_target
  and spu.brand_account_id=info.brand_account_id
）t


left join 
--spu和账号mapping 
(SELECT
  date_key,
  spu_id,
  brand_account_id,
  channel_sales_name,
  channel_operator_name,
  agent_user_id,
  agent_user_name,
  first_cost_date,
  first_note_create_date,
  first_group_name,
  second_group_name,
  third_group_name,
  brand_channel_sales_name
FROM
  redapp.app_ads_insight_spu_account_mapping_df
WHERE
  dtm = '{{ds_nodash}}'
)spu_account 
on t.date_key =spu_account.date_key and t.spu_id =spu_account.spu_id
      left join 
      (select brand_account_id,
      direct_sales_dept1_name,
      direct_sales_dept2_name,
      direct_sales_dept3_name,
      direct_sales_dept4_name,
      direct_sales_dept5_name,
      direct_sales_dept6_name,
      direct_sales_name,
      operator_name,
      operator_code ,
      direct_sales_code,
      brand_user_name as brand_account_name,
      first_industry_name,
      second_industry_name,
      brand_group_tag_name,
      operator_name as cpc_operator_name,
      planner_name,
      first_ad_industry_name,
      second_ad_industry_name,
  company_code,
  company_name
from redapp.app_ads_insight_industry_account_df
where dtm= '{{ds_nodash}}'
)account on account.brand_account_id=coalesce(spu_account.brand_account_id,t.brand_account_id)
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
  )spu on spu.spu_id = t.spu_id
  left join 
  (select spu_id,
    date_key,
    ti_level
  from redcdm.dm_ads_pub_spu_cvr_cost_1d_di 
  where dtm>='20220901' and dtm<='{{ds_nodash}}'
  and module = '整体'
  group by spu_id,
    date_key,
    ti_level
  )ti 
  on ti.date_key=t.date_key and ti.spu_id = t.spu_id
  left join 
  (select spu_id,count(1) as pgc_note_num_6m
  from 
  (select discovery_id as note_id,substring(publish_time,1,10) as dt
  from reddw.dw_soc_discovery_delta_7_day
  where dtm='{{ds_nodash}}' 
  and (is_brand = 1 or is_bind = 1 or is_cps_note = 1)
  and substring(publish_time,1,10)>=add_months('{{ds}}',-6)
  and substring(publish_time,1,10)<='{{ds}}'
  )t1 
  join 
  (select spu_id,note_id
  from ads_databank.dim_spu_note_df 
  where dtm=max_dtm('ads_databank.dim_spu_note_df' ) 
  group by 1,2
  )t2 
  on t1.note_id = t2.note_id
  group by spu_id
  )pgc 
  on pgc.spu_id = t.spu_id
 --20240625加spu的365和ytd广告流水
 left join 
 (select cost1.spu_id,
    cost1.date_key,
    sum(case when substring(cost1.date_key,1,4)=substring(cost2.date_key,1,4) then cost2.cash_income_amt else 0 end) as ytd_ads_cash_cost,  
    sum(case when cost2.date_key>=f_getdate(cost1.date_key,-364) then cost2.cash_income_amt else 0 end) as ads_cash_cost_365d,  
    sum(case when cost2.date_key>=f_getdate(cost1.date_key,-29) then cost2.cash_income_amt else 0 end) as ads_cash_cost_30d
 from
  (select spu_id,
    date_key,
    sum(cash_income_amt) as cash_income_amt
  from  redcdm.dws_ads_note_spu_product_income_detail_td_df a 
  where dtm='{{ds_nodash}}'  and module in ('品牌','薯条','效果','口碑通') 
  and date_key>='2024-01-01' and date_key<='{{ds}}'
  group by spu_id,
    date_key
  )cost1 
  left join 
  (select spu_id,
    date_key,
    sum(cash_income_amt) as cash_income_amt
  from  redcdm.dws_ads_note_spu_product_income_detail_td_df a 
  where dtm='{{ds_nodash}}'  and module in ('品牌','薯条','效果','口碑通')
   and date_key>='2023-01-01' and date_key<='{{ds}}'
  group by spu_id,
    date_key
  )cost2
  on cost1.spu_id=cost2.spu_id and cost1.date_key>=cost2.date_key
  group by 1,2
  )cost_type
  on cost_type.spu_id = t.spu_id and cost_type.date_key= t.date_key
