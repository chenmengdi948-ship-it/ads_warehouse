--消耗数据
--品牌渠道销售用订单挂接所以依赖crm收入宽表
--效果销售对于渠道业务部使用子账号挂机
select *
from ads_data_crm.dim_ads_crm_virtual_seller_id_info_df t1 
left join ads_data_crm.dim_ads_crm_user_department_info_df rtb on  rtb.dtm = '{{ds_nodash}}' and t1.rtb_seller_code = rtb.seller_code
where t1.dtm='{{ds_nodash}}' and t1.brand_user_id='631f1039000000002302552f'

--消耗数据,效果销售对于渠道业务部使用子账号挂机
drop table if exists temp.temp_dm_ads_industry_account_agent_detail_cost_{{ds_nodash}};
create table temp.temp_dm_ads_industry_account_agent_detail_cost_{{ds_nodash}}
--非效果收入
SELECT
  date_key,
  case when module in ('品合','内容加热') then '品合' when product in ('口碑通','口碑通v2.0') then '口碑通' else module end as module,
  product,
  is_marketing_product,
  '整体' as market_target,
  virtual_seller_id,
  brand_user_id,
  agent_user_id,
  -911 as optimize_target,
  -911 as marketing_target,
  '非竞价' as ads_note_type,
  -911 as promotion_target,
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
  agent_user_id
union all
            --效果收入区分营销目标
            select
              date_key,
  			  '效果' as module,
			  t1.product,
			  coalesce(is_marketing_product, '0') as is_marketing_product,
			  dim.market_target,
			  b.virtual_seller_id,
			  t1.brand_user_id,
			  b.agent_user_id,
			  t1.optimize_target,
			  t1.marketing_target,
			  note_ads_type as ads_note_type,
			  promotion_target,
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
		  	  promotion_target;

SET "kyuubi.spark.option.--conf spark.sql.crossJoin.enabled=true";
SET "kyuubi.spark.option.--conf spark.redExtension.allowBroadcastNestedLoopJoin=true"; 
drop table if exists temp.temp_dm_ads_industry_account_agent_detail_cost_type_{{ds_nodash}};
create table temp.temp_dm_ads_industry_account_agent_detail_cost_type_{{ds_nodash}}
select base.brand_account_id,base.date_key,
sum(cash_cost)over(partition by year,qtd order by base.date_key asc rows between unbounded  PRECEDING and current row
) as cash_cost_qtd,
sum(1)over(partition by year,qtd order by base.date_key asc rows between unbounded  PRECEDING and current row
) as days_qtd,
sum(cash_cost)over(partition by year order by base.date_key asc rows between unbounded  PRECEDING and current row
) as cash_cost_ytd,
) as days_ytd
from 
(select brand_account_id,dt as date_key ,year,qtd
from 
(select brand_user_id as brand_account_id 
from temp.temp_dm_ads_industry_account_agent_detail_cost_{{ds_nodash}} 
group by 1 
)t1
left join 
(select dt,ceil(month/3) as qtd,year
from redcdm.dim_ads_date_df
where dtm='all' and dt>='2024-01-01' and dt<='{{ds}}')dt 
on 1=1
)base 
left join 
(select date_key,brand_user_id as brand_account_id,sum(cash_cost) as cash_cost
from temp.temp_dm_ads_industry_account_agent_detail_cost_{{ds_nodash}}
where module<>'品合'
group by 1,2
 ) t2 
on base.date_key=t2.date_key and base.brand_account_id=t2.brand_account_id





--各种广告类型展点转化指标
drop table if exists temp.temp_dm_ads_industry_account_agent_detail_{{ds_nodash}};
create table temp.temp_dm_ads_industry_account_agent_detail_{{ds_nodash}}
SELECT
  f_getdate(a.dtm) as date_key,
  module,
  product,
  a.brand_account_id,
  v_seller_id,
  b.market_target,
  optimize_target,
  marketing_target,
  note_ads_type as ads_note_type,
  promotion_target,
  '0' as is_marketing_product,
  '-911' as agent_user_id,
  sum(unique_imp_cnt) as imp_cnt,
  sum(unique_click_cnt) as click_cnt,
  sum(leads_cnt) as leads_cnt,
  sum(msg_num) as msg_num,
  sum(msg_user_num) as msg_user_num,
  sum(msg_open_num) as msg_open_num,
  sum(like_cnt) as like_cnt,
  sum(fav_cnt) as fav_cnt,
  sum(cmt_cnt) as cmt_cnt,
  sum(share_cnt) as share_cnt,
  sum(follow_cnt) as follow_cnt,
  sum(screenshot_cnt) as screenshot_cnt,
  sum(image_save_cnt) as image_save_cnt,
  sum(live_rgmv) as live_rgmv,
  sum(live_dgmv) as live_dgmv,
  sum(conversion_cnt) as conversion_cnt,
  -- sum(cash_income_amt) as cash_income_amt,
  -- sum(income_amt) as income_amt,
  sum(coalesce(purchase_rgmv,0)+coalesce(mini_purchase_rgmv,0)) as click_rgmv_7d,
  sum(coalesce(purchase_order_num,0)+coalesce(mini_purchase_order_num,0)) as purchase_order_num,
  sum(click_purchase_order_pv_7d) as click_purchase_order_pv_7d,
  sum(live_order_num) as live_order_num,
  sum(ecm_unclosed_purchase_rgmv) as ecm_unclosed_purchase_rgmv,
  sum(leads_submit_cnt) as leads_submit_cnt,
  sum(msg_driving_open_num) as msg_driving_open_num,
  sum(case when optimize_target = 50 then conversion_cnt else 0 end) as msg_leads_num,
  sum(coalesce(deal_order_num,0)+coalesce(mini_deal_order_num,0)) as deal_order_num,
  sum(coalesce(rgmv,0)+coalesce(mini_rgmv,0)) as deal_rgmv_7d
FROM
  redcdm.dm_ads_rtb_creativity_1d_di a 
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
  a.dtm >= '20240701' and a.dtm<='{{ds_nodash}}' and (is_effective=1 or total_amount>0)
group by f_getdate(a.dtm),
  module,
  product,
  a.brand_account_id,
  v_seller_id,
  b.market_target,
  optimize_target,
  marketing_target,
  promotion_target,
  note_ads_type 
union all 
SELECT
  date_key,
  module,
  product,
  brand_account_id,
  v_seller_id,
  market_target,
  -911 as optimize_target,
  -911 as marketing_target,
  '非竞价' as ads_note_type,
  -911 as promotion_target,
  '0' as is_marketing_product,
  '-911' as agent_user_id,
  sum(imp_cnt) as imp_cnt,
  sum(click_cnt) as click_cnt,
  sum(leads_cnt) as leads_cnt,
  sum(msg_num) as msg_num,
  sum(msg_user_num) as msg_user_num,
  sum(msg_open_num) as msg_open_num,
  sum(like_cnt) as like_cnt,
  sum(fav_cnt) as fav_cnt,
  sum(cmt_cnt) as cmt_cnt,
  sum(share_cnt) as share_cnt,
  sum(follow_cnt) as follow_cnt,
  sum(screenshot_cnt) as screenshot_cnt,
  sum(image_save_cnt) as image_save_cnt,
  sum(live_rgmv) as live_rgmv,
  sum(live_dgmv) as live_dgmv,
  sum(conversion_cnt) as conversion_cnt,
  -- sum(cash_income_amt) as cash_income_amt,
  -- sum(income_amt) as income_amt,
  sum(click_rgmv_7d) as click_rgmv_7d,
  sum(coalesce(purchase_order_num,0)+coalesce(mini_purchase_order_num,0)) as purchase_order_num,
  sum(click_purchase_order_pv_7d) as click_purchase_order_pv_7d,
  sum(live_order_num) as live_order_num,
  sum(ecm_unclosed_purchase_rgmv) as ecm_unclosed_purchase_rgmv,
  sum(leads_submit_cnt) as leads_submit_cnt,
  sum(msg_driving_open_num) as msg_driving_open_num,
  sum(msg_leads_num) as msg_leads_num,
  sum(deal_order_num) as deal_order_num,
  sum(deal_order_rgmv) as deal_order_rgmv
FROM
  redcdm.dm_ads_industry_product_advertiser_td_df
WHERE
  dtm = '{{ds_nodash}}' and module<>'效果'
group by date_key,
  module,
  product,
  brand_account_id,
  v_seller_id,
  market_target,
  optimize_target,
  marketing_target
;
  
insert overwrite table redapp.app_ads_industry_product_account_agent_detail_td_df  partition(dtm = '{{ ds_nodash }}') 
--各种广告类型展点转化指标
select date_key,
  t1.brand_account_id,
  module,
  product,
  marketing_target,
  optimize_target,
  market_target,
  is_marketing_product,
  t1.virtual_seller_id,
  t1.agent_user_id,
  ads_note_type,
  promotion_target,
  first_ads_cost_date,
	company_first_ads_cost_date,
	first_recharge_date,
	account.brand_user_name as brand_account_name,
  account.company_code,
  account.company_name,
  track_group_name,
  cpc_direct_sales_code,
  cpc_direct_sales_name,
  cpc_direct_sales_dept1_name,
  cpc_direct_sales_dept2_name,
  cpc_direct_sales_dept3_name,
  cpc_direct_sales_dept4_name,
  cpc_direct_sales_dept5_name,
  cpc_direct_sales_dept6_name,
  brand_tag_name,
  cpc_operator_name,
  cpc_operator_dept1_name,
  cpc_operator_dept2_name,
  cpc_operator_dept3_name,
  cpc_operator_dept4_name,
  cpc_operator_dept5_name,
  cpc_operator_dept6_name,
  track_industry_name,
  track_detail_name,
  adv.channel_sales_code,
  adv.channel_sales_name,
  adv.channel_operator_code,
  adv.channel_operator_name,
  adv.virtual_seller_name,
  rtb_seller_code,
  rtb_seller_name,
  v_seller_dept1_name,
  v_seller_dept2_name,
  v_seller_dept3_name,
  v_seller_dept4_name,
  v_seller_dept5_name,
  v_seller_dept6_name,
  rtb_advertiser_id,
  avg_qtd_cost as dim_avg_qtd_cost,
	avg_ytd_cost as dim_avg_ytd_cost,
	first_pass_time,
	agent_user_name,
	agent_type,
  imp_cnt,
  click_cnt,
  leads_cnt,
  msg_num,
  msg_user_num,
  msg_open_num,
  like_cnt,
  fav_cnt,
  cmt_cnt,
  share_cnt,
  follow_cnt,
  screenshot_cnt,
  image_save_cnt,
  live_rgmv,
  live_dgmv,
  conversion_cnt,
  click_rgmv_7d,
  purchase_order_num,
  click_purchase_order_pv_7d,
  live_order_num,
  ecm_unclosed_purchase_rgmv,
  leads_submit_cnt,
  msg_driving_open_num,
  msg_leads_num,
  deal_order_num,
  deal_rgmv_7d,
  cash_income_amt,
  income_amt
from 
(SELECT
  date_key,
  brand_account_id,
  module,
  product,
  marketing_target,
  optimize_target,
  market_target,
  is_marketing_product,
  v_seller_id as virtual_seller_id,
  agent_user_id,
  ads_note_type,
  promotion_target,
  imp_cnt,
  click_cnt,
  leads_cnt,
  msg_num,
  msg_user_num,
  msg_open_num,
  like_cnt,
  fav_cnt,
  cmt_cnt,
  share_cnt,
  follow_cnt,
  screenshot_cnt,
  image_save_cnt,
  live_rgmv,
  live_dgmv,
  conversion_cnt,
  click_rgmv_7d,
  purchase_order_num,
  click_purchase_order_pv_7d,
  live_order_num,
  ecm_unclosed_purchase_rgmv,
  leads_submit_cnt,
  msg_driving_open_num,
  msg_leads_num,
  deal_order_num,
  deal_rgmv_7d,
  0 as cash_income_amt,
  0 as income_amt
from  temp.temp_dm_ads_industry_account_agent_detail_{{ds_nodash}}
union all 
SELECT
  date_key,
  brand_account_id,
  module,
  product,
  marketing_target,
  optimize_target,
  market_target,
  is_marketing_product,
  virtual_seller_id,
  agent_user_id,
  ads_note_type,
  promotion_target,
	0 as imp_cnt,
	0 as click_cnt,
	0 as leads_cnt,
	0 as msg_num,
	0 as msg_user_num,
	0 as msg_open_num,
	0 as like_cnt,
	0 as fav_cnt,
	0 as cmt_cnt,
	0 as share_cnt,
	0 as follow_cnt,
	0 as screenshot_cnt,
	0 as image_save_cnt,
	0 as live_rgmv,
	0 as live_dgmv,
	0 as conversion_cnt,
	0 as click_rgmv_7d,
	0 as purchase_order_num,
	0 as click_purchase_order_pv_7d,
	0 as live_order_num,
	0 as ecm_unclosed_purchase_rgmv,
	0 as leads_submit_cnt,
	0 as msg_driving_open_num,
	0 as msg_leads_num,
	0 as deal_order_num,
	0 as deal_rgmv_7d,
  cash_income_amt,
  income_amt
FROM
  redapp.app_ads_industry_product_account_agent_cost_detail_td_df
WHERE
  dtm = '{{ds_nodash}}'
)t1 
left join 
--首次广告投放时间
(select brand_account_id,
	min(first_ads_cost_date) as first_ads_cost_date,
	min(company_first_ads_cost_date) as company_first_ads_cost_date
from redapp.app_ads_industry_product_account_agent_cost_detail_td_df
where  dtm = '{{ds_nodash}}'
group by brand_account_id
)cost_type 
on cost_type.brand_account_id=t1.brand_account_id 
-- left join 
-- --coalesce(company_code,brand_account_id)首次投放时间
-- (select company_id,
-- 	min(company_first_ads_cost_date) as company_first_ads_cost_date
-- from redapp.app_ads_industry_product_account_agent_cost_detail_td_df
-- where  dtm = '{{ds_nodash}}'
-- group by company_id
-- )cost_type 
-- on cost_type.company_id=t1.company_id
left join 
--首次充值时间 
(SELECT
  user_id,
  min(first_recharge_date) as first_recharge_date
FROM
  redapp.app_ads_industry_account_apply_recharge_df
WHERE
  dtm = '{{ds_nodash}}'
group by 1
)recharge 
on recharge.brand_account_id=t1.brand_account_id
left join 
--企业号维度
(SELECT
  brand_account_id,
  brand_user_name,
  company_code,
  company_name,
  track_group_name,
  cpc_direct_sales_code,
  cpc_direct_sales_name,
  cpc_direct_sales_dept1_name,
  cpc_direct_sales_dept2_name,
  cpc_direct_sales_dept3_name,
  cpc_direct_sales_dept4_name,
  cpc_direct_sales_dept5_name,
  cpc_direct_sales_dept6_name,
  brand_tag_name,
  cpc_operator_name,
  cpc_operator_dept1_name,
  cpc_operator_dept2_name,
  cpc_operator_dept3_name,
  cpc_operator_dept4_name,
  cpc_operator_dept5_name,
  cpc_operator_dept6_name,
  track_industry_name,
  track_detail_name
from redcdm.dim_ads_industry_account_df
WHERE
  dtm = '{{ds_nodash}}'
)account 
on account.brand_account_id = t1.brand_account_id
left join 
--子账户维度
(SELECT
  virtual_seller_id,
  virtual_seller_name,
  rtb_advertiser_id,
  
  channel_sales_code,
  channel_sales_name,
  channel_operator_code,
  channel_operator_name,
  rtb_seller_code,
  rtb_seller_name,
  v_seller_dept1_name,
  v_seller_dept2_name,
  v_seller_dept3_name,
  v_seller_dept4_name,
  v_seller_dept5_name,
  v_seller_dept6_name
FROM
  redcdm.dim_ads_advertiser_df
WHERE
  dtm = '{{ds_nodash}}'
  )adv
on adv.virtual_seller_id=t1.virtual_seller_id
left join 
--分层
(select date_key,brand_account_id,
	max(dim_ads_cash_cost_qtd)/max(dim_ads_cash_cost_qtd) as avg_qtd_cost,
	max(dim_ads_cash_cost_ytd)/max(dim_ads_cash_cost_ytd) as avg_ytd_cost
from redapp.app_ads_industry_product_account_agent_cost_detail_td_df
where  dtm = '{{ds_nodash}}'
group by date_key,brand_account_id
)cost_type 
on cost_type.brand_account_id=t1.brand_account_id and cost_type.date_key=t1.date_key
left join 
--开户日期
(
select 
	user_id,
	max(audit_time) as first_pass_time
from redods.ods_uranus_ba_ads_industry_qualification_apply_df
where dtm ='{{ds_nodash}}'
and apply_status = 'auditPass'
group by 1
)t3 
on t3.user_id = t1.user_id  
left join 
--代理商
(select agent_user_id, agent_user_name
from reddim.dim_ads_crm_agent_day
where dtm='{{ds_nodash}}' 

)ag 
on ag.agent_user_id = t1.agent_user_id
left join 
--代理商
(select agent_user_id, max(agent_type) as agent_type --防止主键重复
from reddm.dm_ads_crm_advertiser_income_wide_day 
where dtm='{{ds_nodash}}' and agent_user_id is not null 
group by 1
)agent 
on agent.agent_user_id=t1.agent_user_id