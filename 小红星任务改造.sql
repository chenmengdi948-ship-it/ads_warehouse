select  1 as tag,
  f_getdate(coalesce(t1.settle_date,t2.dtm)) as date_key,
  coalesce(t1.note_id,t2.note_id) as note_id,
  coalesce(t1.brand_account_user_id,t2.brand_account_user_id) as brand_account_id,
  task_period_ads_cost as ads_cost, --广告运营消耗
  task_period_ads_cash_cost as ads_cash_cost,--广告现金消耗
  task_period_ads_chips_cost as chips_cost,
  bcoo_create_cost,
  bcoo_cost,
  task_period_brand_cost as brand_cost,
  rtb_cost,
  read_num,
  taobao_enter_num,
  proportion_enter_num
  -- case when bcoo_create_cost>0 then 1 else 0 end as is_bcoo,
  -- case when task_period_brand_cost>0 then 1 else 0 end as is_brand,
  -- case when  task_period_ads_chips_cost>0 then 1 else 0 end as is_chips,
  
  -- case when  rtb_cost>0 then 1 else 0 end as is_rtb
from 
(select
  settle_date,
  note_id,
  brand_account_user_id,
  sum(task_period_ads_cost) as task_period_ads_cost, --广告运营消耗
  sum(task_period_ads_cash_cost) as task_period_ads_cash_cost,--广告现金消耗
  sum(task_period_ads_chips_cost) as task_period_ads_chips_cost,
  sum(coalesce(bcoo_create_content_price,0)+coalesce(bcoo_create_service_fee,0)) as bcoo_create_cost,
  sum(coalesce(bcoo_content_price,0) + coalesce(bcoo_service_fee,0)) as bcoo_cost,
  sum(coalesce(task_period_ads_open_cost,0)+coalesce(task_period_ads_search_third_cost,0)+coalesce(task_period_ads_feed_gd_cost,0))as task_period_brand_cost,
  sum(coalesce(task_period_ads_feed_cpc_cost,0)+coalesce(task_period_ads_search_cpc_cost,0)+coalesce(task_period_ads_internal_flow_cost,0)) as rtb_cost
from redapp.app_ads_taolian_brief_note_cost_df --t+2更新
where dtm='{{ds_nodash}}'  and settle_date>='20230101' and settle_date<='{{ds_nodash}}' 
group by 1,2,3
)t1 
full outer join 
(select dtm,
note_id,
brand_account_user_id,
sum(read_num) as read_num,
sum(ads_third_active_user_num_15d) as taobao_enter_num,
sum(case when trans_ratio=50 then ads_third_active_user_num_15d*2 else ads_third_active_user_num_15d end) as proportion_enter_num
--sum(ads_total_cost) as ads_total_cost
from redapp.app_ads_taolian_note_metrics_di 
where dtm>='20230101' 
and dtm<='{{ds_nodash}}' 
group by 1,2,3
)t2 
on t1.settle_date = t2.dtm and t1.note_id = t2.note_id  and t1.brand_account_user_id=t2.brand_account_user_id 


select date_key,
note_id,
report_brand_user_id,
sum(case when module in ('效果','品牌','薯条','口碑通') then ads_income_amt else 0 end) as task_period_ads_cost, --广告运营消耗
sum(case when module in ('效果','品牌','薯条','口碑通') then ads_cash_income_amt else 0 end) as task_period_ads_cash_cost,--广告现金消耗
  sum(case when module in ('薯条') then ads_cash_income_amt else 0 end) as task_period_ads_chips_cost,
  sum(coalesce(bcoo_create_content_price,0)+coalesce(bcoo_create_service_fee,0)) as bcoo_create_cost,
  sum(coalesce(bcoo_content_price,0) + coalesce(bcoo_service_fee,0)) as bcoo_cost,


  sum(case when module in ('品牌') then ads_income_amt else)as task_period_brand_cost,
  sum(case when module in ('效果') then ads_cash_income_amt else) as rtb_cost
sum(case when attribution_type = 1 then read_num else 0 end) as read_num,
sum(case when attribution_type = 1 then third_active_user_num_15d else 0 end) as taobao_enter_num,
sum(case when trans_ratio=50 and attribution_type = 1 then third_active_user_num_15d*2 else third_active_user_num_15d end) as proportion_enter_num
from redapp.app_ads_bcoo_third_note_attribution_metrics_1d_df
where dtm='{{ds_nodash}}' and third_platform='TAOBAO' 

group by 1,2,3
