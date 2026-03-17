drop table
  if exists temp.temp_app_ads_insight_account_diagnosis_info_df_{{ds_nodash}}_001;

create table
  temp.temp_app_ads_insight_account_diagnosis_info_df_{{ds_nodash}}_001 as
SELECT
  date_key,
  substring(date_key, 1, 7) as stat_month,
  if(
    month(date_key) & 1 = 1,
    trunc(date_key, 'MM'),
    trunc(add_months(date_key, -1), 'MM')
  ) as stat_bimonthly_month,
  add_months(date_key, 12) as next_date_key,
  if(
    month(add_months(date_key, 12)) & 1 = 1,
    trunc(add_months(date_key, 12), 'MM'),
    trunc(add_months(add_months(date_key, 12), -1), 'MM')
  ) as next_stat_bimonthly_month,
  brand_account_id,
  ads_first_cost_date,
  brand_user_name,
  company_code,
  company_name,
  group_code,
  group_name,
  trade_type_first_name,
  trade_type_second_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_tag_code,
  brand_tag_name,
  brand_group_tag_code,
  brand_group_tag_name,
  first_industry_name,
  second_industry_name,
  brand_active_level,
  max(direct_sales_name) as direct_sales_name,
  max(cpc_operator_name) as cpc_operator_name,
  sum(
    case
      when module in ('效果', '品牌', '薯条') then cash_cost
      else 0
    end
  ) as ads_cash_income_amt,
  sum(
    case
      when module in ('效果') then cash_cost
      else 0
    end
  ) as rtb_cash_income_amt,
  sum(
    case
      when module in ('品牌') then cash_cost
      else 0
    end
  ) as brand_cash_income_amt,
  sum(
    case
      when module in ('薯条') then cash_cost
      else 0
    end
  ) as chips_cash_income_amt,
  sum(
    case
      when module in ('品合') then cash_cost
      else 0
    end
  ) as bcoo_cash_income_amt,
  sum(
    case
      when module in ('效果')
      and product in ('竞价-信息流') then cash_cost
      else 0
    end
  ) as rtb_explore_cash_income_amt,
  sum(
    case
      when module in ('效果')
      and product in ('竞价-视频内流') then cash_cost
      else 0
    end
  ) as rtb_video_cash_income_amt,
  sum(
    case
      when module in ('效果')
      and product in ('竞价-搜索') then cash_cost
      else 0
    end
  ) as rtb_search_cash_income_amt,
  sum(
    case
      when module in ('品牌')
      and product in ('信息流GD') then cash_cost
      else 0
    end
  ) as brand_gd_cash_income_amt,
  sum(
    case
      when module in ('品牌')
      and product in ('开屏') then cash_cost
      else 0
    end
  ) as brand_splash_cash_income_amt,
  sum(
    case
      when module in ('品牌')
      and product in ('品牌专区') then cash_cost
      else 0
    end
  ) as brand_zone_cash_income_amt,
  sum(
    case
      when module in ('品牌')
      and product in ('品牌其他') then cash_cost
      else 0
    end
  ) as brand_other_cash_income_amt,
  sum(
    case
      when module in ('品牌')
      and product in ('搜索第三位') then cash_cost
      else 0
    end
  ) as brand_search_3rd_cash_income_amt,
  sum(cpc_budget) as budget_amt,
  sum(cpc_cost_budget_rate) as cost_amt,
  sum(
    case
      when module in ('效果')
      and product in ('竞价-信息流') then cpc_budget
      else 0
    end
  ) as rtb_explore_budget_income_amt,
  sum(
    case
      when module in ('效果')
      and product in ('竞价-信息流') then cpc_cost_budget_rate
      else 0
    end
  ) as rtb_explore_cost_income_amt,
  sum(
    case
      when module in ('效果')
      and product in ('竞价-搜索') then cpc_budget
      else 0
    end
  ) as rtb_search_budget_income_amt,
  sum(
    case
      when module in ('效果')
      and product in ('竞价-搜索') then cpc_cost_budget_rate
      else 0
    end
  ) as rtb_search_cost_income_amt,
  sum(
    case
      when module in ('效果')
      and product in ('竞价-视频内流') then cpc_budget
      else 0
    end
  ) as rtb_video_budget_income_amt,
  sum(
    case
      when module in ('效果')
      and product in ('竞价-视频内流') then cpc_cost_budget_rate
      else 0
    end
  ) as rtb_video_cost_income_amt,
  sum(
    case
      when module in ('效果')
      and market_target in ('闭环电商') then cash_cost
      else 0
    end
  ) as ecm_closed_cash_income_amt
FROM
  redapp.app_ads_insight_industry_product_account_td_df
WHERE
  dtm = '{{ds_nodash}}'
  and date_key >= '2022-01-01'
group by
  date_key,
  substring(date_key, 1, 7),
  add_months(date_key, 12),
  if(
    month(date_key) & 1 = 1,
    trunc(date_key, 'MM'),
    trunc(add_months(date_key, -1), 'MM')
  ),
  brand_account_id,
  ads_first_cost_date,
  brand_user_name,
  company_code,
  company_name,
  group_code,
  group_name,
  trade_type_first_name,
  trade_type_second_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_tag_code,
  brand_tag_name,
  brand_group_tag_code,
  brand_group_tag_name,
  first_industry_name,
  second_industry_name,
  brand_active_level
;
drop table
  if exists temp.temp_app_ads_insight_account_diagnosis_info_df_{{ds_nodash}}_002;

create table
  temp.temp_app_ads_insight_account_diagnosis_info_df_{{ds_nodash}}_002 as
  select t1.*,
  t3.rtb_last_cost_date,
  t3.ecm_closed_sx_cash_income_amt_2m,
  row_number()over (partition by track_industry_name order by ads_cash_income_amt_30d desc) as income_rank,
  row_number()over (partition by track_industry_name order by budget_amt_30d desc) as budget_rank,
  ads_cash_income_amt_30d/30 as avg_ads_cash_income_amt_30d,
  budget_amt_30d/30 as avg_budget_amt_30d,
  last_ads_cash_income_amt_2m,
  last_rtb_cash_income_amt_2m,
  last_brand_cash_income_amt_2m,
  last_chips_cash_income_amt_2m,
  last_bcoo_cash_income_amt_2m,
  last_rtb_explore_cash_income_amt_2m,
  last_rtb_video_cash_income_amt_2m,
  last_rtb_search_cash_income_amt_2m,
  cps_note_num_60d,
  cps_note_ecm_closed_cost_2m,
  taolian_cash_income_amt_2m,
  case when cps_note_ecm_closed_cost_2m>0 then 0 else 1 end as is_ecm_no_income_cps_note,
  0 as cash_balance,
  0 as ecm_closed_dgmv_2m,
  0 as ecm_closed_dgmv_cash_income_amt_2m,
  is_sx_ti
from 
(select
  brand_account_id,
  brand_user_name,
  company_code,
  company_name,
  group_code,
  group_name,
  trade_type_first_name,
  trade_type_second_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_tag_code,
  brand_tag_name,
  brand_group_tag_code,
  brand_group_tag_name,
  first_industry_name,
  second_industry_name,
  brand_active_level,
  sum(ads_cash_income_amt) as ads_cash_income_amt_2m,
  sum(rtb_cash_income_amt) as rtb_cash_income_amt_2m,
  sum(brand_cash_income_amt) as brand_cash_income_amt_2m,
  sum(chips_cash_income_amt) as chips_cash_income_amt_2m,
  sum(bcoo_cash_income_amt) as bcoo_cash_income_amt_2m,
  sum(rtb_explore_cash_income_amt) as rtb_explore_cash_income_amt_2m,
  sum(rtb_video_cash_income_amt) as rtb_video_cash_income_amt_2m,
  sum(rtb_search_cash_income_amt) as rtb_search_cash_income_amt_2m,
  sum(brand_gd_cash_income_amt) as brand_gd_cash_income_amt_2m,
  sum(brand_splash_cash_income_amt) as brand_splash_cash_income_amt_2m,
  sum(brand_zone_cash_income_amt) as brand_zone_cash_income_amt_2m,
  sum(brand_other_cash_income_amt) as brand_other_cash_income_amt_2m,
  sum(brand_search_3rd_cash_income_amt) as brand_search_3rd_cash_income_amt_2m,
  sum(ecm_closed_cash_income_amt) as ecm_closed_cash_income_amt_2m,

   sum(case when date_key between f_getdate('{{ds}}', -29) and '{{ds}}' then ads_cash_income_amt else 0 end) as ads_cash_income_amt_30d,
  sum(case when date_key between f_getdate('{{ds}}', -29) and '{{ds}}' then rtb_cash_income_amt else 0 end) as rtb_cash_income_amt_30d,
  sum(case when date_key between f_getdate('{{ds}}', -29) and '{{ds}}' then brand_cash_income_amt else 0 end) as brand_cash_income_amt_30d,
  sum(case when date_key between f_getdate('{{ds}}', -29) and '{{ds}}' then chips_cash_income_amt else 0 end) as chips_cash_income_amt_30d,
  sum(case when date_key between f_getdate('{{ds}}', -29) and '{{ds}}' then bcoo_cash_income_amt else 0 end) as bcoo_cash_income_amt_30d,
  sum(case when date_key between f_getdate('{{ds}}', -29) and '{{ds}}' then rtb_explore_cash_income_amt else 0 end) as rtb_explore_cash_income_amt_30d,
  sum(case when date_key between f_getdate('{{ds}}', -29) and '{{ds}}' then rtb_video_cash_income_amt else 0 end) as rtb_video_cash_income_amt_30d,
  sum(case when date_key between f_getdate('{{ds}}', -29) and '{{ds}}' then rtb_search_cash_income_amt else 0 end) as rtb_search_cash_income_amt_30d,
  sum(case when date_key between f_getdate('{{ds}}', -29) and '{{ds}}' then brand_gd_cash_income_amt else 0 end) as brand_gd_cash_income_amt_30d,
  sum(case when date_key between f_getdate('{{ds}}', -29) and '{{ds}}' then brand_splash_cash_income_amt else 0 end) as brand_splash_cash_income_amt_30d,
  sum(case when date_key between f_getdate('{{ds}}', -29) and '{{ds}}' then brand_zone_cash_income_amt else 0 end) as brand_zone_cash_income_amt_30d,
  sum(case when date_key between f_getdate('{{ds}}', -29) and '{{ds}}' then brand_other_cash_income_amt else 0 end) as brand_other_cash_income_amt_30d,
  sum(case when date_key between f_getdate('{{ds}}', -29) and '{{ds}}' then brand_search_3rd_cash_income_amt else 0 end) as brand_search_3rd_cash_income_amt_30d,
  sum(case when date_key between f_getdate('{{ds}}', -29) and '{{ds}}' then ecm_closed_cash_income_amt else 0 end) as ecm_closed_cash_income_amt_30d,
  sum(case when date_key between f_getdate('{{ds}}', -29) and '{{ds}}' then budget_amt else 0 end) as budget_amt_30d,
  --昨日
  sum(case when date_key = '{{ds}}' then budget_amt else 0 end) as rtb_budget_income_amt,
   sum(case when date_key = '{{ds}}' then cost_amt else 0 end) as rtb_cost_income_amt,
   sum(case when date_key = '{{ds}}' then rtb_explore_budget_income_amt else 0 end) as rtb_explore_budget_income_amt,
   sum(case when date_key = '{{ds}}' then rtb_explore_cost_income_amt else 0 end) as rtb_explore_cost_income_amt,
   sum(case when date_key = '{{ds}}' then rtb_search_budget_income_amt else 0 end) as rtb_search_budget_income_amt,
   sum(case when date_key = '{{ds}}' then rtb_search_cost_income_amt else 0 end) as rtb_search_cost_income_amt,
   sum(case when date_key = '{{ds}}' then rtb_video_budget_income_amt else 0 end) as rtb_video_budget_income_amt,
   sum(case when date_key = '{{ds}}' then rtb_video_cost_income_amt else 0 end) as rtb_video_cost_income_amt,
   sum(case when date_key = '{{ds}}' then ads_cash_income_amt else 0 end) as ads_cash_income_amt,
   sum(case when date_key between f_getdate('{{ds}}', -6) and '{{ds}}' then rtb_cash_income_amt else 0 end) as rtb_cash_income_amt_7d
from
  temp.temp_app_ads_insight_account_diagnosis_info_df_{{ds_nodash}}_001
where
  stat_bimonthly_month = if(
    month('{{ds}}') & 1 = 1,
    trunc('{{ds}}', 'MM'),
    trunc(add_months('{{ds}}', -1), 'MM')
  ) --本双月
group by
  brand_account_id,
  brand_user_name,
  company_code,
  company_name,
  group_code,
  group_name,
  trade_type_first_name,
  trade_type_second_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_tag_code,
  brand_tag_name,
  brand_group_tag_code,
  brand_group_tag_name,
  first_industry_name,
  second_industry_name,
  brand_active_level
  )t1
  left join 
  (select
  brand_account_id,
  sum(ads_cash_income_amt) as last_ads_cash_income_amt_2m,
  sum(rtb_cash_income_amt) as last_rtb_cash_income_amt_2m,
  sum(brand_cash_income_amt) as last_brand_cash_income_amt_2m,
  sum(chips_cash_income_amt) as last_chips_cash_income_amt_2m,
  sum(bcoo_cash_income_amt) as last_bcoo_cash_income_amt_2m,
  sum(rtb_explore_cash_income_amt) as last_rtb_explore_cash_income_amt_2m,
  sum(rtb_video_cash_income_amt) as last_rtb_video_cash_income_amt_2m,
  sum(rtb_search_cash_income_amt) as last_rtb_search_cash_income_amt_2m
from
  temp.temp_app_ads_insight_account_diagnosis_info_df_{{ds_nodash}}_001
where
  next_stat_bimonthly_month = if(
    month('{{ds}}') & 1 = 1,
    trunc('{{ds}}', 'MM'),
    trunc(add_months('{{ds}}', -1), 'MM')
  ) --去年同期双月
  and  next_date_key<='{{ds}}'
group by
  brand_account_id
  )t2 
  on t1.brand_account_id=t2.brand_account_id
  left join 
  --最近一次竞价消耗
  (select brand_account_id,
    sum(case when marketing_target=3 and module in ('效果') and if(
      month(date_key) & 1 = 1,
      trunc(date_key, 'MM'),
      trunc(add_months(date_key, -1), 'MM')
        )=if(
          month('{{ds}}') & 1 = 1,
          trunc('{{ds}}', 'MM'),
          trunc(add_months('{{ds}}', -1), 'MM')
    ) then cash_income_amt else 0 end) as ecm_closed_sx_cash_income_amt_2m,--双月商销收入
    max(case when module in ('效果') and date_key <= '{{ds}}' then date_key else '0000-00-00' end) as rtb_last_cost_date -- 除去t-1的最后一次消耗时间
  from redcdm.dm_ads_pub_product_account_detail_td_df
  where dtm='{{ds_nodash}}' and cash_income_amt>0
  group by brand_account_id
  )t3 
  on t3.brand_account_id=t1.brand_account_id
left join 
(select 
  brand_account_user_id,
  sum(bcoo_content_price) AS taolian_cash_income_amt_2m
from
  redapp.app_ads_taolian_brief_note_cost_df
where
  dtm = f_getdate('{{ ds_nodash }}', -1) --产出完t+2
  and brief_start_time between f_getdate('{{ ds_nodash }}', -30) and f_getdate('{{ ds_nodash }}', -1)
group by 1
)t4
on t1.brand_account_id = t4.brand_account_user_id
left join 
(select user_id as brand_account_id,
  count(distinct discovery_id) as cps_note_num_60d,
  sum(ecm_closed_cost) as cps_note_ecm_closed_cost_2m
from 
(select user_id,discovery_id
from reddw.dw_soc_discovery_delta_7_day 
where dtm = '{{ ds_nodash }}' 
and substring(publish_time,1,10)>=f_getdate('{{ ds }}', -59) 
and substring(publish_time,1,10)<='{{ds}}'--近60日发布笔记
and is_cps_note = 1
)cps 
left join 
(select brand_account_id,
  ads_material_id,
  sum(cost) as ecm_closed_cost
from redcdm.dws_ads_log_creativity_1d_di
where dtm between f_getdate(if(month('{{ds}}') & 1 = 1, trunc('{{ds}}', 'MM'),trunc(add_months('{{ds}}', -1), 'MM') ))  and'{{ ds_nodash }}'
and ads_material_type='post'
and cost>0
and marketing_target in (3,8,14)
group by brand_account_id,
  ads_material_id
)cost 
on cps.discovery_id = cost.ads_material_id
and cps.user_id=cost.brand_account_id
group by user_id
)t5 
on t5.brand_account_id=t1.brand_account_id
--商销未投种草人群
left join 
(select brand_account_id,
  max(is_ti_user) as is_sx_ti
from 
(select brand_account_id,is_ti_user,sum(cost)
from redapp.app_ads_cvr_account_detail_di
where  dtm between f_getdate(if(month('{{ds}}') & 1 = 1, trunc('{{ds}}', 'MM'),trunc(add_months('{{ds}}', -1), 'MM') ))  and'{{ ds_nodash }}'
and marketing_target='3' and cost>0
group by brand_account_id,is_ti_user
)ecm
group by brand_account_id 
)t6 
on t6.brand_account_id=t1.brand_account_id
;
drop table
  if exists temp.temp_app_ads_insight_account_diagnosis_info_df_{{ds_nodash}}_003;

create table
  temp.temp_app_ads_insight_account_diagnosis_info_df_{{ds_nodash}}_003 as
select track_industry_name,
  sum(rtb_cost_income_amt) as track_rtb_cost_income_amt,
  sum(rtb_budget_income_amt) as track_rtb_budget_income_amt,
  sum(bcoo_cash_income_amt_2m) as track_bcoo_cash_income_amt_2m,
  sum(ecm_closed_cash_income_amt_2m) as track_ecm_closed_cash_income_amt_2m,
  sum(taolian_cash_income_amt_2m) as track_taolian_cash_income_amt_2m,
  sum(ecm_closed_dgmv_2m) as track_ecm_closed_dgmv_2m,
  sum(ecm_closed_dgmv_cash_income_amt_2m) as track_ecm_closed_dgmv_cash_income_amt_2m
from temp.temp_app_ads_insight_account_diagnosis_info_df_{{ds_nodash}}_002 --加工赛道数据
group by track_industry_name
;
select 
  1 as account_type,
  t1.brand_account_id as account_id,
  t1.brand_account_id,
  brand_user_name as brand_account_name,
  active_level,
  ads_cash_income_amt_30d,
  rtb_cash_income_amt_30d,
  brand_cash_income_amt_30d,
  chips_cash_income_amt_30d,
  bcoo_cash_income_amt_30d,
  rtb_explore_cash_income_amt_30d,
  rtb_search_cash_income_amt_30d,
  rtb_video_cash_income_amt_30d,
  brand_gd_cash_income_amt_30d,
  brand_splash_cash_income_amt_30d,
  brand_zone_cash_income_amt_30d,
  brand_search_3rd_cash_income_amt_30d,
  brand_other_cash_income_amt_30d,
  avg_ads_cash_income_amt_30d,
  budget_amt_30d,
  avg_budget_amt_30d,
  budget_rank,
  income_rank,
  rtb_last_cost_date,
  ads_cash_income_amt,
  rtb_cost_income_amt,
  rtb_budget_income_amt,
  cash_balance,
  rtb_cash_income_amt_7d,
  rtb_explore_cost_income_amt,
  rtb_explore_budget_income_amt,
  rtb_search_cost_income_amt,
  rtb_search_budget_income_amt,
  rtb_video_cost_income_amt,
  rtb_video_budget_income_amt,
  ads_cash_income_amt_2m,
  rtb_cash_income_amt_2m,
  brand_cash_income_amt_2m,
  chips_cash_income_amt_2m,
  bcoo_cash_income_amt_2m,
  rtb_explore_cash_income_amt_2m,
  rtb_search_cash_income_amt_2m,
  rtb_video_cash_income_amt_2m,
  last_ads_cash_income_amt_2m,
  last_rtb_cash_income_amt_2m,
  last_brand_cash_income_amt_2m,
  last_chips_cash_income_amt_2m,
  last_bcoo_cash_income_amt_2m,
  last_rtb_explore_cash_income_amt_2m,
  last_rtb_search_cash_income_amt_2m,
  last_rtb_video_cash_income_amt_2m,
  taolian_cash_income_amt_2m,
  cps_note_num_60d,
  is_ecm_no_income_cps_note,
  ecm_closed_cash_income_amt_2m,
  ecm_closed_dgmv_2m,
  ecm_closed_dgmv_cash_income_amt_2m,
  ecm_closed_sx_cash_income_amt_2m,
  is_sx_ti,
  track_rtb_cost_income_amt,
  track_rtb_budget_income_amt,
  track_bcoo_cash_income_amt_2m,
  track_taolian_cash_income_amt_2m,
  track_ecm_closed_cash_income_amt_2m,
  track_ecm_closed_dgmv_2m,
  track_ecm_closed_dgmv_cash_income_amt_2m,
  company_code,
  company_name,
  group_code,
  group_name,
  trade_type_first_name,
  trade_type_second_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_tag_code,
  brand_tag_name,
  brand_group_tag_code,
  brand_group_tag_name,
  first_industry_name,
  second_industry_name,
  direct_sales_name,
  cpc_operator_name
from temp.temp_app_ads_insight_account_diagnosis_info_df_{{ds_nodash}}_002 t1
left join temp.temp_app_ads_insight_account_diagnosis_info_df_{{ds_nodash}}_003 t2
on t1.track_industry_name=t2.track_industry_name
