SET "kyuubi.spark.option.--conf spark.sql.crossJoin.enabled=true";
SET "kyuubi.spark.option.--conf spark.redExtension.allowBroadcastNestedLoopJoin=true"; 
drop table if exists temp.temp_app_ads_industry_account_seller_30d_di_{{ds_nodash}}_1;

create table
  temp.temp_app_ads_industry_account_seller_30d_di_{{ds_nodash}}_1 as
select
  brand_account_id,
  day_dtm,
  substring(day_dtm,1,6) as stat_mt,
  dt
  --if(month(dt) & 1 = 1,trunc(dt,'MM'),trunc(add_months(dt,-1),'MM')) as stat_mt
from
  ( --账户基础信息
    select
      brand_account_id
    from
      redapp.app_ads_industry_account_seller_di 
    where
      dtm >= '20220901'
      and (deal_gmv>0 or rtb_cash_cost>0 or brand_cash_cost>0 or ti_cash_cost>0 or brand_zone_cash_cost>0 or ecm_closed_cash_cost>0 or sx_cash_cost>0 or brand_ti_cost>0 or brand_target_type_cost>0)
      --and track_industry_dept_group_name in ('奢品', '美妆', '服饰潮流')
      --and date_key = '{{ds}}'
    group by
      1
  ) a
  join (
    select
      day_dtm,
      dt
    from
      redcdm.dim_ads_date_df
    where
      dtm = 'all'
      and day_dtm between '20220901' and '{{ds_nodash}}'
  ) dt on 1 = 1;
drop table if exists temp.temp_app_ads_industry_account_seller_30d_di_{{ds_nodash}};

create table
  temp.temp_app_ads_industry_account_seller_30d_di_{{ds_nodash}} as
--   select t1.brand_account_id,
--   seller_id,
--   user_name,
--   shopname,
--   state,
--   frozen,
--   seller_type,
--   seller_level,
--   main_category_name,
--   industry,
--   first_category_name,
--   second_category_name,
--   third_category_name,
--   first_audit_time,
--   create_dt,
--   shop_user_type,
--   deal_gmv,
--   rtb_cash_cost,
--   brand_cash_cost,
--   ti_cash_cost,
--   brand_zone_cash_cost,
--   ecm_closed_cash_cost,
--   sx_cash_cost,
--   brand_ti_cost,
--   brand_target_type_cost,
--   deal_gmv_1m,
--   rtb_cash_cost_1m,
--   brand_cash_cost_1m,
--   ti_cash_cost_1m,
--   brand_zone_cash_cost_1m,
--   ecm_closed_cash_cost_1m,
--   sx_cash_cost_1m,
--   brand_ti_cost_1m,
--   brand_target_type_cost_1m,
--    dtm
-- from 
select 
 --近30日指标
  brand_account_id,
  day_dtm as dtm,
  stat_month,
  deal_gmv,
  rtb_cash_cost,
  brand_cash_cost,
  ti_cash_cost,
  brand_zone_cash_cost,
  ecm_closed_cash_cost,
  sx_cash_cost,
  brand_ti_cost,
  brand_target_type_cost,
  seller_id,
  user_name,
  shopname,
  state,
  frozen,
  seller_type,
  seller_level,
  main_category_name,
  industry,
  first_category_name,
  second_category_name,
  third_category_name,
  first_audit_time,
  create_dt,
  shop_user_type,
  sum(deal_gmv) over (
    partition by
      brand_account_id,stat_mt
    order by
      cast(date_key as date) asc rows between unbounded  PRECEDING
      and current row
  ) as deal_gmv_1m,
  sum(rtb_cash_cost) over (
    partition by
      brand_account_id,stat_mt
    order by
      cast(date_key as date) asc rows between unbounded  PRECEDING
      and current row
  ) as rtb_cash_cost_1m,
  sum(brand_cash_cost) over (
    partition by
      brand_account_id,stat_mt
    order by
      cast(date_key as date) asc rows between unbounded  PRECEDING
      and current row
  ) as brand_cash_cost_1m,
  sum(ti_cash_cost) over (
    partition by
      brand_account_id,stat_mt
    order by
      cast(date_key as date) asc rows between unbounded  PRECEDING
      and current row
  ) as ti_cash_cost_1m,
  sum(brand_zone_cash_cost) over (
    partition by
      brand_account_id,stat_mt
    order by
      cast(date_key as date) asc rows between unbounded  PRECEDING
      and current row
  ) as brand_zone_cash_cost_1m,
  sum(ecm_closed_cash_cost) over (
    partition by
      brand_account_id,stat_mt
    order by
      cast(date_key as date) asc rows between unbounded  PRECEDING
      and current row
  ) as ecm_closed_cash_cost_1m,
  sum(sx_cash_cost) over (
    partition by
      brand_account_id,stat_mt
    order by
      cast(date_key as date) asc rows between unbounded  PRECEDING
      and current row
  ) as sx_cash_cost_1m,
  sum(brand_ti_cost) over (
    partition by
      brand_account_id,stat_mt
    order by
      cast(date_key as date) asc rows between unbounded  PRECEDING
      and current row
  ) as brand_ti_cost_1m,
  sum(brand_target_type_cost) over (
    partition by
      brand_account_id,stat_mt
    order by
      cast(date_key as date) asc rows between unbounded  PRECEDING
      and current row
  ) as brand_target_type_cost_1m
from 
(select dt.brand_account_id,
  day_dtm,
  dt as date_key,
  stat_mt,
  stat_month,
  seller_id,
  user_name,
  shopname,
  state,
  frozen,
  seller_type,
  seller_level,
  main_category_name,
  industry,
  first_category_name,
  second_category_name,
  third_category_name,
  first_audit_time,
  create_dt,
  shop_user_type,
  coalesce(deal_gmv,0) as  deal_gmv,
  coalesce(rtb_cash_cost,0) as rtb_cash_cost,
  coalesce(brand_cash_cost,0) as brand_cash_cost,
  coalesce(ti_cash_cost,0) as ti_cash_cost,
  coalesce(brand_zone_cash_cost,0) as brand_zone_cash_cost,
  coalesce(ecm_closed_cash_cost,0) as ecm_closed_cash_cost,
  coalesce(sx_cash_cost,0) as sx_cash_cost,
  coalesce(brand_ti_cost,0) as brand_ti_cost,
  coalesce(brand_target_type_cost,0) as brand_target_type_cost
from 
(select brand_account_id,
  day_dtm,
  dt,
  stat_mt
from temp.temp_app_ads_industry_account_seller_30d_di_{{ds_nodash}}_1
)dt 
left join 
(select --近30日指标
brand_account_id,
dtm,
max(seller_id) as seller_id,
  max(user_name) as user_name,
  max(shopname) as shopname,
  max(state) as state,
  max(frozen) as frozen,
  max(seller_type) as seller_type,
  max(seller_level) as seller_level,
  max(main_category_name) as main_category_name,
  max(industry) as industry,
  max(first_category_name) as first_category_name,
  max(second_category_name) as second_category_name,
  max(third_category_name) as third_category_name,
  max(first_audit_time) as first_audit_time,
  max(create_dt) as create_dt,
  max(shop_user_type) as shop_user_type,
substring(dtm,1,6) as stat_month,
  sum(deal_gmv) as deal_gmv,
  sum(rtb_cash_cost) as rtb_cash_cost,
  sum(brand_cash_cost) as brand_cash_cost,
  sum(ti_cash_cost) as ti_cash_cost,
  sum(brand_zone_cash_cost) as brand_zone_cash_cost,
  sum(ecm_closed_cash_cost) as ecm_closed_cash_cost,
  sum(sx_cash_cost) as sx_cash_cost,
  sum(brand_ti_cost) as brand_ti_cost,
  sum(brand_target_type_cost) as brand_target_type_cost
from redapp.app_ads_industry_account_seller_di 
where dtm>='20220901'
and (deal_gmv>0 or rtb_cash_cost>0 or brand_cash_cost>0 or ti_cash_cost>0 or brand_zone_cash_cost>0 or ecm_closed_cash_cost>0 or sx_cash_cost>0 or brand_ti_cost>0 or brand_target_type_cost>0)
group by brand_account_id,
dtm,
substring(dtm,1,6)
)base 
on dt.day_dtm=base.dtm  
and dt.brand_account_id=base.brand_account_id
)tt


-- left join 
-- (select brand_account_id,
--   seller_id,
--   user_name,
--   shopname,
--   state,
--   frozen,
--   seller_type,
--   seller_level,
--   main_category_name,
--   industry,
--   first_category_name,
--   second_category_name,
--   third_category_name,
--   first_audit_time,
--   create_dt,
--   shop_user_type
-- from 
-- (select brand_account_id,
--   seller_id,
--   user_name,
--   shopname,
--   state,
--   frozen,
--   seller_type,
--   seller_level,
--   main_category_name,
--   industry,
--   first_category_name,
--   second_category_name,
--   third_category_name,
--   first_audit_time,
--   create_dt,
--   shop_user_type,
--   deal_gmv,
--   rtb_cash_cost,
--   brand_cash_cost,
--   ti_cash_cost,
--   brand_zone_cash_cost,
--   ecm_closed_cash_cost,
--   sx_cash_cost,
--   brand_ti_cost,
--   brand_target_type_cost,
--   dtm,
--   row_number()over(partition by  brand_account_id order by dtm desc) as rn 
-- from redapp.app_ads_industry_account_seller_di 
-- where dtm <='{{ds_nodash}}' and substring(dtm,1,6)=substring('{{ds_nodash}}',1,6)
-- )info 
-- where rn = 1
-- )dim
-- on t1.brand_account_id = dim.brand_account_id
-- group by t1.brand_account_id,
--   seller_id,
--   user_name,
--   shopname,
--   state,
--   frozen,
--   seller_type,
--   seller_level,
--   main_category_name,
--   industry,
--   first_category_name,
--   second_category_name,
--   third_category_name,
--   first_audit_time,
--   create_dt,
--   shop_user_type,
--   deal_gmv,
--   rtb_cash_cost,
--   brand_cash_cost,
--   ti_cash_cost,
--   brand_zone_cash_cost,
--   ecm_closed_cash_cost,
--   sx_cash_cost,
--   brand_ti_cost,
--   brand_target_type_cost,
--   dtm
  ;
  --月力度企业号聚合
select t1.brand_account_id,
  seller_id,
  user_name,
  shopname,
  state,
  frozen,
  seller_type,
  seller_level,
  main_category_name,
  industry,
  first_category_name,
  second_category_name,
  third_category_name,
  first_audit_time,
  create_dt,
  coalesce(t1.shop_user_type,t2.shop_user_type) as shop_user_type,
  deal_gmv,
  rtb_cash_cost,
  brand_cash_cost,
  ti_cash_cost,
  brand_zone_cash_cost,
  ecm_closed_cash_cost,
  sx_cash_cost,
  brand_ti_cost,
  brand_target_type_cost,
  deal_gmv_1m,
  rtb_cash_cost_1m,
  brand_cash_cost_1m,
  ti_cash_cost_1m,
  brand_zone_cash_cost_1m,
  ecm_closed_cash_cost_1m,
  sx_cash_cost_1m,
  brand_ti_cost_1m,
  brand_target_type_cost_1m,
  company_code,
  company_id,
  track_industry_name,
  process_track_industry_name,
  fans_num,
  fans_level,
  cash_cost_180d,
  ti_cash_cost_180d_type,
  account_ecm_type,
  track_industry_dept_group_name,
  company_name,
  track_group_name,
  'brand_account' as account_type,
  f_getdate(dtm) as date_key
from 
(select brand_account_id,
  seller_id,
  max(user_name) as user_name,
  max(shopname) as shopname,
  max(state) as state,
  max(frozen) as frozen,
  max(seller_type) as seller_type,
  max(seller_level) as seller_level,
  max(main_category_name) as main_category_name,
  max(industry) as industry,
  max(first_category_name) as first_category_name,
  max(second_category_name) as second_category_name,
  max(third_category_name) as third_category_name,
  max(first_audit_time) as first_audit_time,
  max(create_dt) as create_dt,
  max(shop_user_type) as shop_user_type,
  sum(deal_gmv) as deal_gmv,
  sum(rtb_cash_cost) as rtb_cash_cost,
  sum(brand_cash_cost) as brand_cash_cost,
  sum(ti_cash_cost) as ti_cash_cost,
  sum(brand_zone_cash_cost) as brand_zone_cash_cost,
  sum(ecm_closed_cash_cost) as ecm_closed_cash_cost,
  sum(sx_cash_cost) as sx_cash_cost,
  sum(brand_ti_cost) as brand_ti_cost,
  sum(brand_target_type_cost) as brand_target_type_cost,
  sum(deal_gmv_1m) as deal_gmv_1m,
  sum(rtb_cash_cost_1m) as rtb_cash_cost_1m,
  sum(brand_cash_cost_1m) as brand_cash_cost_1m,
  sum(ti_cash_cost_1m) as ti_cash_cost_1m,
  sum(brand_zone_cash_cost_1m) as brand_zone_cash_cost_1m,
  sum(ecm_closed_cash_cost_1m) as ecm_closed_cash_cost_1m,
  sum(sx_cash_cost_1m) as sx_cash_cost_1m,
  sum(brand_ti_cost_1m) as brand_ti_cost_1m,
  sum(brand_target_type_cost_1m) as brand_target_type_cost_1m,
  dtm,
  substring(f_getdate(dtm),1,7) as stat_month
from temp.temp_app_ads_industry_account_seller_30d_di_{{ds_nodash}}
--where dtm = (select max(dtm) from redapp.app_ads_industry_account_ecm_type_df)
group by substring(f_getdate(dtm),1,7),
brand_account_id,
  seller_id,
  dtm
)t1 
left join 
(select stat_month,
  brand_account_id,
  company_code,
  company_id,
  track_industry_name,
  process_track_industry_name,
  fans_num,
  fans_level,
  cash_cost_180d,
  ti_cash_cost_180d_type,
  account_ecm_type,
  shop_user_type,
  track_industry_dept_group_name,
  company_name,
  track_group_name
from redapp.app_ads_industry_account_ecm_type_df
where dtm = '{{ds_nodash}}'
)t2 
on t1.brand_account_id = t2.brand_account_id  and t1.stat_month=t2.stat_month

union all 
--月力度企业号聚合
select null as brand_account_id,
  max(seller_id) as seller_id,
  null as user_name,
  null as shopname,
  null as state,
  null as frozen,
  null as seller_type,
  null as seller_level,
  null as main_category_name,
  null as industry,
  null as first_category_name,
  null as second_category_name,
  null as third_category_name,
  null as first_audit_time,
  null as create_dt,
  null as shop_user_type,
  sum(deal_gmv) as deal_gmv,
  sum(rtb_cash_cost) as rtb_cash_cost,
  sum(brand_cash_cost) as brand_cash_cost,
  sum(ti_cash_cost) as ti_cash_cost,
  sum(brand_zone_cash_cost) as brand_zone_cash_cost,
  sum(ecm_closed_cash_cost) as ecm_closed_cash_cost,
  sum(sx_cash_cost) as sx_cash_cost,
  sum(brand_ti_cost) as brand_ti_cost,
  sum(brand_target_type_cost) as brand_target_type_cost,
  sum(deal_gmv_1m) as deal_gmv_1m,
  sum(rtb_cash_cost_1m) as rtb_cash_cost_1m,
  sum(brand_cash_cost_1m) as brand_cash_cost_1m,
  sum(ti_cash_cost_1m) as ti_cash_cost_1m,
  sum(brand_zone_cash_cost_1m) as brand_zone_cash_cost_1m,
  sum(ecm_closed_cash_cost_1m) as ecm_closed_cash_cost_1m,
  sum(sx_cash_cost_1m) as sx_cash_cost_1m,
  sum(brand_ti_cost_1m) as brand_ti_cost_1m,
  sum(brand_target_type_cost_1m) as brand_target_type_cost_1m,
  company_code,
  company_id,
  null as track_industry_name,
  '总计' as process_track_industry_name,
  null as fans_num,
  null as fans_level,
  cash_cost_180d,
  ti_cash_cost_180d_type,
  account_ecm_type,
  '总计' as  track_industry_dept_group_name,
  company_name,
  null as track_group_name,
  
  'company' as account_type,
  f_getdate(t1.dtm) as date_key,
from 
(select brand_account_id,
  seller_id,
  max(user_name) as user_name,
  max(shopname) as shopname,
  max(state) as state,
  max(frozen) as frozen,
  max(seller_type) as seller_type,
  max(seller_level) as seller_level,
  max(main_category_name) as main_category_name,
  max(industry) as industry,
  max(first_category_name) as first_category_name,
  max(second_category_name) as second_category_name,
  max(third_category_name) as third_category_name,
  max(first_audit_time) as first_audit_time,
  max(create_dt) as create_dt,
  max(shop_user_type) as shop_user_type,
  sum(deal_gmv) as deal_gmv,
  sum(rtb_cash_cost) as rtb_cash_cost,
  sum(brand_cash_cost) as brand_cash_cost,
  sum(ti_cash_cost) as ti_cash_cost,
  sum(brand_zone_cash_cost) as brand_zone_cash_cost,
  sum(ecm_closed_cash_cost) as ecm_closed_cash_cost,
  sum(sx_cash_cost) as sx_cash_cost,
  sum(brand_ti_cost) as brand_ti_cost,
  sum(brand_target_type_cost) as brand_target_type_cost,
  sum(deal_gmv_1m) as deal_gmv_1m,
  sum(rtb_cash_cost_1m) as rtb_cash_cost_1m,
  sum(brand_cash_cost_1m) as brand_cash_cost_1m,
  sum(ti_cash_cost_1m) as ti_cash_cost_1m,
  sum(brand_zone_cash_cost_1m) as brand_zone_cash_cost_1m,
  sum(ecm_closed_cash_cost_1m) as ecm_closed_cash_cost_1m,
  sum(sx_cash_cost_1m) as sx_cash_cost_1m,
  sum(brand_ti_cost_1m) as brand_ti_cost_1m,
  sum(brand_target_type_cost_1m) as brand_target_type_cost_1m,
  dtm,
  substring(f_getdate(dtm),1,7) as stat_month
from temp.temp_app_ads_industry_account_seller_30d_di_{{ds_nodash}}
--where dtm = (select max(dtm) from redapp.app_ads_industry_account_ecm_type_df)
group by substring(f_getdate(dtm),1,7),
dtm,
brand_account_id,
  seller_id
)t1 
left join 
(select stat_month,
  brand_account_id,
  company_code,
  company_id,
  track_industry_name,
  process_track_industry_name,
  fans_num,
  fans_level,
  cash_cost_180d,
  ti_cash_cost_180d_type,
  account_ecm_type,
  shop_user_type,
  track_industry_dept_group_name,
  company_name,
  track_group_name
from redapp.app_ads_industry_account_ecm_type_df
where dtm = '{{ds_nodash}}'
)t2 
on t1.brand_account_id = t2.brand_account_id  and t1.stat_month=t2.stat_month
group by company_code,
  company_id,
  --track_industry_name,
  ti_cash_cost_180d_type,
  account_ecm_type,
  --track_industry_dept_group_name,
  company_name,
  t1.stat_month,
  cash_cost_180d,
  dtm