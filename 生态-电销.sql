SET "kyuubi.spark.option.--conf spark.sql.crossJoin.enabled=true";
SET "kyuubi.spark.option.--conf spark.redExtension.allowBroadcastNestedLoopJoin=true";
drop table if exists temp.temp_app_ads_insight_shengtai_df_{{ds_nodash}};

create table
  temp.temp_app_ads_insight_shengtai_df_{{ds_nodash}} as
--生态员工当日指标
select date_key,
  seller_red_name,
  seller_code,
  sum(recharge_clue_cnt) as recharge_clue_cnt,
  sum(recharge_5000_clue_cnt) as recharge_5000_clue_cnt,
  sum(online_clue_cnt) as online_clue_cnt,
  --充值线索数
  sum(new_clue_cnt) as new_clue_cnt,
  sum(old_clue_cnt) as old_clue_cnt,
  sum(cost) as  cost,
sum(month_clue_cnt) as  month_clue_cnt
from 
(
select
  coalesce(t1.date_key, t2.date_key) as date_key,
  coalesce(t1.seller_red_name, t2.seller_red_name) as seller_red_name,
  coalesce(t1.assigned_seller_code, t2.assigned_seller_code) as seller_code,
  recharge_clue_cnt,
  recharge_5000_clue_cnt,
  online_clue_cnt,
  --充值线索数
  new_clue_cnt,
  old_clue_cnt,
  0 as cost,
0 as month_clue_cnt
from
  (
    select
      pay_date as date_key,
      seller_red_name,
      assigned_seller_code,
      count(1) as recharge_clue_cnt,
      --充值线索数
      count(
        case
          when pay_amount >= 5000 then 1
          else null
        end
      ) as recharge_5000_clue_cnt
    from
      reddw.dw_ads_crm_sme_clue_performance_v2_day
    where
      (
        dtm = '{{ds_nodash}}'
        or (
          dtm < '{{ds_nodash}}'
          and dtm in (
            '20240131',
            '20240229',
            '20240331',
            '20240430',
            '20240531',
            '20240630',
            '20240731',
            '20240831',
            '20240930',
            '20241031',
            '20241130',
            '20241231',
            '20250131',
            '20250228',
            '20250331',
            '20250430'
          )
        )
      )
      and substring(replace(pay_date, '-', ''), 1, 6) = substring(dtm, 1, 6) --月底最后一天归档当月
    group by
      1,
      2,3
  ) t1
  full outer join (
    select
      first_consume_date as date_key,
      seller_red_name,
      assigned_seller_code,
      count(1) as online_clue_cnt,
      --充值线索数
      count(
        case
          when first_consume_date >= rtb_first_consume_date then 1
          else null
        end
      ) as new_clue_cnt,
      count(
        case
          when first_consume_date < rtb_first_consume_date then 1
          else null
        end
      ) as old_clue_cnt
      --sum(coalesce(60d_cost_cur_month,0)+coalesce(61_90d_cost_cur_month,0)+coalesce(91_120d_cost_cur_month,0)) as cost
    from
      reddw.dw_ads_crm_sme_clue_performance_v2_day
    where
      (
        dtm = '{{ds_nodash}}'
        or (
          dtm < '{{ds_nodash}}'
          and dtm in (
            '20240131',
            '20240229',
            '20240331',
            '20240430',
            '20240531',
            '20240630',
            '20240731',
            '20240831',
            '20240930',
            '20241031',
            '20241130',
            '20241231',
            '20250131',
            '20250228',
            '20250331',
            '20250430'
          )
        )
      )
      and substring(replace(first_consume_date, '-', ''), 1, 6) = substring(dtm, 1, 6) --月底最后一天归档当月
    group by
      1,
      2,
      3
  ) t2 on t1.date_key = t2.date_key
  and t1.seller_red_name = t2.seller_red_name
  and t1.assigned_seller_code=t2.assigned_seller_code
union all 
--当月指标
select f_getdate(concat(substring(dtm,1,6),'01' )) as date_key,
seller_red_name,
assigned_seller_code,
0 as recharge_clue_cnt,
 0 as  recharge_5000_clue_cnt,
 0 as  online_clue_cnt,
  --充值线索数
 0 as  new_clue_cnt,
 0 as  old_clue_cnt,
sum(coalesce(60d_cost_cur_month,0)+coalesce(61_90d_cost_cur_month,0)+coalesce(91_120d_cost_cur_month,0)) as cost,
sum(case when  substring(replace(pay_date, '-', ''), 1, 6) = substring(dtm, 1, 6) and substring(replace(first_consume_date, '-', ''), 1, 6) = substring(dtm, 1, 6) then 1 else null end) as month_clue_cnt--当月充值且上线线索数
from reddw.dw_ads_crm_sme_clue_performance_v2_day
    where
      (
        dtm = '{{ds_nodash}}'
        or (
          dtm < '{{ds_nodash}}'
          and dtm in (
            '20240131',
            '20240229',
            '20240331',
            '20240430',
            '20240531',
            '20240630',
            '20240731',
            '20240831',
            '20240930',
            '20241031',
            '20241130',
            '20241231',
            '20250131',
            '20250228',
            '20250331',
            '20250430'
          )
        )
      )
group by 1,2,3
)base 
group by 1,2,3
;

drop table if exists temp.temp_app_ads_insight_shengtai_df2_{{ds_nodash}};

create table
  temp.temp_app_ads_insight_shengtai_df2_{{ds_nodash}} as
select base.seller_code,
  base.department_code,
  base.department_name,
  base.department_1_code,
  base.department_1_name,
  base.department_2_code,
  base.department_2_name,
  base.department_3_code,
  base.department_3_name,
  base.department_4_code,
  base.department_4_name,
  base.department_5_code,
  base.department_5_name,
  base.department_6_code,
  base.department_6_name,
  base.seller_name,
  out_date,
  f_getdate(base.dtm) as date_key,
  total_clue_cnt,
  seller_clue_cnt as valid_clue_cnt
from
(--员工人数
select t1.seller_code,
  t1.department_code,
  t1.department_name,
  t1.department_1_code,
  t1.department_1_name,
  t1.department_2_code,
  t1.department_2_name,
  t1.department_3_code,
  t1.department_3_name,
  t1.department_4_code,
  t1.department_4_name,
  t1.department_5_code,
  t1.department_5_name,
  t1.department_6_code,
  t1.department_6_name,
  t1.seller_name,
  out_date,
  t1.dtm
from ads_data_crm.dim_ads_crm_user_department_info_df t1 
left join reddim.dim_ads_crm_user_department_leader_relation_day t2 on t2.dtm='{{ds_nodash}}' and t2.seller_code = t1.seller_code
-- left join 
-- (select day_dtm
-- from redcdm.dim_ads_date_df 
-- where dtm='all' and day_dtm>='20240101' and day_dtm<='{{ds_nodash}}'
-- )t3 on 1=1
where t1.dtm>='20240101' and t1.department_2_name='生态客户业务部' --and out_date=''
)base 

left join 
(select dtm,
  last_assign_user_code,
  --last_assign_red_name,
  count(1) as total_clue_cnt,
  count(case when clue_status_cn not in ('长期冻结','未分配','已冻结') then 1 else null end) as seller_clue_cnt
from reddm.dm_ads_crm_clue_info_wide_day
where dtm>='20240101' and clue_pool_name='生态电销线索池' 
group by 1,2
)clue 
on base.dtm=clue.dtm and base.seller_code=clue.last_assign_user_code;


select   coalesce(t1.date_key,t2.date_key) as date_key,
	coalesce(t1.seller_code,t2.seller_code) as seller_code,
  base.department_code,
  base.department_name,
  base.department_1_code,
  base.department_1_name,
  base.department_2_code,
  base.department_2_name,
  base.department_3_code,
  base.department_3_name,
  base.department_4_code,
  base.department_4_name,
  base.department_5_code,
  base.department_5_name,
  base.department_6_code,
  base.department_6_name,
  coalesce(t1.seller_name,t2.seller_red_name) as seller_name,
  base.out_date,
  total_clue_cnt,
  valid_clue_cnt,
  recharge_clue_cnt,
  recharge_5000_clue_cnt,
  online_clue_cnt,
  --充值线索数
  new_clue_cnt,
  old_clue_cnt,
  cost,
  month_clue_cnt
from temp.temp_app_ads_insight_shengtai_df2_20240804 t1
full outer join 
temp.temp_app_ads_insight_shengtai_df_20240804 t2
on t1.date_key =t2.date_key and t1.seller_code=t2.seller_code
left join  
(select seller_code,
  department_code,
  department_name,
  department_1_code,
	department_1_name,
	department_2_code,
	department_2_name,
	department_3_code,
	department_3_name,
	department_4_code,
	department_4_name,
	department_5_code,
	department_5_name,
	department_6_code,
	department_6_name,
	seller_name,
  out_date
from temp.temp_app_ads_insight_shengtai_df2_20240804 
where date_key='{{ds}}'
)base on coalesce(t1.seller_code,t2.seller_code)=base.seller_code
