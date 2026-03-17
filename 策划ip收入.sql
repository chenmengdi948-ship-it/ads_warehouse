create table
  temp.temp_app_ads_industry_brand_ip_product_metric_df_{{ds_nodash}}_online as
  select
  t1.investment_project_code as project_code,
  t1.date_key,
  project_name,
  project_status_cn,
  start_date,
  end_date,
  if_selling,
  planner_email,
  planner_user_code,
  planner_red_name,
  planner_department_code,
  planner_department_name,
  planned_total_amount,
  planned_total_cost,
  participant,
  sum(ip_income_amt) as ip_income_amt,
  sum(ip_order_income_amt) as ip_order_income_amt,
  sum(project_amt) as project_amt
from
  (
    select
      t1.investment_project_code,
      t1.date_key,
      ip_income_amt,
      ip_order_income_amt,
      0 as project_amt
    from
      (
        select
          a.investment_project_code,
          a.date_key,
          sum(income_amount) as ip_income_amt
        from
          reddw.dw_ads_crm_brand_stats_day a
        where
          a.dtm = '{{ds_nodash}}'
          and a.is_marketing_product = 1
        group by
          1,
          2
      ) t1
      left join (
        select
          a.investment_project_code,
          a.order_date,
          sum(income_amount) as ip_order_income_amt
        from
          reddw.dw_ads_crm_brand_stats_day a
        where
          a.dtm = '{{ds_nodash}}'
          and a.order_date between '2022-01-01' and '{{ds}}'
          and a.is_marketing_product = 1
        group by
          1,
          2
      ) t2 on t1.investment_project_code = t2.investment_project_code
      and t1.date_key = t2.order_date
    union all
    select
      project_code,
      start_date as date_key,
      0 as ip_income_amt,
      0 as ip_order_income_amt,
      planned_total_amount as project_amt
    from
      ads_data_crm.dw_ip_project_detail_df
    WHERE
      dtm = '{{ds_nodash}}'
  ) t1
  left join (
    SELECT
      project_code,
      project_name,
      project_level,
      project_level_cn,
      project_type,
      project_type_cn,
      project_status,
      project_status_cn,
      start_date,
      end_date,
      if_selling,
      planner_email,
      planner_user_code,
      planner_red_name,
      planner_department_code,
      planner_department_name,
      planned_total_amount,
      planned_total_cost,
      submit_roi,
      current_roi,
      roi_gap,
      order_cnt,
      order_amount,
      booked_unorder_amount,
      ttl_booked_amount,
      paid_cost,
      un_standard_total_amount,
      trump_ads_total_amount,
      participant
    FROM
      ads_data_crm.dw_ip_project_detail_df
    WHERE
      dtm = '{{ds_nodash}}'
  ) t3 on t3.project_code = t1.investment_project_code
  group by t1.investment_project_code,
  t1.date_key,
  project_name,
  project_status_cn,
  start_date,
  end_date,
  if_selling,
  planner_email,
  planner_user_code,
  planner_red_name,
  planner_department_code,
  planner_department_name,
  planned_total_amount,
  planned_total_cost,
  participant