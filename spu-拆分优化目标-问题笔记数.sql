
    -- ************************************************
    -- Author: chenmengdi
    -- CreateTime:2023-12-20T16:56:06+08:00
    -- Update: Task Update Description
    -- ************************************************
drop table if exists temp.temp_dm_ads_pub_spu_cvr_cost_1d_di_campaign_{{ds_nodash}};

create table
  temp.temp_dm_ads_pub_spu_cvr_cost_1d_di_campaign_{{ds_nodash}} as
select campaign_id,spu_id,a.module,a.product,case
     when a.marketing_target in (3, 8, 14,15) then '闭环电商广告'
     when a.marketing_target in (13) then '非闭环电商广告'
     when a.marketing_target in (2, 5, 9) then '线索广告'
     when a.marketing_target not in (3, 8, 2, 5, 9, 13, 14,15)
     and a.module in ('效果') then '种草广告'
     when a.module in ('品牌', '薯条', '品合') then '整体'
     else null
   end as market_target,a.marketing_target,a.optimize_target,min(date_key) as cam_dt,sum(case when date_key='{{ds}}' then cash_income_amt else 0 end) as cam_income_amt
from redcdm.dws_ads_note_spu_product_income_detail_td_df a
left join 
redcdm.dim_ads_creativity_core_df b on a.creativity_id = b.creativity_id and b.dtm=max_dtm('redcdm.dim_ads_creativity_core_df')
where a.dtm='{{ds_nodash}}' and a.cash_income_amt<>0
and campaign_id is not null and spu_id is not null and a.module in ('效果')
group by campaign_id,spu_id,a.module,a.product,a.marketing_target,a.optimize_target,
case
     when a.marketing_target in (3, 8, 14,15) then '闭环电商广告'
     when a.marketing_target in (13) then '非闭环电商广告'
     when a.marketing_target in (2, 5, 9) then '线索广告'
     when a.marketing_target not in (3, 8, 2, 5, 9, 13, 14,15)
     and a.module in ('效果') then '种草广告'
     when a.module in ('品牌', '薯条', '品合') then '整体'
     else null
   end 
;

drop table if exists temp.temp_dm_ads_pub_spu_cvr_cost_1d_di_note_{{ds_nodash}};

create table
  temp.temp_dm_ads_pub_spu_cvr_cost_1d_di_note_{{ds_nodash}} as
select 1 as tag,note_id,spu_id,a.module,a.product,a.marketing_target,
a.optimize_target,
case
     when a.marketing_target in (3, 8, 14,15) then '闭环电商广告'
     when a.marketing_target in (13) then '非闭环电商广告'
     when a.marketing_target in (2, 5, 9) then '线索广告'
     when a.marketing_target not in (3, 8, 2, 5, 9, 13, 14,15)
     and a.module in ('效果') then '种草广告'
     when a.module in ('品牌', '薯条', '品合') then '整体'
     else null
   end as market_target,min(date_key) as note_dt,sum(case when date_key='{{ds}}' then cash_income_amt else 0 end) as note_income_amt
from redcdm.dws_ads_note_spu_product_income_detail_td_df a
where a.dtm='{{ds_nodash}}' and a.cash_income_amt<>0 and  note_id is not null and module in ('效果')
 and spu_id is not null
group by note_id,spu_id,a.module,a.product,a.marketing_target,
a.optimize_target,
case
     when a.marketing_target in (3, 8, 14,15) then '闭环电商广告'
     when a.marketing_target in (13) then '非闭环电商广告'
     when a.marketing_target in (2, 5, 9) then '线索广告'
     when a.marketing_target not in (3, 8, 2, 5, 9, 13, 14,15)
     and a.module in ('效果') then '种草广告'
     when a.module in ('品牌', '薯条', '品合') then '整体'
     else null
   end 
union all 
select 2 as tag,note_id,spu_id,a.module,a.product,-911 as marketing_target,
-911 as optimize_target,
case
     when a.marketing_target in (3, 8, 14,15) then '闭环电商广告'
     when a.marketing_target in (13) then '非闭环电商广告'
     when a.marketing_target in (2, 5, 9) then '线索广告'
     when a.marketing_target not in (3, 8, 2, 5, 9, 13, 14,15)
     and a.module in ('效果') then '种草广告'
     when a.module in ('品牌', '薯条', '品合') then '整体'
     else null
   end as market_target,min(date_key) as note_dt,sum(case when date_key='{{ds}}' then cash_income_amt else 0 end) as note_income_amt
from redcdm.dws_ads_note_spu_product_income_detail_td_df a
where a.dtm='{{ds_nodash}}' and a.cash_income_amt<>0 and  note_id is not null and module in ('效果')
 and spu_id is not null
group by note_id,spu_id,a.module,a.product,
case
     when a.marketing_target in (3, 8, 14,15) then '闭环电商广告'
     when a.marketing_target in (13) then '非闭环电商广告'
     when a.marketing_target in (2, 5, 9) then '线索广告'
     when a.marketing_target not in (3, 8, 2, 5, 9, 13, 14,15)
     and a.module in ('效果') then '种草广告'
     when a.module in ('品牌', '薯条', '品合') then '整体'
     else null
   end;
-- drop table if exists temp.temp_dm_ads_pub_spu_cvr_cost_1d_di_noteall_{{ds_nodash}};

-- create table
--   temp.temp_dm_ads_pub_spu_cvr_cost_1d_di_noteall_{{ds_nodash}} as
-- select spu_id,count(1) as note_num_6m,count(case when dt='{{ds}}' then 1 else null end ) as new_note_num
-- from 
-- (select discovery_id as note_id,substring(publish_time,1,10) as dt
-- from reddw.dw_soc_discovery_delta_7_day
-- where dtm='{{ds_nodash}}' 
-- and (is_brand = 1 or is_bind = 1 or is_cps_note = 1)
-- and substring(publish_time,1,10)>=add_months('{{ds}}',-6)
-- and substring(publish_time,1,10)<='{{ds}}'
-- )t1 
-- join 
-- (select spu_id,note_id
-- from ads_databank.dim_spu_note_df 
-- where dtm=max_dtm('ads_databank.dim_spu_note_df' ) 
-- group by 1,2
-- )t2 
-- on t1.note_id = t2.note_id
-- group by spu_id;

drop table
  if exists temp.temp_dm_ads_pub_spu_cvr_cost_1d_di_eng_{{ds_nodash}};

create table
  temp.temp_dm_ads_pub_spu_cvr_cost_1d_di_eng_{{ds_nodash}} as
select
  f_getdate(dtm) as date_key,
  dtm,
  creativity_id,
  t1.note_id,
  spu_id,
  module,
  product,
  case
    when marketing_target in (3, 8, 14, 15) then '闭环电商广告'
    when marketing_target in (13) then '非闭环电商广告'
    when marketing_target in (2, 5, 9) then '线索广告'
    when marketing_target not in (3, 8, 2, 5, 9, 13, 14, 15)
    and module in ('效果') then '种草广告'
    when module in ('品牌', '薯条', '品合') then '整体'
    else null
  end as market_target,
  marketing_target,
  optimize_target,
  sum(unique_imp_cnt) as imp_cnt,
  sum(unique_click_cnt) as click_cnt,
  sum(cost_amount) as income_amt,
  sum(
    case
      when marketing_target in (3, 8, 15) then click_rgmv_7d
      else 0
    end
  ) as click_rgmv_7d,
  sum(
    case
      when marketing_target in (3, 8, 15) then coalesce(purchase_order_num, 0) + coalesce(mini_purchase_order_num, 0)
      else 0
    end
  ) as total_purchase_order_num,
  sum(
    case
      when marketing_target in (3, 8, 15) then cost_amount
      else 0
    end
  ) as ecm_income_amt
from
  (
    select
      dtm,
      creativity_id,
      optimize_target,
      case
        when ads_material_type = 'post' then ads_material_id
        else null
      end as note_id,
      unique_imp_cnt,
      unique_click_cnt,
      cost_amount,
      marketing_target,
      click_rgmv_7d,
      purchase_order_num,
      mini_purchase_order_num,
      module,
      product
    from
      redcdm.dm_ads_rtb_creativity_1d_di
    where
      dtm >= '20220901'
      and dtm <= '{{ds_nodash}}'
  ) t1
  left join (
    --互动看算法+人工
    select
      note_id,
      spu_id
    from
      ads_databank.dim_spu_note_df
    where
      dtm = greatest('{{ds_nodash}}', '20231210') --and bind_type = 2
    group by
      1,
      2
  ) spu_note_engage on spu_note_engage.note_id = t1.note_id
group by
  dtm,
  creativity_id,
  t1.note_id,
  spu_id,
  module,
  product,
  marketing_target,
  optimize_target,
  case
    when marketing_target in (3, 8, 14, 15) then '闭环电商广告'
    when marketing_target in (13) then '非闭环电商广告'
    when marketing_target in (2, 5, 9) then '线索广告'
    when marketing_target not in (3, 8, 2, 5, 9, 13, 14, 15)
    and module in ('效果') then '种草广告'
    when module in ('品牌', '薯条', '品合') then '整体'
    else null
  end;
drop table if exists temp.temp_dm_ads_pub_spu_cvr_cost_1d_di_spu_eng_{{ds_nodash}};

create table
  temp.temp_dm_ads_pub_spu_cvr_cost_1d_di_spu_eng_{{ds_nodash}} as
select dtm,
  t1.creativity_id,
  module,
  product,
  market_target,
  marketing_target,
  optimize_target,
  note_id,
  t2.spu_id,
  imp_cnt,
  click_cnt,
  income_amt,
  click_rgmv_7d,
  total_purchase_order_num,
  ecm_income_amt
from 
(select *
  from
temp.temp_dm_ads_pub_spu_cvr_cost_1d_di_eng_{{ds_nodash}}
where spu_id is null --未人工或算法绑定spu
)t1 
join 
(select
  a.element_id,
  a.main_spu_id as spu_id
from
  redods.ods_shequ_feed_ads_tb_material_bind_spu_df a
where
  a.dtm =greatest('20231010','{{ds_nodash}}')
  and a.del = 0
group by
  1,
  2)t2 
on t1.creativity_id=t2.element_id
union all 
select dtm,
  creativity_id,
  module,
  product,
  market_target,
  marketing_target,
  optimize_target,
  note_id,
  spu_id,
  imp_cnt,
  click_cnt,
  income_amt,
  click_rgmv_7d,
  total_purchase_order_num,
  ecm_income_amt
from 
temp.temp_dm_ads_pub_spu_cvr_cost_1d_di_eng_{{ds_nodash}}
where spu_id is not null --未人工或算法绑定spu
;
drop table if exists temp.temp_dm_ads_pub_spu_cvr_cost_1d_di_budget_{{ds_nodash}};

create table
  temp.temp_dm_ads_pub_spu_cvr_cost_1d_di_budget_{{ds_nodash}} as
select
  t2.dtm,
  t1.spu_id,
  '闭环电商广告' as market_target,
   -911 as marketing_target,
   -911 as optimize_target,
  '效果' as module,
  '整体' as product,
  sum(cost_special_campaign) as cost_special_campaign,
  sum(min_campaign_budget) as min_campaign_budget
from
  (
    select
      spu_id,
      campaign_id
    from
      (
        select
          campaign_id,
          ads_material_id as note_id
        from
          redcdm.dim_ads_creativity_core_df
        where
          dtm = '{{ds_nodash}}'
          and marketing_target in (3, 8, 14, 15)
          and ads_material_type = 'post'
        group by
          campaign_id,
          ads_material_id
      ) t1
      left join (
        --互动看算法+人工
        select
          note_id,
          spu_id
        from
          ads_databank.dim_spu_note_df
        where
          dtm = greatest('{{ds_nodash}}', '20231210') --and bind_type = 2
        group by
          1,
          2
      ) spu_note_engage on spu_note_engage.note_id = t1.note_id
    where
      spu_id is not null
    group by
      spu_id,
      campaign_id
  ) t1
  left join (
    select
      dtm,
      campaign_id,
      cost_special_campaign,
      min_campaign_budget
    from
      redapp.app_ads_overall_budget_1d_di
    where
      dtm >= '20230101'
      and dtm <= '{{ds_nodash}}'
      and granularity = '计划粒度'
  ) t2 on t1.campaign_id = t2.campaign_id
group by
  t2.dtm,
  t1.spu_id;
drop table if exists temp.temp_dm_ads_pub_spu_account_{{ds_nodash}};

create table
  temp.temp_dm_ads_pub_spu_account_{{ds_nodash}} as
  select date_key,
    spu.spu_id,
    brand_account.brand_account_id,
    brand_account_name,
    operator_name ,
    direct_sales_name ,
    operator_code ,
    direct_sales_code ,
    direct_sales_dept1_name,
    direct_sales_dept2_name,
    direct_sales_dept3_name,
    first_industry_name,
    second_industry_name,
    planner_name
  from 
  (select
      spu_id,
      brand_id
    from
      ads_databank.dim_spu_df
    where
      dtm = greatest('{{ds_nodash}}', '20231105')
    group by spu_id,
      brand_id
  )spu 
  left join 
 ( --brand_id和brand_account_id映射关系
  select date_key,
    brandz_id as brand_id
    ,brand_user_id as brand_account_id
  from
        (
            select base.date_key,
              brandz_id
              ,brand_user_id
              ,coalesce(cash_cost,0) as cash_cost
              ,row_number() over(partition by base.date_key,brandz_id order by cash_cost desc) as rk
            from
            (select a.date_key,
              a.brandz_id
              ,a.brand_user_id
              ,sum(cash_cost) as cash_cost
            from
            (select dt as date_key,
              brandz_id
              ,brand_user_id
            from 
            (
                select
                  brandz_id
                  ,brand_user_id
                from redods.ods_ads_crm_crm_account_brandz_info
                -- where dtm = '{{ds_nodash}}'
                where dtm = greatest('{{ds_nodash}}', '20231105')
                and state = 1
              ) a 
              left join 
              (select dt
              from redcdm.dim_ads_date_df 
              where dtm='all' and dt>='2023-01-01' and dt<='{{ds}}'
              )dt 
              on 1=1
            )a 
            left join
            (
              select date_key,
                brand_user_id as brand_account_id
                ,sum(cash_cost) as cash_cost
              from reddm.dm_ads_crm_advertiser_income_wide_day
              where dtm = greatest('{{ds_nodash}}', '20231105')
              and module in ('品牌','效果','薯条')
              and date_key>='2022-12-01'
              group by 1,2
            ) b
            on a.brand_user_id = b.brand_account_id
          where b.date_key > f_getdate(a.date_key,-30) and b.date_key <= a.date_key  --近30日流水最大
          group by 1,2,3
          )base  
        ) a 
    where rk = 1
    )brand_account 
    on brand_account.brand_id = spu.brand_id
  left join 
  (select brand_account_id,
      direct_sales_dept1_name,
      direct_sales_dept2_name,
      direct_sales_dept3_name,
      direct_sales_name,
      operator_name,
      operator_code ,
      direct_sales_code,
      brand_user_name as brand_account_name,
      first_industry_name,
      second_industry_name,
      planner_name
  from redapp.app_ads_insight_industry_account_df
  where dtm= greatest('{{ds_nodash}}', '20231105')
  )account 
  on account.brand_account_id=brand_account.brand_account_id
;
drop table if exists temp.temp_dm_ads_pub_spu_agent_account_{{ds_nodash}};

create table
  temp.temp_dm_ads_pub_spu_agent_account_{{ds_nodash}} as
select spu_id
    ,a.agent_user_id
    ,a.agent_user_name
    ,t5.name as channel_sales_name
    ,t6.name as channel_operator_name
  from
        (
            select
              spu_id,
              agent_user_id,
              agent_user_name,
              -- channel_sales_name,
              -- channel_operator_name
              coalesce(cash_cost,0) as cash_cost
              ,row_number() over(partition by spu_id order by cash_cost desc) as rk
            from
            (
              select
                spu_id,
                agent_user_id,
                agent_user_name,
                -- channel_sales_name,
                -- channel_operator_name
                sum(cash_cost) as cash_cost
              from redcdm.dm_ads_spu_account_detail_1d_di
              where dtm > f_getdate('{{ds_nodash}}',-30) and dtm <= '{{ds_nodash}}'  --近30日流水最大
              and module in ('品牌','效果','薯条')
              and coalesce(agent_user_id ,'')<>''
              and cash_cost>0
              group by 1,2,3
            ) b
        ) a 
        --left join 
        --ads_data_crm.dw_ads_crm_agent_user_hook_relation_df b 20240102替换
        left join 
        (SELECT
          agent_user_id,
          brand_channel_code,
          rtb_channel_code,
          primary_channel_code,
          channel_op_code
        FROM
          reddim.dim_ads_crm_agent_day
        WHERE
          dtm=greatest('20231225','{{ds_nodash}}')
          ) b
        on a.agent_user_id=b.agent_user_id 
        left join 
        (select code,
         name
        from
          (select code,
            concat(red_name, '(', true_name, ')') as name,
            row_number()over(partition by  red_name order by create_time desc) as rn --兜底历史脏数据56条署名重复
          from redods.ods_ads_crm_ads_crm_user 
          where dtm=greatest('20231105','{{ds_nodash}}')
          )a 
          where rn=1
        )t5
        on t5.code = coalesce(b.primary_channel_code,b.rtb_channel_code)
        left join 
        (select code,
         name
        from
          (select code,
            concat(red_name, '(', true_name, ')') as name,
            row_number()over(partition by  red_name order by create_time desc) as rn --兜底历史脏数据56条署名重复
          from redods.ods_ads_crm_ads_crm_user 
          where dtm=greatest('20231105','{{ds_nodash}}')
          )a 
          where rn=1
        )t6
        on t6.code = b.channel_op_code
    where rk = 1;  
insert overwrite table redcdm.dm_ads_pub_spu_product_td_df   partition( dtm = '{{ds_nodash}}')
  SELECT
  base.date_key,
  base.module,
  case when base.product='信息流' then '竞价-信息流' when base.product='搜索' then '竞价-搜索' 
  when base.product='视频内流' then '竞价-视频内流' else base.product end as product,
  base.market_target as marketing_target,
  base.spu_id,
  brand_account_id,
  agent_user_id,
  agent_user_name,
  channel_sales_name,
  case when cspu.spu_id is not null then 1 else 0 end  as is_cspu,
  campaign_cnt,
  note_cnt,
  income_cam_cnt as income_campaign_cnt,
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
  marketing_target as marketing_target_id,
  avg_cash_income_amt,
  avg_bind_cash_income_amt,
  avg_bind_splash_cash_income_amt,
  avg_income_amt,
  avg_bind_income_amt,
  data_type,
  optimize_target
from
(select 1 as data_type,
  date_key,
  module,
  product,
  marketing_target,
  market_target,
  spu_id,
  optimize_target,
  sum(campaign_cnt) as campaign_cnt,
  sum(note_cnt) as note_cnt,
  sum(income_cam_cnt) as income_cam_cnt,
  sum(income_note_cnt) as income_note_cnt,
  sum(imp_cnt) as imp_cnt,
  sum(click_cnt) as click_cnt,
  sum(income_amt) as income_amt,
  sum(click_rgmv_7d) as click_rgmv_7d,
  sum(total_purchase_order_num) as total_purchase_order_num,
  sum(ecm_income_amt) as ecm_income_amt,
  sum(cost_special_campaign) as cost_special_campaign,
  sum(min_campaign_budget) as min_campaign_budget,
  sum(new_note_num) as new_note_num,
  0 as avg_cash_income_amt,
  0 as avg_bind_cash_income_amt,
  0 as avg_bind_splash_cash_income_amt,
  0 as avg_income_amt,
  0 as avg_bind_income_amt
  from
  (
    select
      coalesce(t33.date_key, t4.date_key) as date_key,
      coalesce(t33.module, t4.module) as module,
      coalesce(t33.product, t4.product) as product,
      coalesce(t33.marketing_target, t4.marketing_target) as marketing_target,
      coalesce(t33.market_target, t4.market_target) as market_target,
      coalesce(t33.spu_id, t4.spu_id) as spu_id,
      coalesce(t33.optimize_target, t4.optimize_target) as optimize_target,
      campaign_cnt,
      note_cnt,
      income_cam_cnt,
      income_note_cnt,
      imp_cnt,
      click_cnt,
      income_amt,
      click_rgmv_7d,
      total_purchase_order_num,
      ecm_income_amt,
      0 as cost_special_campaign,
      0 as min_campaign_budget,
      0 as new_note_num
    from
      (
        select
         coalesce(t22.date_key, t3.date_key) as date_key,
          coalesce(t22.module, t3.module) as module,
          coalesce(t22.product, t3.product) as product,
          coalesce(t22.marketing_target, t3.marketing_target) as marketing_target,
          coalesce(t22.market_target, t3.market_target) as market_target,
          coalesce(t22.spu_id, t3.spu_id) as spu_id,
          coalesce(t22.optimize_target, t3.optimize_target) as optimize_target,
          campaign_cnt,
          note_cnt,
          income_cam_cnt,
          income_note_cnt
        from
          (
            select
              coalesce(t11.date_key, t2.date_key) as date_key,
              coalesce(t11.module, t2.module) as module,
              coalesce(t11.product, t2.product) as product,
              coalesce(t11.marketing_target, t2.marketing_target) as marketing_target,
              coalesce(t11.market_target, t2.market_target) as market_target,
              coalesce(t11.spu_id, t2.spu_id) as spu_id,
              coalesce(t11.optimize_target, t2.optimize_target) as optimize_target,
              campaign_cnt,
              note_cnt,
              income_cam_cnt
            from
              (
                select
                  coalesce(t1.date_key, t2.date_key) as date_key,
                  coalesce(t1.module, t2.module) as module,
                  coalesce(t1.product, t2.product) as product,
                  coalesce(t1.marketing_target, t2.marketing_target) as marketing_target,
                  coalesce(t1.market_target, t2.market_target) as market_target,
                  coalesce(t1.spu_id, t2.spu_id) as spu_id,
                  coalesce(t1.optimize_target, t2.optimize_target) as optimize_target,
                  campaign_cnt,
                  note_cnt
                from
                  (
                    select
                      spu_id,
                      module,
                      '整体' as product,
                      marketing_target,
                      market_target,
                      cam_dt as date_key,
                      optimize_target,
                      count(distinct campaign_id) as campaign_cnt
                    from
                      temp.temp_dm_ads_pub_spu_cvr_cost_1d_di_campaign_{{ds_nodash}}
                    group by
                      spu_id,
                      module,
                      market_target,
                      --product,
                      marketing_target,
                      cam_dt,
                      optimize_target
                  ) t1
                  full outer join (
                    select
                      spu_id,
                      module,
                      '整体' as  product,
                      marketing_target,
                      market_target,
                      note_dt as date_key,
                      optimize_target,
                      count(distinct note_id) as note_cnt
                    from
                      temp.temp_dm_ads_pub_spu_cvr_cost_1d_di_note_{{ds_nodash}}
                    where tag = 1
                    group by
                      spu_id,
                      module,
                      --product,
                      marketing_target,
                      market_target,
                      note_dt,
                      optimize_target
                    union all 
                    select
                      spu_id,
                      module,
                      '整体' as  product,
                      -911 as marketing_target,
                      market_target,
                      note_dt as date_key,
                      -911 as optimize_target,
                      count(distinct note_id) as note_cnt
                    from
                      temp.temp_dm_ads_pub_spu_cvr_cost_1d_di_note_{{ds_nodash}}
                    where tag = 2
                    group by
                      spu_id,
                      module,
                      --product,
                      --marketing_target,
                      market_target,
                      note_dt
                  ) t2 on t1.date_key = t2.date_key
                  and t1.spu_id = t2.spu_id
                  and t1.module = t2.module
                  and t1.product = t2.product
                  and t1.marketing_target = t2.marketing_target
                  and t1.market_target = t2.market_target
                  and t1.optimize_target = t2.optimize_target
              ) t11
              full outer join (
                select
                  date_key,
                  spu_id,
                  a.module,
                  '整体' as product,
                  a.marketing_target,
                  a.optimize_target,
                  case
                    when a.marketing_target in (3, 8, 14, 15) then '闭环电商广告'
                    when a.marketing_target in (13) then '非闭环电商广告'
                    when a.marketing_target in (2, 5, 9) then '线索广告'
                    when a.marketing_target not in (3, 8, 2, 5, 9, 13, 14, 15)
                    and a.module in ('效果') then '种草广告'
                    when a.module in ('品牌', '薯条', '品合') then '整体'
                    else null
                  end as market_target,
                  count(
                    distinct case
                      when cash_income_amt > 0 then campaign_id
                    end
                  ) as income_cam_cnt
                from
                  redcdm.dws_ads_note_spu_product_income_detail_td_df a
                  left join redcdm.dim_ads_creativity_core_df b on a.creativity_id = b.creativity_id
                  and b.dtm = max_dtm('redcdm.dim_ads_creativity_core_df')
                where
                  a.dtm = '{{ds_nodash}}'
                  and a.cash_income_amt <> 0
                  and campaign_id is not null
                  and spu_id is not null
                  and a.module='效果'
                group by
                  date_key,
                  spu_id,
                  a.module,
                  a.marketing_target,
                  a.optimize_target,
                 -- a.product,
                  case
                    when a.marketing_target in (3, 8, 14, 15) then '闭环电商广告'
                    when a.marketing_target in (13) then '非闭环电商广告'
                    when a.marketing_target in (2, 5, 9) then '线索广告'
                    when a.marketing_target not in (3, 8, 2, 5, 9, 13, 14, 15)
                    and a.module in ('效果') then '种草广告'
                    when a.module in ('品牌', '薯条', '品合') then '整体'
                    else null
                  end
              ) t2 on t11.date_key = t2.date_key
              and t11.spu_id = t2.spu_id
              and t11.module = t2.module
              and t11.product = t2.product
              and t11.marketing_target = t2.marketing_target
              and t11.market_target = t2.market_target
              and t11.optimize_target=t2.optimize_target
          ) t22
          full outer join (
            select
              spu_id,
              a.module,
              '整体' as product,
              a.marketing_target,
              a.optimize_target,
              case
                when a.marketing_target in (3, 8, 14, 15) then '闭环电商广告'
                when a.marketing_target in (13) then '非闭环电商广告'
                when a.marketing_target in (2, 5, 9) then '线索广告'
                when a.marketing_target not in (3, 8, 2, 5, 9, 13, 14, 15)
                and a.module in ('效果') then '种草广告'
                when a.module in ('品牌', '薯条', '品合') then '整体'
                else null
              end as market_target,
              date_key,
              count(
                distinct case
                  when cash_income_amt > 0 then note_id
                end
              ) as income_note_cnt
            from
              redcdm.dws_ads_note_spu_product_income_detail_td_df a
            where
              a.dtm = '{{ds_nodash}}'
              and a.cash_income_amt <> 0
              and note_id is not null
              and spu_id is not null
              and module='效果'
            group by
              date_key,
              spu_id,
              a.module,
              a.marketing_target,
              a.optimize_target,
              --a.product,
              case
                when a.marketing_target in (3, 8, 14, 15) then '闭环电商广告'
                when a.marketing_target in (13) then '非闭环电商广告'
                when a.marketing_target in (2, 5, 9) then '线索广告'
                when a.marketing_target not in (3, 8, 2, 5, 9, 13, 14, 15)
                and a.module in ('效果') then '种草广告'
                when a.module in ('品牌', '薯条', '品合') then '整体'
                else null
              end
              union all 
              select
              spu_id,
              a.module,
              '整体' as product,
              -911 as marketing_target,
              -911 as optimize_target,
              case
                when a.marketing_target in (3, 8, 14, 15) then '闭环电商广告'
                when a.marketing_target in (13) then '非闭环电商广告'
                when a.marketing_target in (2, 5, 9) then '线索广告'
                when a.marketing_target not in (3, 8, 2, 5, 9, 13, 14, 15)
                and a.module in ('效果') then '种草广告'
                when a.module in ('品牌', '薯条', '品合') then '整体'
                else null
              end as market_target,
              date_key,
              count(
                distinct case
                  when cash_income_amt > 0 then note_id
                end
              ) as income_note_cnt
            from
              redcdm.dws_ads_note_spu_product_income_detail_td_df a
            where
              a.dtm = '{{ds_nodash}}'
              and a.cash_income_amt <> 0
              and note_id is not null
              and spu_id is not null
              and module='效果'
            group by
              date_key,
              spu_id,
              a.module,
              --a.marketing_target,
              --a.product,
              case
                when a.marketing_target in (3, 8, 14, 15) then '闭环电商广告'
                when a.marketing_target in (13) then '非闭环电商广告'
                when a.marketing_target in (2, 5, 9) then '线索广告'
                when a.marketing_target not in (3, 8, 2, 5, 9, 13, 14, 15)
                and a.module in ('效果') then '种草广告'
                when a.module in ('品牌', '薯条', '品合') then '整体'
                else null
              end
          ) t3 on t22.date_key = t3.date_key
          and t22.spu_id = t3.spu_id
          and t22.module = t3.module
          and t22.product = t3.product
          and t22.marketing_target = t3.marketing_target
          and t22.market_target = t3.market_target
          and t22.optimize_target=t3.optimize_target
      ) t33
      full outer join (
        select
          f_getdate(dtm) as date_key,
          module,
          product,
          marketing_target,
          market_target,
          optimize_target,
          --note_id,
          spu_id,
          sum(imp_cnt) as imp_cnt,
          sum(click_cnt) as click_cnt,
          sum(income_amt) as income_amt,
          sum(click_rgmv_7d) as click_rgmv_7d,
          sum(total_purchase_order_num) as total_purchase_order_num,
          sum(ecm_income_amt) as ecm_income_amt
        from
          temp.temp_dm_ads_pub_spu_cvr_cost_1d_di_spu_eng_{{ds_nodash}}
        group by
          f_getdate(dtm),
          module,
          product,
          market_target,
          marketing_target,
          optimize_target,
          --note_id,
          spu_id
      ) t4 on t33.date_key = t4.date_key
      and t33.spu_id = t4.spu_id
      and t33.module = t4.module
      and t33.product = t4.product
      and t33.marketing_target = t4.marketing_target
      and t33.market_target = t4.market_target
      and t33.optimize_target = t4.optimize_target
    union all
    select
      f_getdate(dtm) as date_key,
      module,
      product,
      marketing_target,
      market_target,
      spu_id,
      optimize_target,
      0 as campaign_cnt,
      0 as note_cnt,
      0 as income_cam_cnt,
      0 as income_note_cnt,
      0 as imp_cnt,
      0 as click_cnt,
      0 as income_amt,
      0 as click_rgmv_7d,
      0 as total_purchase_order_num,
      0 as ecm_income_amt,
      cost_special_campaign,
      min_campaign_budget,
      0 as new_note_num
    from
      temp.temp_dm_ads_pub_spu_cvr_cost_1d_di_budget_{{ds_nodash}}
    union all
    select
      dt as date_key,
      '整体' as module,
      '整体' as product,
      -911 as marketing_target,
      '整体' as market_target,
      spu_id,
      -911 as optimize_target,
      0 as campaign_cnt,
      0 as note_cnt,
      0 as income_cam_cnt,
      0 as income_note_cnt,
      0 as imp_cnt,
      0 as click_cnt,
      0 as income_amt,
      0 as click_rgmv_7d,
      0 as total_purchase_order_num,
      0 as ecm_income_amt,
      0 as cost_special_campaign,
      0 as min_campaign_budget,
      count(1) as new_note_num
    from
      (
        select
          discovery_id as note_id,
          substring(publish_time, 1, 10) as dt
        from
          reddw.dw_soc_discovery_delta_7_day
        where
          dtm = '{{ds_nodash}}'
          and (
            is_brand = 1
            or is_bind = 1
            or is_cps_note = 1
          )
          and substring(publish_time, 1, 10) >= '2022-07-01'
      ) t1
      join (
        select
          spu_id,
          note_id
        from
          ads_databank.dim_spu_note_df
        where
          dtm = max_dtm('ads_databank.dim_spu_note_df')
        group by
          1,
          2
      ) t2 on t1.note_id = t2.note_id
    group by
      spu_id,
      dt
  )t1
  group by  date_key,
  module,
  product,
  marketing_target,
  market_target,
  spu_id
union all 
select  2 as data_type,
      date_key,
      module,
      product,
      marketing_target,
      case
        when a.marketing_target in (3, 8, 14, 15) then '闭环电商广告'
        when a.marketing_target in (13) then '非闭环电商广告'
        when a.marketing_target in (2, 5, 9) then '线索广告'
        when a.marketing_target not in (3, 8, 2, 5, 9, 13, 14, 15)
        and a.module in ('效果') then '种草广告'
        when a.module in ('品牌', '薯条', '品合') then '整体'
        else null
      end as market_target,
      a.spu_id,
      a.optimize_target,
      0 as campaign_cnt,
      0 as note_cnt,
      0 as income_cam_cnt,
      0 as income_note_cnt,
      0 as imp_cnt,
      0 as click_cnt,
      0 as income_amt,
      0 as click_rgmv_7d,
      0 as total_purchase_order_num,
      0 as ecm_income_amt,
      0 as cost_special_campaign,
      0 as min_campaign_budget,
      0 as new_note_num,
      sum(cash_income_amt) as avg_cash_income_amt,
      sum(case when bind_type=2 then cash_income_amt else 0 end) as avg_bind_cash_income_amt,
      sum(case when product='开屏' and bind_type=2 then cash_income_amt else 0 end) as avg_bind_splash_cash_income_amt,
      sum(income_amt) as avg_income_amt,
      sum(case when bind_type=2 then income_amt else 0 end) as avg_bind_income_amt
    from redcdm.dws_ads_note_spu_product_income_detail_td_df a
    left join 
    (SELECT
      spu_id
    FROM
      ads_databank.dim_spu_note_df
    WHERE
      dtm = '{{ds_nodash}}'
     -- and commercial_code = '108accd165294f44b4b20c2436319f2e'
     and  commercial_code ='8101b7b3cf854e0585a02431c5812a2a' --cspu
    group by 1
    )t2 
    on a.spu_id = t2.spu_id
    where
      a.dtm = '{{ds_nodash}}' and a.spu_id is not null and t2.spu_id is null --非cspu
      and date_key>='2022-09-01' and date_key<='{{ds}}'
group by date_key,
      module,
      product,
      marketing_target,
      a.optimize_target,
      case
        when a.marketing_target in (3, 8, 14, 15) then '闭环电商广告'
        when a.marketing_target in (13) then '非闭环电商广告'
        when a.marketing_target in (2, 5, 9) then '线索广告'
        when a.marketing_target not in (3, 8, 2, 5, 9, 13, 14, 15)
        and a.module in ('效果') then '种草广告'
        when a.module in ('品牌', '薯条', '品合') then '整体'
        else null
      end ,
      a.spu_id
union all 
--cspu 收入
select ----绑定spu的流水分摊 
      2 as data_type,
      date_key,
      module,
      product,
      marketing_target,
      case
        when marketing_target in (3, 8, 14, 15) then '闭环电商广告'
        when marketing_target in (13) then '非闭环电商广告'
        when marketing_target in (2, 5, 9) then '线索广告'
        when marketing_target not in (3, 8, 2, 5, 9, 13, 14, 15)
        and module in ('效果') then '种草广告'
        when module in ('品牌', '薯条', '品合') then '整体'
        else null
      end as market_target,
      spu_id,
      optimize_target,
      0 as campaign_cnt,
      0 as note_cnt,
      0 as income_cam_cnt,
      0 as income_note_cnt,
      0 as imp_cnt,
      0 as click_cnt,
      0 as income_amt,
      0 as click_rgmv_7d,
      0 as total_purchase_order_num,
      0 as ecm_income_amt,
      0 as cost_special_campaign,
      0 as min_campaign_budget,
      0 as new_note_num,
      sum(cash_income_amt) as avg_cash_income_amt,
      sum(cash_income_amt) as avg_bind_cash_income_amt,
      sum(case when product='开屏' then cash_income_amt else 0 end) as avg_bind_splash_cash_income_amt,
      sum(income_amt) as avg_income_amt,
      sum(income_amt)  as avg_bind_income_amt
    from
     redcdm.dws_ads_cspu_income_td_df
    where
      dtm = max_dtm('redcdm.dws_ads_cspu_income_td_df') and spu_id is not null
      and date_key>='2022-09-01' and date_key<='{{ds}}'
    group by date_key,
      module,
      product,
      marketing_target,
      optimize_target,
      case
        when marketing_target in (3, 8, 14, 15) then '闭环电商广告'
        when marketing_target in (13) then '非闭环电商广告'
        when marketing_target in (2, 5, 9) then '线索广告'
        when marketing_target not in (3, 8, 2, 5, 9, 13, 14, 15)
        and module in ('效果') then '种草广告'
        when module in ('品牌', '薯条', '品合') then '整体'
        else null
      end ,
      spu_id  
  ) base
  left join temp.temp_dm_ads_pub_spu_account_{{ds_nodash}} b on base.spu_id = b.spu_id and base.date_key = b.date_key
  left join temp.temp_dm_ads_pub_spu_agent_account_{{ds_nodash}} c on base.spu_id = c.spu_id
  left join 
    ( SELECT
        spu_id
    FROM ads_databank.dim_spu_note_df
    WHERE
        dtm = '{{ds_nodash}}'
        and commercial_code = '108accd165294f44b4b20c2436319f2e'
    group by 1
    )cspu 
    on cspu.spu_id = base.spu_id