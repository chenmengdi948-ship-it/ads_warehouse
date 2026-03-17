
    -- ************************************************
    -- Author: chenmengdi
    -- CreateTime:2025-01-13T17:27:43+08:00
    -- Update: Task Update Description
    -- ************************************************
--计算每天spu的指标情况
drop table if exists temp.temp_dm_ads_pub_spu_product_cvr_cost_td_df_new_{{ds_nodash}}_60d ;
create table if not exists temp.temp_dm_ads_pub_spu_product_cvr_cost_td_df_new_{{ds_nodash}}_60d   as
select t1.date_key,
  date_add(t1.date_key,60) as date_key_60d,
  t1.spu_id,
  
  --module,
  soc_read_feed_num,
  soc_imp_num,
  soc_click_num,--阅读（+额外加二级类目的）
  query_cnt,--搜索
  ti_user_num,
  a_user_num,
  i_user_num,
  s_user_num,
  p_user_num,--aips
  rtb_imp_cnt,--曝光
  rtb_click_cnt,--点击
  rtb_income_amt,--消耗
  --小红星进店
  taobao_ad_click_user_num,--小红星广告曝光
  taobao_ad_ads_cash_income_amt,
  taobao_ad_ads_income_amt,
  taobao_ad_third_active_user_num,
  cash_cost,
  taobao_rgmv_1m,
  tianmao_rgmv_1m,
  jd_rgmv_1m ,
  dou_rgmv_1m ,
  rgmv_1m,
  dgmv,
  buyer_cnt,
  deal_gmv,
  process_taobao_rgmv,
  process_tianmao_rgmv,
  process_jd_rgmv,
  process_dou_rgmv ,
  process_taobao_sales_num,
  process_tianmao_sales_num,
  process_jd_sales_num,
  process_dou_sales_num,
  process_propose_sales_num,
  rtb_double_imp_cnt,--曝光
  rtb_double_click_cnt--点击
from 
(select date_key,
  spu_id,
  --module,--brand_account_id,brand_account_name,

--需关联末级类目对应指标情况+t-30日对应指标情况
--gmv
sum(soc_read_feed_num) as soc_read_feed_num,
sum(soc_imp_num) as soc_imp_num,
sum(soc_click_num) as soc_click_num,--阅读（+额外加二级类目的）
sum(query_cnt) as query_cnt,--搜索
sum(ti_user_num) as ti_user_num,
sum(a_user_num) as a_user_num,
sum(i_user_num) as i_user_num,
sum(s_user_num) as s_user_num,
sum(p_user_num) as p_user_num,--aips
sum(ads_imp_cnt) as rtb_imp_cnt,--曝光
sum(ads_click_cnt) as rtb_click_cnt,--点击
sum(ads_income_amt) as rtb_income_amt,--消耗
--小红星进店
sum(case when module <>'品合' then taobao_ad_click_user_num else 0 end) as taobao_ad_click_user_num,--小红星广告曝光
sum(case when module <>'品合' then taobao_ad_ads_cash_income_amt else 0 end) as taobao_ad_ads_cash_income_amt,
sum(case when module <>'品合' then taobao_ad_ads_income_amt else 0 end) as taobao_ad_ads_income_amt,
sum(case when module <>'品合' then taobao_ad_third_active_user_num  else 0 end) as taobao_ad_third_active_user_num  ,--小红星消耗
sum(case when module <>'品合' then cash_cost else 0 end) as cash_cost,
sum(case when product<>'竞价-视频内流' then ads_imp_cnt else 0 end) as rtb_double_imp_cnt,--曝光
sum(case when product<>'竞价-视频内流' then ads_click_cnt else 0 end) as rtb_double_click_cnt--点击
from redcdm.dm_ads_pub_spu_product_cvr_cost_td_df 
where dtm='{{ds_nodash}}'
group by date_key,
  spu_id
)t1 

  left join 
  (select stat_month,
    spu_id,
    coalesce(module,'整体') as module,
    taobao_rgmv_1m,
    tianmao_rgmv_1m,
    jd_rgmv_1m ,
    dou_rgmv_1m ,
    rgmv_1m,
    date_key,
    dgmv,
    buyer_cnt,
    -- dim_all_rgmv,
    -- dim_cash_income_amt_mtd,
    deal_gmv,
    process_taobao_rgmv,
    process_tianmao_rgmv,
    process_jd_rgmv,
    process_dou_rgmv ,
    process_taobao_sales_num,
    process_tianmao_sales_num,
    process_jd_sales_num,
    process_dou_sales_num,
    process_propose_sales_num
  from redapp.app_ads_insight_ecm_spu_gmv_df
  where dtm= '{{ds_nodash}}'
  )t4
  on t4.spu_id = t1.spu_id and t4.date_key= t1.date_key --and t1.module = t4.module
 ;
  --生成全量spuxdtxdt近30日
drop table if exists temp.temp_dm_ads_pub_spu_product_cvr_cost_td_df_new2_{{ds_nodash}}_60d ;
create table if not exists temp.temp_dm_ads_pub_spu_product_cvr_cost_td_df_new2_{{ds_nodash}}_60d as 
select spu_id,
  dt,
  dt_detail
from
  (select spu_id
  from temp.temp_dm_ads_pub_spu_product_cvr_cost_td_df_new_{{ds_nodash}}_60d 
  group by spu_id
  )t1 
  left join 
  (select t1.dt ,t2.dt as dt_detail
  from 
    (select dt 
    from redcdm.dim_ads_date_df 
    where dtm='all' and dt>='2023-12-01' and dt<='{{ds}}'
    )t1 
    left join 
    (select dt
    from redcdm.dim_ads_date_df 
    where dtm='all' and dt>='2023-12-01' and dt<='{{ds}}'
    )t2 
    on t2.dt>=date_add(t1.dt,-89) and t2.dt<=t1.dt
  )t2 
  on 1=1;
--生成全量spu-每个dt都是近30天累计情况
drop table if exists temp.temp_dm_ads_pub_spu_product_cvr_cost_td_df_new3_{{ds_nodash}}_60d ;
create table if not exists temp.temp_dm_ads_pub_spu_product_cvr_cost_td_df_new3_{{ds_nodash}}_60d   as
select 
  dt as date_key,
-- date_key,
  date_add(t1.dt,60) as date_key_60d,
  t1.spu_id,
  spu.spu_name,
  spu.brand_id,
  spu.brand_name,
  spu.commercial_level,
  spu.commercial_code,
  spu.commercial_name,
  spu.commercial_code1,
  spu.commercial_taxonomy_name1,
  spu.commercial_code2,
  spu.commercial_taxonomy_name2,
  spu.commercial_code3,
  spu.commercial_taxonomy_name3,
  spu.commercial_code4,
  spu.commercial_taxonomy_name4,
  pic_url,
  --module,
  sum(soc_read_feed_num) as soc_read_feed_num,
  sum(soc_imp_num) as soc_imp_num,
  sum(soc_click_num) as soc_click_num,--阅读（+额外加二级类目的）
  sum(query_cnt) as query_cnt,--搜索
  sum(case when t1.dt=t2.date_key then ti_user_num else 0 end) as ti_user_num,
  sum(case when t1.dt=t2.date_key then a_user_num else 0 end) as a_user_num ,
  sum(case when t1.dt=t2.date_key then i_user_num else 0 end) as i_user_num,
  sum(case when t1.dt=t2.date_key then s_user_num else 0 end) as s_user_num,
  sum(case when t1.dt=t2.date_key then p_user_num else 0 end) as p_user_num ,--aips
  sum(case when t1.dt=t2.date_key then coalesce(ti_user_num,0)+coalesce(i_user_num,0) else 0 end) as i_ti_user_num,
  sum(rtb_imp_cnt) as rtb_imp_cnt,--曝光
  sum(rtb_click_cnt) as rtb_click_cnt,--点击
  sum(rtb_income_amt) as rtb_income_amt,--消耗
  sum(rtb_double_imp_cnt) as rtb_double_imp_cnt,--曝光
  sum(rtb_double_click_cnt) as rtb_double_click_cnt,--点击
  --小红星进店
  sum(taobao_ad_click_user_num) as taobao_ad_click_user_num,--小红星广告曝光
  sum(taobao_ad_ads_cash_income_amt) as taobao_ad_ads_cash_income_amt,
  sum(taobao_ad_ads_income_amt) as taobao_ad_ads_income_amt,
  sum(taobao_ad_third_active_user_num) as taobao_ad_third_active_user_num,
  sum(cash_cost) as cash_cost,
  sum(process_taobao_rgmv) as taobao_rgmv_1m,
  sum(process_tianmao_rgmv) as tianmao_rgmv_1m,
  sum(process_jd_rgmv) as jd_rgmv_1m ,
  sum(process_dou_rgmv) as dou_rgmv_1m,
  sum(coalece(process_taobao_rgmv,0)+ coalece(process_tianmao_rgmv) +  coalece(process_jd_rgmv) + coalece(process_dou_rgmv,0)) as external_rgmv_1m,

  sum(process_taobao_sales_num) as taobao_sales_num_1m,
  sum(process_tianmao_sales_num) as tianmao_sales_num_1m,
  sum(process_jd_sales_num) as jd_sales_num_1m ,
  sum(process_dou_sales_num) as dou_sales_num_1m,
  sum(process_propose_sales_num) as propose_sales_num_1m,
  sum(rgmv_1m) as rgmv_1m,
  sum(dgmv) as dgmv,
  sum(buyer_cnt) as buyer_cnt,
  sum(deal_gmv) as deal_gmv,
  --加当日
  sum(case when t1.dt=t2.date_key then soc_read_feed_num else 0 end) as soc_read_feed_num_1d,
  sum(case when t1.dt=t2.date_key then soc_imp_num else 0 end) as soc_imp_num_1d,
  sum(case when t1.dt=t2.date_key then soc_click_num else 0 end) as soc_click_num_1d,--阅读（+额外加二级类目的）
  sum(case when t1.dt=t2.date_key then query_cnt else 0 end) as query_cnt_1d,
  sum(case when t1.dt=t2.date_key then process_taobao_rgmv else 0 end) as taobao_rgmv_1d,
  sum(case when t1.dt=t2.date_key then process_tianmao_rgmv else 0 end) as tianmao_rgmv_1d,
  sum(case when t1.dt=t2.date_key then process_jd_rgmv else 0 end) as jd_rgmv_1d ,
  sum(case when t1.dt=t2.date_key then process_dou_rgmv else 0 end) as dou_rgmv_1d,
  sum(case when t1.dt=t2.date_key then dgmv else 0 end) as dgmv_1d, --aips
  sum(case when t1.dt=t2.date_key then rtb_imp_cnt else 0 end) as rtb_imp_cnt_1d,--曝光
  sum(case when t1.dt=t2.date_key then rtb_click_cnt else 0 end) as rtb_click_cnt_1d,--点击
  sum(case when t1.dt=t2.date_key then rtb_income_amt else 0 end) as rtb_income_amt_1d,--消耗
  sum(case when t1.dt=t2.date_key then taobao_ad_click_user_num else 0 end) as taobao_ad_click_user_num_1d,--小红星广告曝光
  sum(case when t1.dt=t2.date_key then taobao_ad_ads_cash_income_amt else 0 end) as taobao_ad_ads_cash_income_amt_1d,
  sum(case when t1.dt=t2.date_key then taobao_ad_ads_income_amt else 0 end) as taobao_ad_ads_income_amt_1d,
  sum(case when t1.dt=t2.date_key then taobao_ad_third_active_user_num else 0 end) as taobao_ad_third_active_user_num_1d,
  sum(case when t1.dt=t2.date_key then cash_cost else 0 end) as ads_cash_cost_1d,
  max(cash_cost_last_ytd) as cash_cost_last_ytd
from 
  (select spu_id,
    dt,
    dt_detail
  from temp.temp_dm_ads_pub_spu_product_cvr_cost_td_df_new2_{{ds_nodash}}_60d  
  where dt_detail>=date_add(dt,-59) and dt_detail<=dt
  )t1 
  left join temp.temp_dm_ads_pub_spu_product_cvr_cost_td_df_new_{{ds_nodash}}_60d t2 
  on t1.spu_id = t2.spu_id and t1.dt_detail=t2.date_key
  left join 
  --spu类目
  (select spu_id,
    name as spu_name,
    commercial_level,
    commercial_code,
    commercial_name,
    commercial_code1,
    commercial_taxonomy_name1,
    commercial_code2,
    commercial_taxonomy_name2,
    commercial_code3,
    commercial_taxonomy_name3,
    commercial_code4,
    commercial_taxonomy_name4,
    brand_id,
    brand_name,
    split(pic_url_list,';')[0] as pic_url
  from ads_databank.dim_spu_df 
  where dtm='{{ds_nodash}}'
  )spu 
  on spu.spu_id = t1.spu_id
  left join 
  (select spu_id,
    sum(cash_income_amt) as cash_cost_last_ytd
  from redcdm.dws_ads_note_spu_product_income_detail_td_df 
  where dtm='{{ds_nodash}}' and date_key>='2024-01-01' and date_key<='2024-12-31' and module ='效果'
  group by 1
  )ytd 
  on ytd.spu_id = t1.spu_id
group by dt,
-- date_key,
  date_add(t1.dt,60) ,
  t1.spu_id,
  spu.spu_name,
  spu.brand_id,
  spu.brand_name,
  spu.commercial_level,
  spu.commercial_code,
  spu.commercial_name,
  spu.commercial_code1,
  spu.commercial_taxonomy_name1,
  spu.commercial_code2,
  spu.commercial_taxonomy_name2,
  spu.commercial_code3,
  spu.commercial_taxonomy_name3,
  spu.commercial_code4,
  spu.commercial_taxonomy_name4,
  pic_url
;
drop table if exists temp.temp_dm_ads_pub_spu_product_cvr_cost_td_df_new4_{{ds_nodash}}_60d ;
create table if not exists temp.temp_dm_ads_pub_spu_product_cvr_cost_td_df_new4_{{ds_nodash}}_60d   as
select t1.spu_id,
      dt as date_key,
      sum(cash_cost) as cash_cost_90d,
      sum(case when t1.dt_detail>=date_add(dt,-59) and t1.dt_detail<=dt then cash_cost else 0 end) as cash_cost_60d,
      sum(case when t1.dt_detail>=date_add(dt,-59) and t1.dt_detail<=dt then cash_cost else 0 end) as cash_cost_60d,
      sum(case when t1.dt_detail>=date_add(dt,-59) and t1.dt_detail<=dt then bcoo_cash_cost else 0 end) as bcoo_cash_income_amt_60d,
      sum(case when t1.dt_detail>=date_add(dt,-59) and t1.dt_detail<=dt then rtb_feed_cash_cost else 0 end) as rtb_feed_cash_income_amt_60d,
      sum(case when t1.dt_detail>=date_add(dt,-59) and t1.dt_detail<=dt then rtb_search_cash_cost else 0 end) as rtb_search_cash_income_amt_60d,
      sum(case when t1.dt_detail>=date_add(dt,-59) and t1.dt_detail<=date_add(dt,-30) then cash_cost else 0 end) as rtb_cash_income_amt_last_60d,
      sum(case when t1.dt_detail=dt then cash_cost else 0 end) as cash_cost_1d,
      concat_ws(',',collect_set(case when t1.dt_detail>=date_add(dt,-59) and t1.dt_detail<=dt then brand_user_id ELSE NULL END)) as brand_account_id_list,
      concat_ws(',',collect_set(case when t1.dt_detail>=date_add(dt,-59) and t1.dt_detail<=dt then brand_user_name ELSE NULL END)) as brand_user_name_list,
      concat_ws(',',collect_set(case when t1.dt_detail>=date_add(dt,-59) and t1.dt_detail<=dt then cpc_direct_sales_name ELSE NULL END)) as direct_sales_name_list,
      concat_ws(',',collect_set(case when t1.dt_detail>=date_add(dt,-59) and t1.dt_detail<=dt then cpc_operator_name ELSE NULL END)) as operator_name_list,
      --加brandtag
      concat_ws(',',collect_set(case when t1.dt_detail>=date_add(dt,-59) and t1.dt_detail<=dt then brand_tag_name ELSE NULL END)) as brand_tag_name_list,
      --加部门
      concat_ws(',',collect_set(case when t1.dt_detail>=date_add(dt,-59) and t1.dt_detail<=dt then cpc_direct_sales_dept3_name ELSE NULL END)) as direct_sales_dept3_name_list,
      concat_ws(',',collect_set(case when t1.dt_detail>=date_add(dt,-59) and t1.dt_detail<=dt then cpc_direct_sales_dept4_name ELSE NULL END)) as direct_sales_dept4_name_list,
      concat_ws(',',collect_set(case when t1.dt_detail>=date_add(dt,-59) and t1.dt_detail<=dt then cpc_direct_sales_dept5_name ELSE NULL END)) as direct_sales_dept5_name_list,
      concat_ws(',',collect_set(case when t1.dt_detail>=date_add(dt,-59) and t1.dt_detail<=dt then cpc_direct_sales_dept6_name ELSE NULL END)) as direct_sales_dept6_name_list,
      --运营
      concat_ws(',',collect_set(case when t1.dt_detail>=date_add(dt,-59) and t1.dt_detail<=dt then cpc_operator_dept3_name ELSE NULL END)) as operator_dept3_name_list,
      concat_ws(',',collect_set(case when t1.dt_detail>=date_add(dt,-59) and t1.dt_detail<=dt then cpc_operator_dept4_name ELSE NULL END)) as operator_dept4_name_list,
      concat_ws(',',collect_set(case when t1.dt_detail>=date_add(dt,-59) and t1.dt_detail<=dt then cpc_operator_dept5_name ELSE NULL END)) as operator_dept5_name_list,
      concat_ws(',',collect_set(case when t1.dt_detail>=date_add(dt,-59) and t1.dt_detail<=dt then cpc_operator_dept6_name ELSE NULL END)) as operator_dept6_name_list
  from 
    (select spu_id,
      dt,
      dt_detail
    from temp.temp_dm_ads_pub_spu_product_cvr_cost_td_df_new2_{{ds_nodash}}_60d  
  
    )t1 
    left join 
    (select date_key,
      spu_id,
      brand_user_id,
      a2.brand_user_name,
      cpc_direct_sales_name,
      cpc_operator_name,
      cpc_direct_sales_dept3_name,
      cpc_direct_sales_dept4_name,
      cpc_direct_sales_dept5_name,
      cpc_direct_sales_dept6_name,
      cpc_operator_dept3_name,
      cpc_operator_dept4_name,
      cpc_operator_dept5_name,
      cpc_operator_dept6_name,
      brand_tag_name,
      sum(case when module='效果' then cash_income_amt else 0 end) as cash_cost,
      sum(case when module='品合' then cash_income_amt else 0 end) as bcoo_cash_cost,
      sum(case when (module='效果' and product in ('信息流','视频内流')) or module in ('薯条') then cash_income_amt else 0 end) as rtb_feed_cash_cost,
      sum(case when module='效果' and product in ('搜索') then cash_income_amt else 0 end) as rtb_search_cash_cost
    from redcdm.dws_ads_note_spu_product_income_detail_td_df a1 
    left join redcdm.dim_ads_industry_account_df a2 
    on a1.brand_user_id =a2.brand_account_id and a2.dtm='{{ds_nodash}}'
    where a1.dtm='{{ds_nodash}}' and a1.cash_income_amt<>0 and a1.date_key>='2023-12-01'
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
    ) t2 
    on t1.spu_id = t2.spu_id and t1.dt_detail=t2.date_key
    group by t1.spu_id,
      dt;

insert  overwrite table  redapp.app_ads_spu_metrics_detail_60d_df partition(dtm = '{{ ds_nodash }}')
select   t1.date_key,
  -- date_add(t1.date_key,30) as date_key_60d,
  t1.spu_id,
  t1.spu_name,
  t1.brand_id,
  t1.brand_name,
  t1.commercial_level,
  t1.commercial_code,
  t1.commercial_name,
  t1.commercial_code1,
  t1.commercial_taxonomy_name1,
  t1.commercial_code2,
  t1.commercial_taxonomy_name2,
  t1.commercial_code3,
  t1.commercial_taxonomy_name3,
  t1.commercial_code4,
  t1.commercial_taxonomy_name4,
  --module,
  t1.soc_read_feed_num,
  t1.soc_imp_num,
  t1.soc_click_num,--阅读（+额外加二级类目的）
  t1.query_cnt,--搜索
  t1.ti_user_num,
  t1.a_user_num,
  t1.i_user_num,
  t1.s_user_num,
  t1.p_user_num,--aips
  t1.rtb_imp_cnt,--曝光
  t1.rtb_click_cnt,--点击
  t1.rtb_income_amt,--消耗
  --小红星进店
  t1.taobao_ad_click_user_num,--小红星广告曝光
  t1.taobao_ad_ads_cash_income_amt,
  t1.taobao_ad_ads_income_amt,
  t1.taobao_ad_third_active_user_num,
  t1.cash_cost,
  t1.taobao_rgmv_1m as taobao_rgmv,
  t1.tianmao_rgmv_1m as tianmao_rgmv,
  t1.jd_rgmv_1m as jd_rgmv,
  t1.dou_rgmv_1m as dou_rgmv,
  t1.rgmv_1m as rgmv,
  t1.dgmv,
  --t1.buyer_cnt,
  t1.deal_gmv,
  --前30日
t2.soc_read_feed_num as soc_read_feed_num_last_60d ,
t2.soc_imp_num as soc_imp_num_last_60d,
t2.soc_click_num as soc_click_num_last_60d,--阅读（+额外加二级类目的）
t2.query_cnt as query_cnt_last_60d,--搜索
t2.ti_user_num as ti_user_num_last_60d,
t2.a_user_num as a_user_num_last_60d,
t2.i_user_num as i_user_num_last_60d,
t2.s_user_num as s_user_num_last_60d,
t2.p_user_num as p_user_num_last_60d,--aips
t2.rtb_imp_cnt as rtb_imp_cnt_last_60d,--曝光
t2.rtb_click_cnt as rtb_click_cnt_last_60d,--点击
t2.rtb_income_amt as rtb_income_amt_last_60d,--消耗

t2.taobao_ad_click_user_num as taobao_click_user_num_last_60d,--小红星广告曝光
t2.taobao_ad_ads_cash_income_amt as taobao_ads_cash_income_amt_last_60d,
t2.taobao_ad_ads_income_amt as taobao_ads_income_amt_last_60d,
t2.taobao_ad_third_active_user_num as taobao_third_active_user_num_last_60d,
t2.cash_cost as cash_cost_last_60d,
t2.taobao_rgmv_1m as taobao_rgmv_last_60d,
t2.tianmao_rgmv_1m as tianmao_rgmv_last_60d,
t2.jd_rgmv_1m  as jd_rgmv_last_60d,
t2.dou_rgmv_1m  as dou_rgmv_last_60d,
t2.rgmv_1m as rgmv_last_60d,
t2.dgmv as dgmv_last_60d,
--t2.buyer_cnt as buyer_cnt_last_60d,
t2.deal_gmv as deal_gmv_last_60d,
t3.soc_read_feed_num as commercial_soc_read_feed_num ,
t3.soc_imp_num as commercial_soc_imp_num,
t3.soc_click_num as commercial_soc_click_num,--阅读（+额外加二级类目的）
t3.query_cnt as commercial_query_cnt,--搜索
t3.ti_user_num as commercial_ti_user_num,
t3.a_user_num as commercial_a_user_num,
t3.i_user_num as commercial_i_user_num,
t3.s_user_num as commercial_s_user_num,
t3.p_user_num as p_user_num,--aips
t3.rtb_imp_cnt as commercial_rtb_imp_cnt,--曝光
t3.rtb_click_cnt as commercial_rtb_click_cnt,--点击
t3.rtb_income_amt as commercial_rtb_income_amt,--消耗

t3.taobao_ad_click_user_num as commercial_taobao_click_user_num,--小红星广告曝光
t3.taobao_ad_ads_cash_income_amt as commercial_taobao_ads_cash_income_amt,
t3.taobao_ad_ads_income_amt as commercial_taobao_ads_income_amt,
t3.taobao_ad_third_active_user_num as commercial_taobao_third_active_user_num,
t3.cash_cost as commercial_cash_cost,
t3.taobao_rgmv_1m as commercial_taobao_rgmv,
t3.tianmao_rgmv_1m as commercial_tianmao_rgmv,
t3.jd_rgmv_1m  as commercial_jd_rgmv,
t3.dou_rgmv_1m  as commercial_dou_rgmv,
t3.rgmv_1m as commercial_rgmv,
t3.dgmv as commercial_dgmv,
--t3.buyer_cnt as commercial_buyer_cnt,
t3.deal_gmv as commercial_deal_gmv,
-- t4.total_commercial_soc_read_feed_num,
-- t4.total_commercial_query_cnt,--搜索
-- t5.total_commercial_soc_read_feed_num as total_commercial_soc_read_feed_num_last_60d,
-- t5.total_commercial_query_cnt as total_commercial_query_cnt_last_60d,--搜索
-- t6.nps,
-- t6.avg_nps as commercial_nps,
-- t7.nps as nps_last_60d,
t8.new_i_ti_user_num,
t8.new_ads_i_ti_user_num,
t9.cash_cost_90d,
t1.pic_url,
t9.cash_cost_60d,
t9.brand_account_id_list,
t9.brand_user_name_list,
concat_ws(',',t9.direct_sales_name_list,ld_direct_sales_name_list) as direct_sales_name_list,
concat_ws(',',t9.operator_name_list,ld_operator_name_list) as operator_name_list,
t9.cash_cost_1d,
-- t1.rtb_double_imp_cnt,--曝光
-- t1.rtb_double_click_cnt,--点击
-- t3.rtb_double_imp_cnt as commercial_rtb_double_imp_cnt,--曝光
-- t3.rtb_double_click_cnt as commercial_rtb_double_click_cnt,--点击
-- t2.rtb_double_imp_cnt as rtb_double_imp_cnt_last_60d,--曝光
-- t2.rtb_double_click_cnt as rtb_double_click_cnt_last_60d,--点击

t1.soc_read_feed_num_1d,
t1.soc_imp_num_1d,
t1.soc_click_num_1d,--阅读（+额外加二级类目的）
t1.query_cnt_1d,
t1.taobao_rgmv_1d,
t1.tianmao_rgmv_1d,
t1.jd_rgmv_1d ,
t1.dou_rgmv_1d,
t1.dgmv_1d, --aips
t1.rtb_imp_cnt_1d,--曝光
t1.rtb_click_cnt_1d,--点击
t1.rtb_income_amt_1d,--消耗
t1.taobao_ad_click_user_num_1d,--小红星广告曝光
t1.taobao_ad_ads_cash_income_amt_1d,
t1.taobao_ad_ads_income_amt_1d,
t1.taobao_ad_third_active_user_num_1d,
t1.ads_cash_cost_1d,
t4.total_commercial_gmv,
t5.total_commercial_gmv as total_commercial_gmv_last_60d,
concat_ws(',',t9.direct_sales_name_list,ld_direct_sales_name_list_all) as direct_sales_name_list_all,
concat_ws(',',t9.operator_name_list,ld_operator_name_list_all) as operator_name_list_all,
direct_sales_dept3_name_list,
direct_sales_dept4_name_list,
direct_sales_dept5_name_list,
direct_sales_dept6_name_list,
t9.brand_tag_name_list,
t9.bcoo_cash_income_amt_60d, -- '蒲公英流水',
t9.rtb_feed_cash_income_amt_60d,--'竞价（含薯条）双列流水',
t9.rtb_search_cash_income_amt_60d,--'竞价搜索流水',
t9.rtb_cash_income_amt_last_60d,--'竞价t-30流水',
t1.taobao_sales_num_1m as taobao_sales_num,
  t1.tianmao_sales_num_1m as tianmao_sales_num,
  t1.jd_sales_num_1m as jd_sales_num,
  t1.dou_sales_num_1m as dou_sales_num,
  operator_dept3_name_list,
  operator_dept4_name_list,
  operator_dept5_name_list,
  operator_dept6_name_list,
  cash_cost_60d,
  t1.cash_cost_last_ytd,
  spu_account.channel_sales_name,
  spu_account.channel_operator_name,
  spu_account.first_group_name,
  spu_account.second_group_name,
  spu_account.third_group_name,
 t1.propose_sales_num_1m as propose_sales_num,
 process_brand_account_id,
  process_brand_account_name,
  process_direct_sales_dept3_name,
  process_direct_sales_dept4_name,
  process_direct_sales_dept5_name,
  process_direct_sales_dept6_name,
  process_operator_dept3_name,
  process_operator_dept4_name,
  process_operator_dept5_name,
  process_operator_dept6_name,
  process_direct_sales_name,
  process_operator_name,
  process_brand_tag_name,
  spu_create_time,
  t24.cash_cost_24y,
  case when black.cnt>0 then 1 else 0 end as  is_spu_black_list,
 planner_name_list,
 t2.propose_sales_num_1m as propose_sales_num_last_60d,
  --  一方ROI：本来有数据指标对应，但需要支持用户手工校正修改
  -- CID ROI：先展示具体数值，后续系统增加判断标签提示可能提升点
  -- UD ROI：先展示具体数值，后续系统增加判断标签提示可能提升点
  -- 闭环ROI：先展示具体数值，后续系统增加判断标签提示可能提升点
  row_number()over(partition by date_key,commercial_code order by t1.dgmv desc) as gmv_rn,-- 站内GMV排行：SPU对应所在末级类目下所有SPU的排行，呈现样式是No.x，而非Top x%；同时补充对应具体数值（如站内GMV具体是多少）
  row_number()over(partition by date_key,commercial_code order by t1.external_rgmv_1m desc) as gmv_rn,-- 站外GMV排行：同上
  row_number()over(partition by date_key,commercial_code order by coalesce(t1.i_ti_user_num,0)/(coalesce(t1.i_ti_user_num,0)+coalesce(t1.a_user_num,0)+coalesce(t1.p_user_num,0)) desc) as gmv_rn,-- I+TI人群占比排行：同上，因为加工逻辑，建议先按照最近30天
  row_number()over(partition by date_key,commercial_code order by coalesce(t1.ti_user_num,0)+coalesce(t1.a_user_num,0)+coalesce(t1.i_user_num ,0)+coalesce(t1.p_user_num,0) desc) as gmv_rn,-- AIPS人群资产排行：同上，因为加工逻辑，建议先按照最近30天
  row_number()over(partition by date_key,commercial_code order by query_cnt desc) as gmv_rn,-- 搜索量排行：同上
  t1.external_rgmv_1m
FROM
 temp.temp_dm_ads_pub_spu_product_cvr_cost_td_df_new3_{{ds_nodash}}_60d t1 
  left join 
  temp.temp_dm_ads_pub_spu_product_cvr_cost_td_df_new3_{{ds_nodash}}_60d t2 
  on t1.spu_id  = t2.spu_id and t1.date_key = t2.date_key_60d 
  left join 
  (select date_key,
    commercial_code,
    avg(soc_read_feed_num) as soc_read_feed_num, --avg会统计0不统计null
    avg(soc_imp_num) as soc_imp_num,
    avg(soc_click_num) as soc_click_num,--阅读（+额外加二级类目的）
    avg(query_cnt) as query_cnt,--搜索
    avg(ti_user_num) as ti_user_num,
    avg(a_user_num) as a_user_num,
    avg(i_user_num) as i_user_num,
    avg(s_user_num) as s_user_num,
    avg(p_user_num) as p_user_num,--aips
    avg(rtb_imp_cnt) as rtb_imp_cnt,--曝光
    avg(rtb_click_cnt) as rtb_click_cnt,--点击
    avg(rtb_double_imp_cnt) as rtb_double_imp_cnt,--曝光
    avg(rtb_double_click_cnt) as rtb_double_click_cnt,--点击
    avg(rtb_income_amt) as rtb_income_amt,--消耗
    --小红星进店
    avg(taobao_ad_click_user_num) as taobao_ad_click_user_num,--小红星广告曝光
    avg(taobao_ad_ads_cash_income_amt) as taobao_ad_ads_cash_income_amt,
    avg(taobao_ad_ads_income_amt) as taobao_ad_ads_income_amt,
    avg(taobao_ad_third_active_user_num ) as taobao_ad_third_active_user_num  ,--小红星消耗
    avg(cash_cost) as cash_cost,
    avg(taobao_rgmv_1m) as taobao_rgmv_1m,
    avg(tianmao_rgmv_1m) as tianmao_rgmv_1m,
    avg(jd_rgmv_1m ) as jd_rgmv_1m ,
    avg(dou_rgmv_1m ) as dou_rgmv_1m,
    avg(rgmv_1m) as rgmv_1m,
    avg(dgmv) as dgmv,
    avg(buyer_cnt) as buyer_cnt,
    avg(deal_gmv) as deal_gmv
  from temp.temp_dm_ads_pub_spu_product_cvr_cost_td_df_new3_{{ds_nodash}}_60d 
  group by 1,2
  )t3
  on t1.commercial_code = t3.commercial_code and  t1.date_key = t3.date_key


  left join 
  (select t1.spu_id,
      dt as date_key,
      sum(new_i_ti_user_num) as new_i_ti_user_num,
      sum(new_ads_i_ti_user_num) as new_ads_i_ti_user_num
  from 
    (select spu_id,
      dt,
      dt_detail
    from temp.temp_dm_ads_pub_spu_product_cvr_cost_td_df_new2_{{ds_nodash}}_60d  
    where dt_detail>=date_add(dt,-59) and dt_detail<=dt
    )t1 
    left join 
    (SELECT f_getdate(dtm) as date_key,
      spu_id,
      new_i_ti_user_num,
      new_ads_i_ti_user_num
    FROM
      redapp.app_ads_idea_aips_spu_trans_1d_di
    WHERE
      dtm>='20240101' and dtm<='{{ds_nodash}}'
    )t2 
    on t1.spu_id = t2.spu_id and t1.dt_detail=t2.date_key
  group by 1,2
  )t8
  on t8.spu_id= t1.spu_id and t8.date_key = t1.date_key 
  --近90天是否有投放
  left join temp.temp_dm_ads_pub_spu_product_cvr_cost_td_df_new4_{{ds_nodash}}_60d t9 
  on t9.spu_id= t1.spu_id and t9.date_key = t1.date_key 
  left join 
  (select t1.spu_id,
      dt as date_key,     
      concat_ws(',',collect_set(case when t1.dt_detail>=date_add(dt,-59) and t1.dt_detail<=dt then leader_name_list2 ELSE NULL END)) as ld_direct_sales_name_list,
      concat_ws(',',collect_set(case when t1.dt_detail>=date_add(dt,-59) and t1.dt_detail<=dt then leader_name_list1 ELSE NULL END)) as ld_operator_name_list,
      concat_ws(',',collect_set(case when t1.dt_detail>=date_add(dt,-59) and t1.dt_detail<=dt then leader_name_list2_all ELSE NULL END)) as ld_direct_sales_name_list_all,
      concat_ws(',',collect_set(case when t1.dt_detail>=date_add(dt,-59) and t1.dt_detail<=dt then leader_name_list1_all ELSE NULL END)) as ld_operator_name_list_all
  from 
    (select spu_id,
      dt,
      dt_detail
    from temp.temp_dm_ads_pub_spu_product_cvr_cost_td_df_new2_{{ds_nodash}}_60d  
  
    )t1 
    left join 
    (select date_key,
      spu_id,
      brand_user_id,
      a2.brand_user_name,
      cpc_direct_sales_name,
      cpc_operator_name,
      sum(case when module='效果' then cash_income_amt else 0 end) as cash_cost
    from redcdm.dws_ads_note_spu_product_income_detail_td_df a1 
    left join redcdm.dim_ads_industry_account_df a2 
    on a1.brand_user_id =a2.brand_account_id and a2.dtm='{{ds_nodash}}'
    where a1.dtm='{{ds_nodash}}' and a1.cash_income_amt<>0 and a1.date_key>='2023-12-01'
    group by 1,2,3,4,5,6
    ) t2 
    on t1.spu_id = t2.spu_id and t1.dt_detail=t2.date_key
    --20250221加l1-leader
    left join 
    (SELECT
    seller_whole_name,
    case when leader_name rlike '之恒|玄霜|昂扬|欧迪|米欧|觅阳|纳什' then null else leader_name end as leader_name_list1,
    leader_name as leader_name_list1_all
    FROM
      redcdm.dim_ads_insight_user_leader_relation_df
    WHERE
      dtm = '{{ds_nodash}}' -- and leader_name not rlike '之恒|玄霜|昂扬|欧迪|米欧|觅阳|纳什' 
      group by 1,2,3
      )ld1 
      ON ld1.seller_whole_name=t2.cpc_operator_name
      left join 
    (SELECT
    seller_whole_name,
    case when leader_name rlike '之恒|玄霜|昂扬|欧迪|米欧|觅阳|纳什' then null else leader_name end as leader_name_list2,
    leader_name as leader_name_list2_all
    FROM
      redcdm.dim_ads_insight_user_leader_relation_df
    WHERE
      dtm = '{{ds_nodash}}'  --and leader_name not rlike '之恒|玄霜|昂扬|欧迪|米欧|觅阳|纳什' 
      group by 1,2,3
      )ld2 
      ON ld2.seller_whole_name=t2.cpc_direct_sales_name
    group by t1.spu_id,
      dt
  )t11
  on t11.spu_id= t1.spu_id and t11.date_key = t1.date_key 
   join 
   --只看生效类目
  (SELECT
    commercial_code,
    active_state
  FROM
    ads_databank.dim_commercial_taxonomy_df
  WHERE
    dtm = '{{ds_nodash}}' 
    and active_state = '在线'
  )t10 
  on t10.commercial_code = t1.commercial_code

  left join 
--spu和账号mapping 
(SELECT
  date_key,
  spu_id,
  brand_account_id  as process_brand_account_id,
  channel_sales_name,
  channel_operator_name,
  -- agent_user_id,
  -- agent_user_name,
  -- first_cost_date,
  -- first_note_create_date,
  brand_user_name   as process_brand_account_name,
  first_group_name,
  second_group_name,
  third_group_name,
  brand_channel_sales_name,
  cpc_direct_sales_dept3_name as process_direct_sales_dept3_name,
  cpc_direct_sales_dept4_name as   process_direct_sales_dept4_name,
  cpc_direct_sales_dept5_name as   process_direct_sales_dept5_name,
  cpc_direct_sales_dept6_name as   process_direct_sales_dept6_name,
  cpc_operator_dept3_name as   process_operator_dept3_name,
  cpc_operator_dept4_name as   process_operator_dept4_name,
  cpc_operator_dept5_name as   process_operator_dept5_name,
  cpc_operator_dept6_name as   process_operator_dept6_name,
  cpc_direct_sales_name as   process_direct_sales_name,
  cpc_operator_name as   process_operator_name,
   brand_tag_name as   process_brand_tag_name,
   substring(spu_create_time,1,10) as spu_create_time,
    planner_name_list
FROM
  redapp.app_ads_insight_spu_account_mapping_df
WHERE
  dtm = '{{ds_nodash}}'
)spu_account 
on t1.date_key =spu_account.date_key and t1.spu_id =spu_account.spu_id

    left join 
    (select cast(spu_id as int) as spu_id,count(1) as cnt
    from redods.ods_redoc2hive_ads_industry_spu_black_list_df 
    where dtm='{{ds_nodash}}'
    group by 1 
    )black 
    on black.spu_id = t1.spu_id
-----20250909大单品需求新增指标https://docs.xiaohongshu.com/doc/5d295e4f3e760cedbe9c37c96d9f7f97
-- left join
-- --一方roi
-- ()
-- left join
-- --闭环和cid和ud的roi




