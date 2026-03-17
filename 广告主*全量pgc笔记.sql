insert overwrite table redcdm.dm_ads_industry_note_1d_di partition (dtm)
  select base.note_id,
  author_user_id,
  level,
  type,
  enabled,
  create_time,
  duration,
  title,
  is_goods_note,
  bridge_type,
  spam_level,
  taxonomy1,
  taxonomy2,
  taxonomy3,
  taxonomy_type,
  nickname,
  publish_time,
  is_live_report_note,
  is_bind,
  is_kos,
  is_brand,
  is_cps_note,
  is_soft_ads,
  base.brand_account_id,
  author_user_type,
  goods_id,
  seller_id,
  -- certificate_type,
  -- is_brand_partner,
  -- is_kos_author,
  -- is_director,
  note_url, 
  deal_gmv as note_goods_deal_gmv,
  ads_imp_cnt,
  ads_click_cnt,
  ads_engage_cnt,
  cash_income_amt,
  income_amt,
  ads_feed_imp_cnt,
  ads_feed_click_cnt,
  ads_feed_engage_cnt,
  ads_feed_cash_income_amt,
  ads_feed_income_amt,
  purchase_rgmv_1d,
  purchase_rgmv,
  click_purchase_order_pv_1d,
  click_purchase_order_pv_7d,
  enter_seller_cnt,
  msg_open_num,
  leads_cnt,
  leads_success_valid_cnt,
  leads_submit_cnt,
  imp_cnt,
  click_cnt,
  engage_cnt,
  feed_imp_cnt,
  feed_click_cnt,
  t1.dtm
from 
(SELECT
  note_id,
  author_user_id,
  level,
  type,
  enabled,
  create_time,
  duration,
  title,
  is_goods_note,
  bridge_type,
  spam_level,
  taxonomy1,
  taxonomy2,
  taxonomy3,
  taxonomy_type,
  nickname,
  publish_time,
  is_live_report_note,
  is_bind,
  is_kos,
  is_brand,
  is_cps_note,
  is_soft_ads,
  brand_account_id,
  author_user_type,
  goods_id,
  seller_id,
  certificate_type,
  is_brand_partner,
  is_kos_author,
  is_director,
  note_url
FROM
  redcdm.dim_ads_note_extend_df

where dtm='{{ds_nodash}}'
)base 
join 
(select brand_account_id,
  dtm
from redcdm.dim_ads_industry_brand_account_df
where dtm between f_getdate('{{ds_nodash}}',-7) and '{{ds_nodash}}'
)account 
on base.brand_account_id = account.brand_account_id
left join 
(select dtm,
  note_id,
  sum(imp_cnt) as ads_imp_cnt,
  sum(click_cnt) as ads_click_cnt,
  sum(engage_cnt) as ads_engage_cnt,
  sum(cash_income_amt) as cash_income_amt,
  sum(income_amt) as income_amt,
  sum(case when product in ('信息流','搜索') then imp_cnt else 0 end) as ads_feed_imp_cnt,
  sum(case when product in ('信息流','搜索') then click_cnt else 0 end) as ads_feed_click_cnt,
  sum(case when product in ('信息流','搜索') then engage_cnt else 0 end) as ads_feed_engage_cnt,
  sum(case when product in ('信息流','搜索') then cash_income_amt else 0 end) as ads_feed_cash_income_amt,
  sum(case when product in ('信息流','搜索') then income_amt else 0 end) as ads_feed_income_amt,
  sum(purchase_rgmv_1d) as purchase_rgmv_1d,
  sum(purchase_rgmv) as purchase_rgmv,
  sum(click_purchase_order_pv_1d) as click_purchase_order_pv_1d,
  sum(click_purchase_order_pv_7d) as click_purchase_order_pv_7d,
  sum(enter_seller_cnt) as enter_seller_cnt,
  sum(msg_open_num) as msg_open_num,
  sum(leads_cnt) as leads_cnt,
  sum(leads_success_valid_cnt) as leads_success_valid_cnt,
  sum(leads_submit_cnt) as leads_submit_cnt
from redcdm.dm_ads_rtb_note_account_1d_di
where dtm between f_getdate('{{ds_nodash}}',-7) and '{{ds_nodash}}'
group by note_id,dtm
)t1 
on t1.note_id = base.note_id and t1.dtm=account.dtm
left join 
(SELECT
  dtm,
  note_id,
  SUM(imp_num) AS imp_cnt,
  SUM(click_num) AS click_cnt,
  SUM(engage_num) AS engage_cnt,
  SUM(coalesce(imp_homefeed_num,0)+coalesce(imp_search_num,0)) AS feed_imp_cnt,
  SUM(coalesce(click_homefeed_num,0)+coalesce(click_search_num,0)) AS feed_click_cnt
FROM
redapp.app_ads_note_engagement_1d_di
WHERE
  dtm between f_getdate('{{ds_nodash}}',-7) and '{{ds_nodash}}'
group by
  1,2
  )t2 
on t2.note_id = base.note_id and t2.dtm=account.dtm
left join --挂接商品笔记gmv
  (
    select dtm,
      note_id,
      sum(deal_gmv) as deal_gmv
    from
      redcdm.dm_ecm_user_note_goods_entrance_traffic_1d_di
    where
      dtm between f_getdate('{{ds_nodash}}',-7) and '{{ds_nodash}}'
    group by dtm,
      note_id
  ) dgmv on dgmv.note_id = base.note_id and dgmv.dtm=account.dtm







    -- ************************************************
    -- Author: chenmengdi
    -- CreateTime:2024-01-03T20:13:35+08:00
    -- Update: Task Update Description
    -- ************************************************
drop table if exists temp.temp_app_ads_rtb_note_{{ds_nodash}}_nt;
create table temp.temp_app_ads_rtb_note_{{ds_nodash}}_nt as 
select dtm,
  a.user_id as brand_user_id,
  a.note_id,
  min(create_time) as create_time,
  if(concat_ws(',', collect_set(a.type)) like '%kos%','kos',
    if(concat_ws(',', collect_set(a.type)) like '%cps%',
    'cps',
    if(
      concat_ws(',', collect_set(a.type)) like '%pugongying%',
      'pugongying',
      'gh'
    )
  )
  ) as note_type
from
  (
    select dtm,
      discovery_id as note_id,
      user_id,
      substring(create_time, 1, 10) as create_time,
      'guanhao' as type
    from
      reddw.dw_soc_discovery_delta_7_day
    where
      dtm between f_getdate('{{ds_nodash}}', -7) and '{{ds_nodash}}'
      and (
        is_brand = 1
        or is_cps_note = 1
        or is_bind = 1
      )
      and substring(publish_time, 1, 10) >= '2023-01-01'
      and substring(publish_time, 1, 10) <= f_getdate(dtm)
    union all
    select dtm,
      note_id,
      report_brand_user_id as user_id,
      substring(note_publish_time, 1, 10) as create_time,
      'pugongying' as type
    from
      reddw.dw_soc_tb_order_note_detail_day
    where
      dtm between f_getdate('{{ds_nodash}}', -7) and '{{ds_nodash}}'
      and order_status in (401, 402)
      and substr(create_time, 1, 4) >= '2023'
    group by dtm,
      note_id,
      substring(note_publish_time, 1, 10),
      report_brand_user_id
    union all
    select dtm,
      note_id,
      brand_account_id as user_id,
      substring(create_time, 1, 10) as create_time,
      'cps' as type
    from
      redapp.app_ads_matrix_user_shopping_note_info_1d_df
    where
      dtm between f_getdate('{{ds_nodash}}', -7) and '{{ds_nodash}}'
      and substr(create_time, 1, 4) >= '2023'
    group by dtm,
      note_id,
      brand_account_id,
      substring(create_time, 1, 10)
    union all 
    select dtm,
    note_id,
    brand_user_id as user_id,
    substr(note_publish_time,1,10) as create_time,
    'kos' as type
    from  redods.ods_note_trade_tb_grant_note_df
    where dtm between f_getdate('{{ds_nodash}}', -7) and '{{ds_nodash}}' and grant_type =1
  ) a
group by a.dtm,
  a.user_id,
  a.note_id;




select dtm,
  brand_user_id,
  t1.note_id,
  create_date as create_time,
  note_type,
  imp_num,
  click_num,
  engage_num
from temp.temp_app_ads_rtb_note_{{ds_nodash}}_nt t1
left join --笔记消费
(SELECT dtm,
  note_id,
  create_date,
  imp_num,
  click_num,
   -- like_num,
  -- cmt_num,
  -- fav_num,
  -- share_num,
  -- follow_num,
  engage_num
FROM
  redapp.app_ads_note_engagement_1d_di
WHERE
  dtm between f_getdate('{{ds_nodash}}',-7) and  '{{ds_nodash}}'
  )t2
on t1.note_id = t2.note_id and t1.dtm=t2.dtm




select
  t1.note_id,
  t1.author_user_id,
  type,
  create_time,
  title,
  nickname,
  publish_time,
  is_bind,
  is_kos,
  is_brand,
  is_cps_note,
  t1.brand_account_id,
  is_director,
  first_image_url,
  note_url,
  t2.click_cnt,
  t2.imp_cnt,
  income_amt,
  cash_income_amt,
  purchase_order_num,
  deal_order_num,
  purchase_rgmv,
  deal_rgmv,
  t2.engage_cnt,
  t3.imp_cnt as origin_imp_cnt,
  t3.click_cnt as origin_click_cnt,
  t3.engage_cnt as origin_engage_cnt
from
 (
    select
      note_id,
      author_user_id,
      level,
      type,
      enabled,
      create_time,
      duration,
      title,
      nickname,
      publish_time,
      is_live_report_note,
      is_bind,
      is_kos,
      is_brand,
      is_cps_note,
      is_soft_ads,
      brand_account_id,
      is_director,
      first_image_url,
      note_url
    from
      redcdm.dim_ads_note_extend_df
    where
      dtm = '{{ds_nodash}}' and (is_kos = 1 or is_brand=1 or is_bind = 1 or is_cps_note = 1)
  ) t1
  left join 
  (
    select
      note_id,
      brand_account_id,
      product,
      marketing_target,
      optimize_target,
      author_user_id,
      note_ads_type,
      click_cnt,
      imp_cnt,
      income_amt,
      cash_income_amt,
      purchase_order_num,
      deal_order_num,
      purchase_rgmv,
      deal_rgmv,
      engage_cnt
    from
      redcdm.dm_ads_rtb_note_acoount_1d_di
    where
      dtm = '{{ds_nodash}}'
  ) t2 on t1.note_id = t2.note_id
  left join 
  (SELECT dtm,
    note_id,
    SUM(imp_num) AS imp_cnt,
    SUM(click_num) AS click_cnt,
    SUM(engage_num) AS engage_cnt
  FROM
    redapp.app_ads_note_engagement_1d_di
  WHERE
    dtm = '{{ds_nodash}}'
  group by 1,2
  )t3 
  on t1.note_id = t3.note_id


select
  ads.note_id,
  ads.brand_account_id,
  product,
  marketing_target,
  optimize_target,
  author_user_id,
  level,
  type as note_type,
  enabled,
  create_time,
  duration,
  title,
  is_goods_note,
  bridge_type,
  spam_level,
  taxonomy1,
  taxonomy2,
  taxonomy3,
  taxonomy_type,
  nickname,
  publish_time,
  is_live_report_note,
  '' as note_ads_type,
  author_user_type,
  goods_id,
  seller_id,
  certificate_type,
  note_first_cost_date,
  deal_gmv as note_goods_deal_gmv,
  click_cnt,
  imp_cnt,
  cost as income_amt,
  cash_cost as cash_income_amt,
  purchase_order_num,
  deal_order_num,
  click_rgmv as purchase_rgmv,
  deal_rgmv_7d as deal_rgmv,
  click_rgmv_1d as purchase_rgmv_1d,
  click_purchase_order_pv_1d,
  click_purchase_order_pv_7d,
  enter_seller_cnt,
  msg_open_num,
  leads_cnt,
  leads_success_valid_cnt,
  leads_submit_cnt,
  engage_cnt,
  ads.dtm
from
  (
    SELECT
      case when ads_material_type = 'post' then ads_material_id else null  end as note_id,
      brand_account_id,
      product,
      marketing_target,
      optimize_target,
      t1.dtm,
      note_first_cost_date,
      sum(unique_click_cnt) as click_cnt,
      sum(unique_imp_cnt) as imp_cnt,
      sum(total_amount) as cost,
      sum(cost_amount) as cash_cost,
      sum(
        coalesce(purchase_order_num, 0) + coalesce(mini_purchase_order_num, 0)
      ) as purchase_order_num,
      sum(
        coalesce(deal_order_num, 0) + coalesce(mini_deal_order_num, 0)
      ) as deal_order_num,
      sum(
        coalesce(purchase_rgmv, 0) + coalesce(mini_purchase_rgmv, 0)
      ) as click_rgmv,
      sum(coalesce(rgmv, 0) + coalesce(mini_rgmv, 0)) as deal_rgmv_7d,
      sum(click_rgmv_1d) as click_rgmv_1d,
      sum(click_purchase_order_pv_1d) as click_purchase_order_pv_1d,
      sum(click_purchase_order_pv_7d) as click_purchase_order_pv_7d,
      sum(enter_seller_cnt) as enter_seller_cnt,
      sum(msg_open_num) as msg_open_num,
      sum(leads_cnt) as leads_cnt,
      sum(leads_success_valid_cnt) as leads_success_valid_cnt,
      sum(leads_submit_cnt) as leads_submit_cnt,
      sum(coalesce(like_cnt, 0) + coalesce(cmt_cnt, 0) + coalesce(fav_cnt, 0) + coalesce(follow_cnt, 0) + coalesce(share_cnt, 0) ) as engage_cnt
    FROM
      redcdm.dm_ads_rtb_creativity_1d_di t1
      left join --笔记首次消耗时间
      (
        select
          dtm,
          note_id,
          min(first_cost_date) as note_first_cost_date
        from
          redapp.app_ads_industry_rtb_creativity_di
        where
          dtm = '{{ds_nodash}}'
        group by
          1,
          2
      ) t2 on t1.ads_material_id = t2.note_id
    WHERE
      t1.dtm between f_getdate('{{ds_nodash}}', -7)
      and '{{ds_nodash}}'
      and (
        t1.is_effective = 1
        or t1.total_amount > 0
      )
    group by 1,2,3,4,5,6,7
  ) ads
  left join (
    select
      note_id,
      author_user_id,
      level,
      type,
      enabled,
      create_time,
      duration,
      title,
      is_goods_note,
      bridge_type,
      spam_level,
      taxonomy1,
      taxonomy2,
      taxonomy3,
      taxonomy_type,
      nickname,
      publish_time,
      is_live_report_note,
      is_bind,
      is_kos,
      is_brand,
      is_cps_note,
      is_soft_ads,
      brand_account_id,
      author_user_type,
      goods_id,
      seller_id,
      certificate_type
    from
      redcdm.dim_ads_note_extend_df
    where
      dtm = '{{ds_nodash}}'
  ) t1 on ads.note_id = t1.note_id
  left join --挂接商品笔记gmv
  (
    select
      note_id,
      sum(deal_gmv) as deal_gmv
    from
      redcdm.dm_ecm_user_note_goods_entrance_traffic_1d_di
    where
      dtm = '{{ds_nodash}}'
    group by
      note_id
  ) dgmv on dgmv.note_id = t1.note_id

      


insert overwrite table redcdm.dim_ads_note_extend_df partition (dtm='{{ds_nodash}}')
select t1.note_id,
  user_id as author_user_id,
  level,
  type,
  enabled,
  create_time,
  duration,
  title,
  is_goods_note,
  bridge_type,
  spam_level,
  taxonomy1,
  taxonomy2,
  taxonomy3,
  taxonomy_type,
  --is_brand,
  nickname,
  publish_time,
  is_live_report_note ,
  case when t2.note_id is not null then 1 else 0 end as is_bind,
  case when t3.note_id is not null then 1 else 0 end as is_kos,
  case when t4.brand_account_id is not null then 1 else 0 end as is_brand,
  case when t5.note_id is not null then 1 else 0 end as is_cps_note,
  is_soft_ads,
  coalesce(t5.brand_account_id,t2.brand_user_id,t3.brand_user_id,t4.brand_account_id) as brand_account_id,
  '' as author_user_type,
  goods_id,
  seller_id,
  certificate_type
from 
(SELECT
  discovery_id as note_id,
  user_id,
  level,
  type,
  enabled,
  create_time,
  duration,
  title,
  case when is_goods_note='true' then 1 else 0 end as is_goods_note,
  case when bridge_type in('goods_v2','goods_seller') then '商品笔记（不包含购物笔记）' when bridge_type in('goods_catering_tostore') then '到店餐饮笔记'
  when bridge_type in('goods_shopping') then '购物笔记'
  when bridge_type in('goods_order') then '晒单笔记'
  when bridge_type in('goods_seller_red_mp','goods_seller_baidu_mp') then '小程序笔记' else '其他笔记' end as bridge_type,
  spam_level,
  taxonomy1,
  taxonomy2,
  taxonomy3,
  taxonomy_type,
  is_brand,
  nickname,
  publish_time,
  is_live_report_note ,
  goods_id,
  seller_id,
  is_cps_note,
  is_soft_ads,
   certificate_type
FROM
  reddw.dw_soc_discovery_delta_7_day
WHERE
  dtm = '{{ds_nodash}}'
  )t1 
  left join 
  (select note_id,brand_user_id
  from  redcdm.dwd_ads_bcoo_ord_note_df --蒲公英笔记
  where dtm='{{ds_nodash}}'
  group by 1,2 )t2 
  on t1.note_id = t2.note_id
  left join(
  select 
  note_id,brand_user_id--kos笔记类型
  from redods.ods_note_trade_tb_grant_note_df 
  where  dtm='{{ds_nodash}}'
  and grant_type = 1 --kos笔记类型
  and grant_status in (6,10)
  group by 1,2
) as t3
on t1.note_id=t3.note_id
left join 
(
select
  brand_account_id
from redcdm.dim_ads_industry_brand_account_df
where dtm='{{ds_nodash}}'
)t4 
on t1.user_id = t4.brand_account_id 
left join 
(select
      note_id,
      max(brand_account_id) as brand_account_id
    from
      redapp.app_ads_matrix_user_shopping_note_info_1d_df
    where
      dtm = '{{ds_nodash}}'
      
    group by
      note_id
)t5 
on t5.note_id=t1.note_id
left join 
--挂接商品笔记gmv
(select note_id,
      sum(deal_gmv) as deal_gmv,
from redcdm.dm_ecm_user_note_goods_entrance_traffic_1d_di 
where dtm='{{ds_nodash}}'
)dgmv
on dgmv.note_id =  t1.discovery_id







select 
from 


drop table
  if exists temp.temp_app_ads_ecm_note_spu_{{ds_nodash}}_1;

create table
  temp.temp_app_ads_ecm_note_spu_{{ds_nodash}}_1 as
select
  t2.brand_account_id,
  brand_account_name,
  company_code,
  company_name,
  group_code,
  group_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  cpc_direct_sales_code,
  cpc_direct_sales_name,
  cpc_direct_sales_dept1_name,
  cpc_direct_sales_dept2_name,
  cpc_direct_sales_dept3_name,
  cpc_direct_sales_dept4_name,
  cpc_direct_sales_dept5_name,
  cpc_direct_sales_dept6_name,
  brand_tag_name,
  brand_group_tag_name,
  cpc_operator_code,
  cpc_operator_name,
  cpc_operator_dept1_name,
  cpc_operator_dept2_name,
  cpc_operator_dept3_name,
  cpc_operator_dept4_name,
  cpc_operator_dept5_name,
  cpc_operator_dept6_name,
  0 as campaign_id,
  0 as unit_id,
  'ALL' as note_id,
  '' as agent_user_id,
  '' as agent_name,
  -911 as build_type,
  -911 as biz_product_type,
  '' as target_type_msg_list,
  -911 as marketing_target,
  -911 as optimize_target,
  '' as optimize_target_msg,
  '' as publish_time,
  '' as campaign_name,
  '' as campaign_placement,
  '' as product,
  type,
  is_brand,
  is_cps_note,
  is_bind,
  0 as click_cnt,
  0 as imp_cnt,
  0 as cost,
  0 as cash_cost,
  0 as purchase_order_num,
  0 as click_rgmv_7d,
  0 as engage_cnt,
  1 as tag,
  note_num,
  new_note_num,
  cost_spu_cnt, 
  cost_note_num,
  ecm_closed_cost_note_num,
  note.dtm
from
  (
    select
      brand_account_id,
      brand_account_name,
      company_code,
      company_name,
      group_code,
      group_name,
      track_group_id,
      track_group_name,
      track_industry_name,
      track_detail_name,
      cpc_direct_sales_code,
      cpc_direct_sales_name,
      cpc_direct_sales_dept1_code,
      cpc_direct_sales_dept1_name,
      cpc_direct_sales_dept2_code,
      cpc_direct_sales_dept2_name,
      cpc_direct_sales_dept3_code,
      cpc_direct_sales_dept3_name,
      cpc_direct_sales_dept4_code,
      cpc_direct_sales_dept4_name,
      cpc_direct_sales_dept5_code,
      cpc_direct_sales_dept5_name,
      cpc_direct_sales_dept6_code,
      cpc_direct_sales_dept6_name,
      brand_tag_code,
      brand_tag_name,
      brand_group_tag_code,
      brand_group_tag_name,
      cpc_operator_code,
      cpc_operator_name,
      cpc_operator_dept1_code,
      cpc_operator_dept1_name,
      cpc_operator_dept2_code,
      cpc_operator_dept2_name,
      cpc_operator_dept3_code,
      cpc_operator_dept3_name,
      cpc_operator_dept4_code,
      cpc_operator_dept4_name,
      cpc_operator_dept5_code,
      cpc_operator_dept5_name,
      cpc_operator_dept6_code,
      cpc_operator_dept6_name
    from
      redcdm.dim_ads_industry_brand_account_df
    where
      dtm = max_dtm('redcdm.dim_ads_industry_brand_account_df')
  ) t2
  join 
  (select coalesce(note.user_id,all_cost.brand_account_id) as user_id,
      coalesce(note.type,all_cost.type) as type,
      coalesce(note.is_brand,all_cost.is_brand) as  is_brand,
      coalesce(note.is_cps_note,all_cost.is_cps_note) as is_cps_note,
      coalesce(note.is_bind,all_cost.is_bind) as  is_bind,
      coalesce(note.dtm,all_cost.dtm) as dtm,
      note_num,
      new_note_num,
      cost_note_num,
      ecm_closed_cost_note_num
  from 
  (
    select
      brand_user_id as user_id,
      type,
      if(note_type = 'gh',1, 0) as is_brand,
      if(note_type = 'cps',1, 0) as is_cps_note,
      if(note_type = 'pugongying',1, 0) as is_bind,
      dtm,
      count(case when substring(create_time, 1, 10)>=f_getdate('{{ds}}',-364) and  substring(create_time, 1, 10)<='{{ds}}' then 1 else null end) as note_num,
      COUNT(
        CASE
          WHEN substring(create_time, 1, 10) = f_getdate(dtm) then 1
          else null
        end
      ) as new_note_num
    from
      temp.temp_app_ads_ecm_note_spu_{{ds_nodash}}_nt
    group by user_id,
      type,
      if(note_type = 'gh',1, 0) ,
      if(note_type = 'cps',1, 0) ,
      if(note_type = 'pugongying',1, 0) ,
      dtm
  ) note 
  
  full outer join 
  (select   brand_account_id,
    day_dtm as dtm,
    type,
    coalesce(is_brand, 0) as is_brand,
    coalesce(is_cps_note, 0) as is_cps_note,
    coalesce(is_bind, 0) as is_bind,
    count(distinct ads_material_id ) as cost_note_num,
    count(distinct case when marketing_target in (3, 8, 14, 15) then ads_material_id else null end) as ecm_closed_cost_note_num
  from 
    (select brand_account_id,
        dtm,
        ads_material_id,
        marketing_target
    from  redcdm.dm_ads_rtb_creativity_1d_di
    WHERE
      --dtm>='20230101' 
      --2024年改成近365日累计
      dtm>=f_getdate('{{ds_nodash}}',-371)
      and dtm<='{{ds_nodash}}'
      and (
        is_effective = 1
        or total_amount > 0
      )
     -- and marketing_target in (3, 8, 14, 15)
      and ads_material_type = 'post'
      and total_amount>0
      )t1 
      left join 
    ( select
      discovery_id,
      --user_id,
      --substring(publish_time, 1, 10) as publish_time,
      type,
      is_brand,
      is_cps_note,
      is_bind
    from
      reddw.dw_soc_discovery_delta_7_day
    where
      dtm = '{{ds_nodash}}'
    ) note on note.discovery_id = t1.ads_material_id
    join (
      select
        day_dtm,
        dt,
        week_label
      from
        redcdm.dim_ads_date_df
      where
        dtm = 'all'
        and day_dtm between f_getdate('{{ds_nodash}}', -7) and '{{ds_nodash}}'
    ) dt on 1 = 1
    where t1.dtm>=f_getdate(day_dtm,-364) and t1.dtm<=day_dtm
    group by brand_account_id,
        day_dtm,
        type,
        coalesce(is_brand, 0) ,
        coalesce(is_cps_note, 0) ,
        coalesce(is_bind, 0)
    )all_cost 
    on all_cost.dtm=note.dtm and all_cost.brand_account_id = note.user_id
    and all_cost.is_brand=note.is_brand and all_cost.is_bind = note.is_bind
    and all_cost.is_cps_note=note.is_cps_note and all_cost.type = note.type
    
  )note
  on note.user_id = t2.brand_account_id 
;

insert overwrite table redapp.app_ads_insight_ecm_closed_account_note_unit_detail_di   partition( dtm )
select   brand_account_id,
  t1.note_id,
  unit_id,
  t1.campaign_id,
  tag,
  campaign_name,
  campaign_placement,
  agent_user_id,
  agent_name,
  product,
  brand_account_name,
  company_code,
  company_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  cpc_direct_sales_code,
  cpc_direct_sales_name,
  cpc_direct_sales_dept1_name,
  cpc_direct_sales_dept2_name,
  cpc_direct_sales_dept3_name,
  cpc_direct_sales_dept4_name,
  cpc_direct_sales_dept5_name,
  cpc_direct_sales_dept6_name,
  brand_tag_name,
  brand_group_tag_name,
  cpc_operator_code,
  cpc_operator_name,
  cpc_operator_dept1_name,
  cpc_operator_dept2_name,
  cpc_operator_dept3_name,
  cpc_operator_dept4_name,
  cpc_operator_dept5_name,
  cpc_operator_dept6_name,
  build_type,
  biz_product_type,
  --target_type_msg_list,
  marketing_target,
  optimize_target,
  optimize_target_msg,
  publish_time,
  type,
  is_brand,
  is_cps_note,
  is_bind,
  click_cnt,
  imp_cnt,
  cost,
  cash_cost,
  purchase_order_num,
  click_rgmv_7d,
  engage_cnt,
  note_num,
  new_note_num,
  cost_spu_cnt,
  campaign_create_date,
  campaign_start_date,
  note_first_cost_date,
  cam.v_seller_id as virtual_seller_id,
  dim.virtual_seller_name,
  cam.advertiser_id,
  note_first_cost_date as note_ecm_first_cost_date,
  campaign_ecm_first_cost_date,
  campaign_first_cost_date,
  cost_note_num,
  ecm_closed_cost_note_num,
  dtm
from 
(
select
  coalesce(t1.brand_account_id, t2.brand_account_id) as brand_account_id,
  note_id,
  unit_id,
  campaign_id,
  tag,
  campaign_name,
  campaign_placement,
  agent_user_id,
  agent_name,
  product,
  brand_account_name,
  company_code,
  company_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  cpc_direct_sales_code,
  cpc_direct_sales_name,
  cpc_direct_sales_dept1_name,
  cpc_direct_sales_dept2_name,
  cpc_direct_sales_dept3_name,
  cpc_direct_sales_dept4_name,
  cpc_direct_sales_dept5_name,
  cpc_direct_sales_dept6_name,
  brand_tag_name,
  brand_group_tag_name,
  cpc_operator_code,
  cpc_operator_name,
  cpc_operator_dept1_name,
  cpc_operator_dept2_name,
  cpc_operator_dept3_name,
  cpc_operator_dept4_name,
  cpc_operator_dept5_name,
  cpc_operator_dept6_name,
  build_type,
  biz_product_type,
  -- target_type_msg_list,
  marketing_target,
  optimize_target,
  optimize_target_msg,
  publish_time,
  type,
  coalesce(is_brand, 0) as is_brand,
  coalesce(is_cps_note, 0) as is_cps_note,
  coalesce(is_bind, 0) as is_bind,
  sum(unique_click_cnt) as click_cnt,
  sum(unique_imp_cnt) as imp_cnt,
  sum(
    case
      when marketing_target in (3, 8, 15) then total_amount
      else 0
    end
  ) as cost,
  sum(cost_amount) as cash_cost,
  sum(
    case
      when marketing_target in (3, 8, 15) then purchase_order_num
      else 0
    end
  ) as purchase_order_num,
  sum(
    case
      when marketing_target in (3, 8, 15) then click_rgmv_7d
      else 0
    end
  ) as click_rgmv_7d,
  sum(engage_cnt) as engage_cnt,
  0 as note_num,
  0 as new_note_num,
  0 as cost_spu_cnt,
  0 as cost_note_num,
  0 as ecm_closed_cost_note_num,
  dtm
from
  (
    SELECT
      campaign_id,
      case
        when ads_material_type = 'post' then ads_material_id
        else null
      end as note_id,
      brand_account_id,
      agent_user_id,
      agent_name,
      --brand_account_name,
      -- dmp_group_id,
      -- campaign_build_type,
      product,
      build_type,
      biz_product_type,
      target_type_msg_list,
      marketing_target,
      optimize_target,
      optimize_target_msg,
      unique_click_cnt,
      unique_imp_cnt,
      total_amount,
      cost_amount,
      coalesce(purchase_order_num, 0) + coalesce(mini_purchase_order_num, 0) as purchase_order_num,
      purchase_rgmv,
      mini_purchase_rgmv,
      coalesce(purchase_rgmv, 0) + coalesce(mini_purchase_rgmv, 0) as click_rgmv_7d,
      like_cnt,
      fav_cnt,
      cmt_cnt,
      follow_cnt,
      share_cnt,
      unit_id,
      0 as tag,
      campaign_name,
      campaign_placement,
      coalesce(like_cnt, 0) + coalesce(cmt_cnt, 0) + coalesce(fav_cnt, 0) + coalesce(follow_cnt, 0) + coalesce(share_cnt, 0) as engage_cnt,
      dtm
    FROM
      redcdm.dm_ads_rtb_creativity_1d_di
    WHERE
      dtm between f_getdate('{{ds_nodash}}', -7) and '{{ds_nodash}}'
      and (
        is_effective = 1
        or total_amount > 0
      )
      and marketing_target in (3, 8, 14, 15)
  ) t1
  left join (
    select
      brand_account_id,
      brand_account_name,
      company_code,
      company_name,
      group_code,
      group_name,
      track_group_id,
      track_group_name,
      track_industry_name,
      track_detail_name,
      cpc_direct_sales_code,
      cpc_direct_sales_name,
      cpc_direct_sales_dept1_code,
      cpc_direct_sales_dept1_name,
      cpc_direct_sales_dept2_code,
      cpc_direct_sales_dept2_name,
      cpc_direct_sales_dept3_code,
      cpc_direct_sales_dept3_name,
      cpc_direct_sales_dept4_code,
      cpc_direct_sales_dept4_name,
      cpc_direct_sales_dept5_code,
      cpc_direct_sales_dept5_name,
      cpc_direct_sales_dept6_code,
      cpc_direct_sales_dept6_name,
      brand_tag_code,
      brand_tag_name,
      brand_group_tag_code,
      brand_group_tag_name,
      cpc_operator_code,
      cpc_operator_name,
      cpc_operator_dept1_code,
      cpc_operator_dept1_name,
      cpc_operator_dept2_code,
      cpc_operator_dept2_name,
      cpc_operator_dept3_code,
      cpc_operator_dept3_name,
      cpc_operator_dept4_code,
      cpc_operator_dept4_name,
      cpc_operator_dept5_code,
      cpc_operator_dept5_name,
      cpc_operator_dept6_code,
      cpc_operator_dept6_name
    from
      redcdm.dim_ads_industry_brand_account_df
    where
      dtm = max_dtm('redcdm.dim_ads_industry_brand_account_df')
  ) t2 on t1.brand_account_id = t2.brand_account_id
  left join (
    select
      discovery_id,
      user_id,
      substring(publish_time, 1, 10) as publish_time,
      type,
      is_brand,
      is_cps_note,
      is_bind
    from
      reddw.dw_soc_discovery_delta_7_day
    where
      dtm = '{{ds_nodash}}'
  ) note on note.discovery_id = t1.note_id
group by
  coalesce(t1.brand_account_id, t2.brand_account_id),
  t2.brand_account_name,
  company_code,
  company_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  cpc_direct_sales_code,
  cpc_direct_sales_name,
  cpc_direct_sales_dept1_name,
  cpc_direct_sales_dept2_name,
  cpc_direct_sales_dept3_name,
  cpc_direct_sales_dept4_name,
  cpc_direct_sales_dept5_name,
  cpc_direct_sales_dept6_name,
  brand_tag_name,
  brand_group_tag_name,
  cpc_operator_code,
  cpc_operator_name,
  cpc_operator_dept1_name,
  cpc_operator_dept2_name,
  cpc_operator_dept3_name,
  cpc_operator_dept4_name,
  cpc_operator_dept5_name,
  cpc_operator_dept6_name,
  campaign_id,
  note_id,
  agent_user_id,
  agent_name,
  product,
  build_type,
  biz_product_type,
  --target_type_msg_list,
  marketing_target,
  optimize_target,
  optimize_target_msg,
  publish_time,
  type,
  coalesce(is_brand, 0),
  coalesce(is_cps_note, 0),
  coalesce(is_bind, 0),
  tag,
  campaign_name,
  campaign_placement,
  unit_id,
  dtm
union all
--企业号粒度总的投放笔记
select
  brand_account_id,
  note_id,
  unit_id,
  campaign_id,
  tag,
  campaign_name,
  campaign_placement,
  agent_user_id,
  agent_name,
  product,
  brand_account_name,
  company_code,
  company_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  cpc_direct_sales_code,
  cpc_direct_sales_name,
  cpc_direct_sales_dept1_name,
  cpc_direct_sales_dept2_name,
  cpc_direct_sales_dept3_name,
  cpc_direct_sales_dept4_name,
  cpc_direct_sales_dept5_name,
  cpc_direct_sales_dept6_name,
  brand_tag_name,
  brand_group_tag_name,
  cpc_operator_code,
  cpc_operator_name,
  cpc_operator_dept1_name,
  cpc_operator_dept2_name,
  cpc_operator_dept3_name,
  cpc_operator_dept4_name,
  cpc_operator_dept5_name,
  cpc_operator_dept6_name,
  build_type,
  biz_product_type,
  --target_type_msg_list,
  marketing_target,
  optimize_target,
  optimize_target_msg,
  publish_time,
  type,
  coalesce(is_brand, 0) as is_brand,
  coalesce(is_cps_note, 0) as is_cps_note,
  coalesce(is_bind, 0) as is_bind,
  click_cnt,
  imp_cnt,
  cost,
  cash_cost,
  purchase_order_num,
  click_rgmv_7d,
  engage_cnt,
  note_num,
  new_note_num,
  cost_spu_cnt,
  cost_note_num,
  ecm_closed_cost_note_num,
  dtm
from
  temp.temp_app_ads_ecm_note_spu_{{ds_nodash}}_1
  )t1 
  left join 
  --计划创建时间
  (select id,
    from_unixtime(floor(create_time / 1000) + 28800, 'yyyy-MM-dd') as campaign_create_date,
    from_unixtime(floor(start_time / 1000) + 28800, 'yyyy-MM-dd') as campaign_start_date,
    v_seller_id,
    advertiser_id
  from redcdm.dwd_ads_rtb_campaign_df
  where
    dtm = greatest('{{ds_nodash}}', '20231205')
  )cam 
  on cam.id = t1.campaign_id
  left join 
  (select virtual_seller_id,rtb_advertiser_id,virtual_seller_name
  from redcdm.dim_ads_advertiser_df 
  where dtm=max_dtm('redcdm.dim_ads_advertiser_df')
  )dim 
  on dim.virtual_seller_id=cam.v_seller_id
  left join 
  (--笔记首次效果消耗时间
  select  ads_material_id as note_id,f_getdate(min(dtm)) as note_first_cost_date
  from 
      redcdm.dm_ads_rtb_creativity_1d_di
    WHERE
      dtm>='20230101' and dtm<='{{ds_nodash}}'
      and (
        is_effective = 1
        or total_amount > 0
      )
      and marketing_target in (3, 8, 14, 15)
      and ads_material_type = 'post'
      and total_amount>0
      group by ads_material_id 
      )cost 
      on cost.note_id = t1.note_id
left join 
  (--笔记首次效果消耗时间
  select  campaign_id,f_getdate(min(dtm)) as campaign_first_cost_date
  ,f_getdate(min(case when marketing_target in (3, 8, 14, 15) then dtm else '9999-12-31' end)) as campaign_ecm_first_cost_date
  from 
      redcdm.dm_ads_rtb_creativity_1d_di
    WHERE
      dtm>='20230101' and dtm<='{{ds_nodash}}'
      and (
        is_effective = 1
        or total_amount > 0
      )
     -- and marketing_target in (3, 8, 14, 15)
      --and ads_material_type = 'post'
      and total_amount>0
      group by campaign_id
      )cam_cost 
      on cam_cost.campaign_id = t1.campaign_id