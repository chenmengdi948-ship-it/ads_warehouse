
    -- ************************************************
    -- Author: chenmengdi
    -- CreateTime:2023-11-22T19:34:40+08:00
    -- Update: Task Update Description
    -- ************************************************
drop table
  if exists temp.temp_app_ads_industry_ecm_metric_cube_df_{{ds_nodash}};
create table
  temp.temp_app_ads_industry_ecm_metric_cube_df_{{ds_nodash}} as
select coalesce(detail.dtm,rtb.dtm) as dtm,
  coalesce(detail.brand_account_id,rtb.brand_account_id) as brand_account_id,
  coalesce(detail.shop_user_type,rtb.shop_user_type) as shop_user_type,
  coalesce(detail.process_track_industry_name,rtb.process_track_industry_name) as process_track_industry_name,
  coalesce(detail.fans_level,rtb.fans_level) as fans_level,
  coalesce(detail.ti_cash_cost_180d_type,rtb.ti_cash_cost_180d_type) as ti_cash_cost_180d_type,
  coalesce(detail.account_ecm_type,rtb.account_ecm_type) as account_ecm_type,
  coalesce(detail.company_id,rtb.company_id) as company_id,
  -- ds_deal_gmv,
  -- ds_ads_deal_gmv,
  -- seller_cnt,
  -- k_deal_gmv,
  -- s_deal_gmv,
  -- note_deal_gmv,
  -- msg_deal_gmv,
  -- search_deal_gmv,
  --当月有电商投放的店铺gmv
  sum(ecm_ds_deal_gmv) as ecm_ds_deal_gmv,
  sum(ecm_ds_ads_deal_gmv) as ecm_ds_ads_deal_gmv,
  sum(ecm_seller_cnt) as ecm_seller_cnt,
  sum(ecm_k_deal_gmv) as ecm_k_deal_gmv,
  sum(ecm_s_deal_gmv) as ecm_s_deal_gmv,
  sum(ecm_note_deal_gmv) as ecm_note_deal_gmv,
  sum(ecm_msg_deal_gmv) as ecm_msg_deal_gmv,
  sum(ecm_search_deal_gmv) as ecm_search_deal_gmv,
  sum(cash_cost) as cash_cost,
  sum(cost) as cost,
  sum(click_rgmv_7d) as click_rgmv_7d,
  sum(rtb_double_cost) as rtb_double_cost,
  sum(rtb_double_cash_cost) as rtb_double_cash_cost,
  sum(imp_cnt) as imp_cnt,
  sum(click_cnt) as click_cnt,
  sum(rtb_double_imp_cnt) as rtb_double_imp_cnt,
  sum(rtb_double_click_cnt) as rtb_double_click_cnt
from 
(
select
  a.dtm,
  a.brand_account_id,
  --a.seller_id,
  coalesce(shop_user_type, '其他') as shop_user_type,
  company_id,
  process_track_industry_name,
  fans_level,
  ti_cash_cost_180d_type,
  account_ecm_type,
  -- ds_deal_gmv,
  -- ds_ads_deal_gmv,
  -- seller_cnt,
  -- k_deal_gmv,
  -- s_deal_gmv,
  -- note_deal_gmv,
  -- msg_deal_gmv,
  -- search_deal_gmv,
  --当月有电商投放的店铺gmv
  case
    when w1.brand_account_id is not null then ds_deal_gmv
    else 0
  end as ecm_ds_deal_gmv,
  case
    when w1.brand_account_id is not null then ds_ads_deal_gmv
    else 0
  end as ecm_ds_ads_deal_gmv,
  case
    when w1.brand_account_id is not null then seller_cnt
    else 0
  end as ecm_seller_cnt,
  case
    when w1.brand_account_id is not null then k_deal_gmv
    else 0
  end as ecm_k_deal_gmv,
  case
    when w1.brand_account_id is not null then s_deal_gmv
    else 0
  end as ecm_s_deal_gmv,
  case
    when w1.brand_account_id is not null then note_deal_gmv
    else 0
  end as ecm_note_deal_gmv,
  case
    when w1.brand_account_id is not null then msg_deal_gmv
    else 0
  end as ecm_msg_deal_gmv,
  case
    when w1.brand_account_id is not null then search_deal_gmv
    else 0
  end as ecm_search_deal_gmv
from
  (
    select
      dtm,
      substring(f_getdate (dtm), 1, 7) as stat_month,
      coalesce(brand_account_id,'999') as brand_account_id,
      --seller_id,
      sum(deal_gmv) as ds_deal_gmv,
      sum(if(traffic_type = '广告', deal_gmv, 0)) as ds_ads_deal_gmv,
      count(distinct if(rgmv > 0, seller_id, null)) as seller_cnt,
      sum(if(channel = 'K播', deal_gmv, 0)) as k_deal_gmv,
      sum(if(channel = '店播', deal_gmv, 0)) as s_deal_gmv,
      sum(if(channel_group = '笔记', deal_gmv, 0)) as note_deal_gmv,
      sum(if(channel_group = '私信群聊', deal_gmv, 0)) as msg_deal_gmv,
      sum(if(channel_group = '搜索', deal_gmv, 0)) as search_deal_gmv
    from
      redapp.app_ads_trd_user_seller_account_detail_df
    where
      dtm >= '20230601'
    group by
      dtm,
      brand_account_id
  ) a
  -- left join (
  --   select dtm,
  --     seller_id,
  --     coalesce(shop_user_type, '其他') as shop_user_type
  --   from
  --     redapp.app_ads_trd_seller_cvr_detail_1d_di
  --   where
  --    dtm >= '20230601'
  --   group by dtm,
  --     seller_id,
  --     shop_user_type
  -- ) seller on seller.seller_id = a.seller_id and seller.dtm = a.dtm
   join --有电商投放
  (
    select
      stat_month,
      brand_account_id,
      company_code,
      company_id,
      track_industry_name,
      process_track_industry_name,
      fans_num,
      fans_level,
      cash_cost_180d,
      ti_cash_cost_180d_type,
      ti_ecm_closed_cost,
      ecm_closed_cash_cost,
      other_cash_cost,
      account_ecm_type,
      shop_user_type
    from
      redapp.app_ads_industry_account_ecm_type_df
    where
      dtm = '{{ds_nodash}}'
      and ecm_closed_cash_cost > 0 --有闭环电商投放
  ) w1 on a.stat_month = w1.stat_month
  and a.brand_account_id = w1.brand_account_id
  )detail
full outer join 
(select
  dtm,
  t1.brand_account_id,
  company_id,
  shop_user_type,
  process_track_industry_name,
  fans_level,
  ti_cash_cost_180d_type,
  account_ecm_type,
  cash_cost,
  cost,
  click_rgmv_7d,
  rtb_double_cost,
  rtb_double_cash_cost,
  imp_cnt,
  click_cnt,
  rtb_double_imp_cnt,
  rtb_double_click_cnt
from
  (
    select
      dtm,
      brand_account_id,
      substring(f_getdate (dtm), 1, 7) as stat_month,
      sum(cash_cost) as cash_cost,
      sum(cost) as cost,
      sum(7d_click_rgmv) as click_rgmv_7d,
      sum(
        case
          when module in ('发现feed', '搜索feed') then cost
          else 0
        end
      ) as rtb_double_cost,
      sum(
        case
          when module in ('发现feed', '搜索feed') then cash_cost
          else 0
        end
      ) as rtb_double_cash_cost,
      sum(imp_num) as imp_cnt,
      sum(click_num) as click_cnt,
      sum(
        case
          when module in ('发现feed', '搜索feed') then imp_num
          else 0
        end
      ) as rtb_double_imp_cnt,
      sum(
        case
          when module in ('发现feed', '搜索feed') then click_num
          else 0
        end
      ) as rtb_double_click_cnt
    from
      redst.st_ads_wide_cpc_creativity_day_inc
    where
      dtm >= '20230601'
      and marketing_target in (3, 8, 14, 15)
    group by
      brand_account_id,
      dtm
  ) t1
  join (
    select
      stat_month,
      brand_account_id,
      company_code,
      company_id,
      track_industry_name,
      process_track_industry_name,
      fans_num,
      fans_level,
      cash_cost_180d,
      ti_cash_cost_180d_type,
      ti_ecm_closed_cost,
      ecm_closed_cash_cost,
      other_cash_cost,
      account_ecm_type ,shop_user_type
    from
      redapp.app_ads_industry_account_ecm_type_df
    where
      dtm = '{{ds_nodash}}' -- and ecm_closed_cash_cost > 0 --有闭环电商投放
  ) w1 on t1.stat_month = w1.stat_month
  and t1.brand_account_id = w1.brand_account_id
  )rtb 
on rtb.brand_account_id = detail.brand_account_id
and rtb.dtm = detail.dtm
group by 1,2,3,4,5,6,7,8;

drop table
  if exists temp.temp_app_ads_industry_ecm_metric_cube_df_11_{{ds_nodash}};
create table
  temp.temp_app_ads_industry_ecm_metric_cube_df_11_{{ds_nodash}} as
  select 
 coalesce(process_track_industry_name,'整体') as process_track_industry_name,
  coalesce(fans_level,'整体') as fans_level,
  coalesce(ti_cash_cost_180d_type,'整体') as ti_cash_cost_180d_type,
  coalesce(account_ecm_type,'整体') as account_ecm_type,
  coalesce(shop_user_type,'整体') as shop_user_type,
  grouping__id as groups,
-- sum(ds_deal_gmv) as ds_deal_gmv,
-- sum(ds_ads_deal_gmv) as ds_ads_deal_gmv,
-- sum(seller_cnt) as seller_cnt,
-- sum(k_deal_gmv) as k_deal_gmv,
-- sum(s_deal_gmv) as s_deal_gmv,
-- sum(note_deal_gmv) as note_deal_gmv,
-- sum(msg_deal_gmv) as msg_deal_gmv,
-- sum(search_deal_gmv) as search_deal_gmv,
sum(ecm_ds_deal_gmv) as ecm_ds_deal_gmv,
sum(ecm_ds_ads_deal_gmv) as ecm_ds_ads_deal_gmv,
sum(ecm_seller_cnt) as ecm_seller_cnt,
sum(ecm_k_deal_gmv) as ecm_k_deal_gmv,
sum(ecm_s_deal_gmv) as ecm_s_deal_gmv,
sum(ecm_note_deal_gmv) as ecm_note_deal_gmv,
sum(ecm_msg_deal_gmv) as ecm_msg_deal_gmv,
sum(ecm_search_deal_gmv) as ecm_search_deal_gmv,
sum(cash_cost) as cash_cost,
sum(cost) as cost,
sum(click_rgmv_7d) as click_rgmv_7d,
sum(rtb_double_cost) as rtb_double_cost,
sum(rtb_double_cash_cost) as rtb_double_cash_cost,
sum(imp_cnt) as imp_cnt,
sum(click_cnt) as click_cnt,
sum(rtb_double_imp_cnt) as rtb_double_imp_cnt,
sum(rtb_double_click_cnt) as rtb_double_click_cnt,
count(distinct  case when cash_cost>0 then company_id else null end) as company_cnt,
count(distinct case when cash_cost>0 then brand_account_id else null end) as account_cnt,
dtm
from temp.temp_app_ads_industry_ecm_metric_cube_df_{{ds_nodash}} a
group by
  dtm,
  process_track_industry_name,
  fans_level,
  ti_cash_cost_180d_type,
  account_ecm_type,
  shop_user_type
grouping sets((a.dtm),
 (a.dtm, shop_user_type),
 (a.dtm,process_track_industry_name),
 (a.dtm,process_track_industry_name, shop_user_type),
 (a.dtm,fans_level),
 (a.dtm, fans_level,shop_user_type),
 (a.dtm,account_ecm_type),
 (a.dtm, account_ecm_type,shop_user_type),
 (a.dtm,account_ecm_type,ti_cash_cost_180d_type),
 (a.dtm, account_ecm_type,ti_cash_cost_180d_type,shop_user_type),
 (a.dtm,process_track_industry_name, account_ecm_type),
 (a.dtm, process_track_industry_name, account_ecm_type,shop_user_type),
 (a.dtm,process_track_industry_name, account_ecm_type,ti_cash_cost_180d_type),
 (a.dtm, process_track_industry_name, account_ecm_type,ti_cash_cost_180d_type,shop_user_type)
 )
;


--电商total
drop table
  if exists temp.temp_app_ads_industry_ecm_metric_cube_df_{{ds_nodash}}_total;
create table
  temp.temp_app_ads_industry_ecm_metric_cube_df_{{ds_nodash}}_total as
select
  a.dtm,
  a.brand_account_id,
  a.seller_id,
  coalesce(shop_user_type, '其他') as shop_user_type,
  '整体' as process_track_industry_name,
  '整体' as fans_level,
  '整体' as ti_cash_cost_180d_type,
  '整体' as account_ecm_type,
  ds_deal_gmv,
  ds_ads_deal_gmv,
  seller_cnt,
  k_deal_gmv,
  s_deal_gmv,
  note_deal_gmv,
  msg_deal_gmv,
  search_deal_gmv,
  rgmv
  --当月有电商投放的店铺gmv
  
from
  (
    select
      dtm,
      substring(f_getdate (dtm), 1, 7) as stat_month,
      coalesce(brand_account_id,'999') as brand_account_id,
      seller_id,
      sum(deal_gmv) as ds_deal_gmv,
      sum(if(traffic_type = '广告', deal_gmv, 0)) as ds_ads_deal_gmv,
      count(distinct if(rgmv > 0, seller_id, null)) as seller_cnt,
      sum(if(channel = 'K播', deal_gmv, 0)) as k_deal_gmv,
      sum(if(channel = '店播', deal_gmv, 0)) as s_deal_gmv,
      sum(if(channel_group = '笔记', deal_gmv, 0)) as note_deal_gmv,
      sum(if(channel_group = '私信群聊', deal_gmv, 0)) as msg_deal_gmv,
      sum(if(channel_group = '搜索', deal_gmv, 0)) as search_deal_gmv,
      sum(rgmv) as rgmv
    from
      redapp.app_ads_trd_user_seller_account_detail_df
    where
      dtm >= '20230601'
    group by
      dtm,
      brand_account_id,
      seller_id
  ) a
  left join (
    select dtm,
      seller_id,
      coalesce(shop_user_type, '其他') as shop_user_type
    from
      redapp.app_ads_trd_seller_cvr_detail_1d_di
    where
     dtm >= '20230601'
    group by dtm,
      seller_id,
      shop_user_type
  ) seller on seller.seller_id = a.seller_id and seller.dtm = a.dtm
  
;

drop table
  if exists temp.temp_app_ads_industry_ecm_metric_cube_df_11_{{ds_nodash}}_total;
create table
  temp.temp_app_ads_industry_ecm_metric_cube_df_11_{{ds_nodash}}_total as
  select 
    '整体' as process_track_industry_name,
    '整体' as fans_level,
    '整体' as ti_cash_cost_180d_type,
    '整体' as account_ecm_type,
    coalesce(shop_user_type,'整体') as shop_user_type,
    grouping__id as groups,
    sum(ds_deal_gmv) as ds_deal_gmv,
    sum(ds_ads_deal_gmv) as ds_ads_deal_gmv,
    count(distinct if(rgmv > 0, seller_id, null)) as seller_cnt,
    sum(k_deal_gmv) as k_deal_gmv,
    sum(s_deal_gmv) as s_deal_gmv,
    sum(note_deal_gmv) as note_deal_gmv,
    sum(msg_deal_gmv) as msg_deal_gmv,
    sum(search_deal_gmv) as search_deal_gmv,
    dtm
from temp.temp_app_ads_industry_ecm_metric_cube_df_{{ds_nodash}} a
 group by
      a.dtm,
      shop_user_type grouping sets ((a.dtm), (a.dtm, shop_user_type))
;
insert overwrite table redapp.app_ads_industry_ecm_metric_cube_df    partition( dtm = '{{ds_nodash}}')
select 
    coalesce(t1.date_key,t2.date_key) as date_key,
    coalesce(t1.process_track_industry_name,t2.process_track_industry_name) as process_track_industry_name,
    coalesce(t1.fans_level,t2.fans_level) as fans_level,
    coalesce(t1.ti_cash_cost_180d_type,t2.ti_cash_cost_180d_type) as ti_cash_cost_180d_type,
    coalesce(t1.account_ecm_type,t2.account_ecm_type) as account_ecm_type,
    coalesce(t1.shop_user_type,t2.shop_user_type) as shop_user_type,
    t1.groups,
    ds_deal_gmv,
    ds_ads_deal_gmv,
    seller_cnt,
    k_deal_gmv,
    s_deal_gmv,
    note_deal_gmv,
    msg_deal_gmv,
    search_deal_gmv,
    ecm_ds_deal_gmv,
    ecm_ds_ads_deal_gmv,
    ecm_seller_cnt,
    ecm_k_deal_gmv,
    ecm_s_deal_gmv,
    ecm_note_deal_gmv,
    ecm_msg_deal_gmv,
    ecm_search_deal_gmv,
    cash_cost,
    cost,
    click_rgmv_7d,
    rtb_double_cost,
    rtb_double_cash_cost,
    imp_cnt,
    click_cnt,
    rtb_double_imp_cnt,
    rtb_double_click_cnt,
    company_cnt,
    account_cnt,
    buyer_cnt,
    new_platform_user_cnt,
    new_seller_user_cnt,
    total_buyer_cnt,
    total_new_platform_user_cnt,
    total_new_seller_user_cnt,
    ds_buyer_cnt,
    ds_new_platform_user_cnt,
    ds_new_seller_user_cnt
from 
(select *,
    f_getdate(dtm) as date_key
from temp.temp_app_ads_industry_ecm_metric_cube_df_11_{{ds_nodash}}
)t1
left join 
(select *,
    f_getdate(dtm) as date_key
from temp.temp_app_ads_industry_ecm_metric_cube_df_11_{{ds_nodash}}_total
)t3 on t1.date_key = t3.date_key 
and t1.process_track_industry_name=t3.process_track_industry_name and 
  t1.fans_level=t3.fans_level and 
  t1.ti_cash_cost_180d_type=t3.ti_cash_cost_180d_type and 
  t1.account_ecm_type=t3.account_ecm_type and 
  t1.shop_user_type=t3.shop_user_type

full outer join 
(select *,
    f_getdate(dtm) as date_key
from redapp.app_ads_industry_ecm_user_di
where dtm>='20230601'
)t2
on t1.date_key = t2.date_key 
and t1.process_track_industry_name=t2.process_track_industry_name and 
  t1.fans_level=t2.fans_level and 
  t1.ti_cash_cost_180d_type=t2.ti_cash_cost_180d_type and 
  t1.account_ecm_type=t2.account_ecm_type and 
  t1.shop_user_type=t2.shop_user_type
