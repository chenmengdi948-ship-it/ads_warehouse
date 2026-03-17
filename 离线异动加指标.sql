
    -- ************************************************
    -- Author: chenmengdi
    -- CreateTime:2024-01-30T20:52:59+08:00
    -- Update: Task Update Description
    -- ************************************************
drop table if exists temp.advertiser_campaign_alias{{ds_nodash}}_1;
create table temp.advertiser_campaign_alias{{ds_nodash}}_1 as 
select t1.creativity_id,
  coalesce(t1.campaign_id,t4.id) as campaign_id,
  coalesce(t1.advertiser_id,t4.advertiser_id) as advertiser_id,
  t1.note_id,
  ads_material_type,
  t1.cost,
  first_cost_date,
  first_ecm_cost_date,
  note_first_cost_date,
  note_first_ecm_cost_date,
  create_date,
  coalesce(t1.marketing_target,t4.marketing_target) as marketing_target,
  adv.brand_account_id
from 
--昨日有消耗的所有创意
(select creativity_id,
    campaign_id, 
    note_id,
    ads_material_type,
    advertiser_id,
    income_amt as cost,
    marketing_target
from redapp.app_ads_industry_rtb_creativity_di 
where dtm='{{ds_nodash}}' and income_amt>0
)t1
left join 
  --创意首次投放时间
  (select   campaign_id,min(case when first_cost_date='9999-12-31' then null else first_cost_date end) as first_cost_date,
    min(case when marketing_target in (3,8,14,15) and first_cost_date<>'9999-12-31' then first_cost_date else null  end) as first_ecm_cost_date
  FROM
    redapp.app_ads_industry_rtb_creativity_di
  WHERE
    dtm = max_dtm('redapp.app_ads_industry_rtb_creativity_di') 
    --and marketing_target in (3,8,14,15)
    group by 1
    )t2 
  on t1.campaign_id =cast( t2.campaign_id as string)
  left join 
  --笔记首次投放时间
  (select   note_id,min(case when first_cost_date='9999-12-31' then null else first_cost_date end) as note_first_cost_date,
     min(case when marketing_target in (3,8,14,15) and first_cost_date<>'9999-12-31' then first_cost_date else null  end) as note_first_ecm_cost_date
  FROM
    redapp.app_ads_industry_rtb_creativity_di
  WHERE
    dtm = max_dtm('redapp.app_ads_industry_rtb_creativity_di') 
    --and marketing_target in (3,8,14,15)
    group by 1
    )t3
on t1.note_id =t3.note_id
full outer join 
--计划创建时间
(select id,
    from_unixtime(floor(ca.create_time / 1000 + 28800), 'yyyy-MM-dd') as create_date,
    from_unixtime(floor(ca.start_time / 1000 + 28800), 'yyyy-MM-dd') as start_dt,
    from_unixtime(floor(ca.start_time / 1000 + 28800), 'yyyyMMdd') as start_dtm,
    advertiser_id,
    marketing_target,
    dtm
from  redcdm.dwd_ads_rtb_campaign_df ca
where dtm =  '{{ds_nodash}}' and platform in (0,1,2,7)
--and marketing_target in (3,8,14,15)
and  from_unixtime(floor(ca.create_time / 1000 + 28800), 'yyyy-MM-dd')='{{ds}}'
)t4
on t1.campaign_id=t4.id
left join 
(select rtb_advertiser_id,brand_account_id
  from redcdm.dim_ads_advertiser_df 
  where dtm='{{ds_nodash}}'
  )adv 
on adv.rtb_advertiser_id = coalesce(t1.advertiser_id,t4.advertiser_id);

--2024新增昨日有roi今日停投的计划list 
drop table if exists temp.advertiser_campaign_roi_alias{{ds_nodash}}_1;
create table temp.advertiser_campaign_roi_alias{{ds_nodash}}_1 as 
select
  t1.campaign_id as campaign_id,
  t1.brand_account_id as brand_account_id,
  t1.cost,
  t1.ystd_cost as ystd_cost_1d,
  t1.ystd_roi,
  t1.ystd_cost,
  t1.ystd_rgmv,
  if(t1.ystd_roi>0.6 and (coalesce(t1.cost,0)-coalesce(t1.ystd_cost,0))/coalesce(t1.ystd_cost,0)<-0.6,1,0) as roi_1,
  if(t1.ystd_roi>0.8 and (coalesce(t1.cost,0)-coalesce(t1.ystd_cost,0))/coalesce(t1.ystd_cost,0)<-0.6,1,0) as roi_2, 
  if(t1.ystd_roi>1.2 and (coalesce(t1.cost,0)-coalesce(t1.ystd_cost,0))/coalesce(t1.ystd_cost,0)<-0.6,1,0) as roi_3,
  if(t1.ystd_roi>1.5 and (coalesce(t1.cost,0)-coalesce(t1.ystd_cost,0))/coalesce(t1.ystd_cost,0)<-0.6,1,0) as roi_4, 
  if(t1.ystd_roi>2 and (coalesce(t1.cost,0)-coalesce(t1.ystd_cost,0))/coalesce(t1.ystd_cost,0)<-0.6,1,0) as roi_5
from 
  --计划昨日roi
  (select   campaign_id,
  brand_account_id,
    sum(case when dtm='{{ds_nodash}}' and marketing_target in (3,8,15) then income_amt else 0 end) as cost,
   --昨天roi 
   sum(case when dtm= f_getdate('{{ds_nodash}}',-1)  and marketing_target in (3,8,15) then income_amt else 0 end) as ystd_cost,
   sum(case when dtm= f_getdate('{{ds_nodash}}',-1)  and marketing_target in (3,8,15) then purchase_rgmv else 0 end) as ystd_rgmv,
   sum(case when dtm= f_getdate('{{ds_nodash}}',-1)  and marketing_target in (3,8,15) then purchase_rgmv else 0 end)/sum(case when dtm= f_getdate('{{ds_nodash}}',-1)  and marketing_target in (3,8,15) then income_amt else 0 end) as ystd_roi
  FROM
    redapp.app_ads_industry_rtb_creativity_di
  WHERE
    dtm>= f_getdate('{{ds_nodash}}',-1) and dtm<='{{ds_nodash}}'
    and marketing_target in (3,8,14,15)
    group by 1,2
    )t1
  ;
insert overwrite table redapp.app_ads_insight_account_diagnosis_campaign_process_di partition(dtm = '{{ ds_nodash }}') 
select coalesce(t11.brand_account_id,t3.brand_account_id) as brand_account_id,
    create_campaign_cnt,
    ecm_create_campaign_cnt,
    create_cost_campaign_cnt,
    cost_campaign_cnt,
    cost_note_cnt,
    ecm_cost_campaign_cnt,
    ecm_cost_note_cnt,
    new_cost_note_cnt,
    ecm_new_cost_note_cnt,
    roi_1_campaign_list,
    roi_2_campaign_list,
    roi_3_campaign_list,
    roi_4_campaign_list,
    roi_5_campaign_list,
    process_list
from 
(select coalesce(t1.brand_account_id,t2.brand_account_id) as brand_account_id,
    create_campaign_cnt,
    ecm_create_campaign_cnt,
    create_cost_campaign_cnt,
    cost_campaign_cnt,
    cost_note_cnt,
    ecm_cost_campaign_cnt,
    ecm_cost_note_cnt,
    new_cost_note_cnt,
    ecm_new_cost_note_cnt,
    roi_1_campaign_list,
    roi_2_campaign_list,
    roi_3_campaign_list,
    roi_4_campaign_list,
    roi_5_campaign_list
from
(select brand_account_id,
concat_ws(',',collect_set(if(roi_1 = 1 ,campaign_id,null))) as roi_1_campaign_list,
concat_ws(',',collect_set(if(roi_2 = 1 ,campaign_id,null))) as roi_2_campaign_list,
concat_ws(',',collect_set(if(roi_3 = 1 ,campaign_id,null))) as roi_3_campaign_list,
concat_ws(',',collect_set(if(roi_4 = 1 ,campaign_id,null))) as roi_4_campaign_list,
concat_ws(',',collect_set(if(roi_5 = 1 ,campaign_id,null))) as roi_5_campaign_list
from temp.advertiser_campaign_roi_alias{{ds_nodash}}_1
group by brand_account_id
)t1
full outer join 
(
select brand_account_id,
    count(distinct case when create_date is not null then campaign_id else null end) as create_campaign_cnt,
    count(distinct case when create_date is not null and marketing_target in (3,8,14,15) then campaign_id else null end) as ecm_create_campaign_cnt,
    count(distinct case when first_cost_date ='{{ds}}' and cost>0 and create_date is not null then campaign_id else null end) as create_cost_campaign_cnt,

    count(distinct case when cost>0 then campaign_id end) as cost_campaign_cnt,
    count(distinct case when cost>0 then note_id end) as cost_note_cnt,
    count(distinct case when cost>0 and marketing_target in (3,8,14,15) then campaign_id end) as ecm_cost_campaign_cnt,
    count(distinct case when cost>0 and marketing_target in (3,8,14,15) then note_id end) as ecm_cost_note_cnt,
    count(distinct case when note_first_cost_date  ='{{ds}}' and cost>0 then note_id else null end) as new_cost_note_cnt,
    count(distinct case when note_first_ecm_cost_date='{{ds}}' and cost>0 then note_id else null end) as ecm_new_cost_note_cnt
from temp.advertiser_campaign_alias{{ds_nodash}}_1
group by brand_account_id
)t2
on t1.brand_account_id=t2.brand_account_id
)t11
full outer join 

(select brand_account_id,
 concat_ws(';', if(  coalesce(note_list,'')<>'' , concat('笔记:',note_list) ,null),
 if( coalesce(user_list,'')<>'' , concat('用户:',user_list) ,null), 
 if( coalesce(brand_list,'')<>'' , concat('品牌',brand_list) ,null), 
 if( coalesce(store_list,'')<>'' , concat('店铺:',store_list),null), 
 if( coalesce(live_list,'')<>'' ,concat('直播间:',live_list) ,null)) as process_list,
'{{ds_nodash}}' as dtm
from 
(SELECT
 
  brand_account_id,
  concat_ws(',',collect_set(case when content_type in ('LIVE') then content_id else null end)) as live_list,
  concat_ws(',',collect_set(case when content_type in ('USER') then content_id else null end)) as user_list,
  concat_ws(',',collect_set(case when content_type in ('BRAND') then content_id else null end)) as brand_list,
  concat_ws(',',collect_set(case when content_type in ('STORE') then content_id else null end)) as store_list,
  concat_ws(',',collect_set(case when content_type in ('NOTE') then content_id else null end)) as note_list
FROM
  redapp.app_ads_insight_rex_process_center_process_log_di
WHERE
  dtm = '{{ds_nodash}}'
group  by 1
)t1
)t3 
on t11.brand_account_id = t3.brand_account_id


