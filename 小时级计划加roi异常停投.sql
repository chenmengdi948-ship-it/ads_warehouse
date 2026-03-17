
    -- ************************************************
    -- Author: chenmengdi
    -- CreateTime:2024-01-30T20:52:59+08:00
    -- Update: Task Update Description
    -- ************************************************
drop table if exists temp.advertiser_campaign_alias{{ds_nodash}}_{{ts[11:13]}};
create table temp.advertiser_campaign_alias{{ds_nodash}}_{{ts[11:13]}}
select t1.creativity_id,
  unit_id,
  coalesce(t1.campaign_id,t4.id) as campaign_id,
  coalesce(t1.advertiser_id,t4.advertiser_id) as advertiser_id,
  t1.note_id,
  ads_material_type,
  cost,
  first_cost_date,
  create_date,
  note_first_cost_date,
  coalesce(t1.dtm,t4.dtm) as dtm,
  coalesce(t1.hh,t4.hh) as hh
from 
(SELECT
  creativity_id,
  unit_id,
  campaign_id,
  advertiser_id,
  
  case when ads_material_type=1 then ads_material_id else null end as note_id,
  ads_material_type,
  dtm,
  sum(cost) as cost,
  '{{ts[11:13]}}' as hh
FROM
  hive_prod.kafka.kafka_qcsh4_rlm1_dw_ads_log_creativity_cube_hh_rt_main
WHERE
  dtm = '{{ds_nodash}}' and hh<='{{ts[11:13]}}'
  and marketing_target in (3,8,14,15)
  and cost>0
  group by 
 creativity_id,
  unit_id,
  campaign_id,
  advertiser_id,
  dtm,
  case when ads_material_type=1 then ads_material_id else null end,
  ads_material_type
  )t1
  left join 
  --创意首次投放时间
  (select   campaign_id,min(case when first_cost_date='9999-12-31' then null else first_cost_date end) as first_cost_date
  FROM
    redapp.app_ads_industry_rtb_creativity_di
  WHERE
    dtm = max_dtm('redapp.app_ads_industry_rtb_creativity_di') 
    and marketing_target in (3,8,14,15)
    group by 1
    )t2 
  on t1.campaign_id =cast( t2.campaign_id as string)
  left join 
  --创意首次投放时间
  (select   note_id,min(case when first_cost_date='9999-12-31' then null else first_cost_date end) as note_first_cost_date
  FROM
    redapp.app_ads_industry_rtb_creativity_di
  WHERE
    dtm = max_dtm('redapp.app_ads_industry_rtb_creativity_di') 
    and marketing_target in (3,8,14,15)
    group by 1
    )t3
on t1.note_id =t3.note_id
full outer join 
--计划创建时间
(select id,
    substring(create_time,1,10) as create_date,
    advertiser_id,
    hh,
    dtm
from redods.ods_shequ_feed_ads_t_ads_rtb_campaign_hf 
where dtm='{{ds_nodash}}' and hh=(select max(hh) from redods.ods_shequ_feed_ads_t_ads_rtb_campaign_hf  where dtm='{{ds_nodash}}')
and marketing_target in (3,8,14,15)
and  substring(create_time,1,10)='{{ds}}'
)t4
on t1.campaign_id=t4.id;

--2024新增昨日有roi今日停投的计划list 
drop table if exists temp.advertiser_campaign_roi_alias{{ds_nodash}}_{{ts[11:13]}};
create table temp.advertiser_campaign_roi_alias{{ds_nodash}}_{{ts[11:13]}}
select
  coalesce(t1.campaign_id,t2.campaign_id) as campaign_id,
  coalesce(t1.advertiser_id,t2.advertiser_id) as advertiser_id,
  t1.cost,
  t2.ystd_roi,
  t2.ystd_cost,
  t2.ystd_rgmv,
  coalesce(t1.dtm,t2.dtm) as dtm,
  coalesce(t1.hh,t2.hh) as hh,
  if(t2.ystd_roi>0.6 and coalesce(t1.cost,0)=0,1,0) as roi_1,
  if(t2.ystd_roi>0.8 and coalesce(t1.cost,0)=0,1,0) as roi_2, 
  if(t2.ystd_roi>1.2 and coalesce(t1.cost,0)=0,1,0) as roi_3,
  if(t2.ystd_roi>1.5 and coalesce(t1.cost,0)=0,1,0) as roi_4, 
  if(t2.ystd_roi>2 and coalesce(t1.cost,0)=0,1,0) as roi_5
from 
(SELECT
  
  campaign_id,
  advertiser_id,
  dtm,
  sum(cost) as cost,
  '{{ts[11:13]}}' as hh
FROM
  hive_prod.kafka.kafka_qcsh4_rlm1_dw_ads_log_creativity_cube_hh_rt_main
WHERE
  dtm = '{{ds_nodash}}' and hh<='{{ts[11:13]}}'
  and marketing_target in (3,8,14,15)
  and cost>0
  group by 
 
  campaign_id,
  advertiser_id,
  dtm
  )t1
full outer join 
  --计划昨日roi
  (select   campaign_id,
  advertiser_id,
  '{{ds_nodash}}' as dtm,
   '{{ts[11:13]}}' as hh,
   --昨天roi 
   sum(case when marketing_target in (3,8,15) then income_amt else 0 end) as ystd_cost,
   sum(case when marketing_target in (3,8,15) then income_amt else 0 end) as ystd_rgmv,
   sum(case when marketing_target in (3,8,15) then income_amt else 0 end)/sum(case when marketing_target in (3,8,15) then income_amt else 0 end) as ystd_roi
  FROM
    redapp.app_ads_industry_rtb_creativity_di
  WHERE
    dtm = max_dtm('redapp.app_ads_industry_rtb_creativity_di') 
    and marketing_target in (3,8,14,15)
    group by 1,2
    )t2 
  on t1.campaign_id =cast( t2.campaign_id as string)
  ;
insert
  overwrite table redcdm.dm_ads_rtb_advertiser_campaign_note_hf partition (dtm, hh)
select advertiser_id,
    count(distinct case when create_date is not null then campaign_id else null end) as create_campaign_cnt,
    count(distinct case when first_cost_date is null and cost>0 and create_date is not null then campaign_id else null end) as create_cost_campaign_cnt,
    count(distinct case when cost>0 then campaign_id end) as cost_campaign_cnt,
    count(distinct case when cost>0 then note_id end) as cost_note_cnt,
    count(distinct case when note_first_cost_date is null and cost>0 then note_id else null end) as new_cost_note_cnt,
    dtm,
    '{{ts[11:13]}}' as hh
from temp.advertiser_campaign_alias{{ds_nodash}}_{{ts[11:13]}}
group by advertiser_id,
    dtm