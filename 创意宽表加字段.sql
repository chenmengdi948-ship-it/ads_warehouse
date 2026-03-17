select
  t1.creativity_id,
  t1.campaign_id,
  first_cost_date,
  campaign_first_cost_date,
  t1.note_id,
  t1.unit_id,
  audit_status,
  first_image_url,
  unit_audit_status,
  campaign_audit_status,
  event_bid,
  campaign_is_valid,
  unit_is_valid,
  cast('{{ds_nodash}}' as int) as dtm
from
  (
    select
      creativity_id,
      unit_id,
      campaign_id,
      case when ads_material_type='post' then ads_material_id else null end as note_id,
      f_getdate (min(case when cost_amount > 0 then dtm else '99991231' end)) as first_cost_date
    from
      redcdm.dm_ads_rtb_creativity_1d_di
    WHERE
      dtm >= '20221101'
      and (
        is_effective = 1
        or total_amount > 0
      )
      --and cost_amount > 0
    group by
      creativity_id,
      campaign_id,
      unit_id,
      case when ads_material_type='post' then ads_material_id else null end 
  ) t1
  left join (
    select
      campaign_id,
      f_getdate (min(dtm)) as campaign_first_cost_date
    from
      redcdm.dm_ads_rtb_creativity_1d_di
    WHERE
      dtm >= '20221101'
      and (
        is_effective = 1
        or total_amount > 0
      )
      and cost_amount > 0
    group by
      campaign_id
  ) t2 on t1.campaign_id = t2.campaign_id
  left join 
  (select id as creativity_id,
      case when audit_status in (2,9) then 300 when audit_status in (1,4,5,6,7) then 100 else 200 end as audit_status
  from redcdm.dwd_ads_rtb_creativity_df 
  where dtm='{{ds_nodash}}'
  )t3 
  on t3.creativity_id=t1.creativity_id
  left join 
  (SELECT
    note_id,
    first_image_url
  FROM
    redapp.app_ads_insight_note_df
  WHERE
   dtm='{{ds_nodash}}'
   )t4 
  on t1.note_id = t4.note_id
  left join 
  (select unit_id,
    max(audit_status) as unit_audit_status
  from 
  (select id as creativity_id,unit_id,
      case when audit_status in (2,9) then 300 when audit_status in (1,4,5,6,7) then 100 else 200 end as audit_status
  from redcdm.dwd_ads_rtb_creativity_df 
  where dtm='{{ds_nodash}}'
  )t3 
  group by unit_id
  )t5 
  on t5.unit_id = t1.unit_id
  left join 
  (select campaign_id,
    max(audit_status) as campaign_audit_status
  from 
  (select a.id as creativity_id,b.campaign_id,
      case when a.audit_status in (2,9) then 300 when a.audit_status in (1,4,5,6,7) then 100 else 200 end as audit_status
  from redcdm.dwd_ads_rtb_creativity_df a 
  left join  redcdm.dim_ads_creativity_core_df b on a.id =b.creativity_id and b.dtm='{{ds_nodash}}'
  where a.dtm='{{ds_nodash}}'
  )t3 
  group by campaign_id
  )t6 
  on t6.campaign_id = t1.campaign_id 
  left join 
  (select id,event_bid/100 as event_bid,case when platform = 1 and state = 1 and from_unixtime(floor(start_time / 1000 + 28800), 'yyyy-MM-dd')<= '{{ds}}' 
        and from_unixtime(floor(expire_time / 1000 + 28800), 'yyyy-MM-dd')>= '{{ds}}'  and enable = 1 
        and campaign_enable = 1 and budget_state = 1 and adv_budget_state = 1 
        and balance_state = 1 and hidden_flag = 0 then 1 else 0 end as unit_is_valid
  from redcdm.dwd_ads_rtb_unit_df 
  where dtm='{{ds_nodash}}'
  )unit 
  on unit.id = t1.unit_id
  left join 
  (select id,case when platform = 1 and state = 1 and enable = 1 and  from_unixtime(floor(start_time / 1000 + 28800), 'yyyy-MM-dd')<= '{{ds}}' 
        and from_unixtime(floor(expire_time / 1000 + 28800), 'yyyy-MM-dd')>= '{{ds}}' 
 and balance_state = 1 and budget_state = 1 and adv_budget_state = 1 
  and hidden_flag = 0 then 1 else 0 end as campaign_is_valid
  from redcdm.dwd_ads_rtb_campaign_df 
  where dtm='{{ds_nodash}}'
  )campaign
  on campaign.id = t1.campaign_id

