insert overwrite table redapp.app_ads_insight_account_agent_group_df partition(dtm = '{{ ds_nodash }}')
select
  a.date_key,
  a.brand_account_id,
  b.agent_user_id,
  b.agent_name,
  b.group_id,
  c.group_name,
  sum(a.imp_num) as imp_num,
  sum(a.click_num) as click_num,
  sum(a.cash_cost) as cash_cost,
  sum(a.cost) as cost
from
  (
    select
      datekey as date_key,
      brand_account_id,
      creativity_id,
      imp_num,
      click_num,
      cost,
      cash_cost,
      dtm
    from
      redst.st_ads_wide_cpc_creativity_day_inc
    where
      dtm >= '20230101'
  ) a
  left join (
    select
      creativity_id,
      dmp_group_id,
      group_id,
      agent_user_id,
      agent_name,
      dtm
    from
      reddim.dim_ads_creativity_day lateral view outer explode(split(dmp_group_id, ',')) tmp as group_id
    where
      dtm >= '20230101'
  ) b on a.creativity_id = b.creativity_id and a.dtm = b.dtm
  join (
    select
      group_id,
      group_name
    from
      reddim.dim_ads_dmp_group_day
    where
      dtm = '{{ds_nodash}}'
      and group_state = 1
      and group_id in (
        14191778,
        14191780,
        14189170,
        14189311,
        14189249,
        14189248,
        14191719,
        14184692,
        14184998,
        14184736
      ) --美妆人群包
  ) c on c.group_id = b.group_id
group by
  date_key,
  brand_account_id,
  b.agent_user_id,
  b.agent_name,
  b.group_id,
  c.group_name