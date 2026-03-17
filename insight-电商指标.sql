create table temp.seller_cmd_03
select
  date_key,
  seller_user_id,--主键（过滤null，487）
  t1.seller_id,
  is_seller_user,
  shopname,
  user_name,
  main_category_name,
  industry,
  first_category_name,
  second_category_name,
  third_category_name,
  live_dgmv,
  k_live_dgmv,
  s_live_dgmv,
  other_live_dgmv,
  dgmv,
  ads_live_dgmv,
  ads_sx_live_dgmv,
  ads_dgmv
from
  (
    select coalesce(t1.date_key, t2.date_key) as date_key,
      coalesce(t1.seller_id, t2.seller_id) as seller_id,
      coalesce(t1.seller_user_id, t2.brand_account_id) as seller_user_id,
      coalesce(t1.is_seller_user, t2.is_seller_user) as is_seller_user,
      live_dgmv,
      k_live_dgmv,
      s_live_dgmv,
      other_live_dgmv,
      dgmv,
      ads_live_dgmv,
      ads_sx_live_dgmv,
      ads_dgmv
    from
      (
        select f_getdate(dtm) as date_key,
          seller_id,
          seller_user_id,
          --carrier_user_id,
          1 as is_seller_user,
          sum(
            case
              when carrier_page_name = '直播' then deal_gmv
              else 0
            end
          ) as live_dgmv,
          sum(
            case
              when carrier_page_name = '直播'
              and channel1 = 'K播' then deal_gmv
              else 0
            end
          ) as k_live_dgmv,
          sum(
            case
              when carrier_page_name = '直播'
              and channel1 = '店铺' then deal_gmv
              else 0
            end
          ) as s_live_dgmv,
          sum(
            case
              when carrier_page_name = '直播'
              and channel1 = '其他' then deal_gmv
              else 0
            end
          ) as other_live_dgmv,
          sum(deal_gmv) as dgmv
        from
          reddm.dm_trd_user_channel_goods_indicators_lv1_day_inc
        where
          dtm between f_getdate('{{ds_nodash}}', -7) and '{{ds_nodash}}'
          and deal_gmv > 0
        group by f_getdate(dtm),
          seller_id,
          seller_user_id
          --carrier_user_id,
         -- is_seller_user
      ) t1
      full outer join (
        select date_key,
          brand_account_id,
          --brand_account_name,
          sum(ads_live_dgmv) as ads_live_dgmv,
          sum(ads_sx_live_dgmv) as ads_sx_live_dgmv,
          coalesce(sum(ads_live_dgmv),0)+coalesce(sum(ads_sx_live_dgmv),0) as ads_dgmv,
          coalesce(seller_id, -911) as seller_id,
          case
            when seller_id is not null then 1
            else 0
          end as is_seller_user
        from
          (
            select date_key,
              brand_account_id,
              brand_account_name,
              sum(
                case
                  when marketing_target in (8, 14) then coalesce(live_dgmv, live_rgmv)
                  else 0
                end
              ) / 100.0 as ads_live_dgmv,
              
              0 as ads_sx_live_dgmv
            from
              redcdm.dm_ads_industry_product_advertiser_td_df
            where
              dtm = '{{ds_nodash}}' and date_key>=f_getdate('{{ds}}', -7) and coalesce(live_dgmv,0)+coalesce(live_rgmv,0)>0
            group by date_key,
              brand_account_id,
              brand_account_name
              union all
            select f_getdate(dtm) as date_key,
                brand_account_id,
                brand_account_name,
                0 as ads_live_dgmv,
                sum(coalesce(purchase_dgmv,0)+coalesce(mini_purchase_dgmv,0)) as ads_sx_live_dgmv
            from redcdm.dws_ads_cvr_event_detail_1d_di
             where dtm between f_getdate('{{ds_nodash}}', -7) and '{{ds_nodash}}' and coalesce(purchase_dgmv,0)+coalesce(mini_purchase_dgmv,0)>0 
             and marketing_target in (3, 15) 
             group by f_getdate(dtm) ,brand_account_id,brand_account_name
          ) a1
          left join (
            select
              user_id as seller_user_id,
              seller_id,
              relation_type
            from
              redcdm.dim_pro_soc_user_relation_df
            WHERE
              dtm = '{{ds_nodash}}'
              and seller_id <> 'UNKNOWN'
              and is_valid = 1
              and bind_type in (1, 2)
          ) account on a1.brand_account_id = account.seller_user_id
          group by date_key,
          brand_account_id,
          --brand_account_name,
          coalesce(seller_id, -911),
          case
            when seller_id is not null then 1
            else 0
          end
      ) t2 on t1.seller_user_id = t2.brand_account_id and t1.date_key=t2.date_key 
      union all 
      --历史全量
      select date_key,
      seller_id,
      seller_user_id,
       is_seller_user,
      live_dgmv,
      k_live_dgmv,
      s_live_dgmv,
      other_live_dgmv,
      dgmv,
      ads_live_dgmv,
      ads_sx_live_dgmv,
      ads_dgmv
      from redapp.app_ads_insight_account_dgmv_df 
      where dtm =f_getdate('{{ds_nodash}}', -1) and date_key<f_getdate('{{ds}}', -7)
  ) t1
  left join (
    select
      seller_id,
      shopname,
      user_id,
      user_name,
      main_category_name,
      industry,
      first_category_name,
      second_category_name,
      third_category_name
    from
      reddw.dw_trd_seller_base_metrics_day
    where
      dtm = '{{ds_nodash}}'
  ) t2 on t1.seller_id = t2.seller_id


  --------------全量
  insert overwrite table redapp.app_ads_insight_account_dgmv_df partition(dtm = '{{ds_nodash}}') 
select t1.date_key,
  t1.seller_id,
  seller_user_id,--主键（过滤null，487）
  is_seller_user,
  shopname,
  user_name,
  main_category_name,
  industry,
  first_category_name,
  second_category_name,
  third_category_name,
  live_dgmv,
  k_live_dgmv,
  s_live_dgmv,
  other_live_dgmv,
  dgmv,
  ads_live_dgmv,
  ads_sx_live_dgmv,
  rgmv
from
  (
    select coalesce(t1.date_key, t2.date_key) as date_key,
      coalesce(t1.seller_id, t2.seller_id) as seller_id,
      coalesce(t1.seller_user_id, t2.brand_account_id) as seller_user_id,
      coalesce(t1.is_seller_user, t2.is_seller_user) as is_seller_user,
      live_dgmv,
      k_live_dgmv,
      s_live_dgmv,
      other_live_dgmv,
      dgmv,
      ads_live_dgmv,
      ads_sx_live_dgmv,
      rgmv
    from
      (
        select  date_key,
          seller_id,
          seller_user_id,
           is_seller_user,
           live_dgmv,
           k_live_dgmv,
           s_live_dgmv,
           other_live_dgmv,
           dgmv
       
         from temp.temp_seller_date 
      ) t1
      full outer join (
        select date_key,
          brand_account_id,
          --brand_account_name,
          sum(ads_live_dgmv) as ads_live_dgmv,
          sum(ads_sx_live_dgmv) as ads_sx_live_dgmv,
          coalesce(sum(ads_live_dgmv),0)+coalesce(sum(ads_sx_live_dgmv),0) as rgmv,
          coalesce(seller_id, -911) as seller_id,
          case
            when seller_id is not null then 1
            else 0
          end as is_seller_user
        from
          (
            select date_key,
              brand_account_id,
              --brand_account_name,
              sum(
                case
                  when marketing_target in (8, 14) then coalesce(live_dgmv, live_rgmv)
                  else 0
                end
              ) / 100.0 as ads_live_dgmv,
              
              0 as ads_sx_live_dgmv
            from
              redcdm.dm_ads_industry_product_advertiser_td_df
            where
              dtm = '{{ds_nodash}}' and coalesce(live_dgmv,0)+coalesce(live_rgmv,0)>0
            group by date_key,
              brand_account_id
              union all
            select f_getdate(dtm) as date_key,
                brand_account_id,
                --brand_account_name,
                0 as ads_live_dgmv,
                sum(coalesce(purchase_dgmv,0)+coalesce(mini_purchase_dgmv,0)) as ads_sx_live_dgmv
            from redcdm.dws_ads_cvr_event_detail_1d_di
             where dtm>='20230601' and coalesce(purchase_dgmv,0)+coalesce(mini_purchase_dgmv,0)>0 
             and marketing_target in (3, 15) 
             group by f_getdate(dtm),
              brand_account_id
          ) a1
          left join (
            select
              user_id as seller_user_id,
              seller_id,
              relation_type
            from
              redcdm.dim_pro_soc_user_relation_df
            WHERE
              dtm = '{{ds_nodash}}'
              and seller_id <> 'UNKNOWN'
              and is_valid = 1
              and bind_type in (1, 2)
          ) account on a1.brand_account_id = account.seller_user_id
          group by date_key,
          brand_account_id,
          coalesce(seller_id, -911),
          case
            when seller_id is not null then 1
            else 0
          end
      ) t2 on t1.seller_user_id = t2.brand_account_id 
      and t1.date_key = t2.date_key 
      
  ) t1
  left join (
    select
      seller_id,
      shopname,
      user_id,
      user_name,
      main_category_name,
      industry,
      first_category_name,
      second_category_name,
      third_category_name
    from
      reddw.dw_trd_seller_base_metrics_day
    where
      dtm = '{{ds_nodash}}'
  ) t2 on t1.seller_id = t2.seller_id

---全量v2
insert overwrite table redapp.app_ads_insight_account_dgmv_df partition(dtm = '{{ds_nodash}}') 
select t1.date_key,
  
  seller_user_id,--主键（过滤null，487）
  t1.seller_id,
  is_seller_user,
  shopname,
  user_name,
  main_category_name,
  industry,
  first_category_name,
  second_category_name,
  third_category_name,
  live_dgmv,
  k_live_dgmv,
  s_live_dgmv,
  other_live_dgmv,
  dgmv,
  ads_live_dgmv,
  ads_sx_live_dgmv,
  rgmv
from
  (
    select coalesce(t1.date_key, t2.date_key) as date_key,
      coalesce(t1.seller_id, t2.seller_id) as seller_id,
      coalesce(t1.seller_user_id, t2.brand_account_id) as seller_user_id,
      coalesce(t1.is_seller_user, t2.is_seller_user) as is_seller_user,
      live_dgmv,
      k_live_dgmv,
      s_live_dgmv,
      other_live_dgmv,
      dgmv,
      ads_live_dgmv,
      ads_sx_live_dgmv,
      rgmv
    from
      (
        select f_getdate(dtm) as date_key,
          seller_id,
          seller_user_id,
          --carrier_user_id,
          1 as is_seller_user,
          sum(
            case
              when carrier_page_name = '直播' then deal_gmv
              else 0
            end
          ) as live_dgmv,
          sum(
            case
              when carrier_page_name = '直播'
              and channel1 = 'K播' then deal_gmv
              else 0
            end
          ) as k_live_dgmv,
          sum(
            case
              when carrier_page_name = '直播'
              and channel1 = '店铺' then deal_gmv
              else 0
            end
          ) as s_live_dgmv,
          sum(
            case
              when carrier_page_name = '直播'
              and channel1 = '其他' then deal_gmv
              else 0
            end
          ) as other_live_dgmv,
          sum(deal_gmv) as dgmv
        from
          reddm.dm_trd_user_channel_goods_indicators_lv1_day_inc
        where
          dtm >= '20230101'
          and deal_gmv > 0
        group by f_getdate(dtm),
          seller_id,
          seller_user_id
          --carrier_user_id,
         -- is_seller_user
      ) t1
      full outer join (
        select date_key,
          brand_account_id,
          --brand_account_name,
          sum(ads_live_dgmv) as ads_live_dgmv,
          sum(ads_sx_live_dgmv) as ads_sx_live_dgmv,
          coalesce(sum(ads_live_dgmv),0)+coalesce(sum(ads_sx_live_dgmv),0) as rgmv,
          coalesce(seller_id, -911) as seller_id,
          case
            when seller_id is not null then 1
            else 0
          end as is_seller_user
        from
          (
            select date_key,
              brand_account_id,
              --brand_account_name,
              sum(
                case
                  when marketing_target in (8, 14) then coalesce(live_dgmv, live_rgmv)
                  else 0
                end
              ) / 100.0 as ads_live_dgmv,
              
              0 as ads_sx_live_dgmv
            from
              redcdm.dm_ads_industry_product_advertiser_td_df
            where
              dtm = '{{ds_nodash}}' and coalesce(live_dgmv,0)+coalesce(live_rgmv,0)>0
            group by date_key,
              brand_account_id
              --brand_account_name
              union all
            select  date_key,
                brand_account_id,
                --brand_account_name,
                 ads_live_dgmv,
                ads_sx_live_dgmv
            from temp.temp_dws_ads_cvr_event_detail_1d_di_history
             
          ) a1
          left join (
            select
              user_id as seller_user_id,
              seller_id,
              relation_type
            from
              redcdm.dim_pro_soc_user_relation_df
            WHERE
              dtm = '{{ds_nodash}}'
              and seller_id <> 'UNKNOWN'
              and is_valid = 1
              and bind_type in (1, 2)
          ) account on a1.brand_account_id = account.seller_user_id
          group by date_key,
          brand_account_id,
          --brand_account_name,
          coalesce(seller_id, -911),
          case
            when seller_id is not null then 1
            else 0
          end
      ) t2 on t1.seller_user_id = t2.brand_account_id 
      and t1.date_key = t2.date_key
      and t1.seller_id=t2.seller_id
      and t1.is_seller_user=t2.is_seller_user
  ) t1
  left join (
    select
      seller_id,
      shopname,
      user_id,
      user_name,
      main_category_name,
      industry,
      first_category_name,
      second_category_name,
      third_category_name
    from
      reddw.dw_trd_seller_base_metrics_day
    where
      dtm = '{{ds_nodash}}'
  ) t2 on t1.seller_id = t2.seller_id
