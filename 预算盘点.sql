with
  base as (
    SELECT
      date_key,
      substring(date_key, 1, 7) as stat_month,
      if(
        month(date_key) & 1 = 1,
        trunc(date_key, 'MM'),
        trunc(add_months(date_key, -1), 'MM')
      ) as stat_bimonthly_month,
      add_months(date_key, 12) as next_date_key,
      if(
        month(add_months(date_key, 12)) & 1 = 1,
        trunc(add_months(date_key, 12), 'MM'),
        trunc(add_months(add_months(date_key, 12), -1), 'MM')
      ) as next_stat_bimonthly_month,
      brand_account_id,
      ads_first_cost_date,
      brand_user_name,
      company_code,
      company_name,
      group_code,
      group_name,
      trade_type_first_name,
      trade_type_second_name,
      track_group_id,
      track_group_name,
      track_industry_name,
      track_detail_name,
      direct_sales_dept3_name,
      direct_sales_dept4_name,
      direct_sales_dept5_name,
      direct_sales_dept6_name,
      brand_tag_code,
      brand_tag_name,
      brand_group_tag_code,
      brand_group_tag_name,
      first_industry_name,
      second_industry_name,
      direct_sales_name,
      sum(
        case
          when module in ('效果', '品牌', '薯条') then cash_cost
          else 0
        end
      ) as ads_cash_income_amt,
      sum(
        case
          when module in ('效果') then cash_cost
          else 0
        end
      ) as rtb_cash_income_amt,
      sum(
        case
          when module in ( '品牌') then cash_cost
          else 0
        end
      ) as brand_cash_income_amt,
      sum(
        case
          when module in ('效果')
          and product in ('竞价-信息流') then cash_cost
          else 0
        end
      ) as rtb_explore_cash_income_amt,
      sum(
        case
          when module in ('效果')
          and product in ('竞价-视频内流') then cash_cost
          else 0
        end
      ) as rtb_video_cash_income_amt,
      sum(
        case
          when module in ('效果')
          and product in ('竞价-搜索') then cash_cost
          else 0
        end
      ) as rtb_search_cash_income_amt,
      sum(
        case
          when module in ('品牌')
          and product in ('信息流GD') then cash_cost
          else 0
        end
      ) as brand_gd_cash_income_amt,
      sum(
        case
          when module in ('品牌')
          and product in ('开屏') then cash_cost
          else 0
        end
      ) as brand_splash_cash_income_amt,
      sum(
        case
          when module in ('品牌')
          and product in ('品牌专区') then cash_cost
          else 0
        end
      ) as brand_zone_cash_income_amt,
      sum(
        case
          when module in ('品牌')
          and product in ('品牌其他') then cash_cost
          else 0
        end
      ) as brand_other_cash_income_amt,
      sum(
        case
          when module in ('品牌')
          and product in ('搜索第三位') then cash_cost
          else 0
        end
      ) as brand_search_3rd_cash_income_amt,
      sum(
        case
          when module in ('品牌')
          and is_marketing_product = '1' then cash_cost
          else 0
        end
      ) as brand_ip_cash_income_amt
    FROM
      redapp.app_ads_insight_industry_product_account_td_df
    WHERE
      dtm = '{{ds_nodash}}'
      and date_key >= '2022-01-01'
    group by
      date_key,
      substring(date_key, 1, 7),
      add_months(date_key, 12),
      if(
        month(date_key) & 1 = 1,
        trunc(date_key, 'MM'),
        trunc(add_months(date_key, -1), 'MM')
      ),
      brand_account_id,
      ads_first_cost_date,
      brand_user_name,
      company_code,
      company_name,
      group_code,
      group_name,
      trade_type_first_name,
      trade_type_second_name,
      track_group_id,
      track_group_name,
      track_industry_name,
      track_detail_name,
      direct_sales_dept3_name,
      direct_sales_dept4_name,
      direct_sales_dept5_name,
      direct_sales_dept6_name,
      brand_tag_code,
      brand_tag_name,
      brand_group_tag_code,
      brand_group_tag_name,
      first_industry_name,
      second_industry_name,
      direct_sales_name
  ),
  cost_detail as (
    select
      stat_bimonthly_month,
      t1.brand_account_id,
      account_type,
      brand_user_name,
      company_code,
      company_name,
      group_code,
      group_name,
      trade_type_first_name,
      trade_type_second_name,
      track_group_id,
      track_group_name,
      track_industry_name,
      track_detail_name,
      direct_sales_dept3_name,
      direct_sales_dept4_name,
      direct_sales_dept5_name,
      direct_sales_dept6_name,
      brand_tag_code,
      brand_tag_name,
      brand_group_tag_code,
      brand_group_tag_name,
      first_industry_name,
      second_industry_name,
      direct_sales_name,
      ads_cash_income_amt,
      rtb_cash_income_amt,
      brand_cash_income_amt,
      rtb_explore_cash_income_amt,
      rtb_video_cash_income_amt,
      rtb_search_cash_income_amt,
      brand_gd_cash_income_amt,
      brand_splash_cash_income_amt,
      brand_zone_cash_income_amt,
      brand_other_cash_income_amt,
      brand_search_3rd_cash_income_amt,
      brand_ip_cash_income_amt,
      last_stat_bimonthly_month,
      last_account_type,
      last_rtb_cash_income_amt,
      last_brand_cash_income_amt,
      last_ads_cash_income_amt,
      last_rtb_explore_cash_income_amt,
      last_rtb_video_cash_income_amt,
      last_rtb_search_cash_income_amt,
      last_brand_gd_cash_income_amt,
      last_brand_splash_cash_income_amt,
      last_brand_zone_cash_income_amt,
      last_brand_other_cash_income_amt,
      last_brand_search_3rd_cash_income_amt,
      last_brand_ip_cash_income_amt
    from
      (
        select
          stat_bimonthly_month,
          brand_account_id,
          case
            when ads_first_cost_date >= stat_bimonthly_month
            and ads_first_cost_date < trunc(add_months(stat_bimonthly_month, 2), 'MM') then '新客'
            when ads_first_cost_date < stat_bimonthly_month then '老客'
            else '未投放客户'
          end as account_type,
          brand_user_name,
          company_code,
          company_name,
          group_code,
          group_name,
          trade_type_first_name,
          trade_type_second_name,
          track_group_id,
          track_group_name,
          track_industry_name,
          track_detail_name,
          direct_sales_dept3_name,
          direct_sales_dept4_name,
          direct_sales_dept5_name,
          direct_sales_dept6_name,
          brand_tag_code,
          brand_tag_name,
          brand_group_tag_code,
          brand_group_tag_name,
          first_industry_name,
          second_industry_name,
          direct_sales_name,
          sum(ads_cash_income_amt) as ads_cash_income_amt,
          sum(rtb_cash_income_amt) as rtb_cash_income_amt,
          sum(brand_cash_income_amt) as brand_cash_income_amt,
          sum(rtb_explore_cash_income_amt) as rtb_explore_cash_income_amt,
          sum(rtb_video_cash_income_amt) as rtb_video_cash_income_amt,
          sum(rtb_search_cash_income_amt) as rtb_search_cash_income_amt,
          sum(brand_gd_cash_income_amt) as brand_gd_cash_income_amt,
          sum(brand_splash_cash_income_amt) as brand_splash_cash_income_amt,
          sum(brand_zone_cash_income_amt) as brand_zone_cash_income_amt,
          sum(brand_other_cash_income_amt) as brand_other_cash_income_amt,
          sum(brand_search_3rd_cash_income_amt) as brand_search_3rd_cash_income_amt,
          sum(brand_ip_cash_income_amt) as brand_ip_cash_income_amt
        from
          base
        where
          date_key >= '2023-01-01'
        group by
          stat_bimonthly_month,
          brand_account_id,
          case
            when ads_first_cost_date >= stat_bimonthly_month
            and ads_first_cost_date < trunc(add_months(stat_bimonthly_month, 2), 'MM') then '新客'
            when ads_first_cost_date < stat_bimonthly_month then '老客'
            else '未投放客户'
          end,
          brand_user_name,
          company_code,
          company_name,
          group_code,
          group_name,
          trade_type_first_name,
          trade_type_second_name,
          track_group_id,
          track_group_name,
          track_industry_name,
          track_detail_name,
          direct_sales_dept3_name,
          direct_sales_dept4_name,
          direct_sales_dept5_name,
          direct_sales_dept6_name,
          brand_tag_code,
          brand_tag_name,
          brand_group_tag_code,
          brand_group_tag_name,
          first_industry_name,
          second_industry_name,
          direct_sales_name
      ) t1
      left join (
        select
          next_stat_bimonthly_month,
          stat_bimonthly_month as last_stat_bimonthly_month,
          brand_account_id,
          case
            when ads_first_cost_date >= stat_bimonthly_month
            and ads_first_cost_date < trunc(add_months(stat_bimonthly_month, 2), 'MM') then '新客'
            when ads_first_cost_date < stat_bimonthly_month then '老客'
            else '未投放客户'
          end as last_account_type,
          sum(ads_cash_income_amt) as last_ads_cash_income_amt,
          sum(rtb_cash_income_amt) as last_rtb_cash_income_amt,
          sum(brand_cash_income_amt) as last_brand_cash_income_amt,
          sum(rtb_explore_cash_income_amt) as last_rtb_explore_cash_income_amt,
          sum(rtb_video_cash_income_amt) as last_rtb_video_cash_income_amt,
          sum(rtb_search_cash_income_amt) as last_rtb_search_cash_income_amt,
          sum(brand_gd_cash_income_amt) as last_brand_gd_cash_income_amt,
          sum(brand_splash_cash_income_amt) as last_brand_splash_cash_income_amt,
          sum(brand_zone_cash_income_amt) as last_brand_zone_cash_income_amt,
          sum(brand_other_cash_income_amt) as last_brand_other_cash_income_amt,
          sum(brand_search_3rd_cash_income_amt) as last_brand_search_3rd_cash_income_amt,
          sum(brand_ip_cash_income_amt) as last_brand_ip_cash_income_amt
        from
          base
        group by
          next_stat_bimonthly_month,
          stat_bimonthly_month,
          brand_account_id,
          case
            when ads_first_cost_date >= stat_bimonthly_month
            and ads_first_cost_date < trunc(add_months(stat_bimonthly_month, 2), 'MM') then '新客'
            when ads_first_cost_date < stat_bimonthly_month then '老客'
            else '未投放客户'
          end
      ) t2 on t1.brand_account_id = t2.brand_account_id
      and t1.stat_bimonthly_month = t2.next_stat_bimonthly_month
  ),
  budget as (
    select
      t1.*,
      brand_user_name,
      company_code,
      company_name,
      group_code,
      group_name,
      trade_type_first_name,
      trade_type_second_name,
      track_group_id,
      track_group_name,
      track_industry_name,
      track_detail_name,
      direct_sales_dept3_name,
      direct_sales_dept4_name,
      direct_sales_dept5_name,
      direct_sales_dept6_name,
      brand_tag_code,
      brand_tag_name,
      brand_group_tag_code,
      brand_group_tag_name,
      first_industry_name,
      second_industry_name,
      direct_sales_name,
      account_type
    from
      (
        SELECT
          f_getdate(stat_months) as stat_months,
          brand_account_id,
          market_target_type,
          campaign_name,
          peiod,
          ads_budget_amt,
          rtb_search_budget_amt,
          rtb_explore_budget_amt,
          rtb_video_budget_amt,
          brand_feed_gd_budget_amt,
          brand_splash_budget_amt,
          brand_ip_budget_amt,
          brand_zone_budget_amt,
          brand_other_budget_amt,
          ads_other_budget_amt
        FROM
          redapp.app_ads_insight_account_campaign_budget_df
        WHERE
          dtm = '20230804'
      ) t1
      left join (
        select
          *
        from
          (
            select
              *,
              row_number() over(
                partition by stat_bimonthly_month,
                brand_account_id
                order by
                  ads_cash_income_amt desc
              ) as rn
            from
              cost_detail
          ) t
        where
          rn = 1
      ) t2 on t1.stat_months = t2.stat_bimonthly_month
      and t1.brand_account_id = t2.brand_account_id
  )
select
  stat_bimonthly_month,
  brand_account_id,
  account_type,
  brand_user_name as brand_account_name,
  company_code,
  company_name,
  group_code,
  group_name,
  trade_type_first_name,
  trade_type_second_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_tag_code,
  brand_tag_name,
  brand_group_tag_code,
  brand_group_tag_name,
  first_industry_name,
  second_industry_name,
  direct_sales_name,
  ads_cash_income_amt,
  rtb_cash_income_amt,
  brand_cash_income_amt,
  rtb_explore_cash_income_amt,
  rtb_video_cash_income_amt,
  rtb_search_cash_income_amt,
  brand_gd_cash_income_amt,
  brand_splash_cash_income_amt,
  brand_zone_cash_income_amt,
  brand_other_cash_income_amt,
  brand_search_3rd_cash_income_amt,
  brand_ip_cash_income_amt,
  last_stat_bimonthly_month,
  last_account_type,
  last_ads_cash_income_amt,
  last_rtb_cash_income_amt,
  last_brand_cash_income_amt,
  last_rtb_explore_cash_income_amt,
  last_rtb_video_cash_income_amt,
  last_rtb_search_cash_income_amt,
  last_brand_gd_cash_income_amt,
  last_brand_splash_cash_income_amt,
  last_brand_zone_cash_income_amt,
  last_brand_other_cash_income_amt,
  last_brand_search_3rd_cash_income_amt,
  last_brand_ip_cash_income_amt,
  null as market_target_type,
  null as campaign_name,
  null as peiod,
  0 as ads_budget_amt,
  0 as rtb_search_budget_amt,
  0 as rtb_explore_budget_amt,
  0 as rtb_video_budget_amt,
  0 as brand_feed_gd_budget_amt,
  0 as brand_splash_budget_amt,
  0 as brand_ip_budget_amt,
  0 as brand_zone_budget_amt,
  0 as brand_other_budget_amt,
  0 as ads_other_budget_amt
from
  cost_detail
union all
select
  stat_months as stat_bimonthly_month,
  brand_account_id,
  account_type,
  brand_user_name as brand_account_name,
  company_code,
  company_name,
  group_code,
  group_name,
  trade_type_first_name,
  trade_type_second_name,
  track_group_id,
  track_group_name,
  track_industry_name,
  track_detail_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_tag_code,
  brand_tag_name,
  brand_group_tag_code,
  brand_group_tag_name,
  first_industry_name,
  second_industry_name,
  direct_sales_name,
  0 as ads_cash_income_amt,
  0 as rtb_cash_income_amt,
  0 as brand_cash_income_amt,
  0 as rtb_explore_cash_income_amt,
  0 as rtb_video_cash_income_amt,
  0 as rtb_search_cash_income_amt,
  0 as brand_gd_cash_income_amt,
  0 as brand_splash_cash_income_amt,
  0 as brand_zone_cash_income_amt,
  0 as brand_other_cash_income_amt,
  0 as brand_search_3rd_cash_income_amt,
  0 as brand_ip_cash_income_amt,
  null as last_stat_bimonthly_month,
  null as last_account_type,
  0 as last_ads_cash_income_amt,
  0 as last_rtb_cash_income_amt,
  0 as last_brand_cash_income_amt,
  0 as last_rtb_explore_cash_income_amt,
  0 as last_rtb_video_cash_income_amt,
  0 as last_rtb_search_cash_income_amt,
  0 as last_brand_gd_cash_income_amt,
  0 as last_brand_splash_cash_income_amt,
  0 as last_brand_zone_cash_income_amt,
  0 as last_brand_other_cash_income_amt,
  0 as last_brand_search_3rd_cash_income_amt,
  0 as last_brand_ip_cash_income_amt,
  market_target_type,
  case
    when campaign_name = '' then null
    else campaign_name
  end as campaign_name,
  peiod,
  ads_budget_amt,
  rtb_search_budget_amt,
  rtb_explore_budget_amt,
  rtb_video_budget_amt,
  brand_feed_gd_budget_amt,
  brand_splash_budget_amt,
  brand_ip_budget_amt,
  brand_zone_budget_amt,
  brand_other_budget_amt,
  ads_other_budget_amt
from
  budget
