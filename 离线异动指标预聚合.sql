drop table if exists temp.app_ads_insight_advertiser_product_diagnosis_info_df_cmd_01_{{ds_nodash}};
create table temp.app_ads_insight_advertiser_product_diagnosis_info_df_cmd_01_{{ds_nodash}}
select brand_account_id,
  advertiser_id,
  v_seller_id,
  v_seller_name,
  agent_user_id,
  agent_user_name,
  -- module,
  -- product,
  -- marketing_target,
  -- market_target,
  brand_account_name,
  company_code,
  company_name,
  track_group_name,
  direct_sales_name,
  direct_sales_dept1_name,
  direct_sales_dept2_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_group_tag_code,
  brand_group_tag_name,
  cpc_operator_code,
  cpc_operator_name,
  first_industry_name,
  second_industry_name,
  track_industry_name,
  track_detail_name,
  brand_id,
  brand_name,
  planner_name,
  staff_name,
  process_track_industry_name,
  process_track_group_name,
  process_track_second_name,
  process_track_third_name,
  channel_sales_name,
  channel_sales_code,
  channel_operator_name,
sum(cash_income_amt) as cash_income_amt,
sum(income_amt) as income_amt,
sum(imp_cnt) as imp_cnt,
sum(click_cnt) as click_cnt,
sum(like_cnt) as like_cnt,
sum(fav_cnt) as fav_cnt,
sum(cmt_cnt) as cmt_cnt,
sum(share_cnt) as share_cnt,
sum(follow_cnt) as follow_cnt,
sum(screenshot_cnt) as screenshot_cnt,
sum(engage_cnt) as engage_cnt,
sum(click_rgmv_7d) as click_rgmv_7d,
sum(conversion_cnt) as conversion_cnt,
sum(ecm_unclosed_rgmv) as ecm_unclosed_rgmv,
sum(rtb_cost_income_amt) as rtb_cost_income_amt,
sum(rtb_budget_income_amt) as rtb_budget_income_amt,
sum(cost_income_amt) as cost_income_amt,
sum(budget_income_amt) as budget_income_amt,
sum(advertiser_budget_income_amt) as advertiser_budget_income_amt,
sum(advertiser_cost_income_amt) as advertiser_cost_income_amt,
sum(cash_balance) as cash_balance,
sum(ystd_cash_income_amt_1d) as ystd_cash_income_amt_1d,
sum(ystd_income_amt_1d) as ystd_income_amt_1d,
sum(ystd_imp_cnt_1d) as ystd_imp_cnt_1d,
sum(ystd_click_cnt_1d) as ystd_click_cnt_1d,
sum(ystd_like_cnt_1d) as ystd_like_cnt_1d,
sum(ystd_fav_cnt_1d) as ystd_fav_cnt_1d,
sum(ystd_cmt_cnt_1d) as ystd_cmt_cnt_1d,
sum(ystd_share_cnt_1d) as ystd_share_cnt_1d,
sum(ystd_follow_cnt_1d) as ystd_follow_cnt_1d,
sum(ystd_screenshot_cnt_1d) as ystd_screenshot_cnt_1d,
sum(ystd_engage_cnt) as ystd_engage_cnt_1d,
sum(ystd_click_rgmv_7d_1d) as ystd_click_rgmv_7d_1d,
sum(ystd_conversion_cnt_1d) as ystd_conversion_cnt_1d,
sum(ystd_ecm_unclosed_rgmv_1d) as ystd_ecm_unclosed_rgmv_1d,
sum(ystd_rtb_cost_income_amt_1d) as ystd_rtb_cost_income_amt_1d,
sum(ystd_rtb_budget_income_amt_1d) as ystd_rtb_budget_income_amt_1d,
sum(ystd_cost_income_amt_1d) as ystd_cost_income_amt_1d,
sum(ystd_budget_income_amt_1d) as ystd_budget_income_amt_1d,
sum(ystd_advertiser_budget_income_amt_1d) as ystd_advertiser_budget_income_amt_1d,
sum(ystd_advertiser_cost_income_amt_1d) as ystd_advertiser_cost_income_amt_1d,
sum(ystd_cash_balance_1d) as ystd_cash_balance_1d,
sum(cash_income_amt_2m) as cash_income_amt_2m,
sum(income_amt_2m) as income_amt_2m,
sum(imp_cnt_2m) as imp_cnt_2m,
sum(click_cnt_2m) as click_cnt_2m,
sum(like_cnt_2m) as like_cnt_2m,
sum(fav_cnt_2m) as fav_cnt_2m,
sum(cmt_cnt_2m) as cmt_cnt_2m,
sum(share_cnt_2m) as share_cnt_2m,
sum(follow_cnt_2m) as follow_cnt_2m,
sum(screenshot_cnt_2m) as screenshot_cnt_2m,
sum(engage_cnt_2m) as engage_cnt_2m,
sum(click_rgmv_7d_2m) as click_rgmv_7d_2m,
sum(conversion_cnt_2m) as conversion_cnt_2m,
sum(ecm_unclosed_rgmv_2m) as ecm_unclosed_rgmv_2m,
sum(rtb_cost_income_amt_2m) as rtb_cost_income_amt_2m,
sum(rtb_budget_income_amt_2m) as rtb_budget_income_amt_2m,
sum(cost_income_amt_2m) as cost_income_amt_2m,
sum(budget_income_amt_2m) as budget_income_amt_2m,
sum(advertiser_budget_income_amt_2m) as advertiser_budget_income_amt_2m,
sum(advertiser_cost_income_amt_2m) as advertiser_cost_income_amt_2m,
sum(cash_balance_2m) as cash_balance_2m,
sum(cash_income_amt_last_2m) as cash_income_amt_last_2m,
sum(income_amt_last_2m) as income_amt_last_2m,
sum(imp_cnt_last_2m) as imp_cnt_last_2m,
sum(click_cnt_last_2m) as click_cnt_last_2m,
sum(like_cnt_last_2m) as like_cnt_last_2m,
sum(fav_cnt_last_2m) as fav_cnt_last_2m,
sum(cmt_cnt_last_2m) as cmt_cnt_last_2m,
sum(share_cnt_last_2m) as share_cnt_last_2m,
sum(follow_cnt_last_2m) as follow_cnt_last_2m,
sum(screenshot_cnt_last_2m) as screenshot_cnt_last_2m,
sum(engage_cnt_last_2m) as engage_cnt_last_2m,
sum(click_rgmv_7d_last_2m) as click_rgmv_7d_last_2m,
sum(conversion_cnt_last_2m) as conversion_cnt_last_2m,
sum(ecm_unclosed_rgmv_last_2m) as ecm_unclosed_rgmv_last_2m,
sum(rtb_cost_income_amt_last_2m) as rtb_cost_income_amt_last_2m,
sum(rtb_budget_income_amt_last_2m) as rtb_budget_income_amt_last_2m,
sum(cost_income_amt_last_2m) as cost_income_amt_last_2m,
sum(budget_income_amt_last_2m) as budget_income_amt_last_2m,
sum(advertiser_budget_income_amt_last_2m) as advertiser_budget_income_amt_last_2m,
sum(advertiser_cost_income_amt_last_2m) as advertiser_cost_income_amt_last_2m,
sum(cash_balance_last_2m) as cash_balance_last_2m,
sum(cash_income_amt_before_2m) as cash_income_amt_before_2m,
sum(income_amt_before_2m) as income_amt_before_2m,
sum(imp_cnt_before_2m) as imp_cnt_before_2m,
sum(click_cnt_before_2m) as click_cnt_before_2m,
sum(like_cnt_before_2m) as like_cnt_before_2m,
sum(fav_cnt_before_2m) as fav_cnt_before_2m,
sum(cmt_cnt_before_2m) as cmt_cnt_before_2m,
sum(share_cnt_before_2m) as share_cnt_before_2m,
sum(follow_cnt_before_2m) as follow_cnt_before_2m,
sum(screenshot_cnt_before_2m) as screenshot_cnt_before_2m,
sum(engage_cnt_before_2m) as engage_cnt_before_2m,
sum(click_rgmv_7d_before_2m) as click_rgmv_7d_before_2m,
sum(conversion_cnt_before_2m) as conversion_cnt_before_2m,
sum(ecm_unclosed_rgmv_before_2m) as ecm_unclosed_rgmv_before_2m,
sum(rtb_cost_income_amt_before_2m) as rtb_cost_income_amt_before_2m,
sum(rtb_budget_income_amt_before_2m) as rtb_budget_income_amt_before_2m,
sum(cost_income_amt_before_2m) as cost_income_amt_before_2m,
sum(budget_income_amt_before_2m) as budget_income_amt_before_2m,
sum(advertiser_budget_income_amt_before_2m) as advertiser_budget_income_amt_before_2m,
sum(advertiser_cost_income_amt_before_2m) as advertiser_cost_income_amt_before_2m,
sum(cash_balance_before_2m) as cash_balance_before_2m,
max(track_group_cost_income_amt) as track_group_cost_income_amt,
max(track_group_budget_income_amt) as track_group_budget_income_amt,
max(track_group_advertiser_cost_income_amt) as track_group_advertiser_cost_income_amt,
max(track_group_advertiser_budget_income_amt) as track_group_advertiser_budget_income_amt,
max(track_group_rtb_cost_income_amt) as track_group_rtb_cost_income_amt,
max(track_group_rtb_budget_income_amt) as track_group_rtb_budget_income_amt,
max(track_group_cash_income_amt_before_2m) as track_group_cash_income_amt_before_2m,
max(track_group_cash_income_amt_2m) as track_group_cash_income_amt_2m,
max(track_group_cash_income_amt_last_2m) as track_group_cash_income_amt_last_2m,
max(track_cost_income_amt) as track_cost_income_amt,
max(track_budget_income_amt) as track_budget_income_amt,
max(track_advertiser_cost_income_amt) as track_advertiser_cost_income_amt,
max(track_advertiser_budget_income_amt) as track_advertiser_budget_income_amt,
max(track_rtb_cost_income_amt) as track_rtb_cost_income_amt,
max(track_rtb_budget_income_amt) as track_rtb_budget_income_amt,
sum(cash_income_amt_1y) as cash_income_amt_1y,
sum(cash_income_amt_before_1y) as cash_income_amt_before_1y,

      sum(
        case
          when market_target = '种草' then cash_income_amt
          else 0
        end
      ) as ti_cash_income_amt,
      sum(
        case
          when market_target = '种草' then ystd_cash_income_amt_1d
          else 0
        end
      ) as ystd_ti_cash_income_amt_1d,
      sum(
        case
          when market_target = '种草' then cash_income_amt_2m
          else 0
        end
      ) as ti_cash_income_amt_2m,
      sum(
        case
          when market_target = '种草' then cash_income_amt_last_2m
          else 0
        end
      ) as ti_cash_income_amt_last_2m,
      sum(
        case
          when market_target = '种草' then cash_income_amt_before_2m
          else 0
        end
      ) as ti_cash_income_amt_before_2m,

      sum(
        case
          when market_target = '种草' then income_amt
          else 0
        end
      ) as ti_income_amt,
      sum(
        case
          when market_target = '种草' then ystd_income_amt_1d
          else 0
        end
      ) as ystd_ti_income_amt_1d,
      sum(
        case
          when market_target = '种草' then income_amt_2m
          else 0
        end
      ) as ti_income_amt_2m,
      sum(
        case
          when market_target = '种草' then income_amt_last_2m
          else 0
        end
      ) as ti_income_amt_last_2m,
      sum(
        case
          when market_target = '种草' then income_amt_before_2m
          else 0
        end
      ) as ti_income_amt_before_2m,


      sum(
        case
          when market_target = '线索' then cash_income_amt
          else 0
        end
      ) as leads_cash_income_amt,
      sum(
        case
          when market_target = '线索' then ystd_cash_income_amt_1d
          else 0
        end
      ) as ystd_leads_cash_income_amt_1d,
      sum(
        case
          when market_target = '线索' then cash_income_amt_2m
          else 0
        end
      ) as leads_cash_income_amt_2m,
      sum(
        case
          when market_target = '线索' then cash_income_amt_last_2m
          else 0
        end
      ) as leads_cash_income_amt_last_2m,
      sum(
        case
          when market_target = '线索' then cash_income_amt_before_2m
          else 0
        end
      ) as leads_cash_income_amt_before_2m,

      sum(
        case
          when market_target = '线索' then income_amt
          else 0
        end
      ) as leads_income_amt,
      sum(
        case
          when market_target = '线索' then ystd_income_amt_1d
          else 0
        end
      ) as ystd_leads_income_amt_1d,
      sum(
        case
          when market_target = '线索' then income_amt_2m
          else 0
        end
      ) as leads_income_amt_2m,
      sum(
        case
          when market_target = '线索' then income_amt_last_2m
          else 0
        end
      ) as leads_income_amt_last_2m,
      sum(
        case
          when market_target = '线索' then income_amt_before_2m
          else 0
        end
      ) as leads_income_amt_before_2m,


            sum(
        case
          when market_target = '闭环电商' then cash_income_amt
          else 0
        end
      ) as ecm_closed_cash_income_amt,
      sum(
        case
          when market_target = '闭环电商' then ystd_cash_income_amt_1d
          else 0
        end
      ) as ystd_ecm_closed_cash_income_amt_1d,
      sum(
        case
          when market_target = '闭环电商' then cash_income_amt_2m
          else 0
        end
      ) as ecm_closed_cash_income_amt_2m,
      sum(
        case
          when market_target = '闭环电商' then cash_income_amt_last_2m
          else 0
        end
      ) as ecm_closed_cash_income_amt_last_2m,
      sum(
        case
          when market_target = '闭环电商' then cash_income_amt_before_2m
          else 0
        end
      ) as ecm_closed_cash_income_amt_before_2m,

      sum(
        case
          when market_target = '闭环电商' then income_amt
          else 0
        end
      ) as ecm_closed_income_amt,
      sum(
        case
          when market_target = '闭环电商' then ystd_income_amt_1d
          else 0
        end
      ) as ystd_ecm_closed_income_amt_1d,
      sum(
        case
          when market_target = '闭环电商' then income_amt_2m
          else 0
        end
      ) as ecm_closed_income_amt_2m,
      sum(
        case
          when market_target = '闭环电商' then income_amt_last_2m
          else 0
        end
      ) as ecm_closed_income_amt_last_2m,
      sum(
        case
          when market_target = '闭环电商' then income_amt_before_2m
          else 0
        end
      ) as ecm_closed_income_amt_before_2m,

            sum(
        case
          when market_target = '非闭环电商' then cash_income_amt
          else 0
        end
      ) as ecm_unclosed_cash_income_amt,
      sum(
        case
          when market_target = '非闭环电商' then ystd_cash_income_amt_1d
          else 0
        end
      ) as ystd_ecm_unclosed_cash_income_amt_1d,
      sum(
        case
          when market_target = '非闭环电商' then cash_income_amt_2m
          else 0
        end
      ) as ecm_unclosed_cash_income_amt_2m,
      sum(
        case
          when market_target = '非闭环电商' then cash_income_amt_last_2m
          else 0
        end
      ) as ecm_unclosed_cash_income_amt_last_2m,
      sum(
        case
          when market_target = '非闭环电商' then cash_income_amt_before_2m
          else 0
        end
      ) as ecm_unclosed_cash_income_amt_before_2m,

      sum(
        case
          when market_target = '非闭环电商' then income_amt
          else 0
        end
      ) as ecm_unclosed_income_amt,
      sum(
        case
          when market_target = '非闭环电商' then ystd_income_amt_1d
          else 0
        end
      ) as ystd_ecm_unclosed_income_amt_1d,
      sum(
        case
          when market_target = '非闭环电商' then income_amt_2m
          else 0
        end
      ) as ecm_unclosed_income_amt_2m,
      sum(
        case
          when market_target = '非闭环电商' then income_amt_last_2m
          else 0
        end
      ) as ecm_unclosed_income_amt_last_2m,
      sum(
        case
          when market_target = '非闭环电商' then income_amt_before_2m
          else 0
        end
      ) as ecm_unclosed_income_amt_before_2m,
      --互动和预算
       sum(
        case
          when market_target = '种草' then engage_cnt
          else 0
        end
      ) as ti_engage_cnt,
      sum(
        case
          when market_target = '种草' then ystd_engage_cnt
          else 0
        end
      ) as ystd_ti_engage_cnt_1d,
      sum(
        case
          when market_target = '种草' then engage_cnt_2m
          else 0
        end
      ) as ti_engage_cnt_2m,
      sum(
        case
          when market_target = '种草' then engage_cnt_last_2m
          else 0
        end
      ) as ti_engage_cnt_last_2m,
      sum(
        case
          when market_target = '种草' then engage_cnt_before_2m
          else 0
        end
      ) as ti_engage_cnt_before_2m,

      sum(
        case
          when market_target = '种草' then rtb_budget_income_amt
          else 0
        end
      ) as ti_rtb_budget_income_amt,
      sum(
        case
          when market_target = '种草' then ystd_rtb_budget_income_amt_1d
          else 0
        end
      ) as ystd_ti_rtb_budget_income_amt_1d,
      sum(
        case
          when market_target = '种草' then rtb_budget_income_amt_2m
          else 0
        end
      ) as ti_rtb_budget_income_amt_2m,
      sum(
        case
          when market_target = '种草' then rtb_budget_income_amt_last_2m
          else 0
        end
      ) as ti_rtb_budget_income_amt_last_2m,
      sum(
        case
          when market_target = '种草' then rtb_budget_income_amt_before_2m
          else 0
        end
      ) as ti_rtb_budget_income_amt_before_2m,


      sum(
        case
          when market_target = '线索' then engage_cnt
          else 0
        end
      ) as leads_engage_cnt,
      sum(
        case
          when market_target = '线索' then ystd_engage_cnt
          else 0
        end
      ) as ystd_leads_engage_cnt_1d,
      sum(
        case
          when market_target = '线索' then engage_cnt_2m
          else 0
        end
      ) as leads_engage_cnt_2m,
      sum(
        case
          when market_target = '线索' then engage_cnt_last_2m
          else 0
        end
      ) as leads_engage_cnt_last_2m,
      sum(
        case
          when market_target = '线索' then engage_cnt_before_2m
          else 0
        end
      ) as leads_engage_cnt_before_2m,

      sum(
        case
          when market_target = '线索' then rtb_budget_income_amt
          else 0
        end
      ) as leads_rtb_budget_income_amt,
      sum(
        case
          when market_target = '线索' then ystd_rtb_budget_income_amt_1d
          else 0
        end
      ) as ystd_leads_rtb_budget_income_amt_1d,
      sum(
        case
          when market_target = '线索' then rtb_budget_income_amt_2m
          else 0
        end
      ) as leads_rtb_budget_income_amt_2m,
      sum(
        case
          when market_target = '线索' then rtb_budget_income_amt_last_2m
          else 0
        end
      ) as leads_rtb_budget_income_amt_last_2m,
      sum(
        case
          when market_target = '线索' then rtb_budget_income_amt_before_2m
          else 0
        end
      ) as leads_rtb_budget_income_amt_before_2m,


            sum(
        case
          when market_target = '闭环电商' then engage_cnt
          else 0
        end
      ) as ecm_closed_engage_cnt,
      sum(
        case
          when market_target = '闭环电商' then ystd_engage_cnt
          else 0
        end
      ) as ystd_ecm_closed_engage_cnt_1d,
      sum(
        case
          when market_target = '闭环电商' then engage_cnt_2m
          else 0
        end
      ) as ecm_closed_engage_cnt_2m,
      sum(
        case
          when market_target = '闭环电商' then engage_cnt_last_2m
          else 0
        end
      ) as ecm_closed_engage_cnt_last_2m,
      sum(
        case
          when market_target = '闭环电商' then engage_cnt_before_2m
          else 0
        end
      ) as ecm_closed_engage_cnt_before_2m,

      sum(
        case
          when market_target = '闭环电商' then rtb_budget_income_amt
          else 0
        end
      ) as ecm_closed_rtb_budget_income_amt,
      sum(
        case
          when market_target = '闭环电商' then ystd_rtb_budget_income_amt_1d
          else 0
        end
      ) as ystd_ecm_closed_rtb_budget_income_amt_1d,
      sum(
        case
          when market_target = '闭环电商' then rtb_budget_income_amt_2m
          else 0
        end
      ) as ecm_closed_rtb_budget_income_amt_2m,
      sum(
        case
          when market_target = '闭环电商' then rtb_budget_income_amt_last_2m
          else 0
        end
      ) as ecm_closed_rtb_budget_income_amt_last_2m,
      sum(
        case
          when market_target = '闭环电商' then rtb_budget_income_amt_before_2m
          else 0
        end
      ) as ecm_closed_rtb_budget_income_amt_before_2m,

            sum(
        case
          when market_target = '非闭环电商' then engage_cnt
          else 0
        end
      ) as ecm_unclosed_engage_cnt,
      sum(
        case
          when market_target = '非闭环电商' then ystd_engage_cnt
          else 0
        end
      ) as ystd_ecm_unclosed_engage_cnt_1d,
      sum(
        case
          when market_target = '非闭环电商' then engage_cnt_2m
          else 0
        end
      ) as ecm_unclosed_engage_cnt_2m,
      sum(
        case
          when market_target = '非闭环电商' then engage_cnt_last_2m
          else 0
        end
      ) as ecm_unclosed_engage_cnt_last_2m,
      sum(
        case
          when market_target = '非闭环电商' then engage_cnt_before_2m
          else 0
        end
      ) as ecm_unclosed_engage_cnt_before_2m,

      sum(
        case
          when market_target = '非闭环电商' then rtb_budget_income_amt
          else 0
        end
      ) as ecm_unclosed_rtb_budget_income_amt,
      sum(
        case
          when market_target = '非闭环电商' then ystd_rtb_budget_income_amt_1d
          else 0
        end
      ) as ystd_ecm_unclosed_rtb_budget_income_amt_1d,
      sum(
        case
          when market_target = '非闭环电商' then rtb_budget_income_amt_2m
          else 0
        end
      ) as ecm_unclosed_rtb_budget_income_amt_2m,
      sum(
        case
          when market_target = '非闭环电商' then rtb_budget_income_amt_last_2m
          else 0
        end
      ) as ecm_unclosed_rtb_budget_income_amt_last_2m,
      sum(
        case
          when market_target = '非闭环电商' then rtb_budget_income_amt_before_2m
          else 0
        end
      ) as ecm_unclosed_rtb_budget_income_amt_before_2m,
      
 sum(
        case
          when market_target = '闭环电商' then click_rgmv_7d
          else 0
        end
      ) as ecm_closed_click_rgmv_7d,
      sum(
        case
          when market_target = '闭环电商' then ystd_click_rgmv_7d_1d
          else 0
        end
      ) as ystd_ecm_closed_click_rgmv_7d_1d,
      sum(
        case
          when market_target = '闭环电商' then click_rgmv_7d_2m
          else 0
        end
      ) as ecm_closed_click_rgmv_7d_2m,
      sum(
        case
          when market_target = '闭环电商' then click_rgmv_7d_last_2m
          else 0
        end
      ) as ecm_closed_click_rgmv_7d_last_2m,
      sum(
        case
          when market_target = '闭环电商' then click_rgmv_7d_before_2m
          else 0
        end
      ) as ecm_closed_click_rgmv_7d_before_2m,
            sum(
        case
          when market_target = '非闭环电商' then ecm_unclosed_rgmv
          else 0
        end
      ) as ecm_unclosed_ecm_unclosed_rgmv,
      sum(
        case
          when market_target = '非闭环电商' then ystd_ecm_unclosed_rgmv_1d
          else 0
        end
      ) as ystd_ecm_unclosed_ecm_unclosed_rgmv_1d,
      sum(
        case
          when market_target = '非闭环电商' then ecm_unclosed_rgmv_2m
          else 0
        end
      ) as ecm_unclosed_ecm_unclosed_rgmv_2m,
      sum(
        case
          when market_target = '非闭环电商' then ecm_unclosed_rgmv_last_2m
          else 0
        end
      ) as ecm_unclosed_ecm_unclosed_rgmv_last_2m,
      sum(
        case
          when market_target = '非闭环电商' then ecm_unclosed_rgmv_before_2m
          else 0
        end
      ) as ecm_unclosed_ecm_unclosed_rgmv_before_2m,

      
sum(
        case
          when market_target = '线索' then conversion_cnt
          else 0
        end
      ) as leads_conversion_cnt,
      sum(
        case
          when market_target = '线索' then ystd_conversion_cnt_1d
          else 0
        end
      ) as ystd_leads_conversion_cnt_1d,
      sum(
        case
          when market_target = '线索' then conversion_cnt_2m
          else 0
        end
      ) as leads_conversion_cnt_2m,
      sum(
        case
          when market_target = '线索' then conversion_cnt_last_2m
          else 0
        end
      ) as leads_conversion_cnt_last_2m,
      sum(
        case
          when market_target = '线索' then conversion_cnt_before_2m
          else 0
        end
      ) as leads_conversion_cnt_before_2m,

      sum(
        case
          when market_target = '种草' then rtb_cost_income_amt
          else 0
        end
      ) as ti_rtb_cost_income_amt,
      sum(
        case
          when market_target = '种草' then ystd_rtb_cost_income_amt_1d
          else 0
        end
      ) as ystd_ti_rtb_cost_income_amt_1d,
      sum(
        case
          when market_target = '种草' then rtb_cost_income_amt_2m
          else 0
        end
      ) as ti_rtb_cost_income_amt_2m,
      sum(
        case
          when market_target = '种草' then rtb_cost_income_amt_last_2m
          else 0
        end
      ) as ti_rtb_cost_income_amt_last_2m,
      sum(
        case
          when market_target = '种草' then rtb_cost_income_amt_before_2m
          else 0
        end
      ) as ti_rtb_cost_income_amt_before_2m,


      
      sum(
        case
          when market_target = '线索' then rtb_cost_income_amt
          else 0
        end
      ) as leads_rtb_cost_income_amt,
      sum(
        case
          when market_target = '线索' then ystd_rtb_cost_income_amt_1d
          else 0
        end
      ) as ystd_leads_rtb_cost_income_amt_1d,
      sum(
        case
          when market_target = '线索' then rtb_cost_income_amt_2m
          else 0
        end
      ) as leads_rtb_cost_income_amt_2m,
      sum(
        case
          when market_target = '线索' then rtb_cost_income_amt_last_2m
          else 0
        end
      ) as leads_rtb_cost_income_amt_last_2m,
      sum(
        case
          when market_target = '线索' then rtb_cost_income_amt_before_2m
          else 0
        end
      ) as leads_rtb_cost_income_amt_before_2m,


           

      sum(
        case
          when market_target = '闭环电商' then rtb_cost_income_amt
          else 0
        end
      ) as ecm_closed_rtb_cost_income_amt,
      sum(
        case
          when market_target = '闭环电商' then ystd_rtb_cost_income_amt_1d
          else 0
        end
      ) as ystd_ecm_closed_rtb_cost_income_amt_1d,
      sum(
        case
          when market_target = '闭环电商' then rtb_cost_income_amt_2m
          else 0
        end
      ) as ecm_closed_rtb_cost_income_amt_2m,
      sum(
        case
          when market_target = '闭环电商' then rtb_cost_income_amt_last_2m
          else 0
        end
      ) as ecm_closed_rtb_cost_income_amt_last_2m,
      sum(
        case
          when market_target = '闭环电商' then rtb_cost_income_amt_before_2m
          else 0
        end
      ) as ecm_closed_rtb_cost_income_amt_before_2m,

           

      sum(
        case
          when market_target = '非闭环电商' then rtb_cost_income_amt
          else 0
        end
      ) as ecm_unclosed_rtb_cost_income_amt,
      sum(
        case
          when market_target = '非闭环电商' then ystd_rtb_cost_income_amt_1d
          else 0
        end
      ) as ystd_ecm_unclosed_rtb_cost_income_amt_1d,
      sum(
        case
          when market_target = '非闭环电商' then rtb_cost_income_amt_2m
          else 0
        end
      ) as ecm_unclosed_rtb_cost_income_amt_2m,
      sum(
        case
          when market_target = '非闭环电商' then rtb_cost_income_amt_last_2m
          else 0
        end
      ) as ecm_unclosed_rtb_cost_income_amt_last_2m,
      sum(
        case
          when market_target = '非闭环电商' then rtb_cost_income_amt_before_2m
          else 0
        end
      ) as ecm_unclosed_rtb_cost_income_amt_before_2m,
      --搜索流水双月 
      sum(
        case
          when product = '搜索' then cash_income_amt_2m
          else 0
        end
      ) as search_cash_income_amt_2m,
      sum(
        case
          when marketing_target in (8,14) then cash_income_amt_2m
          else 0
        end
      ) as ecm_closed_live_cash_income_amt_2m,
      sum(
        case
          when marketing_target in (3,8,14,15) then cash_income_amt_2m
          else 0
        end
      ) as ecm_closed_sx_cash_income_amt_2m
--直播推广广告流水
--本双月商销广告流水   

    from
      redapp.app_ads_insight_advertiser_product_diagnosis_info_df
    where
      dtm = '{{ds_nodash}}'
     and module in ('效果')
   group by brand_account_id,
  advertiser_id,
  v_seller_id,
  v_seller_name,
  agent_user_id,
  agent_user_name,
  -- module,
  -- product,
  -- marketing_target,
  -- market_target,
  brand_account_name,
  company_code,
  company_name,
  track_group_name,
  direct_sales_name,
  direct_sales_dept1_name,
  direct_sales_dept2_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_group_tag_code,
  brand_group_tag_name,
  cpc_operator_code,
  cpc_operator_name,
  first_industry_name,
  second_industry_name,
  track_industry_name,
  track_detail_name,
  brand_id,
  brand_name,
  planner_name,
  staff_name,
  process_track_industry_name,
  process_track_group_name,
  process_track_second_name,
  process_track_third_name,
  channel_sales_name,
  channel_sales_code,
  channel_operator_name;

  --广告主指标
  drop table if exists temp.app_ads_insight_advertiser_product_diagnosis_info_df_cmd_01_{{ds_nodash}}_account;
create table temp.app_ads_insight_advertiser_product_diagnosis_info_df_cmd_01_{{ds_nodash}}_account
select t1.brand_account_id,
  t1.brand_account_name,
  t1.company_code,
  t1.company_name,
  t1.track_group_name,
  t1.direct_sales_name,
  t1.direct_sales_dept1_name,
  t1.direct_sales_dept2_name,
  t1.direct_sales_dept3_name,
  t1.direct_sales_dept4_name,
  t1.direct_sales_dept5_name,
  t1.direct_sales_dept6_name,
  t1.brand_group_tag_code,
  t1.brand_group_tag_name,
  t1.cpc_operator_code,
  t1.cpc_operator_name,
  t1.first_industry_name,
  t1.second_industry_name,
  t1.track_industry_name,
  t1.track_detail_name,
  t1.brand_id,
  t1.brand_name,
  t1.planner_name,
  t1.staff_name,
  t1.process_track_industry_name,
  t1.process_track_group_name,
  t1.process_track_second_name,
  t1.process_track_third_name,
  '' as channel_sales_name,
  '' as channel_sales_code,
  '' as channel_operator_name,
  --广告流水
  t1.ads_cash_income_amt,
  t1.ystd_ads_cash_income_amt_1d,
  t1.ads_cash_income_amt_2m,
  t1.ads_cash_income_amt_last_2m,
  t1.ads_cash_income_amt_before_2m,
  --其他双月指标
  t1.ecm_closed_dgmv_2m,
  t1.cps_note_num_2m,
  t1.s_live_dgmv_2m,
  t1.is_sx_ti_2m,
  t1.taolian_cash_income_amt_2m,
  t1.bcoo_cash_income_amt_2m,
  t1.track_taolian_cash_income_amt_2m,
  t1.track_bcoo_cash_income_amt_2m,
  t1.ecm_closed_dgmv_cash_income_amt_2m,
  t1.track_ecm_closed_dgmv_2m,
  t1.track_ecm_closed_dgmv_cash_income_amt_2m,
cash_income_amt,
income_amt,
imp_cnt,
click_cnt,
like_cnt,
fav_cnt,
cmt_cnt,
share_cnt,
follow_cnt,
screenshot_cnt,
engage_cnt,
click_rgmv_7d,
conversion_cnt,
ecm_unclosed_rgmv,
rtb_cost_income_amt,
rtb_budget_income_amt,
cost_income_amt,
budget_income_amt,
advertiser_budget_income_amt,
advertiser_cost_income_amt,
cash_balance,
ystd_cash_income_amt_1d,
ystd_income_amt_1d,
ystd_imp_cnt_1d,
ystd_click_cnt_1d,
ystd_like_cnt_1d,
ystd_fav_cnt_1d,
ystd_cmt_cnt_1d,
ystd_share_cnt_1d,
ystd_follow_cnt_1d,
ystd_screenshot_cnt_1d,
ystd_engage_cnt_1d,
ystd_click_rgmv_7d_1d,
ystd_conversion_cnt_1d,
ystd_ecm_unclosed_rgmv_1d,
ystd_rtb_cost_income_amt_1d,
ystd_rtb_budget_income_amt_1d,
ystd_cost_income_amt_1d,
ystd_budget_income_amt_1d,
ystd_advertiser_budget_income_amt_1d,
ystd_advertiser_cost_income_amt_1d,
ystd_cash_balance_1d,
cash_income_amt_2m,
income_amt_2m,
imp_cnt_2m,
click_cnt_2m,
like_cnt_2m,
fav_cnt_2m,
cmt_cnt_2m,
share_cnt_2m,
follow_cnt_2m,
screenshot_cnt_2m,
engage_cnt_2m,
click_rgmv_7d_2m,
conversion_cnt_2m,
ecm_unclosed_rgmv_2m,
rtb_cost_income_amt_2m,
rtb_budget_income_amt_2m,
cost_income_amt_2m,
budget_income_amt_2m,
advertiser_budget_income_amt_2m,
advertiser_cost_income_amt_2m,
cash_balance_2m,
cash_income_amt_last_2m,
income_amt_last_2m,
imp_cnt_last_2m,
click_cnt_last_2m,
like_cnt_last_2m,
fav_cnt_last_2m,
cmt_cnt_last_2m,
share_cnt_last_2m,
follow_cnt_last_2m,
screenshot_cnt_last_2m,
engage_cnt_last_2m,
click_rgmv_7d_last_2m,
conversion_cnt_last_2m,
ecm_unclosed_rgmv_last_2m,
rtb_cost_income_amt_last_2m,
rtb_budget_income_amt_last_2m,
cost_income_amt_last_2m,
budget_income_amt_last_2m,
advertiser_budget_income_amt_last_2m,
advertiser_cost_income_amt_last_2m,
cash_balance_last_2m,
cash_income_amt_before_2m,
income_amt_before_2m,
imp_cnt_before_2m,
click_cnt_before_2m,
like_cnt_before_2m,
fav_cnt_before_2m,
cmt_cnt_before_2m,
share_cnt_before_2m,
follow_cnt_before_2m,
screenshot_cnt_before_2m,
engage_cnt_before_2m,
click_rgmv_7d_before_2m,
conversion_cnt_before_2m,
ecm_unclosed_rgmv_before_2m,
rtb_cost_income_amt_before_2m,
rtb_budget_income_amt_before_2m,
cost_income_amt_before_2m,
budget_income_amt_before_2m,
advertiser_budget_income_amt_before_2m,
advertiser_cost_income_amt_before_2m,
cash_balance_before_2m,
track_group_cost_income_amt,
track_group_budget_income_amt,
track_group_advertiser_cost_income_amt,
track_group_advertiser_budget_income_amt,
track_group_rtb_cost_income_amt,
track_group_rtb_budget_income_amt,
track_group_cash_income_amt_before_2m,
track_group_cash_income_amt_2m,
track_group_cash_income_amt_last_2m,
track_cost_income_amt,
track_budget_income_amt,
track_advertiser_cost_income_amt,
track_advertiser_budget_income_amt,
track_rtb_cost_income_amt,
track_rtb_budget_income_amt,
cash_income_amt_1y,
cash_income_amt_before_1y,
ti_cash_income_amt,
ystd_ti_cash_income_amt_1d,
ti_cash_income_amt_2m,
ti_cash_income_amt_last_2m,
ti_cash_income_amt_before_2m,
ti_income_amt,
ystd_ti_income_amt_1d,
ti_income_amt_2m,
ti_income_amt_last_2m,
ti_income_amt_before_2m,
leads_cash_income_amt,
ystd_leads_cash_income_amt_1d,
leads_cash_income_amt_2m,
leads_cash_income_amt_last_2m,
leads_cash_income_amt_before_2m,
leads_income_amt,
ystd_leads_income_amt_1d,
leads_income_amt_2m,
leads_income_amt_last_2m,
leads_income_amt_before_2m,
ecm_closed_cash_income_amt,
ystd_ecm_closed_cash_income_amt_1d,
ecm_closed_cash_income_amt_2m,
ecm_closed_cash_income_amt_last_2m,
ecm_closed_cash_income_amt_before_2m,
ecm_closed_income_amt,
ystd_ecm_closed_income_amt_1d,
ecm_closed_income_amt_2m,
ecm_closed_income_amt_last_2m,
ecm_closed_income_amt_before_2m,
ecm_unclosed_cash_income_amt,
ystd_ecm_unclosed_cash_income_amt_1d,
ecm_unclosed_cash_income_amt_2m,
ecm_unclosed_cash_income_amt_last_2m,
ecm_unclosed_cash_income_amt_before_2m,
ecm_unclosed_income_amt,
ystd_ecm_unclosed_income_amt_1d,
ecm_unclosed_income_amt_2m,
ecm_unclosed_income_amt_last_2m,
ecm_unclosed_income_amt_before_2m,
ti_engage_cnt,
ystd_ti_engage_cnt_1d,
ti_engage_cnt_2m,
ti_engage_cnt_last_2m,
ti_engage_cnt_before_2m,
ti_rtb_budget_income_amt,
ystd_ti_rtb_budget_income_amt_1d,
ti_rtb_budget_income_amt_2m,
ti_rtb_budget_income_amt_last_2m,
ti_rtb_budget_income_amt_before_2m,
leads_engage_cnt,
ystd_leads_engage_cnt_1d,
leads_engage_cnt_2m,
leads_engage_cnt_last_2m,
leads_engage_cnt_before_2m,
leads_rtb_budget_income_amt,
ystd_leads_rtb_budget_income_amt_1d,
leads_rtb_budget_income_amt_2m,
leads_rtb_budget_income_amt_last_2m,
leads_rtb_budget_income_amt_before_2m,
ecm_closed_engage_cnt,
ystd_ecm_closed_engage_cnt_1d,
ecm_closed_engage_cnt_2m,
ecm_closed_engage_cnt_last_2m,
ecm_closed_engage_cnt_before_2m,
ecm_closed_rtb_budget_income_amt,
ystd_ecm_closed_rtb_budget_income_amt_1d,
ecm_closed_rtb_budget_income_amt_2m,
ecm_closed_rtb_budget_income_amt_last_2m,
ecm_closed_rtb_budget_income_amt_before_2m,
ecm_unclosed_engage_cnt,
ystd_ecm_unclosed_engage_cnt_1d,
ecm_unclosed_engage_cnt_2m,
ecm_unclosed_engage_cnt_last_2m,
ecm_unclosed_engage_cnt_before_2m,
ecm_unclosed_rtb_budget_income_amt,
ystd_ecm_unclosed_rtb_budget_income_amt_1d,
ecm_unclosed_rtb_budget_income_amt_2m,
ecm_unclosed_rtb_budget_income_amt_last_2m,
ecm_unclosed_rtb_budget_income_amt_before_2m,
ecm_closed_click_rgmv_7d,
ystd_ecm_closed_click_rgmv_7d_1d,
ecm_closed_click_rgmv_7d_2m,
ecm_closed_click_rgmv_7d_last_2m,
ecm_closed_click_rgmv_7d_before_2m,
ecm_unclosed_ecm_unclosed_rgmv,
ystd_ecm_unclosed_ecm_unclosed_rgmv_1d,
ecm_unclosed_ecm_unclosed_rgmv_2m,
ecm_unclosed_ecm_unclosed_rgmv_last_2m,
ecm_unclosed_ecm_unclosed_rgmv_before_2m,
leads_conversion_cnt,
ystd_leads_conversion_cnt_1d,
leads_conversion_cnt_2m,
leads_conversion_cnt_last_2m,
leads_conversion_cnt_before_2m,
ti_rtb_cost_income_amt,
ystd_ti_rtb_cost_income_amt_1d,
ti_rtb_cost_income_amt_2m,
ti_rtb_cost_income_amt_last_2m,
ti_rtb_cost_income_amt_before_2m,
leads_rtb_cost_income_amt,
ystd_leads_rtb_cost_income_amt_1d,
leads_rtb_cost_income_amt_2m,
leads_rtb_cost_income_amt_last_2m,
leads_rtb_cost_income_amt_before_2m,
ecm_closed_rtb_cost_income_amt,
ystd_ecm_closed_rtb_cost_income_amt_1d,
ecm_closed_rtb_cost_income_amt_2m,
ecm_closed_rtb_cost_income_amt_last_2m,
ecm_closed_rtb_cost_income_amt_before_2m,
ecm_unclosed_rtb_cost_income_amt,
ystd_ecm_unclosed_rtb_cost_income_amt_1d,
ecm_unclosed_rtb_cost_income_amt_2m,
ecm_unclosed_rtb_cost_income_amt_last_2m,
ecm_unclosed_rtb_cost_income_amt_before_2m,
search_cash_income_amt_2m,
ecm_closed_live_cash_income_amt_2m,
ecm_closed_sx_cash_income_amt_2m
from 
(select brand_account_id,
  brand_account_name,
  company_code,
  company_name,
  track_group_name,
  max(direct_sales_name) as direct_sales_name,
  max(direct_sales_dept1_name) as direct_sales_dept1_name,
  max(direct_sales_dept2_name) as direct_sales_dept2_name,
  max(direct_sales_dept3_name) as direct_sales_dept3_name,
  max(direct_sales_dept4_name) as direct_sales_dept4_name,
  max(direct_sales_dept5_name) as direct_sales_dept5_name,
  max(direct_sales_dept6_name) as direct_sales_dept6_name,
  brand_group_tag_code,
  brand_group_tag_name,
  max(cpc_operator_code) as cpc_operator_code,
  max(cpc_operator_name) as cpc_operator_name,
  first_industry_name,
  second_industry_name,
  track_industry_name,
  track_detail_name,
  brand_id,
  brand_name,
  planner_name,
  max(staff_name) as staff_name,
  process_track_industry_name,
  process_track_group_name,
  process_track_second_name,
  process_track_third_name,
  
  --广告流水
  sum(cash_income_amt) as ads_cash_income_amt,
sum(ystd_cash_income_amt_1d) as ystd_ads_cash_income_amt_1d,
sum(cash_income_amt_2m) as ads_cash_income_amt_2m,
sum(cash_income_amt_last_2m) as ads_cash_income_amt_last_2m,
sum(cash_income_amt_before_2m) as ads_cash_income_amt_before_2m,
--其他双月指标
sum(ecm_closed_dgmv_2m) as ecm_closed_dgmv_2m,
sum(  cps_note_num_2m) as   cps_note_num_2m,
sum(  s_live_dgmv_2m) as   s_live_dgmv_2m,
max(  is_sx_ti) as   is_sx_ti_2m,
sum(  taolian_cash_income_amt_2m) as   taolian_cash_income_amt_2m,
sum(  bcoo_cash_income_amt_2m) as   bcoo_cash_income_amt_2m,
max(  track_taolian_cash_income_amt_2m) as   track_taolian_cash_income_amt_2m,
max(  track_bcoo_cash_income_amt_2m) as   track_bcoo_cash_income_amt_2m,
sum(  ecm_closed_dgmv_cash_income_amt_2m) as   ecm_closed_dgmv_cash_income_amt_2m,
max(  track_ecm_closed_dgmv_2m) as   track_ecm_closed_dgmv_2m,
max(  track_ecm_closed_dgmv_cash_income_amt_2m) as   track_ecm_closed_dgmv_cash_income_amt_2m
from
      redapp.app_ads_insight_advertiser_product_diagnosis_info_df
where
  dtm = '{{ds_nodash}}'
 and module in ('效果','品牌','薯条','整体')
group by brand_account_id,
  brand_account_name,
  company_code,
  company_name,
  track_group_name,
  -- max(direct_sales_name) as direct_sales_name,
  -- max(direct_sales_dept1_name) as direct_sales_dept1_name,
  -- max(direct_sales_dept2_name) as direct_sales_dept2_name,
  -- max(direct_sales_dept3_name) as direct_sales_dept3_name,
  -- max(direct_sales_dept4_name) as direct_sales_dept4_name,
  -- max(direct_sales_dept5_name) as direct_sales_dept5_name,
  -- max(direct_sales_dept6_name) as direct_sales_dept6_name,
  brand_group_tag_code,
  brand_group_tag_name,
  -- max(cpc_operator_code) as cpc_operator_code,
  -- max(cpc_operator_name) as cpc_operator_name,
  first_industry_name,
  second_industry_name,
  track_industry_name,
  track_detail_name,
  brand_id,
  brand_name,
  planner_name,
  --staff_name,
  process_track_industry_name,
  process_track_group_name,
  process_track_second_name,
  process_track_third_name
)t1 --广告已广告主独有双月指标
left join 
(select brand_account_id,
  -- advertiser_id,
  -- v_seller_id,
  -- v_seller_name,
  -- agent_user_id,
  -- agent_user_name,
  -- module,
  -- product,
  -- marketing_target,
  -- market_target,
  brand_account_name,
  company_code,
  company_name,
  track_group_name,
  direct_sales_name,
  direct_sales_dept1_name,
  direct_sales_dept2_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_group_tag_code,
  brand_group_tag_name,
  cpc_operator_code,
  cpc_operator_name,
  first_industry_name,
  second_industry_name,
  track_industry_name,
  track_detail_name,
  brand_id,
  brand_name,
  planner_name,
  --staff_name,
  process_track_industry_name,
  process_track_group_name,
  process_track_second_name,
  process_track_third_name,
  -- channel_sales_name,
  -- channel_sales_code,
  -- channel_operator_name,
sum(cash_income_amt) as cash_income_amt,
sum(income_amt) as income_amt,
sum(imp_cnt) as imp_cnt,
sum(click_cnt) as click_cnt,
sum(like_cnt) as like_cnt,
sum(fav_cnt) as fav_cnt,
sum(cmt_cnt) as cmt_cnt,
sum(share_cnt) as share_cnt,
sum(follow_cnt) as follow_cnt,
sum(screenshot_cnt) as screenshot_cnt,
sum(engage_cnt) as engage_cnt,
sum(click_rgmv_7d) as click_rgmv_7d,
sum(conversion_cnt) as conversion_cnt,
sum(ecm_unclosed_rgmv) as ecm_unclosed_rgmv,
sum(rtb_cost_income_amt) as rtb_cost_income_amt,
sum(rtb_budget_income_amt) as rtb_budget_income_amt,
sum(cost_income_amt) as cost_income_amt,
sum(budget_income_amt) as budget_income_amt,
sum(advertiser_budget_income_amt) as advertiser_budget_income_amt,
sum(advertiser_cost_income_amt) as advertiser_cost_income_amt,
sum(cash_balance) as cash_balance,
sum(ystd_cash_income_amt_1d) as ystd_cash_income_amt_1d,
sum(ystd_income_amt_1d) as ystd_income_amt_1d,
sum(ystd_imp_cnt_1d) as ystd_imp_cnt_1d,
sum(ystd_click_cnt_1d) as ystd_click_cnt_1d,
sum(ystd_like_cnt_1d) as ystd_like_cnt_1d,
sum(ystd_fav_cnt_1d) as ystd_fav_cnt_1d,
sum(ystd_cmt_cnt_1d) as ystd_cmt_cnt_1d,
sum(ystd_share_cnt_1d) as ystd_share_cnt_1d,
sum(ystd_follow_cnt_1d) as ystd_follow_cnt_1d,
sum(ystd_screenshot_cnt_1d) as ystd_screenshot_cnt_1d,
sum(ystd_engage_cnt_1d) as ystd_engage_cnt_1d,
sum(ystd_click_rgmv_7d_1d) as ystd_click_rgmv_7d_1d,
sum(ystd_conversion_cnt_1d) as ystd_conversion_cnt_1d,
sum(ystd_ecm_unclosed_rgmv_1d) as ystd_ecm_unclosed_rgmv_1d,
sum(ystd_rtb_cost_income_amt_1d) as ystd_rtb_cost_income_amt_1d,
sum(ystd_rtb_budget_income_amt_1d) as ystd_rtb_budget_income_amt_1d,
sum(ystd_cost_income_amt_1d) as ystd_cost_income_amt_1d,
sum(ystd_budget_income_amt_1d) as ystd_budget_income_amt_1d,
sum(ystd_advertiser_budget_income_amt_1d) as ystd_advertiser_budget_income_amt_1d,
sum(ystd_advertiser_cost_income_amt_1d) as ystd_advertiser_cost_income_amt_1d,
sum(ystd_cash_balance_1d) as ystd_cash_balance_1d,
sum(cash_income_amt_2m) as cash_income_amt_2m,
sum(income_amt_2m) as income_amt_2m,
sum(imp_cnt_2m) as imp_cnt_2m,
sum(click_cnt_2m) as click_cnt_2m,
sum(like_cnt_2m) as like_cnt_2m,
sum(fav_cnt_2m) as fav_cnt_2m,
sum(cmt_cnt_2m) as cmt_cnt_2m,
sum(share_cnt_2m) as share_cnt_2m,
sum(follow_cnt_2m) as follow_cnt_2m,
sum(screenshot_cnt_2m) as screenshot_cnt_2m,
sum(engage_cnt_2m) as engage_cnt_2m,
sum(click_rgmv_7d_2m) as click_rgmv_7d_2m,
sum(conversion_cnt_2m) as conversion_cnt_2m,
sum(ecm_unclosed_rgmv_2m) as ecm_unclosed_rgmv_2m,
sum(rtb_cost_income_amt_2m) as rtb_cost_income_amt_2m,
sum(rtb_budget_income_amt_2m) as rtb_budget_income_amt_2m,
sum(cost_income_amt_2m) as cost_income_amt_2m,
sum(budget_income_amt_2m) as budget_income_amt_2m,
sum(advertiser_budget_income_amt_2m) as advertiser_budget_income_amt_2m,
sum(advertiser_cost_income_amt_2m) as advertiser_cost_income_amt_2m,
sum(cash_balance_2m) as cash_balance_2m,
sum(cash_income_amt_last_2m) as cash_income_amt_last_2m,
sum(income_amt_last_2m) as income_amt_last_2m,
sum(imp_cnt_last_2m) as imp_cnt_last_2m,
sum(click_cnt_last_2m) as click_cnt_last_2m,
sum(like_cnt_last_2m) as like_cnt_last_2m,
sum(fav_cnt_last_2m) as fav_cnt_last_2m,
sum(cmt_cnt_last_2m) as cmt_cnt_last_2m,
sum(share_cnt_last_2m) as share_cnt_last_2m,
sum(follow_cnt_last_2m) as follow_cnt_last_2m,
sum(screenshot_cnt_last_2m) as screenshot_cnt_last_2m,
sum(engage_cnt_last_2m) as engage_cnt_last_2m,
sum(click_rgmv_7d_last_2m) as click_rgmv_7d_last_2m,
sum(conversion_cnt_last_2m) as conversion_cnt_last_2m,
sum(ecm_unclosed_rgmv_last_2m) as ecm_unclosed_rgmv_last_2m,
sum(rtb_cost_income_amt_last_2m) as rtb_cost_income_amt_last_2m,
sum(rtb_budget_income_amt_last_2m) as rtb_budget_income_amt_last_2m,
sum(cost_income_amt_last_2m) as cost_income_amt_last_2m,
sum(budget_income_amt_last_2m) as budget_income_amt_last_2m,
sum(advertiser_budget_income_amt_last_2m) as advertiser_budget_income_amt_last_2m,
sum(advertiser_cost_income_amt_last_2m) as advertiser_cost_income_amt_last_2m,
sum(cash_balance_last_2m) as cash_balance_last_2m,
sum(cash_income_amt_before_2m) as cash_income_amt_before_2m,
sum(income_amt_before_2m) as income_amt_before_2m,
sum(imp_cnt_before_2m) as imp_cnt_before_2m,
sum(click_cnt_before_2m) as click_cnt_before_2m,
sum(like_cnt_before_2m) as like_cnt_before_2m,
sum(fav_cnt_before_2m) as fav_cnt_before_2m,
sum(cmt_cnt_before_2m) as cmt_cnt_before_2m,
sum(share_cnt_before_2m) as share_cnt_before_2m,
sum(follow_cnt_before_2m) as follow_cnt_before_2m,
sum(screenshot_cnt_before_2m) as screenshot_cnt_before_2m,
sum(engage_cnt_before_2m) as engage_cnt_before_2m,
sum(click_rgmv_7d_before_2m) as click_rgmv_7d_before_2m,
sum(conversion_cnt_before_2m) as conversion_cnt_before_2m,
sum(ecm_unclosed_rgmv_before_2m) as ecm_unclosed_rgmv_before_2m,
sum(rtb_cost_income_amt_before_2m) as rtb_cost_income_amt_before_2m,
sum(rtb_budget_income_amt_before_2m) as rtb_budget_income_amt_before_2m,
sum(cost_income_amt_before_2m) as cost_income_amt_before_2m,
sum(budget_income_amt_before_2m) as budget_income_amt_before_2m,
sum(advertiser_budget_income_amt_before_2m) as advertiser_budget_income_amt_before_2m,
sum(advertiser_cost_income_amt_before_2m) as advertiser_cost_income_amt_before_2m,
sum(cash_balance_before_2m) as cash_balance_before_2m,
max(track_group_cost_income_amt) as track_group_cost_income_amt,
max(track_group_budget_income_amt) as track_group_budget_income_amt,
max(track_group_advertiser_cost_income_amt) as track_group_advertiser_cost_income_amt,
max(track_group_advertiser_budget_income_amt) as track_group_advertiser_budget_income_amt,
max(track_group_rtb_cost_income_amt) as track_group_rtb_cost_income_amt,
max(track_group_rtb_budget_income_amt) as track_group_rtb_budget_income_amt,
max(track_group_cash_income_amt_before_2m) as track_group_cash_income_amt_before_2m,
max(track_group_cash_income_amt_2m) as track_group_cash_income_amt_2m,
max(track_group_cash_income_amt_last_2m) as track_group_cash_income_amt_last_2m,
max(track_cost_income_amt) as track_cost_income_amt,
max(track_budget_income_amt) as track_budget_income_amt,
max(track_advertiser_cost_income_amt) as track_advertiser_cost_income_amt,
max(track_advertiser_budget_income_amt) as track_advertiser_budget_income_amt,
max(track_rtb_cost_income_amt) as track_rtb_cost_income_amt,
max(track_rtb_budget_income_amt) as track_rtb_budget_income_amt,
sum(cash_income_amt_1y) as cash_income_amt_1y,
sum(cash_income_amt_before_1y) as cash_income_amt_before_1y,
sum(ti_cash_income_amt) as ti_cash_income_amt,
sum(ystd_ti_cash_income_amt_1d) as ystd_ti_cash_income_amt_1d,
sum(ti_cash_income_amt_2m) as ti_cash_income_amt_2m,
sum(ti_cash_income_amt_last_2m) as ti_cash_income_amt_last_2m,
sum(ti_cash_income_amt_before_2m) as ti_cash_income_amt_before_2m,
sum(ti_income_amt) as ti_income_amt,
sum(ystd_ti_income_amt_1d) as ystd_ti_income_amt_1d,
sum(ti_income_amt_2m) as ti_income_amt_2m,
sum(ti_income_amt_last_2m) as ti_income_amt_last_2m,
sum(ti_income_amt_before_2m) as ti_income_amt_before_2m,
sum(leads_cash_income_amt) as leads_cash_income_amt,
sum(ystd_leads_cash_income_amt_1d) as ystd_leads_cash_income_amt_1d,
sum(leads_cash_income_amt_2m) as leads_cash_income_amt_2m,
sum(leads_cash_income_amt_last_2m) as leads_cash_income_amt_last_2m,
sum(leads_cash_income_amt_before_2m) as leads_cash_income_amt_before_2m,
sum(leads_income_amt) as leads_income_amt,
sum(ystd_leads_income_amt_1d) as ystd_leads_income_amt_1d,
sum(leads_income_amt_2m) as leads_income_amt_2m,
sum(leads_income_amt_last_2m) as leads_income_amt_last_2m,
sum(leads_income_amt_before_2m) as leads_income_amt_before_2m,
sum(ecm_closed_cash_income_amt) as ecm_closed_cash_income_amt,
sum(ystd_ecm_closed_cash_income_amt_1d) as ystd_ecm_closed_cash_income_amt_1d,
sum(ecm_closed_cash_income_amt_2m) as ecm_closed_cash_income_amt_2m,
sum(ecm_closed_cash_income_amt_last_2m) as ecm_closed_cash_income_amt_last_2m,
sum(ecm_closed_cash_income_amt_before_2m) as ecm_closed_cash_income_amt_before_2m,
sum(ecm_closed_income_amt) as ecm_closed_income_amt,
sum(ystd_ecm_closed_income_amt_1d) as ystd_ecm_closed_income_amt_1d,
sum(ecm_closed_income_amt_2m) as ecm_closed_income_amt_2m,
sum(ecm_closed_income_amt_last_2m) as ecm_closed_income_amt_last_2m,
sum(ecm_closed_income_amt_before_2m) as ecm_closed_income_amt_before_2m,
sum(ecm_unclosed_cash_income_amt) as ecm_unclosed_cash_income_amt,
sum(ystd_ecm_unclosed_cash_income_amt_1d) as ystd_ecm_unclosed_cash_income_amt_1d,
sum(ecm_unclosed_cash_income_amt_2m) as ecm_unclosed_cash_income_amt_2m,
sum(ecm_unclosed_cash_income_amt_last_2m) as ecm_unclosed_cash_income_amt_last_2m,
sum(ecm_unclosed_cash_income_amt_before_2m) as ecm_unclosed_cash_income_amt_before_2m,
sum(ecm_unclosed_income_amt) as ecm_unclosed_income_amt,
sum(ystd_ecm_unclosed_income_amt_1d) as ystd_ecm_unclosed_income_amt_1d,
sum(ecm_unclosed_income_amt_2m) as ecm_unclosed_income_amt_2m,
sum(ecm_unclosed_income_amt_last_2m) as ecm_unclosed_income_amt_last_2m,
sum(ecm_unclosed_income_amt_before_2m) as ecm_unclosed_income_amt_before_2m,
sum(ti_engage_cnt) as ti_engage_cnt,
sum(ystd_ti_engage_cnt_1d) as ystd_ti_engage_cnt_1d,
sum(ti_engage_cnt_2m) as ti_engage_cnt_2m,
sum(ti_engage_cnt_last_2m) as ti_engage_cnt_last_2m,
sum(ti_engage_cnt_before_2m) as ti_engage_cnt_before_2m,
sum(ti_rtb_budget_income_amt) as ti_rtb_budget_income_amt,
sum(ystd_ti_rtb_budget_income_amt_1d) as ystd_ti_rtb_budget_income_amt_1d,
sum(ti_rtb_budget_income_amt_2m) as ti_rtb_budget_income_amt_2m,
sum(ti_rtb_budget_income_amt_last_2m) as ti_rtb_budget_income_amt_last_2m,
sum(ti_rtb_budget_income_amt_before_2m) as ti_rtb_budget_income_amt_before_2m,
sum(leads_engage_cnt) as leads_engage_cnt,
sum(ystd_leads_engage_cnt_1d) as ystd_leads_engage_cnt_1d,
sum(leads_engage_cnt_2m) as leads_engage_cnt_2m,
sum(leads_engage_cnt_last_2m) as leads_engage_cnt_last_2m,
sum(leads_engage_cnt_before_2m) as leads_engage_cnt_before_2m,
sum(leads_rtb_budget_income_amt) as leads_rtb_budget_income_amt,
sum(ystd_leads_rtb_budget_income_amt_1d) as ystd_leads_rtb_budget_income_amt_1d,
sum(leads_rtb_budget_income_amt_2m) as leads_rtb_budget_income_amt_2m,
sum(leads_rtb_budget_income_amt_last_2m) as leads_rtb_budget_income_amt_last_2m,
sum(leads_rtb_budget_income_amt_before_2m) as leads_rtb_budget_income_amt_before_2m,
sum(ecm_closed_engage_cnt) as ecm_closed_engage_cnt,
sum(ystd_ecm_closed_engage_cnt_1d) as ystd_ecm_closed_engage_cnt_1d,
sum(ecm_closed_engage_cnt_2m) as ecm_closed_engage_cnt_2m,
sum(ecm_closed_engage_cnt_last_2m) as ecm_closed_engage_cnt_last_2m,
sum(ecm_closed_engage_cnt_before_2m) as ecm_closed_engage_cnt_before_2m,
sum(ecm_closed_rtb_budget_income_amt) as ecm_closed_rtb_budget_income_amt,
sum(ystd_ecm_closed_rtb_budget_income_amt_1d) as ystd_ecm_closed_rtb_budget_income_amt_1d,
sum(ecm_closed_rtb_budget_income_amt_2m) as ecm_closed_rtb_budget_income_amt_2m,
sum(ecm_closed_rtb_budget_income_amt_last_2m) as ecm_closed_rtb_budget_income_amt_last_2m,
sum(ecm_closed_rtb_budget_income_amt_before_2m) as ecm_closed_rtb_budget_income_amt_before_2m,
sum(ecm_unclosed_engage_cnt) as ecm_unclosed_engage_cnt,
sum(ystd_ecm_unclosed_engage_cnt_1d) as ystd_ecm_unclosed_engage_cnt_1d,
sum(ecm_unclosed_engage_cnt_2m) as ecm_unclosed_engage_cnt_2m,
sum(ecm_unclosed_engage_cnt_last_2m) as ecm_unclosed_engage_cnt_last_2m,
sum(ecm_unclosed_engage_cnt_before_2m) as ecm_unclosed_engage_cnt_before_2m,
sum(ecm_unclosed_rtb_budget_income_amt) as ecm_unclosed_rtb_budget_income_amt,
sum(ystd_ecm_unclosed_rtb_budget_income_amt_1d) as ystd_ecm_unclosed_rtb_budget_income_amt_1d,
sum(ecm_unclosed_rtb_budget_income_amt_2m) as ecm_unclosed_rtb_budget_income_amt_2m,
sum(ecm_unclosed_rtb_budget_income_amt_last_2m) as ecm_unclosed_rtb_budget_income_amt_last_2m,
sum(ecm_unclosed_rtb_budget_income_amt_before_2m) as ecm_unclosed_rtb_budget_income_amt_before_2m,
sum(ecm_closed_click_rgmv_7d) as ecm_closed_click_rgmv_7d,
sum(ystd_ecm_closed_click_rgmv_7d_1d) as ystd_ecm_closed_click_rgmv_7d_1d,
sum(ecm_closed_click_rgmv_7d_2m) as ecm_closed_click_rgmv_7d_2m,
sum(ecm_closed_click_rgmv_7d_last_2m) as ecm_closed_click_rgmv_7d_last_2m,
sum(ecm_closed_click_rgmv_7d_before_2m) as ecm_closed_click_rgmv_7d_before_2m,
sum(ecm_unclosed_ecm_unclosed_rgmv) as ecm_unclosed_ecm_unclosed_rgmv,
sum(ystd_ecm_unclosed_ecm_unclosed_rgmv_1d) as ystd_ecm_unclosed_ecm_unclosed_rgmv_1d,
sum(ecm_unclosed_ecm_unclosed_rgmv_2m) as ecm_unclosed_ecm_unclosed_rgmv_2m,
sum(ecm_unclosed_ecm_unclosed_rgmv_last_2m) as ecm_unclosed_ecm_unclosed_rgmv_last_2m,
sum(ecm_unclosed_ecm_unclosed_rgmv_before_2m) as ecm_unclosed_ecm_unclosed_rgmv_before_2m,
sum(leads_conversion_cnt) as leads_conversion_cnt,
sum(ystd_leads_conversion_cnt_1d) as ystd_leads_conversion_cnt_1d,
sum(leads_conversion_cnt_2m) as leads_conversion_cnt_2m,
sum(leads_conversion_cnt_last_2m) as leads_conversion_cnt_last_2m,
sum(leads_conversion_cnt_before_2m) as leads_conversion_cnt_before_2m,
sum(ti_rtb_cost_income_amt) as ti_rtb_cost_income_amt,
sum(ystd_ti_rtb_cost_income_amt_1d) as ystd_ti_rtb_cost_income_amt_1d,
sum(ti_rtb_cost_income_amt_2m) as ti_rtb_cost_income_amt_2m,
sum(ti_rtb_cost_income_amt_last_2m) as ti_rtb_cost_income_amt_last_2m,
sum(ti_rtb_cost_income_amt_before_2m) as ti_rtb_cost_income_amt_before_2m,
sum(leads_rtb_cost_income_amt) as leads_rtb_cost_income_amt,
sum(ystd_leads_rtb_cost_income_amt_1d) as ystd_leads_rtb_cost_income_amt_1d,
sum(leads_rtb_cost_income_amt_2m) as leads_rtb_cost_income_amt_2m,
sum(leads_rtb_cost_income_amt_last_2m) as leads_rtb_cost_income_amt_last_2m,
sum(leads_rtb_cost_income_amt_before_2m) as leads_rtb_cost_income_amt_before_2m,
sum(ecm_closed_rtb_cost_income_amt) as ecm_closed_rtb_cost_income_amt,
sum(ystd_ecm_closed_rtb_cost_income_amt_1d) as ystd_ecm_closed_rtb_cost_income_amt_1d,
sum(ecm_closed_rtb_cost_income_amt_2m) as ecm_closed_rtb_cost_income_amt_2m,
sum(ecm_closed_rtb_cost_income_amt_last_2m) as ecm_closed_rtb_cost_income_amt_last_2m,
sum(ecm_closed_rtb_cost_income_amt_before_2m) as ecm_closed_rtb_cost_income_amt_before_2m,
sum(ecm_unclosed_rtb_cost_income_amt) as ecm_unclosed_rtb_cost_income_amt,
sum(ystd_ecm_unclosed_rtb_cost_income_amt_1d) as ystd_ecm_unclosed_rtb_cost_income_amt_1d,
sum(ecm_unclosed_rtb_cost_income_amt_2m) as ecm_unclosed_rtb_cost_income_amt_2m,
sum(ecm_unclosed_rtb_cost_income_amt_last_2m) as ecm_unclosed_rtb_cost_income_amt_last_2m,
sum(ecm_unclosed_rtb_cost_income_amt_before_2m) as ecm_unclosed_rtb_cost_income_amt_before_2m,
sum(search_cash_income_amt_2m) as search_cash_income_amt_2m,
sum(ecm_closed_live_cash_income_amt_2m) as ecm_closed_live_cash_income_amt_2m,
sum(ecm_closed_sx_cash_income_amt_2m) as ecm_closed_sx_cash_income_amt_2m

    from
      temp.app_ads_insight_advertiser_product_diagnosis_info_df_cmd_01_{{ds_nodash}}
   group by brand_account_id,
  -- advertiser_id,
  -- v_seller_id,
  -- v_seller_name,
  -- agent_user_id,
  -- agent_user_name,
  -- module,
  -- product,
  -- marketing_target,
  -- market_target,
  brand_account_name,
  company_code,
  company_name,
  track_group_name,
  direct_sales_name,
  direct_sales_dept1_name,
  direct_sales_dept2_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_group_tag_code,
  brand_group_tag_name,
  cpc_operator_code,
  cpc_operator_name,
  first_industry_name,
  second_industry_name,
  track_industry_name,
  track_detail_name,
  brand_id,
  brand_name,
  planner_name,
  --staff_name,
  process_track_industry_name,
  process_track_group_name,
  process_track_second_name,
  process_track_third_name
  -- channel_sales_name,
  -- channel_sales_code,
  -- channel_operator_name
  )t2 
on t1.brand_account_id = t2.brand_account_id;


--插入数据--------
--插入数据
select  account_id,
   acccount_type,
   account_name,
  brand_account_id,
   advertiser_id,
 v_seller_id,
 v_seller_name,
agent_user_id,
agent_user_name,
--  module,
--   product,
--   marketing_target,
--   market_target,
  brand_account_name,
  company_code,
  company_name,
  track_group_name,
  direct_sales_name,
  direct_sales_dept1_name,
  direct_sales_dept2_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_group_tag_code,
  brand_group_tag_name,
  cpc_operator_code,
  cpc_operator_name,
  first_industry_name,
  second_industry_name,
  track_industry_name,
  track_detail_name,
  brand_id,
  brand_name,
  planner_name,
  staff_name,
  process_track_industry_name,
  process_track_group_name,
  process_track_second_name,
  process_track_third_name,
  channel_sales_name,
  channel_sales_code,
  channel_operator_name,
  cash_income_amt,
income_amt,
imp_cnt,
click_cnt,
like_cnt,
fav_cnt,
cmt_cnt,
share_cnt,
follow_cnt,
screenshot_cnt,
engage_cnt,
click_rgmv_7d,
conversion_cnt,
ecm_unclosed_rgmv,
rtb_cost_income_amt,
rtb_budget_income_amt,
cost_income_amt,
budget_income_amt,
advertiser_budget_income_amt,
advertiser_cost_income_amt,
cash_balance,
ystd_cash_income_amt_1d,
ystd_income_amt_1d,
ystd_imp_cnt_1d,
ystd_click_cnt_1d,
ystd_like_cnt_1d,
ystd_fav_cnt_1d,
ystd_cmt_cnt_1d,
ystd_share_cnt_1d,
ystd_follow_cnt_1d,
ystd_screenshot_cnt_1d,
ystd_engage_cnt_1d,
ystd_click_rgmv_7d_1d,
ystd_conversion_cnt_1d,
ystd_ecm_unclosed_rgmv_1d,
ystd_rtb_cost_income_amt_1d,
ystd_rtb_budget_income_amt_1d,
ystd_cost_income_amt_1d,
ystd_budget_income_amt_1d,
ystd_advertiser_budget_income_amt_1d,
ystd_advertiser_cost_income_amt_1d,
ystd_cash_balance_1d,
cash_income_amt_2m,
income_amt_2m,
imp_cnt_2m,
click_cnt_2m,
like_cnt_2m,
fav_cnt_2m,
cmt_cnt_2m,
share_cnt_2m,
follow_cnt_2m,
screenshot_cnt_2m,
engage_cnt_2m,
click_rgmv_7d_2m,
conversion_cnt_2m,
ecm_unclosed_rgmv_2m,
rtb_cost_income_amt_2m,
rtb_budget_income_amt_2m,
cost_income_amt_2m,
budget_income_amt_2m,
advertiser_budget_income_amt_2m,
advertiser_cost_income_amt_2m,
cash_balance_2m,
cash_income_amt_last_2m,
income_amt_last_2m,
imp_cnt_last_2m,
click_cnt_last_2m,
like_cnt_last_2m,
fav_cnt_last_2m,
cmt_cnt_last_2m,
share_cnt_last_2m,
follow_cnt_last_2m,
screenshot_cnt_last_2m,
engage_cnt_last_2m,
click_rgmv_7d_last_2m,
conversion_cnt_last_2m,
ecm_unclosed_rgmv_last_2m,
rtb_cost_income_amt_last_2m,
rtb_budget_income_amt_last_2m,
cost_income_amt_last_2m,
budget_income_amt_last_2m,
advertiser_budget_income_amt_last_2m,
advertiser_cost_income_amt_last_2m,
cash_balance_last_2m,
cash_income_amt_before_2m,
income_amt_before_2m,
imp_cnt_before_2m,
click_cnt_before_2m,
like_cnt_before_2m,
fav_cnt_before_2m,
cmt_cnt_before_2m,
share_cnt_before_2m,
follow_cnt_before_2m,
screenshot_cnt_before_2m,
engage_cnt_before_2m,
click_rgmv_7d_before_2m,
conversion_cnt_before_2m,
ecm_unclosed_rgmv_before_2m,
rtb_cost_income_amt_before_2m,
rtb_budget_income_amt_before_2m,
cost_income_amt_before_2m,
budget_income_amt_before_2m,
advertiser_budget_income_amt_before_2m,
advertiser_cost_income_amt_before_2m,
cash_balance_before_2m,
track_group_cost_income_amt,
track_group_budget_income_amt,
track_group_advertiser_cost_income_amt,
track_group_advertiser_budget_income_amt,
track_group_rtb_cost_income_amt,
track_group_rtb_budget_income_amt,
track_group_cash_income_amt_before_2m,
track_group_cash_income_amt_2m,
track_group_cash_income_amt_last_2m,
track_cost_income_amt,
track_budget_income_amt,
track_advertiser_cost_income_amt,
track_advertiser_budget_income_amt,
track_rtb_cost_income_amt,
track_rtb_budget_income_amt,
cash_income_amt_1y,
cash_income_amt_before_1y,
ti_cash_income_amt,
ystd_ti_cash_income_amt_1d,
ti_cash_income_amt_2m,
ti_cash_income_amt_last_2m,
ti_cash_income_amt_before_2m,
ti_income_amt,
ystd_ti_income_amt_1d,
ti_income_amt_2m,
ti_income_amt_last_2m,
ti_income_amt_before_2m,
leads_cash_income_amt,
ystd_leads_cash_income_amt_1d,
leads_cash_income_amt_2m,
leads_cash_income_amt_last_2m,
leads_cash_income_amt_before_2m,
leads_income_amt,
ystd_leads_income_amt_1d,
leads_income_amt_2m,
leads_income_amt_last_2m,
leads_income_amt_before_2m,
ecm_closed_cash_income_amt,
ystd_ecm_closed_cash_income_amt_1d,
ecm_closed_cash_income_amt_2m,
ecm_closed_cash_income_amt_last_2m,
ecm_closed_cash_income_amt_before_2m,
ecm_closed_income_amt,
ystd_ecm_closed_income_amt_1d,
ecm_closed_income_amt_2m,
ecm_closed_income_amt_last_2m,
ecm_closed_income_amt_before_2m,
ecm_unclosed_cash_income_amt,
ystd_ecm_unclosed_cash_income_amt_1d,
ecm_unclosed_cash_income_amt_2m,
ecm_unclosed_cash_income_amt_last_2m,
ecm_unclosed_cash_income_amt_before_2m,
ecm_unclosed_income_amt,
ystd_ecm_unclosed_income_amt_1d,
ecm_unclosed_income_amt_2m,
ecm_unclosed_income_amt_last_2m,
ecm_unclosed_income_amt_before_2m,
ti_engage_cnt,
ystd_ti_engage_cnt_1d,
ti_engage_cnt_2m,
ti_engage_cnt_last_2m,
ti_engage_cnt_before_2m,
ti_rtb_budget_income_amt,
ystd_ti_rtb_budget_income_amt_1d,
ti_rtb_budget_income_amt_2m,
ti_rtb_budget_income_amt_last_2m,
ti_rtb_budget_income_amt_before_2m,
leads_engage_cnt,
ystd_leads_engage_cnt_1d,
leads_engage_cnt_2m,
leads_engage_cnt_last_2m,
leads_engage_cnt_before_2m,
leads_rtb_budget_income_amt,
ystd_leads_rtb_budget_income_amt_1d,
leads_rtb_budget_income_amt_2m,
leads_rtb_budget_income_amt_last_2m,
leads_rtb_budget_income_amt_before_2m,
ecm_closed_engage_cnt,
ystd_ecm_closed_engage_cnt_1d,
ecm_closed_engage_cnt_2m,
ecm_closed_engage_cnt_last_2m,
ecm_closed_engage_cnt_before_2m,
ecm_closed_rtb_budget_income_amt,
ystd_ecm_closed_rtb_budget_income_amt_1d,
ecm_closed_rtb_budget_income_amt_2m,
ecm_closed_rtb_budget_income_amt_last_2m,
ecm_closed_rtb_budget_income_amt_before_2m,
ecm_unclosed_engage_cnt,
ystd_ecm_unclosed_engage_cnt_1d,
ecm_unclosed_engage_cnt_2m,
ecm_unclosed_engage_cnt_last_2m,
ecm_unclosed_engage_cnt_before_2m,
ecm_unclosed_rtb_budget_income_amt,
ystd_ecm_unclosed_rtb_budget_income_amt_1d,
ecm_unclosed_rtb_budget_income_amt_2m,
ecm_unclosed_rtb_budget_income_amt_last_2m,
ecm_unclosed_rtb_budget_income_amt_before_2m,
ecm_closed_click_rgmv_7d,
ystd_ecm_closed_click_rgmv_7d_1d,
ecm_closed_click_rgmv_7d_2m,
ecm_closed_click_rgmv_7d_last_2m,
ecm_closed_click_rgmv_7d_before_2m,
ecm_unclosed_ecm_unclosed_rgmv,
ystd_ecm_unclosed_ecm_unclosed_rgmv_1d,
ecm_unclosed_ecm_unclosed_rgmv_2m,
ecm_unclosed_ecm_unclosed_rgmv_last_2m,
ecm_unclosed_ecm_unclosed_rgmv_before_2m,
leads_conversion_cnt,
ystd_leads_conversion_cnt_1d,
leads_conversion_cnt_2m,
leads_conversion_cnt_last_2m,
leads_conversion_cnt_before_2m,
ti_rtb_cost_income_amt,
ystd_ti_rtb_cost_income_amt_1d,
ti_rtb_cost_income_amt_2m,
ti_rtb_cost_income_amt_last_2m,
ti_rtb_cost_income_amt_before_2m,
leads_rtb_cost_income_amt,
ystd_leads_rtb_cost_income_amt_1d,
leads_rtb_cost_income_amt_2m,
leads_rtb_cost_income_amt_last_2m,
leads_rtb_cost_income_amt_before_2m,
ecm_closed_rtb_cost_income_amt,
ystd_ecm_closed_rtb_cost_income_amt_1d,
ecm_closed_rtb_cost_income_amt_2m,
ecm_closed_rtb_cost_income_amt_last_2m,
ecm_closed_rtb_cost_income_amt_before_2m,
ecm_unclosed_rtb_cost_income_amt,
ystd_ecm_unclosed_rtb_cost_income_amt_1d,
ecm_unclosed_rtb_cost_income_amt_2m,
ecm_unclosed_rtb_cost_income_amt_last_2m,
ecm_unclosed_rtb_cost_income_amt_before_2m,
search_cash_income_amt_2m,
ecm_closed_live_cash_income_amt_2m,
ecm_closed_sx_cash_income_amt_2m,
ads_cash_income_amt,
ystd_ads_cash_income_amt_1d,
ads_cash_income_amt_2m,
ads_cash_income_amt_last_2m,
ads_cash_income_amt_before_2m,
ecm_closed_dgmv_2m,
cps_note_num_2m,
s_live_dgmv_2m,
is_sx_ti_2m,
taolian_cash_income_amt_2m,
bcoo_cash_income_amt_2m,
track_taolian_cash_income_amt_2m,
track_bcoo_cash_income_amt_2m,
ecm_closed_dgmv_cash_income_amt_2m,
track_ecm_closed_dgmv_2m,
track_ecm_closed_dgmv_cash_income_amt_2m,
cast('{{ds_nodash}}' as int) as dtm
from 
--1-广告主，2-子账号，3-代理商 
(--1广告主
select brand_account_id as account_id,
  1 as acccount_type,
  brand_account_name as account_name,
  brand_account_id,
  null as advertiser_id,
  null as v_seller_id,
  null as v_seller_name,
  null as agent_user_id,
  null as agent_user_name,
  null as  module,
  null as  product,
  null as  marketing_target,
  null as  market_target,
  brand_account_name,
  company_code,
  company_name,
  track_group_name,
  direct_sales_name,
  direct_sales_dept1_name,
  direct_sales_dept2_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_group_tag_code,
  brand_group_tag_name,
  cpc_operator_code,
  cpc_operator_name,
  first_industry_name,
  second_industry_name,
  track_industry_name,
  track_detail_name,
  brand_id,
  brand_name,
  planner_name,
  staff_name,
  process_track_industry_name,
  process_track_group_name,
  process_track_second_name,
  process_track_third_name,
  channel_sales_name,
  channel_sales_code,
  channel_operator_name,
cash_income_amt,
income_amt,
imp_cnt,
click_cnt,
like_cnt,
fav_cnt,
cmt_cnt,
share_cnt,
follow_cnt,
screenshot_cnt,
engage_cnt,
click_rgmv_7d,
conversion_cnt,
ecm_unclosed_rgmv,
rtb_cost_income_amt,
rtb_budget_income_amt,
cost_income_amt,
budget_income_amt,
advertiser_budget_income_amt,
advertiser_cost_income_amt,
cash_balance,
ystd_cash_income_amt_1d,
ystd_income_amt_1d,
ystd_imp_cnt_1d,
ystd_click_cnt_1d,
ystd_like_cnt_1d,
ystd_fav_cnt_1d,
ystd_cmt_cnt_1d,
ystd_share_cnt_1d,
ystd_follow_cnt_1d,
ystd_screenshot_cnt_1d,
ystd_engage_cnt_1d,
ystd_click_rgmv_7d_1d,
ystd_conversion_cnt_1d,
ystd_ecm_unclosed_rgmv_1d,
ystd_rtb_cost_income_amt_1d,
ystd_rtb_budget_income_amt_1d,
ystd_cost_income_amt_1d,
ystd_budget_income_amt_1d,
ystd_advertiser_budget_income_amt_1d,
ystd_advertiser_cost_income_amt_1d,
ystd_cash_balance_1d,
cash_income_amt_2m,
income_amt_2m,
imp_cnt_2m,
click_cnt_2m,
like_cnt_2m,
fav_cnt_2m,
cmt_cnt_2m,
share_cnt_2m,
follow_cnt_2m,
screenshot_cnt_2m,
engage_cnt_2m,
click_rgmv_7d_2m,
conversion_cnt_2m,
ecm_unclosed_rgmv_2m,
rtb_cost_income_amt_2m,
rtb_budget_income_amt_2m,
cost_income_amt_2m,
budget_income_amt_2m,
advertiser_budget_income_amt_2m,
advertiser_cost_income_amt_2m,
cash_balance_2m,
cash_income_amt_last_2m,
income_amt_last_2m,
imp_cnt_last_2m,
click_cnt_last_2m,
like_cnt_last_2m,
fav_cnt_last_2m,
cmt_cnt_last_2m,
share_cnt_last_2m,
follow_cnt_last_2m,
screenshot_cnt_last_2m,
engage_cnt_last_2m,
click_rgmv_7d_last_2m,
conversion_cnt_last_2m,
ecm_unclosed_rgmv_last_2m,
rtb_cost_income_amt_last_2m,
rtb_budget_income_amt_last_2m,
cost_income_amt_last_2m,
budget_income_amt_last_2m,
advertiser_budget_income_amt_last_2m,
advertiser_cost_income_amt_last_2m,
cash_balance_last_2m,
cash_income_amt_before_2m,
income_amt_before_2m,
imp_cnt_before_2m,
click_cnt_before_2m,
like_cnt_before_2m,
fav_cnt_before_2m,
cmt_cnt_before_2m,
share_cnt_before_2m,
follow_cnt_before_2m,
screenshot_cnt_before_2m,
engage_cnt_before_2m,
click_rgmv_7d_before_2m,
conversion_cnt_before_2m,
ecm_unclosed_rgmv_before_2m,
rtb_cost_income_amt_before_2m,
rtb_budget_income_amt_before_2m,
cost_income_amt_before_2m,
budget_income_amt_before_2m,
advertiser_budget_income_amt_before_2m,
advertiser_cost_income_amt_before_2m,
cash_balance_before_2m,
track_group_cost_income_amt,
track_group_budget_income_amt,
track_group_advertiser_cost_income_amt,
track_group_advertiser_budget_income_amt,
track_group_rtb_cost_income_amt,
track_group_rtb_budget_income_amt,
track_group_cash_income_amt_before_2m,
track_group_cash_income_amt_2m,
track_group_cash_income_amt_last_2m,
track_cost_income_amt,
track_budget_income_amt,
track_advertiser_cost_income_amt,
track_advertiser_budget_income_amt,
track_rtb_cost_income_amt,
track_rtb_budget_income_amt,
cash_income_amt_1y,
cash_income_amt_before_1y,
ti_cash_income_amt,
ystd_ti_cash_income_amt_1d,
ti_cash_income_amt_2m,
ti_cash_income_amt_last_2m,
ti_cash_income_amt_before_2m,
ti_income_amt,
ystd_ti_income_amt_1d,
ti_income_amt_2m,
ti_income_amt_last_2m,
ti_income_amt_before_2m,
leads_cash_income_amt,
ystd_leads_cash_income_amt_1d,
leads_cash_income_amt_2m,
leads_cash_income_amt_last_2m,
leads_cash_income_amt_before_2m,
leads_income_amt,
ystd_leads_income_amt_1d,
leads_income_amt_2m,
leads_income_amt_last_2m,
leads_income_amt_before_2m,
ecm_closed_cash_income_amt,
ystd_ecm_closed_cash_income_amt_1d,
ecm_closed_cash_income_amt_2m,
ecm_closed_cash_income_amt_last_2m,
ecm_closed_cash_income_amt_before_2m,
ecm_closed_income_amt,
ystd_ecm_closed_income_amt_1d,
ecm_closed_income_amt_2m,
ecm_closed_income_amt_last_2m,
ecm_closed_income_amt_before_2m,
ecm_unclosed_cash_income_amt,
ystd_ecm_unclosed_cash_income_amt_1d,
ecm_unclosed_cash_income_amt_2m,
ecm_unclosed_cash_income_amt_last_2m,
ecm_unclosed_cash_income_amt_before_2m,
ecm_unclosed_income_amt,
ystd_ecm_unclosed_income_amt_1d,
ecm_unclosed_income_amt_2m,
ecm_unclosed_income_amt_last_2m,
ecm_unclosed_income_amt_before_2m,
ti_engage_cnt,
ystd_ti_engage_cnt_1d,
ti_engage_cnt_2m,
ti_engage_cnt_last_2m,
ti_engage_cnt_before_2m,
ti_rtb_budget_income_amt,
ystd_ti_rtb_budget_income_amt_1d,
ti_rtb_budget_income_amt_2m,
ti_rtb_budget_income_amt_last_2m,
ti_rtb_budget_income_amt_before_2m,
leads_engage_cnt,
ystd_leads_engage_cnt_1d,
leads_engage_cnt_2m,
leads_engage_cnt_last_2m,
leads_engage_cnt_before_2m,
leads_rtb_budget_income_amt,
ystd_leads_rtb_budget_income_amt_1d,
leads_rtb_budget_income_amt_2m,
leads_rtb_budget_income_amt_last_2m,
leads_rtb_budget_income_amt_before_2m,
ecm_closed_engage_cnt,
ystd_ecm_closed_engage_cnt_1d,
ecm_closed_engage_cnt_2m,
ecm_closed_engage_cnt_last_2m,
ecm_closed_engage_cnt_before_2m,
ecm_closed_rtb_budget_income_amt,
ystd_ecm_closed_rtb_budget_income_amt_1d,
ecm_closed_rtb_budget_income_amt_2m,
ecm_closed_rtb_budget_income_amt_last_2m,
ecm_closed_rtb_budget_income_amt_before_2m,
ecm_unclosed_engage_cnt,
ystd_ecm_unclosed_engage_cnt_1d,
ecm_unclosed_engage_cnt_2m,
ecm_unclosed_engage_cnt_last_2m,
ecm_unclosed_engage_cnt_before_2m,
ecm_unclosed_rtb_budget_income_amt,
ystd_ecm_unclosed_rtb_budget_income_amt_1d,
ecm_unclosed_rtb_budget_income_amt_2m,
ecm_unclosed_rtb_budget_income_amt_last_2m,
ecm_unclosed_rtb_budget_income_amt_before_2m,
ecm_closed_click_rgmv_7d,
ystd_ecm_closed_click_rgmv_7d_1d,
ecm_closed_click_rgmv_7d_2m,
ecm_closed_click_rgmv_7d_last_2m,
ecm_closed_click_rgmv_7d_before_2m,
ecm_unclosed_ecm_unclosed_rgmv,
ystd_ecm_unclosed_ecm_unclosed_rgmv_1d,
ecm_unclosed_ecm_unclosed_rgmv_2m,
ecm_unclosed_ecm_unclosed_rgmv_last_2m,
ecm_unclosed_ecm_unclosed_rgmv_before_2m,
leads_conversion_cnt,
ystd_leads_conversion_cnt_1d,
leads_conversion_cnt_2m,
leads_conversion_cnt_last_2m,
leads_conversion_cnt_before_2m,
ti_rtb_cost_income_amt,
ystd_ti_rtb_cost_income_amt_1d,
ti_rtb_cost_income_amt_2m,
ti_rtb_cost_income_amt_last_2m,
ti_rtb_cost_income_amt_before_2m,
leads_rtb_cost_income_amt,
ystd_leads_rtb_cost_income_amt_1d,
leads_rtb_cost_income_amt_2m,
leads_rtb_cost_income_amt_last_2m,
leads_rtb_cost_income_amt_before_2m,
ecm_closed_rtb_cost_income_amt,
ystd_ecm_closed_rtb_cost_income_amt_1d,
ecm_closed_rtb_cost_income_amt_2m,
ecm_closed_rtb_cost_income_amt_last_2m,
ecm_closed_rtb_cost_income_amt_before_2m,
ecm_unclosed_rtb_cost_income_amt,
ystd_ecm_unclosed_rtb_cost_income_amt_1d,
ecm_unclosed_rtb_cost_income_amt_2m,
ecm_unclosed_rtb_cost_income_amt_last_2m,
ecm_unclosed_rtb_cost_income_amt_before_2m,
search_cash_income_amt_2m,
ecm_closed_live_cash_income_amt_2m,
ecm_closed_sx_cash_income_amt_2m,
--广告流水
  ads_cash_income_amt,
  ystd_ads_cash_income_amt_1d,
  ads_cash_income_amt_2m,
  ads_cash_income_amt_last_2m,
  ads_cash_income_amt_before_2m,
  --其他双月指标
  ecm_closed_dgmv_2m,
  cps_note_num_2m,
  s_live_dgmv_2m,
  is_sx_ti_2m,
  taolian_cash_income_amt_2m,
  bcoo_cash_income_amt_2m,
  track_taolian_cash_income_amt_2m,
  track_bcoo_cash_income_amt_2m,
  ecm_closed_dgmv_cash_income_amt_2m,
  track_ecm_closed_dgmv_2m,
  track_ecm_closed_dgmv_cash_income_amt_2m
from  temp.app_ads_insight_advertiser_product_diagnosis_info_df_cmd_01_{{ds_nodash}}_account
union all 
--2子账号
select v_seller_id as account_id,
  2 as acccount_type,
  v_seller_name as account_name,
  brand_account_id,
  advertiser_id,
   v_seller_id,
  v_seller_name,
  agent_user_id,
 agent_user_name,
  null as  module,
  null as  product,
  null as  marketing_target,
  null as  market_target,
  brand_account_name,
  company_code,
  company_name,
  track_group_name,
  direct_sales_name,
  direct_sales_dept1_name,
  direct_sales_dept2_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_group_tag_code,
  brand_group_tag_name,
  cpc_operator_code,
  cpc_operator_name,
  first_industry_name,
  second_industry_name,
  track_industry_name,
  track_detail_name,
  brand_id,
  brand_name,
  planner_name,
  staff_name,
  process_track_industry_name,
  process_track_group_name,
  process_track_second_name,
  process_track_third_name,
  channel_sales_name,
  channel_sales_code,
  channel_operator_name,
cash_income_amt,
income_amt,
imp_cnt,
click_cnt,
like_cnt,
fav_cnt,
cmt_cnt,
share_cnt,
follow_cnt,
screenshot_cnt,
engage_cnt,
click_rgmv_7d,
conversion_cnt,
ecm_unclosed_rgmv,
rtb_cost_income_amt,
rtb_budget_income_amt,
cost_income_amt,
budget_income_amt,
advertiser_budget_income_amt,
advertiser_cost_income_amt,
cash_balance,
ystd_cash_income_amt_1d,
ystd_income_amt_1d,
ystd_imp_cnt_1d,
ystd_click_cnt_1d,
ystd_like_cnt_1d,
ystd_fav_cnt_1d,
ystd_cmt_cnt_1d,
ystd_share_cnt_1d,
ystd_follow_cnt_1d,
ystd_screenshot_cnt_1d,
ystd_engage_cnt_1d,
ystd_click_rgmv_7d_1d,
ystd_conversion_cnt_1d,
ystd_ecm_unclosed_rgmv_1d,
ystd_rtb_cost_income_amt_1d,
ystd_rtb_budget_income_amt_1d,
ystd_cost_income_amt_1d,
ystd_budget_income_amt_1d,
ystd_advertiser_budget_income_amt_1d,
ystd_advertiser_cost_income_amt_1d,
ystd_cash_balance_1d,
cash_income_amt_2m,
income_amt_2m,
imp_cnt_2m,
click_cnt_2m,
like_cnt_2m,
fav_cnt_2m,
cmt_cnt_2m,
share_cnt_2m,
follow_cnt_2m,
screenshot_cnt_2m,
engage_cnt_2m,
click_rgmv_7d_2m,
conversion_cnt_2m,
ecm_unclosed_rgmv_2m,
rtb_cost_income_amt_2m,
rtb_budget_income_amt_2m,
cost_income_amt_2m,
budget_income_amt_2m,
advertiser_budget_income_amt_2m,
advertiser_cost_income_amt_2m,
cash_balance_2m,
cash_income_amt_last_2m,
income_amt_last_2m,
imp_cnt_last_2m,
click_cnt_last_2m,
like_cnt_last_2m,
fav_cnt_last_2m,
cmt_cnt_last_2m,
share_cnt_last_2m,
follow_cnt_last_2m,
screenshot_cnt_last_2m,
engage_cnt_last_2m,
click_rgmv_7d_last_2m,
conversion_cnt_last_2m,
ecm_unclosed_rgmv_last_2m,
rtb_cost_income_amt_last_2m,
rtb_budget_income_amt_last_2m,
cost_income_amt_last_2m,
budget_income_amt_last_2m,
advertiser_budget_income_amt_last_2m,
advertiser_cost_income_amt_last_2m,
cash_balance_last_2m,
cash_income_amt_before_2m,
income_amt_before_2m,
imp_cnt_before_2m,
click_cnt_before_2m,
like_cnt_before_2m,
fav_cnt_before_2m,
cmt_cnt_before_2m,
share_cnt_before_2m,
follow_cnt_before_2m,
screenshot_cnt_before_2m,
engage_cnt_before_2m,
click_rgmv_7d_before_2m,
conversion_cnt_before_2m,
ecm_unclosed_rgmv_before_2m,
rtb_cost_income_amt_before_2m,
rtb_budget_income_amt_before_2m,
cost_income_amt_before_2m,
budget_income_amt_before_2m,
advertiser_budget_income_amt_before_2m,
advertiser_cost_income_amt_before_2m,
cash_balance_before_2m,
track_group_cost_income_amt,
track_group_budget_income_amt,
track_group_advertiser_cost_income_amt,
track_group_advertiser_budget_income_amt,
track_group_rtb_cost_income_amt,
track_group_rtb_budget_income_amt,
track_group_cash_income_amt_before_2m,
track_group_cash_income_amt_2m,
track_group_cash_income_amt_last_2m,
track_cost_income_amt,
track_budget_income_amt,
track_advertiser_cost_income_amt,
track_advertiser_budget_income_amt,
track_rtb_cost_income_amt,
track_rtb_budget_income_amt,
cash_income_amt_1y,
cash_income_amt_before_1y,
ti_cash_income_amt,
ystd_ti_cash_income_amt_1d,
ti_cash_income_amt_2m,
ti_cash_income_amt_last_2m,
ti_cash_income_amt_before_2m,
ti_income_amt,
ystd_ti_income_amt_1d,
ti_income_amt_2m,
ti_income_amt_last_2m,
ti_income_amt_before_2m,
leads_cash_income_amt,
ystd_leads_cash_income_amt_1d,
leads_cash_income_amt_2m,
leads_cash_income_amt_last_2m,
leads_cash_income_amt_before_2m,
leads_income_amt,
ystd_leads_income_amt_1d,
leads_income_amt_2m,
leads_income_amt_last_2m,
leads_income_amt_before_2m,
ecm_closed_cash_income_amt,
ystd_ecm_closed_cash_income_amt_1d,
ecm_closed_cash_income_amt_2m,
ecm_closed_cash_income_amt_last_2m,
ecm_closed_cash_income_amt_before_2m,
ecm_closed_income_amt,
ystd_ecm_closed_income_amt_1d,
ecm_closed_income_amt_2m,
ecm_closed_income_amt_last_2m,
ecm_closed_income_amt_before_2m,
ecm_unclosed_cash_income_amt,
ystd_ecm_unclosed_cash_income_amt_1d,
ecm_unclosed_cash_income_amt_2m,
ecm_unclosed_cash_income_amt_last_2m,
ecm_unclosed_cash_income_amt_before_2m,
ecm_unclosed_income_amt,
ystd_ecm_unclosed_income_amt_1d,
ecm_unclosed_income_amt_2m,
ecm_unclosed_income_amt_last_2m,
ecm_unclosed_income_amt_before_2m,
ti_engage_cnt,
ystd_ti_engage_cnt_1d,
ti_engage_cnt_2m,
ti_engage_cnt_last_2m,
ti_engage_cnt_before_2m,
ti_rtb_budget_income_amt,
ystd_ti_rtb_budget_income_amt_1d,
ti_rtb_budget_income_amt_2m,
ti_rtb_budget_income_amt_last_2m,
ti_rtb_budget_income_amt_before_2m,
leads_engage_cnt,
ystd_leads_engage_cnt_1d,
leads_engage_cnt_2m,
leads_engage_cnt_last_2m,
leads_engage_cnt_before_2m,
leads_rtb_budget_income_amt,
ystd_leads_rtb_budget_income_amt_1d,
leads_rtb_budget_income_amt_2m,
leads_rtb_budget_income_amt_last_2m,
leads_rtb_budget_income_amt_before_2m,
ecm_closed_engage_cnt,
ystd_ecm_closed_engage_cnt_1d,
ecm_closed_engage_cnt_2m,
ecm_closed_engage_cnt_last_2m,
ecm_closed_engage_cnt_before_2m,
ecm_closed_rtb_budget_income_amt,
ystd_ecm_closed_rtb_budget_income_amt_1d,
ecm_closed_rtb_budget_income_amt_2m,
ecm_closed_rtb_budget_income_amt_last_2m,
ecm_closed_rtb_budget_income_amt_before_2m,
ecm_unclosed_engage_cnt,
ystd_ecm_unclosed_engage_cnt_1d,
ecm_unclosed_engage_cnt_2m,
ecm_unclosed_engage_cnt_last_2m,
ecm_unclosed_engage_cnt_before_2m,
ecm_unclosed_rtb_budget_income_amt,
ystd_ecm_unclosed_rtb_budget_income_amt_1d,
ecm_unclosed_rtb_budget_income_amt_2m,
ecm_unclosed_rtb_budget_income_amt_last_2m,
ecm_unclosed_rtb_budget_income_amt_before_2m,
ecm_closed_click_rgmv_7d,
ystd_ecm_closed_click_rgmv_7d_1d,
ecm_closed_click_rgmv_7d_2m,
ecm_closed_click_rgmv_7d_last_2m,
ecm_closed_click_rgmv_7d_before_2m,
ecm_unclosed_ecm_unclosed_rgmv,
ystd_ecm_unclosed_ecm_unclosed_rgmv_1d,
ecm_unclosed_ecm_unclosed_rgmv_2m,
ecm_unclosed_ecm_unclosed_rgmv_last_2m,
ecm_unclosed_ecm_unclosed_rgmv_before_2m,
leads_conversion_cnt,
ystd_leads_conversion_cnt_1d,
leads_conversion_cnt_2m,
leads_conversion_cnt_last_2m,
leads_conversion_cnt_before_2m,
ti_rtb_cost_income_amt,
ystd_ti_rtb_cost_income_amt_1d,
ti_rtb_cost_income_amt_2m,
ti_rtb_cost_income_amt_last_2m,
ti_rtb_cost_income_amt_before_2m,
leads_rtb_cost_income_amt,
ystd_leads_rtb_cost_income_amt_1d,
leads_rtb_cost_income_amt_2m,
leads_rtb_cost_income_amt_last_2m,
leads_rtb_cost_income_amt_before_2m,
ecm_closed_rtb_cost_income_amt,
ystd_ecm_closed_rtb_cost_income_amt_1d,
ecm_closed_rtb_cost_income_amt_2m,
ecm_closed_rtb_cost_income_amt_last_2m,
ecm_closed_rtb_cost_income_amt_before_2m,
ecm_unclosed_rtb_cost_income_amt,
ystd_ecm_unclosed_rtb_cost_income_amt_1d,
ecm_unclosed_rtb_cost_income_amt_2m,
ecm_unclosed_rtb_cost_income_amt_last_2m,
ecm_unclosed_rtb_cost_income_amt_before_2m,
search_cash_income_amt_2m,
ecm_closed_live_cash_income_amt_2m,
ecm_closed_sx_cash_income_amt_2m,
--广告流水
null as ads_cash_income_amt,
null as ystd_ads_cash_income_amt_1d,
null as ads_cash_income_amt_2m,
null as ads_cash_income_amt_last_2m,
null as ads_cash_income_amt_before_2m,
  --其他双月指标
null as ecm_closed_dgmv_2m,
null as cps_note_num_2m,
null as s_live_dgmv_2m,
null as is_sx_ti_2m,
null as taolian_cash_income_amt_2m,
null as bcoo_cash_income_amt_2m,
null as track_taolian_cash_income_amt_2m,
null as track_bcoo_cash_income_amt_2m,
null as ecm_closed_dgmv_cash_income_amt_2m,
null as track_ecm_closed_dgmv_2m,
null as track_ecm_closed_dgmv_cash_income_amt_2m
from  temp.app_ads_insight_advertiser_product_diagnosis_info_df_cmd_01_{{ds_nodash}}
union all 
--3代理商
select agent_user_id as account_id,
  3 as acccount_type,
  agent_user_name as account_name,
  brand_account_id,
  null as advertiser_id,
  null as v_seller_id,
  null as v_seller_name,
  agent_user_id,
  agent_user_name,
  null as  module,
  null as  product,
  null as  marketing_target,
  null as  market_target,
  brand_account_name,
  company_code,
  company_name,
  track_group_name,
  direct_sales_name,
  direct_sales_dept1_name,
  direct_sales_dept2_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_group_tag_code,
  brand_group_tag_name,
  cpc_operator_code,
  cpc_operator_name,
  first_industry_name,
  second_industry_name,
  track_industry_name,
  track_detail_name,
  brand_id,
  brand_name,
  planner_name,
  staff_name,
  process_track_industry_name,
  process_track_group_name,
  process_track_second_name,
  process_track_third_name,
  channel_sales_name,
  channel_sales_code,
  channel_operator_name,
sum(cash_income_amt) as cash_income_amt,
sum(income_amt) as income_amt,
sum(imp_cnt) as imp_cnt,
sum(click_cnt) as click_cnt,
sum(like_cnt) as like_cnt,
sum(fav_cnt) as fav_cnt,
sum(cmt_cnt) as cmt_cnt,
sum(share_cnt) as share_cnt,
sum(follow_cnt) as follow_cnt,
sum(screenshot_cnt) as screenshot_cnt,
sum(engage_cnt) as engage_cnt,
sum(click_rgmv_7d) as click_rgmv_7d,
sum(conversion_cnt) as conversion_cnt,
sum(ecm_unclosed_rgmv) as ecm_unclosed_rgmv,
sum(rtb_cost_income_amt) as rtb_cost_income_amt,
sum(rtb_budget_income_amt) as rtb_budget_income_amt,
sum(cost_income_amt) as cost_income_amt,
sum(budget_income_amt) as budget_income_amt,
sum(advertiser_budget_income_amt) as advertiser_budget_income_amt,
sum(advertiser_cost_income_amt) as advertiser_cost_income_amt,
sum(cash_balance) as cash_balance,
sum(ystd_cash_income_amt_1d) as ystd_cash_income_amt_1d,
sum(ystd_income_amt_1d) as ystd_income_amt_1d,
sum(ystd_imp_cnt_1d) as ystd_imp_cnt_1d,
sum(ystd_click_cnt_1d) as ystd_click_cnt_1d,
sum(ystd_like_cnt_1d) as ystd_like_cnt_1d,
sum(ystd_fav_cnt_1d) as ystd_fav_cnt_1d,
sum(ystd_cmt_cnt_1d) as ystd_cmt_cnt_1d,
sum(ystd_share_cnt_1d) as ystd_share_cnt_1d,
sum(ystd_follow_cnt_1d) as ystd_follow_cnt_1d,
sum(ystd_screenshot_cnt_1d) as ystd_screenshot_cnt_1d,
sum(ystd_engage_cnt_1d) as ystd_engage_cnt_1d,
sum(ystd_click_rgmv_7d_1d) as ystd_click_rgmv_7d_1d,
sum(ystd_conversion_cnt_1d) as ystd_conversion_cnt_1d,
sum(ystd_ecm_unclosed_rgmv_1d) as ystd_ecm_unclosed_rgmv_1d,
sum(ystd_rtb_cost_income_amt_1d) as ystd_rtb_cost_income_amt_1d,
sum(ystd_rtb_budget_income_amt_1d) as ystd_rtb_budget_income_amt_1d,
sum(ystd_cost_income_amt_1d) as ystd_cost_income_amt_1d,
sum(ystd_budget_income_amt_1d) as ystd_budget_income_amt_1d,
sum(ystd_advertiser_budget_income_amt_1d) as ystd_advertiser_budget_income_amt_1d,
sum(ystd_advertiser_cost_income_amt_1d) as ystd_advertiser_cost_income_amt_1d,
sum(ystd_cash_balance_1d) as ystd_cash_balance_1d,
sum(cash_income_amt_2m) as cash_income_amt_2m,
sum(income_amt_2m) as income_amt_2m,
sum(imp_cnt_2m) as imp_cnt_2m,
sum(click_cnt_2m) as click_cnt_2m,
sum(like_cnt_2m) as like_cnt_2m,
sum(fav_cnt_2m) as fav_cnt_2m,
sum(cmt_cnt_2m) as cmt_cnt_2m,
sum(share_cnt_2m) as share_cnt_2m,
sum(follow_cnt_2m) as follow_cnt_2m,
sum(screenshot_cnt_2m) as screenshot_cnt_2m,
sum(engage_cnt_2m) as engage_cnt_2m,
sum(click_rgmv_7d_2m) as click_rgmv_7d_2m,
sum(conversion_cnt_2m) as conversion_cnt_2m,
sum(ecm_unclosed_rgmv_2m) as ecm_unclosed_rgmv_2m,
sum(rtb_cost_income_amt_2m) as rtb_cost_income_amt_2m,
sum(rtb_budget_income_amt_2m) as rtb_budget_income_amt_2m,
sum(cost_income_amt_2m) as cost_income_amt_2m,
sum(budget_income_amt_2m) as budget_income_amt_2m,
sum(advertiser_budget_income_amt_2m) as advertiser_budget_income_amt_2m,
sum(advertiser_cost_income_amt_2m) as advertiser_cost_income_amt_2m,
sum(cash_balance_2m) as cash_balance_2m,
sum(cash_income_amt_last_2m) as cash_income_amt_last_2m,
sum(income_amt_last_2m) as income_amt_last_2m,
sum(imp_cnt_last_2m) as imp_cnt_last_2m,
sum(click_cnt_last_2m) as click_cnt_last_2m,
sum(like_cnt_last_2m) as like_cnt_last_2m,
sum(fav_cnt_last_2m) as fav_cnt_last_2m,
sum(cmt_cnt_last_2m) as cmt_cnt_last_2m,
sum(share_cnt_last_2m) as share_cnt_last_2m,
sum(follow_cnt_last_2m) as follow_cnt_last_2m,
sum(screenshot_cnt_last_2m) as screenshot_cnt_last_2m,
sum(engage_cnt_last_2m) as engage_cnt_last_2m,
sum(click_rgmv_7d_last_2m) as click_rgmv_7d_last_2m,
sum(conversion_cnt_last_2m) as conversion_cnt_last_2m,
sum(ecm_unclosed_rgmv_last_2m) as ecm_unclosed_rgmv_last_2m,
sum(rtb_cost_income_amt_last_2m) as rtb_cost_income_amt_last_2m,
sum(rtb_budget_income_amt_last_2m) as rtb_budget_income_amt_last_2m,
sum(cost_income_amt_last_2m) as cost_income_amt_last_2m,
sum(budget_income_amt_last_2m) as budget_income_amt_last_2m,
sum(advertiser_budget_income_amt_last_2m) as advertiser_budget_income_amt_last_2m,
sum(advertiser_cost_income_amt_last_2m) as advertiser_cost_income_amt_last_2m,
sum(cash_balance_last_2m) as cash_balance_last_2m,
sum(cash_income_amt_before_2m) as cash_income_amt_before_2m,
sum(income_amt_before_2m) as income_amt_before_2m,
sum(imp_cnt_before_2m) as imp_cnt_before_2m,
sum(click_cnt_before_2m) as click_cnt_before_2m,
sum(like_cnt_before_2m) as like_cnt_before_2m,
sum(fav_cnt_before_2m) as fav_cnt_before_2m,
sum(cmt_cnt_before_2m) as cmt_cnt_before_2m,
sum(share_cnt_before_2m) as share_cnt_before_2m,
sum(follow_cnt_before_2m) as follow_cnt_before_2m,
sum(screenshot_cnt_before_2m) as screenshot_cnt_before_2m,
sum(engage_cnt_before_2m) as engage_cnt_before_2m,
sum(click_rgmv_7d_before_2m) as click_rgmv_7d_before_2m,
sum(conversion_cnt_before_2m) as conversion_cnt_before_2m,
sum(ecm_unclosed_rgmv_before_2m) as ecm_unclosed_rgmv_before_2m,
sum(rtb_cost_income_amt_before_2m) as rtb_cost_income_amt_before_2m,
sum(rtb_budget_income_amt_before_2m) as rtb_budget_income_amt_before_2m,
sum(cost_income_amt_before_2m) as cost_income_amt_before_2m,
sum(budget_income_amt_before_2m) as budget_income_amt_before_2m,
sum(advertiser_budget_income_amt_before_2m) as advertiser_budget_income_amt_before_2m,
sum(advertiser_cost_income_amt_before_2m) as advertiser_cost_income_amt_before_2m,
sum(cash_balance_before_2m) as cash_balance_before_2m,
max(track_group_cost_income_amt) as track_group_cost_income_amt,
max(track_group_budget_income_amt) as track_group_budget_income_amt,
max(track_group_advertiser_cost_income_amt) as track_group_advertiser_cost_income_amt,
max(track_group_advertiser_budget_income_amt) as track_group_advertiser_budget_income_amt,
max(track_group_rtb_cost_income_amt) as track_group_rtb_cost_income_amt,
max(track_group_rtb_budget_income_amt) as track_group_rtb_budget_income_amt,
max(track_group_cash_income_amt_before_2m) as track_group_cash_income_amt_before_2m,
max(track_group_cash_income_amt_2m) as track_group_cash_income_amt_2m,
max(track_group_cash_income_amt_last_2m) as track_group_cash_income_amt_last_2m,
max(track_cost_income_amt) as track_cost_income_amt,
max(track_budget_income_amt) as track_budget_income_amt,
max(track_advertiser_cost_income_amt) as track_advertiser_cost_income_amt,
max(track_advertiser_budget_income_amt) as track_advertiser_budget_income_amt,
max(track_rtb_cost_income_amt) as track_rtb_cost_income_amt,
max(track_rtb_budget_income_amt) as track_rtb_budget_income_amt,
sum(cash_income_amt_1y) as cash_income_amt_1y,
sum(cash_income_amt_before_1y) as cash_income_amt_before_1y,
sum(ti_cash_income_amt) as ti_cash_income_amt,
sum(ystd_ti_cash_income_amt_1d) as ystd_ti_cash_income_amt_1d,
sum(ti_cash_income_amt_2m) as ti_cash_income_amt_2m,
sum(ti_cash_income_amt_last_2m) as ti_cash_income_amt_last_2m,
sum(ti_cash_income_amt_before_2m) as ti_cash_income_amt_before_2m,
sum(ti_income_amt) as ti_income_amt,
sum(ystd_ti_income_amt_1d) as ystd_ti_income_amt_1d,
sum(ti_income_amt_2m) as ti_income_amt_2m,
sum(ti_income_amt_last_2m) as ti_income_amt_last_2m,
sum(ti_income_amt_before_2m) as ti_income_amt_before_2m,
sum(leads_cash_income_amt) as leads_cash_income_amt,
sum(ystd_leads_cash_income_amt_1d) as ystd_leads_cash_income_amt_1d,
sum(leads_cash_income_amt_2m) as leads_cash_income_amt_2m,
sum(leads_cash_income_amt_last_2m) as leads_cash_income_amt_last_2m,
sum(leads_cash_income_amt_before_2m) as leads_cash_income_amt_before_2m,
sum(leads_income_amt) as leads_income_amt,
sum(ystd_leads_income_amt_1d) as ystd_leads_income_amt_1d,
sum(leads_income_amt_2m) as leads_income_amt_2m,
sum(leads_income_amt_last_2m) as leads_income_amt_last_2m,
sum(leads_income_amt_before_2m) as leads_income_amt_before_2m,
sum(ecm_closed_cash_income_amt) as ecm_closed_cash_income_amt,
sum(ystd_ecm_closed_cash_income_amt_1d) as ystd_ecm_closed_cash_income_amt_1d,
sum(ecm_closed_cash_income_amt_2m) as ecm_closed_cash_income_amt_2m,
sum(ecm_closed_cash_income_amt_last_2m) as ecm_closed_cash_income_amt_last_2m,
sum(ecm_closed_cash_income_amt_before_2m) as ecm_closed_cash_income_amt_before_2m,
sum(ecm_closed_income_amt) as ecm_closed_income_amt,
sum(ystd_ecm_closed_income_amt_1d) as ystd_ecm_closed_income_amt_1d,
sum(ecm_closed_income_amt_2m) as ecm_closed_income_amt_2m,
sum(ecm_closed_income_amt_last_2m) as ecm_closed_income_amt_last_2m,
sum(ecm_closed_income_amt_before_2m) as ecm_closed_income_amt_before_2m,
sum(ecm_unclosed_cash_income_amt) as ecm_unclosed_cash_income_amt,
sum(ystd_ecm_unclosed_cash_income_amt_1d) as ystd_ecm_unclosed_cash_income_amt_1d,
sum(ecm_unclosed_cash_income_amt_2m) as ecm_unclosed_cash_income_amt_2m,
sum(ecm_unclosed_cash_income_amt_last_2m) as ecm_unclosed_cash_income_amt_last_2m,
sum(ecm_unclosed_cash_income_amt_before_2m) as ecm_unclosed_cash_income_amt_before_2m,
sum(ecm_unclosed_income_amt) as ecm_unclosed_income_amt,
sum(ystd_ecm_unclosed_income_amt_1d) as ystd_ecm_unclosed_income_amt_1d,
sum(ecm_unclosed_income_amt_2m) as ecm_unclosed_income_amt_2m,
sum(ecm_unclosed_income_amt_last_2m) as ecm_unclosed_income_amt_last_2m,
sum(ecm_unclosed_income_amt_before_2m) as ecm_unclosed_income_amt_before_2m,
sum(ti_engage_cnt) as ti_engage_cnt,
sum(ystd_ti_engage_cnt_1d) as ystd_ti_engage_cnt_1d,
sum(ti_engage_cnt_2m) as ti_engage_cnt_2m,
sum(ti_engage_cnt_last_2m) as ti_engage_cnt_last_2m,
sum(ti_engage_cnt_before_2m) as ti_engage_cnt_before_2m,
sum(ti_rtb_budget_income_amt) as ti_rtb_budget_income_amt,
sum(ystd_ti_rtb_budget_income_amt_1d) as ystd_ti_rtb_budget_income_amt_1d,
sum(ti_rtb_budget_income_amt_2m) as ti_rtb_budget_income_amt_2m,
sum(ti_rtb_budget_income_amt_last_2m) as ti_rtb_budget_income_amt_last_2m,
sum(ti_rtb_budget_income_amt_before_2m) as ti_rtb_budget_income_amt_before_2m,
sum(leads_engage_cnt) as leads_engage_cnt,
sum(ystd_leads_engage_cnt_1d) as ystd_leads_engage_cnt_1d,
sum(leads_engage_cnt_2m) as leads_engage_cnt_2m,
sum(leads_engage_cnt_last_2m) as leads_engage_cnt_last_2m,
sum(leads_engage_cnt_before_2m) as leads_engage_cnt_before_2m,
sum(leads_rtb_budget_income_amt) as leads_rtb_budget_income_amt,
sum(ystd_leads_rtb_budget_income_amt_1d) as ystd_leads_rtb_budget_income_amt_1d,
sum(leads_rtb_budget_income_amt_2m) as leads_rtb_budget_income_amt_2m,
sum(leads_rtb_budget_income_amt_last_2m) as leads_rtb_budget_income_amt_last_2m,
sum(leads_rtb_budget_income_amt_before_2m) as leads_rtb_budget_income_amt_before_2m,
sum(ecm_closed_engage_cnt) as ecm_closed_engage_cnt,
sum(ystd_ecm_closed_engage_cnt_1d) as ystd_ecm_closed_engage_cnt_1d,
sum(ecm_closed_engage_cnt_2m) as ecm_closed_engage_cnt_2m,
sum(ecm_closed_engage_cnt_last_2m) as ecm_closed_engage_cnt_last_2m,
sum(ecm_closed_engage_cnt_before_2m) as ecm_closed_engage_cnt_before_2m,
sum(ecm_closed_rtb_budget_income_amt) as ecm_closed_rtb_budget_income_amt,
sum(ystd_ecm_closed_rtb_budget_income_amt_1d) as ystd_ecm_closed_rtb_budget_income_amt_1d,
sum(ecm_closed_rtb_budget_income_amt_2m) as ecm_closed_rtb_budget_income_amt_2m,
sum(ecm_closed_rtb_budget_income_amt_last_2m) as ecm_closed_rtb_budget_income_amt_last_2m,
sum(ecm_closed_rtb_budget_income_amt_before_2m) as ecm_closed_rtb_budget_income_amt_before_2m,
sum(ecm_unclosed_engage_cnt) as ecm_unclosed_engage_cnt,
sum(ystd_ecm_unclosed_engage_cnt_1d) as ystd_ecm_unclosed_engage_cnt_1d,
sum(ecm_unclosed_engage_cnt_2m) as ecm_unclosed_engage_cnt_2m,
sum(ecm_unclosed_engage_cnt_last_2m) as ecm_unclosed_engage_cnt_last_2m,
sum(ecm_unclosed_engage_cnt_before_2m) as ecm_unclosed_engage_cnt_before_2m,
sum(ecm_unclosed_rtb_budget_income_amt) as ecm_unclosed_rtb_budget_income_amt,
sum(ystd_ecm_unclosed_rtb_budget_income_amt_1d) as ystd_ecm_unclosed_rtb_budget_income_amt_1d,
sum(ecm_unclosed_rtb_budget_income_amt_2m) as ecm_unclosed_rtb_budget_income_amt_2m,
sum(ecm_unclosed_rtb_budget_income_amt_last_2m) as ecm_unclosed_rtb_budget_income_amt_last_2m,
sum(ecm_unclosed_rtb_budget_income_amt_before_2m) as ecm_unclosed_rtb_budget_income_amt_before_2m,
sum(ecm_closed_click_rgmv_7d) as ecm_closed_click_rgmv_7d,
sum(ystd_ecm_closed_click_rgmv_7d_1d) as ystd_ecm_closed_click_rgmv_7d_1d,
sum(ecm_closed_click_rgmv_7d_2m) as ecm_closed_click_rgmv_7d_2m,
sum(ecm_closed_click_rgmv_7d_last_2m) as ecm_closed_click_rgmv_7d_last_2m,
sum(ecm_closed_click_rgmv_7d_before_2m) as ecm_closed_click_rgmv_7d_before_2m,
sum(ecm_unclosed_ecm_unclosed_rgmv) as ecm_unclosed_ecm_unclosed_rgmv,
sum(ystd_ecm_unclosed_ecm_unclosed_rgmv_1d) as ystd_ecm_unclosed_ecm_unclosed_rgmv_1d,
sum(ecm_unclosed_ecm_unclosed_rgmv_2m) as ecm_unclosed_ecm_unclosed_rgmv_2m,
sum(ecm_unclosed_ecm_unclosed_rgmv_last_2m) as ecm_unclosed_ecm_unclosed_rgmv_last_2m,
sum(ecm_unclosed_ecm_unclosed_rgmv_before_2m) as ecm_unclosed_ecm_unclosed_rgmv_before_2m,
sum(leads_conversion_cnt) as leads_conversion_cnt,
sum(ystd_leads_conversion_cnt_1d) as ystd_leads_conversion_cnt_1d,
sum(leads_conversion_cnt_2m) as leads_conversion_cnt_2m,
sum(leads_conversion_cnt_last_2m) as leads_conversion_cnt_last_2m,
sum(leads_conversion_cnt_before_2m) as leads_conversion_cnt_before_2m,
sum(ti_rtb_cost_income_amt) as ti_rtb_cost_income_amt,
sum(ystd_ti_rtb_cost_income_amt_1d) as ystd_ti_rtb_cost_income_amt_1d,
sum(ti_rtb_cost_income_amt_2m) as ti_rtb_cost_income_amt_2m,
sum(ti_rtb_cost_income_amt_last_2m) as ti_rtb_cost_income_amt_last_2m,
sum(ti_rtb_cost_income_amt_before_2m) as ti_rtb_cost_income_amt_before_2m,
sum(leads_rtb_cost_income_amt) as leads_rtb_cost_income_amt,
sum(ystd_leads_rtb_cost_income_amt_1d) as ystd_leads_rtb_cost_income_amt_1d,
sum(leads_rtb_cost_income_amt_2m) as leads_rtb_cost_income_amt_2m,
sum(leads_rtb_cost_income_amt_last_2m) as leads_rtb_cost_income_amt_last_2m,
sum(leads_rtb_cost_income_amt_before_2m) as leads_rtb_cost_income_amt_before_2m,
sum(ecm_closed_rtb_cost_income_amt) as ecm_closed_rtb_cost_income_amt,
sum(ystd_ecm_closed_rtb_cost_income_amt_1d) as ystd_ecm_closed_rtb_cost_income_amt_1d,
sum(ecm_closed_rtb_cost_income_amt_2m) as ecm_closed_rtb_cost_income_amt_2m,
sum(ecm_closed_rtb_cost_income_amt_last_2m) as ecm_closed_rtb_cost_income_amt_last_2m,
sum(ecm_closed_rtb_cost_income_amt_before_2m) as ecm_closed_rtb_cost_income_amt_before_2m,
sum(ecm_unclosed_rtb_cost_income_amt) as ecm_unclosed_rtb_cost_income_amt,
sum(ystd_ecm_unclosed_rtb_cost_income_amt_1d) as ystd_ecm_unclosed_rtb_cost_income_amt_1d,
sum(ecm_unclosed_rtb_cost_income_amt_2m) as ecm_unclosed_rtb_cost_income_amt_2m,
sum(ecm_unclosed_rtb_cost_income_amt_last_2m) as ecm_unclosed_rtb_cost_income_amt_last_2m,
sum(ecm_unclosed_rtb_cost_income_amt_before_2m) as ecm_unclosed_rtb_cost_income_amt_before_2m,
sum(search_cash_income_amt_2m) as search_cash_income_amt_2m,
sum(ecm_closed_live_cash_income_amt_2m) as ecm_closed_live_cash_income_amt_2m,
sum(ecm_closed_sx_cash_income_amt_2m) as ecm_closed_sx_cash_income_amt_2m,
null as ads_cash_income_amt,
null as ystd_ads_cash_income_amt_1d,
null as ads_cash_income_amt_2m,
null as ads_cash_income_amt_last_2m,
null as ads_cash_income_amt_before_2m,
  --其他双月指标
null as ecm_closed_dgmv_2m,
null as cps_note_num_2m,
null as s_live_dgmv_2m,
null as is_sx_ti_2m,
null as taolian_cash_income_amt_2m,
null as bcoo_cash_income_amt_2m,
null as track_taolian_cash_income_amt_2m,
null as track_bcoo_cash_income_amt_2m,
null as ecm_closed_dgmv_cash_income_amt_2m,
null as track_ecm_closed_dgmv_2m,
null as track_ecm_closed_dgmv_cash_income_amt_2m

    from
      temp.app_ads_insight_advertiser_product_diagnosis_info_df_cmd_01_{{ds_nodash}}
   group by brand_account_id,
  -- advertiser_id,
  -- v_seller_id,
  -- v_seller_name,
  agent_user_id,
  agent_user_name,
  -- module,
  -- product,
  -- marketing_target,
  -- market_target,
  brand_account_name,
  company_code,
  company_name,
  track_group_name,
  direct_sales_name,
  direct_sales_dept1_name,
  direct_sales_dept2_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_group_tag_code,
  brand_group_tag_name,
  cpc_operator_code,
  cpc_operator_name,
  first_industry_name,
  second_industry_name,
  track_industry_name,
  track_detail_name,
  brand_id,
  brand_name,
  planner_name,
  staff_name,
  process_track_industry_name,
  process_track_group_name,
  process_track_second_name,
  process_track_third_name,
  channel_sales_name,
  channel_sales_code,
  channel_operator_name
)info 
group by account_id,
   acccount_type,
   account_name,
  brand_account_id,
   advertiser_id,
 v_seller_id,
 v_seller_name,
agent_user_id,
agent_user_name,
--  module,
--   product,
--   marketing_target,
--   market_target,
  brand_account_name,
  company_code,
  company_name,
  track_group_name,
  direct_sales_name,
  direct_sales_dept1_name,
  direct_sales_dept2_name,
  direct_sales_dept3_name,
  direct_sales_dept4_name,
  direct_sales_dept5_name,
  direct_sales_dept6_name,
  brand_group_tag_code,
  brand_group_tag_name,
  cpc_operator_code,
  cpc_operator_name,
  first_industry_name,
  second_industry_name,
  track_industry_name,
  track_detail_name,
  brand_id,
  brand_name,
  planner_name,
  staff_name,
  process_track_industry_name,
  process_track_group_name,
  process_track_second_name,
  process_track_third_name,
  channel_sales_name,
  channel_sales_code,
  channel_operator_name,
  cash_income_amt,
income_amt,
imp_cnt,
click_cnt,
like_cnt,
fav_cnt,
cmt_cnt,
share_cnt,
follow_cnt,
screenshot_cnt,
engage_cnt,
click_rgmv_7d,
conversion_cnt,
ecm_unclosed_rgmv,
rtb_cost_income_amt,
rtb_budget_income_amt,
cost_income_amt,
budget_income_amt,
advertiser_budget_income_amt,
advertiser_cost_income_amt,
cash_balance,
ystd_cash_income_amt_1d,
ystd_income_amt_1d,
ystd_imp_cnt_1d,
ystd_click_cnt_1d,
ystd_like_cnt_1d,
ystd_fav_cnt_1d,
ystd_cmt_cnt_1d,
ystd_share_cnt_1d,
ystd_follow_cnt_1d,
ystd_screenshot_cnt_1d,
ystd_engage_cnt_1d,
ystd_click_rgmv_7d_1d,
ystd_conversion_cnt_1d,
ystd_ecm_unclosed_rgmv_1d,
ystd_rtb_cost_income_amt_1d,
ystd_rtb_budget_income_amt_1d,
ystd_cost_income_amt_1d,
ystd_budget_income_amt_1d,
ystd_advertiser_budget_income_amt_1d,
ystd_advertiser_cost_income_amt_1d,
ystd_cash_balance_1d,
cash_income_amt_2m,
income_amt_2m,
imp_cnt_2m,
click_cnt_2m,
like_cnt_2m,
fav_cnt_2m,
cmt_cnt_2m,
share_cnt_2m,
follow_cnt_2m,
screenshot_cnt_2m,
engage_cnt_2m,
click_rgmv_7d_2m,
conversion_cnt_2m,
ecm_unclosed_rgmv_2m,
rtb_cost_income_amt_2m,
rtb_budget_income_amt_2m,
cost_income_amt_2m,
budget_income_amt_2m,
advertiser_budget_income_amt_2m,
advertiser_cost_income_amt_2m,
cash_balance_2m,
cash_income_amt_last_2m,
income_amt_last_2m,
imp_cnt_last_2m,
click_cnt_last_2m,
like_cnt_last_2m,
fav_cnt_last_2m,
cmt_cnt_last_2m,
share_cnt_last_2m,
follow_cnt_last_2m,
screenshot_cnt_last_2m,
engage_cnt_last_2m,
click_rgmv_7d_last_2m,
conversion_cnt_last_2m,
ecm_unclosed_rgmv_last_2m,
rtb_cost_income_amt_last_2m,
rtb_budget_income_amt_last_2m,
cost_income_amt_last_2m,
budget_income_amt_last_2m,
advertiser_budget_income_amt_last_2m,
advertiser_cost_income_amt_last_2m,
cash_balance_last_2m,
cash_income_amt_before_2m,
income_amt_before_2m,
imp_cnt_before_2m,
click_cnt_before_2m,
like_cnt_before_2m,
fav_cnt_before_2m,
cmt_cnt_before_2m,
share_cnt_before_2m,
follow_cnt_before_2m,
screenshot_cnt_before_2m,
engage_cnt_before_2m,
click_rgmv_7d_before_2m,
conversion_cnt_before_2m,
ecm_unclosed_rgmv_before_2m,
rtb_cost_income_amt_before_2m,
rtb_budget_income_amt_before_2m,
cost_income_amt_before_2m,
budget_income_amt_before_2m,
advertiser_budget_income_amt_before_2m,
advertiser_cost_income_amt_before_2m,
cash_balance_before_2m,
track_group_cost_income_amt,
track_group_budget_income_amt,
track_group_advertiser_cost_income_amt,
track_group_advertiser_budget_income_amt,
track_group_rtb_cost_income_amt,
track_group_rtb_budget_income_amt,
track_group_cash_income_amt_before_2m,
track_group_cash_income_amt_2m,
track_group_cash_income_amt_last_2m,
track_cost_income_amt,
track_budget_income_amt,
track_advertiser_cost_income_amt,
track_advertiser_budget_income_amt,
track_rtb_cost_income_amt,
track_rtb_budget_income_amt,
cash_income_amt_1y,
cash_income_amt_before_1y,
ti_cash_income_amt,
ystd_ti_cash_income_amt_1d,
ti_cash_income_amt_2m,
ti_cash_income_amt_last_2m,
ti_cash_income_amt_before_2m,
ti_income_amt,
ystd_ti_income_amt_1d,
ti_income_amt_2m,
ti_income_amt_last_2m,
ti_income_amt_before_2m,
leads_cash_income_amt,
ystd_leads_cash_income_amt_1d,
leads_cash_income_amt_2m,
leads_cash_income_amt_last_2m,
leads_cash_income_amt_before_2m,
leads_income_amt,
ystd_leads_income_amt_1d,
leads_income_amt_2m,
leads_income_amt_last_2m,
leads_income_amt_before_2m,
ecm_closed_cash_income_amt,
ystd_ecm_closed_cash_income_amt_1d,
ecm_closed_cash_income_amt_2m,
ecm_closed_cash_income_amt_last_2m,
ecm_closed_cash_income_amt_before_2m,
ecm_closed_income_amt,
ystd_ecm_closed_income_amt_1d,
ecm_closed_income_amt_2m,
ecm_closed_income_amt_last_2m,
ecm_closed_income_amt_before_2m,
ecm_unclosed_cash_income_amt,
ystd_ecm_unclosed_cash_income_amt_1d,
ecm_unclosed_cash_income_amt_2m,
ecm_unclosed_cash_income_amt_last_2m,
ecm_unclosed_cash_income_amt_before_2m,
ecm_unclosed_income_amt,
ystd_ecm_unclosed_income_amt_1d,
ecm_unclosed_income_amt_2m,
ecm_unclosed_income_amt_last_2m,
ecm_unclosed_income_amt_before_2m,
ti_engage_cnt,
ystd_ti_engage_cnt_1d,
ti_engage_cnt_2m,
ti_engage_cnt_last_2m,
ti_engage_cnt_before_2m,
ti_rtb_budget_income_amt,
ystd_ti_rtb_budget_income_amt_1d,
ti_rtb_budget_income_amt_2m,
ti_rtb_budget_income_amt_last_2m,
ti_rtb_budget_income_amt_before_2m,
leads_engage_cnt,
ystd_leads_engage_cnt_1d,
leads_engage_cnt_2m,
leads_engage_cnt_last_2m,
leads_engage_cnt_before_2m,
leads_rtb_budget_income_amt,
ystd_leads_rtb_budget_income_amt_1d,
leads_rtb_budget_income_amt_2m,
leads_rtb_budget_income_amt_last_2m,
leads_rtb_budget_income_amt_before_2m,
ecm_closed_engage_cnt,
ystd_ecm_closed_engage_cnt_1d,
ecm_closed_engage_cnt_2m,
ecm_closed_engage_cnt_last_2m,
ecm_closed_engage_cnt_before_2m,
ecm_closed_rtb_budget_income_amt,
ystd_ecm_closed_rtb_budget_income_amt_1d,
ecm_closed_rtb_budget_income_amt_2m,
ecm_closed_rtb_budget_income_amt_last_2m,
ecm_closed_rtb_budget_income_amt_before_2m,
ecm_unclosed_engage_cnt,
ystd_ecm_unclosed_engage_cnt_1d,
ecm_unclosed_engage_cnt_2m,
ecm_unclosed_engage_cnt_last_2m,
ecm_unclosed_engage_cnt_before_2m,
ecm_unclosed_rtb_budget_income_amt,
ystd_ecm_unclosed_rtb_budget_income_amt_1d,
ecm_unclosed_rtb_budget_income_amt_2m,
ecm_unclosed_rtb_budget_income_amt_last_2m,
ecm_unclosed_rtb_budget_income_amt_before_2m,
ecm_closed_click_rgmv_7d,
ystd_ecm_closed_click_rgmv_7d_1d,
ecm_closed_click_rgmv_7d_2m,
ecm_closed_click_rgmv_7d_last_2m,
ecm_closed_click_rgmv_7d_before_2m,
ecm_unclosed_ecm_unclosed_rgmv,
ystd_ecm_unclosed_ecm_unclosed_rgmv_1d,
ecm_unclosed_ecm_unclosed_rgmv_2m,
ecm_unclosed_ecm_unclosed_rgmv_last_2m,
ecm_unclosed_ecm_unclosed_rgmv_before_2m,
leads_conversion_cnt,
ystd_leads_conversion_cnt_1d,
leads_conversion_cnt_2m,
leads_conversion_cnt_last_2m,
leads_conversion_cnt_before_2m,
ti_rtb_cost_income_amt,
ystd_ti_rtb_cost_income_amt_1d,
ti_rtb_cost_income_amt_2m,
ti_rtb_cost_income_amt_last_2m,
ti_rtb_cost_income_amt_before_2m,
leads_rtb_cost_income_amt,
ystd_leads_rtb_cost_income_amt_1d,
leads_rtb_cost_income_amt_2m,
leads_rtb_cost_income_amt_last_2m,
leads_rtb_cost_income_amt_before_2m,
ecm_closed_rtb_cost_income_amt,
ystd_ecm_closed_rtb_cost_income_amt_1d,
ecm_closed_rtb_cost_income_amt_2m,
ecm_closed_rtb_cost_income_amt_last_2m,
ecm_closed_rtb_cost_income_amt_before_2m,
ecm_unclosed_rtb_cost_income_amt,
ystd_ecm_unclosed_rtb_cost_income_amt_1d,
ecm_unclosed_rtb_cost_income_amt_2m,
ecm_unclosed_rtb_cost_income_amt_last_2m,
ecm_unclosed_rtb_cost_income_amt_before_2m,
search_cash_income_amt_2m,
ecm_closed_live_cash_income_amt_2m,
ecm_closed_sx_cash_income_amt_2m,
ads_cash_income_amt,
ystd_ads_cash_income_amt_1d,
ads_cash_income_amt_2m,
ads_cash_income_amt_last_2m,
ads_cash_income_amt_before_2m,
ecm_closed_dgmv_2m,
cps_note_num_2m,
s_live_dgmv_2m,
is_sx_ti_2m,
taolian_cash_income_amt_2m,
bcoo_cash_income_amt_2m,
track_taolian_cash_income_amt_2m,
track_bcoo_cash_income_amt_2m,
ecm_closed_dgmv_cash_income_amt_2m,
track_ecm_closed_dgmv_2m,
track_ecm_closed_dgmv_cash_income_amt_2m