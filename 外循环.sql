--step1：开屏+品专+GD 外链建中间表 （修改成新的表）
drop table
  if exists temp.wailiancid_01;

create table
  temp.wailiancid_01 as
select dtm,
  brand_account_id,
  a.unit_id,
  product,
  b.wailian_jump_type,
  b.second_jump_type,
  sum(revenue) as revenue,
  sum(imp_num) as imp_num,
  sum(click_num) as click_num,
  sum(second_jump_imp_num) as second_jump_imp_num,
  sum(second_jump_click_num) as second_jump_click_num
from
  (
    select dtm,
      brand_account_id,
      unit_id,
      product,
      sum(ad_cost) as revenue,
      sum(imp_num) as imp_num,
      sum(click_num) as click_num,
      sum(2nd_jump_imp_num) as second_jump_imp_num,
      sum(2nd_jump_click_num) as second_jump_click_num
    from
      reddw.dw_ads_creativity_wide_base_detail_day_inc
    where
      dtm between f_getdate('{{ ds_nodash }}', -7) and '{{ ds_nodash }}'
    group by
      1,
      2,
      3,
      4
  ) a
  left join (
    select
      a.unit_id,
      case
        when (
          first_jump_type = 36
          and e.unit_id is not null
        )
        or first_jump_type = 37 then '微信小程序'
        when first_jump_type = 36
        or (
          first_jump_type = 8
          and (
            lower(jump_url) like '%tmall%'
            or lower(jump_url) like '%jd%'
            or lower(jump_url) like '%taobao%'
          )
        ) then '电商'
        when first_jump_type = 8 then '官网'
        else '非外链'
      end as wailian_jump_type,
      second_jump_type
    from
      reddw.dw_ads_creativity_day a
      left join (
        select
          a.creativity_id,
          a.unit_id
        from
          reddw.dw_ads_creativity_day a
          left join redods.ods_shequ_feed_ads_t_boot_screen_ext d on a.ext_boot_id = d.id
          and d.dtm = greatest('{{ds_nodash}}', '20230801')
        where
          a.dtm = greatest('{{ds_nodash}}', '20230801')
          and (
            is_wechat_app_mini_program = 1
            or d.landing_page_type = 37
          )
        group by
          1,
          2
      ) e on a.unit_id = e.unit_id
    where
      dtm = '{{ds_nodash}}'
    group by
      1,
      2,
      3
  ) b on a.unit_id = b.unit_id
group by
  1,
  2,
  3,
  4,
  5,
  6 ;
insert overwrite table redapp.app_ads_industry_cid_account_di partition(dtm)

select t1.brand_account_id,
  module,
  tag,
  type,
  sub_type,
  cost,
  cash_cost,
  imp_num,
  click_num,
  feed_imp_num,
  feed_click_num,
  second_jump_click_num,
  order_num,
  rgmv,
  enter_seller_uv,
  read_uv,
  brand_user_name,
  track_group_name,
  track_industry_name,
  track_detail_name,
  cpc_operator_name,
  case when module in ('品牌') then brand_direct_sales_name else cpc_direct_sales_name end as direct_sales_name,
  case when module in ('品牌') then brand_direct_sales_dept1_name else cpc_direct_sales_dept1_name end as direct_sales_dept1_name,
  case when module in ('品牌') then brand_direct_sales_dept2_name else cpc_direct_sales_dept2_name end as direct_sales_dept2_name,
  case when module in ('品牌') then brand_direct_sales_dept3_name else cpc_direct_sales_dept3_name end as direct_sales_dept3_name,
  case when module in ('品牌') then brand_direct_sales_dept4_name else cpc_direct_sales_dept4_name end as direct_sales_dept4_name,
  case when module in ('品牌') then brand_direct_sales_dept5_name else cpc_direct_sales_dept5_name end as direct_sales_dept5_name,
  case when module in ('品牌') then brand_direct_sales_dept6_name else cpc_direct_sales_dept6_name end as direct_sales_dept6_name,
  dtm
from
(select dtm,
  brand_account_id,
  '品牌' as module,
  '外链' as tag,
  '开屏一跳' as type,
  wailian_jump_type as sub_type,
  sum(revenue) as cost,
  sum(revenue) as cash_cost,
  sum(imp_num) as imp_num,
  sum(click_num) as click_num,
  0 as feed_imp_num,
  0 as feed_click_num,
  sum(second_jump_click_num) as second_jump_click_num,
  0 as order_num,
  0 as rgmv,
  0 as enter_seller_uv,
  0 as read_uv
from
  temp.wailiancid_01
where
  (wailian_jump_type in ('电商', '微信小程序', '官网'))
  and product = '开屏'
group by
  1,
  2,
  3,
  4,
  5,6
union all
--二跳外链
select dtm,
  brand_account_id,
  '品牌' as module,
  '外链' as tag,
  case
    when product = '开屏' then '开屏二跳'
    when product = '信息流GD' then 'GD二跳'
    when product = '品牌专区' then '品专二跳'
  end as type,
  case
    when second_jump_type = 8 then '官网/电商'
    when second_jump_type = 37 then '微信小程序'
  end as sub_type,
  sum(revenue) as cost,
  sum(revenue) as cash_cost,
  sum(imp_num) as imp_num,
  sum(click_num) as click_num,
  0 as feed_imp_num,
  0 as feed_click_num,
  sum(second_jump_click_num) as second_jump_click_num,
  0 as order_num,
  0 as rgmv,
  0 as enter_seller_uv,
  0 as read_uv
from
  temp.wailiancid_01
where
  second_jump_type in (8, 12, 37)
  and wailian_jump_type not in ('电商', '微信小程序', '官网')
  and product in ('品牌专区', '信息流GD', '开屏')
group by
  1,
  2,
  3,
  4,
  5,6
union all
--竞价种草电商 外链
select dtm,
  brand_account_id,
  '竞价' as module,
  '外链' as tag,
  case
    when marketing_target in (2, 5, 9) then '竞价-线索二跳'
    when marketing_target in (3, 8, 14) then '竞价-电商二跳'
    else '竞价-种草二跳'
  end as type,
  '官网/电商' as sub_type,
  sum(cost) as cost,
  sum(cash_cost) as cash_cost,
  sum(imp_num) as imp_num,
  sum(click_num) as click_num,
  sum(
    case
      when module in ('搜索feed', '发现feed') then imp_num
      else 0
    end
  ) as feed_imp_num,
  sum(
    case
      when module in ('搜索feed', '发现feed') then click_num
      else 0
    end
  ) as feed_click_num,
  sum(2nd_jump_click_num) as second_jump_click_num,
  0 as order_num,
  0 as rgmv,
  0 as enter_seller_uv,
  0 as read_uv
from
  redst.st_ads_wide_cpc_creativity_day_inc
where
  dtm between f_getdate('{{ ds_nodash }}', -7) and '{{ ds_nodash }}'
  and is_effective = 1
  and second_jump_type in (8,37)
  and (
    (
      marketing_target not in (3, 8, 13, 14)
      
    )
    or
    marketing_target in (3, 8, 14)
  ) --种草和线索和闭环电商
group by
  1,
  2,
  3,
  4,
  5,6
   --CID
union all
select dtm,
  brand_account_id,
  '竞价' as module,
  'CID' as tag,
  '' as type,
  '' as sub_type,
  sum(cost) as cost,
  sum(cash_cost) as cash_cost,
  sum(imp_num) as imp_num,
  sum(click_num) as click_num,
  sum(
    case
      when module in ('搜索feed', '发现feed') then imp_num
      else 0
    end
  ) as feed_imp_num,
  sum(
    case
      when module in ('搜索feed', '发现feed') then click_num
      else 0
    end
  ) as feed_click_num,
  sum(new_goods_component_click_num) as second_jump_click_num,
  sum(total_order) as order_num,
  sum(rgmv) as rgmv,
  0 as enter_seller_uv,
  0 as read_uv
from
  redst.st_ads_wide_cpc_creativity_day_inc
where
  dtm between f_getdate('{{ ds_nodash }}', -7) and '{{ ds_nodash }}'
  and is_effective = 1 -- and second_jump_type=8
  and marketing_target in (13) --CID
group by
  1,
  2,
  3,
  4,
  5,6
union all
--小红星 看流水 淘联
select as brief_start_time as dtm,
  brand_account_user_id as brand_account_id,
  '品合' as module,
  '小红星' as tag,
  '' as type,
  '' as sub_type,
  sum(cost) AS cost,
  sum(cash_cost) AS cash_cost,
  0 as imp_num,
  0 as click_num,
  0 as feed_imp_num,
  0 as feed_click_num,
  0 as second_jump_click_num,
  0 as order_num,
  0 as rgmv,
  sum(enter_seller_uv) as enter_seller_uv,
  sum(read_uv) as read_uv
from
  (
    select brief_start_time as dtm,
      brand_account_user_id,
      '品合' as module,
      '小红星' as tag,
      '' as type,
      '' as sub_type,
      sum(bcoo_content_price) AS cost,
      sum(bcoo_content_price) AS cash_cost,
      0 as enter_seller_uv,
      0 as read_uv
    from
      redapp.app_ads_taolian_brief_note_cost_df
    where
      dtm = '{{ds_nodash}}'
      and brief_start_time between f_getdate('{{ ds_nodash }}', -7) and '{{ ds_nodash }}'
    group by
      1,
      2,
      3,
      4,
      5,6
    union all
    --小红星 看流水 淘联
    select dtm,
      brand_account_user_id,
      '品合' as module,
      '小红星' as tag,
      '' as type,
      '' as sub_type,
      0 AS cost,
      0 AS cash_cost,
      sum(taobao_enter_num) as enter_seller_uv,
      sum(read_num) as read_uv
    from
      redapp.app_ads_taolian_note_metrics_di
    where
      dtm between f_getdate('{{ ds_nodash }}', -7) and '{{ ds_nodash }}'
    group by
      1,
      2,
      3,
      4,
      5,6
  ) t1
group by
  1,
  2,
  3,
  4,
  5,6
union all
--京东合作
select dtm,
  brand_account_id,
  '品合' as module,
  '京东合作' as tag,
  '' as type,
  '' as sub_type,
  sum(cost) as cost,
  sum(cash_cost) as cash_cost,
  0 as imp_num,
  0 as click_num,
  0 as feed_imp_num,
  0 as feed_click_num,
  0 as second_jump_click_num,
  null as order_num,
  null as rgmv,
  sum(enter_uv) as enter_seller_uv,
  sum(read_uv) as read_uv
from
  (
    SELECT dtm,
      order_id,
      read_uv,
      like_uv,
      share_uv,
      fav_uv,
      cmt_uv,
      enter_uv
    from
      redapp.app_ads_jd_alliance_note_stat_di
    where
      dtm between f_getdate('{{ ds_nodash }}', -7) and '{{ ds_nodash }}'
  ) t1
  left join (
    select
      virtual_object_id as order_id,
      brand_user_id as brand_account_id,
      sum(income_amt) as cost,
      sum(cash_income_amt) as cash_cost
    from
      redcdm.dwd_ads_ord_income_df
    where
      dtm = greatest('{{ds_nodash}}', '20230820')
      and date_key = '{{ds}}'
    group by
      1,
      2
  ) t2 on t1.order_id = t2.order_id
group by
  1,
  2,
  3,
  4
  )t1  
left join 
(select brand_account_id,
  brand_user_name,
  cpc_direct_sales_name,
  brand_direct_sales_name,
  cpc_operator_name,
  brand_direct_sales_dept1_name,
  cpc_direct_sales_dept1_name,
  brand_direct_sales_dept2_name,
  cpc_direct_sales_dept2_name,
  brand_direct_sales_dept3_name,
  cpc_direct_sales_dept3_name,
  brand_direct_sales_dept4_name,
  cpc_direct_sales_dept4_name,
  brand_direct_sales_dept5_name,
  cpc_direct_sales_dept5_name,
  brand_direct_sales_dept6_name,
  cpc_direct_sales_dept6_name,
  track_group_name,
  track_industry_name,
  track_detail_name
from redcdm.dim_ads_industry_account_df
where dtm= greatest('{{ds_nodash}}', '20230820')
)t2 
on t1.brand_account_id=t2.brand_account_id