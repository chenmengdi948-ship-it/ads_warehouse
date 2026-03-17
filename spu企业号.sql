with
bcoo_note as --蒲公英笔记
(
     -- 提升时效，从reddm.dm_soc_brand_coo_order_note_detail_day 抠出来的口径，预计202304月份会有base层的模型，需要替换上线
     select 
           t3.order_id
          ,t3.note_id
          ,t3.task_no
          ,a.report_brand_user_id
     from(
       select 
        order_id
        ,note_id
        ,task_no
       from reddw.dw_soc_tb_order_detail_day
       where dtm = '{{ds_nodash}}' and order_status in(401,402) and order_category not in(1,3) --非共创
       group by 1,2,3

       union all

        select
                t2.order_id
              ,t2.note_id
              ,t1.task_no
          from reddw.dw_soc_brand_coo_crowd_creation_task_day t1
     left join reddw.dw_soc_brand_coo_crowd_creation_note_day t2 on t1.task_no=t2.task_no and t2.dtm='{{ds_nodash}}' and t2.del=0
         where t1.dtm='{{ds_nodash}}' 
         and t1.task_status not in ('501','502','503') 
         and t1.task_title not like '%测试%'
         and t2.publish_mark=1
         group by 1,2,3  

     )t3
     left join
     (
  select order_id,
    report_brand_user_id
  from redods.ods_note_trade_tb_order
  WHERE dtm='{{ds_nodash}}'
  ) a
  on t3.order_id = a.order_id
     where note_id is not null and note_id<>'' and note_id<>' '
     group by t3.order_id
          ,t3.note_id
          ,t3.task_no
          ,a.report_brand_user_id
),
log_info as  (--品牌
select
      '{{ds}}' as date_key
      ,'品牌' as module
      ,case 
        when a.ads_container = 'app_open' then '开屏' 
        when a.ads_container = 'search_third' then '搜索第三位' 
        when a.ads_container = 'feed' then '信息流GD' 
        when a.ads_container = 'search_brand_area' then '品牌专区' 
        -- when a.ads_container = 'fire_topic' then '火焰话题' 
        else '品牌其他' 
      end as product
      ,a.advertiser_id as brand_account_id
      ,b.note_id
      ,a.creativity_id
      ,'整体' as marketing_target
      ,sum(a.imp_num) as imp_cnt
      ,sum(a.click_num) as click_cnt
      ,sum(a.like_num) as like_cnt
      ,sum(a.fav_num) as fav_cnt
      ,sum(a.cmt_num) as cmt_cnt
      ,sum(a.share_num) as share_cnt
      ,sum(a.follow_num) as follow_cnt
    from 
      redst.st_ads_brand_creativity_loc_metrics_day_inc a
    left join  
    (select 
      creativity_id,
      ads_material_id as note_id
    from redcdm.dim_ads_creativity_core_df
    where dtm = '{{ds_nodash}}'
      and ads_material_type = 'post'
    group by creativity_id,
      ads_material_id
    )b 
    on a.creativity_id = b.creativity_id
    join 
      reddw.dw_ads_account_day c on c.dtm = '{{ds_nodash}}' and a.advertiser_id = c.user_id and coalesce(c.company_name,'') <> 'offlineMockCompanyName' --剔除内广
    where
      a.dtm = '{{ds_nodash}}'
    group by 
      1,2,3,4,5,6,7
    union all 
    -- 效果
    select 
      '{{ds}}' as date_key
      ,'效果' as module
      ,case 
        when a.module = '发现feed' then '竞价-信息流' 
        when a.module = '搜索feed' then '竞价-搜索' 
        when a.module = '视频内流' then '竞价-视频内流'
      end as product
      ,a.brand_account_id
      ,ads_material_id as note_id
      ,creativity_id
      ,case when  a.marketing_target in (3, 8) then '闭环电商广告'
            when a.marketing_target in (13) then '非闭环电商广告'
            when a.marketing_target in (2, 5, 9) then '线索广告'
            when  a.marketing_target  not in (2,5,8,9,3,13) then '种草广告' else '其他' end as marketing_target
      ,sum(a.imp_num) as imp_cnt
      ,sum(a.click_num) as click_cnt
      ,sum(a.like_num) as like_cnt
      ,sum(a.fav_num) as fav_cnt
      ,sum(a.comment_num) as cmt_cnt
      ,sum(a.share_num) as share_cnt
      ,sum(a.follow_num) as follow_cnt
      
    from 
      reddw.dw_ads_wide_cpc_creativity_base_day_inc a
    where
      a.dtm = '{{ds_nodash}}'
      and a.is_effective = 1
      --and ads_material_type='post'
    group by 
      1,2,3,4,5,6,7
    union all 
    -- 品合
    select 
      '{{ds}}' as date_key
      ,'品合' as module
      ,'品合' as product
      ,report_brand_user_id as brand_account_id
      ,a.note_id
      ,'' as creativity_id
      ,'整体' as marketing_target
      ,sum(b.ads_imp_num) as imp_cnt
      ,sum(b.ads_click_num) as click_cnt
      ,sum(b.ads_like_num) as like_cnt
      ,sum(b.ads_fav_num) as fav_cnt
      ,sum(b.ads_cmt_num) as cmt_cnt
      ,sum(b.ads_share_num) as share_cnt
      ,sum(b.ads_follow_num) as follow_cnt
    from 
      bcoo_note a 
    left join 
      redapp.app_ads_note_engagement_1d_di b 
    on 
      b.dtm = '{{ds_nodash}}'
      and a.note_id = b.note_id
    -- where 
    --   a.dtm = '{{ds_nodash}}'
    --   and f_getdate(a.note_publish_time) <= '{{ds}}' -- 发布在今天之前的
    group by 
      1,2,3,4,5,6,7
    union all 
    -- 薯条
    select 
      '{{ds}}' as date_key
      ,'薯条' as module
      ,'薯条' as product
      ,chips_user_id as brand_account_id
      ,discovery_id as note_id
      ,'' as creativity_id
      ,'整体' as marketing_target
      ,sum(chips_imp_num) as imp_cnt
      ,sum(chips_click_num) as click_cnt
      ,sum(chips_like_num) as like_cnt
      ,sum(chips_fav_num) as fav_cnt
      ,sum(chips_cmt_num) as cmt_cnt
      ,sum(chips_share_num) as share_cnt
      ,sum(chips_follow_num) as follow_cnt
    from 
      redst.st_ads_chips_engagement_day_inc a 
    where
      dtm = '{{ds_nodash}}'
    group by 
      1,2,3,4,5,6,7
  
  ),
  all_cost as (
    select date_key,
      creativity_id,
      brand_account_id,
      t1.note_id,
      engage_spu_id,
      spu_id,
      module,
      product,
      marketing_target,
      sum(cash_cost) as cash_cost,
      sum(imp_cnt) as imp_cnt,
      sum(click_cnt) as click_cnt,
      sum(like_cnt) as like_cnt,
      sum(fav_cnt) as fav_cnt,
      sum(cmt_cnt) as cmt_cnt,
      sum(share_cnt) as share_cnt,
      sum(follow_cnt) as follow_cnt
    from
      (
        select '{{ds}}' as date_key,
          virtual_object_id as creativity_id,
          brand_account_id,
          note_id,
          module,
          case when product='发现feed' then '竞价-信息流' 
            when product='搜索feed' then '竞价-搜索'
            when product='视频内流' then '竞价-视频内流' 
            when module = '薯条' then '薯条' when module ='品合' then '品合' else product end as product,
          case when module = '效果'
            then
              case
                when marketing_target in (3, 8) then '闭环电商广告'
                when marketing_target in (13) then '非闭环电商广告'
                when marketing_target in (2, 5, 9) then '线索广告'
                else '种草广告'
              end
          else '其他'
          end as marketing_target,
          sum(cash_income_amt) as cash_cost,
          0 as imp_cnt,
          0 as click_cnt,
          0 as like_cnt,
          0 as fav_cnt,
          0 as cmt_cnt,
          0 as share_cnt,
          0 as follow_cnt
        from
          redcdm.dws_ads_pub_creativity_order_share_nd_df
        where
          dtm =  greatest('{{ds_nodash}}', '20230625')
          and date_key = '{{ds}}'
        group by
          virtual_object_id,
          brand_account_id,
          note_id,
          module,
          case when product='发现feed' then '竞价-信息流' 
            when product='搜索feed' then '竞价-搜索'
            when product='视频内流' then '竞价-视频内流' 
            when module = '薯条' then '薯条' when module ='品合' then '品合' else product end,
          case when module = '效果'
            then
              case
                when marketing_target in (3, 8) then '闭环电商广告'
                when marketing_target in (13) then '非闭环电商广告'
                when marketing_target in (2, 5, 9) then '线索广告'
                else '种草广告'
              end
          else '其他'
          end 
        union all 
        --流量
        select date_key,
          creativity_id,
          brand_account_id,
          note_id,
          module,
          product,
          marketing_target,
          0 as cash_cost,
          imp_cnt,
          click_cnt,
          like_cnt,
          fav_cnt,
          cmt_cnt,
          share_cnt,
          follow_cnt
        from log_info
      ) t1
      left join (--互动看算法+人工
        select
          note_id,
          spu_id as engage_spu_id
        from
          ads_databank.dim_spu_note_df
        where
          dtm = greatest('{{ds_nodash}}', '20230625')
          --and bind_type = 2
        group by 1,2
      ) spu_note_engage on spu_note_engage.note_id = t1.note_id
      left join (--收入只看人工绑定
        select
          note_id,
          spu_id
        from
          ads_databank.dim_spu_note_df
        where
          dtm = greatest('{{ds_nodash}}', '20230625')
          and bind_type = 2
        group by 1,2
      ) spu_note on spu_note.note_id = t1.note_id and spu_note.spu_id = spu_note_engage.engage_spu_id
    group by
      date_key,
      creativity_id,
      brand_account_id,
      t1.note_id,
      spu_id,
      engage_spu_id,
      module,
      product,
      marketing_target
  ),
spu_cost_log as (
    select
      spu_id,
      module,
      product,
      marketing_target,
      brand_account_id,
      sum(cash_cost) as cash_cost,
      sum(imp_cnt) as imp_cnt,
      sum(click_cnt) as click_cnt,
      sum(like_cnt) as like_cnt,
      sum(fav_cnt) as fav_cnt,
      sum(cmt_cnt) as cmt_cnt,
      sum(share_cnt) as share_cnt,
      sum(follow_cnt) as follow_cnt
    from
      (
        select
          creativity_id,
          brand_account_id,
          note_id,
          spu_id,
          module,
          product,
          marketing_target,
          cash_cost,
          imp_cnt,
          click_cnt,
          like_cnt,
          fav_cnt,
          cmt_cnt,
          share_cnt,
          follow_cnt
        from
          all_cost
        where
          spu_id is not null --笔记人工绑定spu
        union all 
        --互动算法绑定
        select
          creativity_id,
          brand_account_id,
          note_id,
          spu_id,
          module,
          product,
          marketing_target,
          0 as cash_cost,
          imp_cnt,
          click_cnt,
          like_cnt,
          fav_cnt,
          cmt_cnt,
          share_cnt,
          follow_cnt
        from
          all_cost
        where
          spu_id is null and engage_spu_id is not null --算法绑定spu
        union all
        select
          creativity_id,
          brand_account_id,
          note_id,
          ele_note.spu_id,
          module,
          product,
          marketing_target,
          cash_cost,
          imp_cnt,
          click_cnt,
          like_cnt,
          fav_cnt,
          cmt_cnt,
          share_cnt,
          follow_cnt
        from
          (
            select
              date_key,
              creativity_id,
              brand_account_id,
              note_id,
              engage_spu_id,
              spu_id,
              module,
              product,
              marketing_target,
              cash_cost,
              imp_cnt,
              click_cnt,
              like_cnt,
              fav_cnt,
              cmt_cnt,
              share_cnt,
              follow_cnt
            from
              all_cost
            where
              engage_spu_id is null --未人工或算法绑定spu
            union all 
            select
              date_key,
              creativity_id,
              brand_account_id,
              note_id,
              engage_spu_id,
              spu_id,
              module,
              product,
              marketing_target,
              cash_cost,
              0 as imp_cnt,
              0 as click_cnt,
              0 as like_cnt,
              0 as fav_cnt,
              0 as cmt_cnt,
              0 as share_cnt,
              0 as follow_cnt
            from
              all_cost
            where
              engage_spu_id is not null and spu_id is null--未人工绑定spu
          ) t1
          left join (
            select
              a.element_id,
              a.main_spu_id as spu_id
            from
              redods.ods_shequ_feed_ads_tb_material_bind_spu_df a
            where
              a.dtm = '{{ds_nodash}}'
              and a.bind_status = 2
              and a.del = 0
            group by
              1,
              2
          ) ele_note on ele_note.element_id = t1.creativity_id
        where
          ele_note.spu_id is not null --element人工绑定spu
      ) info
    group by
      spu_id,
      brand_account_id,
      module,
      product,
      marketing_target
  )
insert overwrite table redcdm.dm_ads_spu_account_detail_1d_di   partition( dtm = '{{ds_nodash}}')
select  detail.date_key,
  detail.spu_id,
  detail.brand_account_id,
  detail.module,
  case when detail.product='发现feed' then '竞价-信息流' 
    when detail.product='搜索feed' then '竞价-搜索'
    when detail.product='视频内流' then '竞价-视频内流' 
    when detail.product='蒲公英' then '品合' else detail.product end as product ,
  detail.marketing_target,
  tt1.spu_name,
  tt1.brand_id,
  tt1.brand_name,
  tt1.commercial_taxonomy_name1,
  tt1.commercial_code2,
  tt1.commercial_taxonomy_name2,
  tt1.commercial_code3,
  tt1.commercial_taxonomy_name3,
  tt1.commercial_code4,
  tt1.commercial_taxonomy_name4,
  spu_account.brand_account_name,
  spu_account.operator_code,
  spu_account.operator_name ,
  spu_account.direct_sales_code,
  spu_account.direct_sales_name ,
  spu_account.direct_sales_dept1_name,
  spu_account.direct_sales_dept2_name ,
  spu_account.direct_sales_dept3_name,
  sum(imp_num) as imp_num,
  sum(click_num) as click_num,
  sum(like_num) as like_num,
  sum(fav_num) as fav_num,
  sum(read_feed_num) as read_feed_num,
  sum(share_num) as share_num,
  sum(cash_cost) as cash_cost,
  sum(bind_cash_cost) as bind_cash_cost
from 
( 
select '{{ds}}' as date_key,
  module,
  product,
  marketing_target,
  cost.spu_id,
  cost.brand_account_id,
  sum(imp_num) as imp_num,
  sum(click_num) as click_num,
  sum(like_num) as like_num,
  sum(fav_num) as fav_num,
  sum(read_feed_num) as read_feed_num,
  sum(share_num) as share_num,
  sum(cash_cost) as cash_cost,
  sum(bind_cash_cost) as bind_cash_cost
from
   (
    select ----绑定spu的流水分摊 
      spu_id,
      brand_user_id as brand_account_id,
      case when module = '蒲公英' then '品合' 
      when module =  '竞价' then '效果' 
      when module =  '品牌广告' then '品牌' else module end as module,
      case when module = '薯条' then '薯条' when module = '蒲公英' then '品合' else product end as product,
      case when marketing_target is null or marketing_target='' then '其他' else marketing_target end as marketing_target,
      cash_cost,
      0 as bind_cash_cost,
      0 as imp_num,
      0 as click_num,
      0 as like_num,
      0 as fav_num,
      0 as read_feed_num,
      0 as share_num
    from
      redcdm.dm_ads_note_spu_engage_cost_avg_1d_di
    where
      dtm = '{{ds_nodash}}'
      and spu_id is not null
    union all --人工绑定spu的流水加和以及流量数据
    select
      spu_id,
      brand_account_id,
      module,
      case when module = '薯条' then '薯条' when module ='品合' then '品合' else product end as product,
      case when marketing_target is null or marketing_target='' then '其他' else marketing_target end as marketing_target,
      0 as cash_cost,
      sum(cash_cost) as bind_cash_cost,
      sum(imp_cnt) as imp_num,
      sum(click_cnt) as click_num,
      sum(like_cnt) as like_num,
      sum( fav_cnt) as fav_num,
      null as read_feed_num,
      sum(share_cnt) as share_num
    from spu_cost_log
    group by spu_id,
      brand_account_id,
      module,
      case when module = '薯条' then '薯条' when module ='品合' then '品合' else product end,
      case when marketing_target is null or marketing_target='' then '其他' else marketing_target end
  ) cost 
  
  group by  module,
    product,
    marketing_target,
    cost.spu_id,
    cost.brand_account_id
)detail
left join (
    select
      spu_id,
      brand_id,
      brand_name,
      name as spu_name,
      commercial_taxonomy_name1,
      commercial_code2,
      commercial_taxonomy_name2,
      commercial_code3,
      commercial_taxonomy_name3,
      commercial_code4,
      commercial_taxonomy_name4
    from
      ads_databank.dim_spu_df
    where
      dtm = '{{ds_nodash}}'
  ) tt1 on tt1.spu_id = detail.spu_id
left join 
  (select brand_account_id,
      direct_sales_dept1_name,
      direct_sales_dept2_name,
      direct_sales_dept3_name,
      direct_sales_name,
      operator_name,
      operator_code ,
      direct_sales_code,
      brand_user_name as brand_account_name
  from redapp.app_ads_insight_industry_account_df
  where dtm= greatest('20220626','{{ds_nodash}}')
  )spu_account 
  on spu_account.brand_account_id=detail.brand_account_id
group by detail.date_key,
  detail.spu_id,
  detail.brand_account_id,
  detail.module,
  case when detail.product='发现feed' then '竞价-信息流' 
    when detail.product='搜索feed' then '竞价-搜索'
    when detail.product='视频内流' then '竞价-视频内流' 
    when detail.product='蒲公英' then '品合' else detail.product end,
  detail.marketing_target,
  tt1.spu_name,
  tt1.brand_id,
  tt1.brand_name,
  tt1.commercial_taxonomy_name1,
  tt1.commercial_code2,
  tt1.commercial_taxonomy_name2,
  tt1.commercial_code3,
  tt1.commercial_taxonomy_name3,
  tt1.commercial_code4,
  tt1.commercial_taxonomy_name4,
  spu_account.brand_account_name,
  spu_account.operator_code,
  spu_account.operator_name ,
  spu_account.direct_sales_code,
  spu_account.direct_sales_name ,
  spu_account.direct_sales_dept1_name,
  spu_account.direct_sales_dept2_name ,
  spu_account.direct_sales_dept3_name