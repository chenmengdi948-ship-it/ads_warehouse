CREATE EXTERNAL TABLE `redcdm.dm_ads_spu_cvr_cost_di`(
  date_key string comment'统计日期，yyyy-MM-dd',
  spu_id bigint comment'spu_id',
  `brand_id` bigint COMMENT '雅典娜品牌ID', 
   `brand_name` string COMMENT '品牌名', 
  spu_name string COMMENT '产品名称', 
  `commercial_taxonomy_name1` string COMMENT '一级类目名称', 
  `commercial_code2` string COMMENT '二级类目ID', 
  `commercial_taxonomy_name2` string COMMENT '二级类目名称', 
  `commercial_code3` string COMMENT '三级类目ID', 
  `commercial_taxonomy_name3` string COMMENT '三级类目名称', 
  `commercial_code4` string COMMENT '四级类目ID', 
  `commercial_taxonomy_name4` string COMMENT '四级类目名称', 
  brand_account_id string comment'企业号id',
  cpc_operator_code string comment'效果运营id',
  cpc_operator_name string comment'效果运营姓名',
  cpc_direct_sales_code string comment'效果直客销售id',
  cpc_direct_sales_name string comment'效果直客销售',
  cpc_direct_sales_dept1_name string comment'销售一级部门',
  cpc_direct_sales_dept2_name string comment'销售二级部门',
  cpc_direct_sales_dept3_name string comment'销售三级部门',
  imp_num bigint comment'曝光量',
  click_num bigint comment'点击量',
  like_num bigint comment'点赞量',
  fav_num bigint comment'收藏',
  read_feed_num bigint comment'阅读量',
  share_num bigint comment'分享量',
  query_cnt bigint comment'搜索量',
  note_screenshot_cnt bigint comment'截屏量',
  ti_user_num bigint comment'TI规模（30d口径）',
  ti_level string comment'TI规模分层',
  imp_note_num bigint comment'笔记数（有曝光）',
  bind_note_num bigint comment'绑定笔记数',
  softad_imp_note_num bigint comment'软广笔记数（有曝光）',
  pos_imp_note_num bigint comment'正面笔记数（有曝光）',
  new_note_num bigint comment'新发笔记数',
  cash_cost double comment'spu分摊现金消耗',
  bind_cash_cost double comment'人工绑定spu笔记现金消耗，单位元'
  )
COMMENT 'SPU指标宽表'
PARTITIONED BY ( 
  `dtm` string)

  with
  all_cost as (
    select
      virtual_object_id,
      t1.note_id,
      spu_id,
      module,
      product,
      case
        when marketing_target in(3, 8) then '闭环电商广告'
        when marketing_target in(13) then '非闭环电商广告'
        when marketing_target in(2, 5, 9) then '线索广告'
        else '种草广告'
      end as marketing_target,
      sum(cash_cost) as cash_cost
    from
      (
        select
          virtual_object_id,
          note_id,
          module,
          product,
          marketing_target,
          sum(cash_income_amt) as cash_cost
        from
          redcdm.dws_ads_pub_creativity_order_share_nd_df
        where
          dtm =  greatest('{{ds_nodash}}', '20230625')
          and date_key = '{{ds}}'
        group by
          virtual_object_id,
          note_id,
          module,
          product,
          marketing_target
      ) t1
      left join (
        select
          note_id,
          spu_id,
          bind_type -- 1、算法，2、人工 
        from
          ads_databank.dim_spu_note_df
        where
          dtm = greatest('{{ds_nodash}}', '20230625')
          and bind_type = 2
        group by 1,2,3
      ) spu_note on spu_note.note_id = t1.note_id
    group by
      virtual_object_id,
      t1.note_id,
      spu_id,
      module,
      product,
      case
        when marketing_target in(3, 8) then '闭环电商广告'
        when marketing_target in(13) then '非闭环电商广告'
        when marketing_target in(2, 5, 9) then '线索广告'
        else '种草广告'
      end
  ),
  spu_cost as (
    select
      spu_id,
      module,
      product,
      marketing_target,
      sum(cash_cost) as cash_cost
    from
      (
        select
          virtual_object_id,
          note_id,
          spu_id,
          module,
          product,
          marketing_target,
          cash_cost
        from
          all_cost
        where
          spu_id is not null --笔记人工绑定spu
        union all
        select
          virtual_object_id,
          note_id,
          ele_note.spu_id,
          module,
          product,
          marketing_target,
          cash_cost
        from
          (
            select
              *
            from
              all_cost
            where
              spu_id is null --笔记未人工绑定spu
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
          ) ele_note on ele_note.element_id = t1.virtual_object_id
        where
          ele_note.spu_id is not null --element人工绑定spu
      ) info
    group by
      spu_id,
      module,
      product,
      marketing_target
  ),
  spu_account as 
  (select spu.spu_id,
    brand_account.brand_account_id,
    brand_account_name,
    cpc_operator_code,
    cpc_operator_name ,
    cpc_direct_sales_code,
    cpc_direct_sales_name ,
    cpc_direct_sales_dept1_name,
    cpc_direct_sales_dept2_name ,
    cpc_direct_sales_dept3_name,
  from 
  (select
      spu_id,
      brand_id
    from
      ads_databank.dim_spu_df
    where
      dtm = '{{ds_nodash}}'
  )spu 
  left join 
 ( --brand_id和brand_account_id映射关系
  select
    brandz_id as brand_id
    ,brand_user_id as brand_account_id
  from
        (
            select
              brandz_id
              ,brand_user_id
              ,coalesce(cash_cost,0) as cash_cost
              ,row_number() over(partition by brandz_id order by cash_cost desc) as rk
            from
            (
              select
                brandz_id
                ,brand_user_id
              from redods.ods_ads_crm_crm_account_brandz_info
              -- where dtm = '{{ds_nodash}}'
              where dtm = greatest('20220625','{{ds_nodash}}')
              and state = 1
            ) a 
            left join
            (
              select
                brand_user_id as brand_account_id
                ,sum(cash_cost) as cash_cost
              from reddm.dm_ads_crm_advertiser_income_wide_day
              where dtm = '{{ds_nodash}}'
              and module in ('品牌','效果','品合','薯条')
              -- and launch_date between '2021-01-01' and '2022-05-06'
              and date_key > f_getdate('{{ds}}',-30) --近30日流水最大
              group by 1
            ) b
            on a.brand_user_id = b.brand_account_id
            
          
        ) a 
    where rk = 1
    )brand_account 
    on brand_account.brand_id = spu.brand_id
  left join 
  (select 
  from )account 
  
)
insert overwrite table redcdm.dm_ads_pub_spu_cvr_cost_1d_di   partition( dtm = '{{ds_nodash}}')
select  date_key,
  module,
  product,
  marketing_target,
  spu_id,
  brand_id,
  brand_name,
  spu_name,
  commercial_taxonomy_name1,
  commercial_code2,
  commercial_taxonomy_name2,
  commercial_code3,
  commercial_taxonomy_name3,
  commercial_code4,
  commercial_taxonomy_name4,
  null as brand_account_id,
  null as brand_account_name,
  null as cpc_operator_code,
  null as cpc_operator_name ,
  null as cpc_direct_sales_code,
  null as cpc_direct_sales_name ,
  null as cpc_direct_sales_dept1_name,
  null as cpc_direct_sales_dept2_name ,
  null as cpc_direct_sales_dept3_name,
  imp_num,
  click_num,
  like_num,
  fav_num,
  read_feed_num,
  share_num,
  query_cnt as query_num,
  note_screenshot_cnt as note_screenshot_num,
  ti_user_num,
  ti_level,
  imp_note_num,
  bind_note_num,
  softad_imp_note_num,
  pos_imp_note_num,
  new_note_num,
  cash_cost,
  bind_cash_cost
from 
(select
  t1.date_key,
  '整体' as module,
  '整体' as product,
  '整体' as marketing_target,
  t1.spu_id,
  t2.brand_id,
  t2.brand_name,
  t2.spu_name,
  t2.commercial_taxonomy_name1,
  t2.commercial_code2,
  t2.commercial_taxonomy_name2,
  t2.commercial_code3,
  t2.commercial_taxonomy_name3,
  t2.commercial_code4,
  t2.commercial_taxonomy_name4,
  t1.imp_num,
  t1.click_num,
  t1.like_num,
  t1.fav_num,
  t1.read_feed_num,
  t1.share_num,
  t1.query_cnt,
  t1.note_screenshot_cnt,
  aips.ti_user_num,
  case
    when ti_user_num > 1000000 then '百万以上'
    when ti_user_num > 500000 then '五十万-百万'
    when ti_user_num > 100000 then '十万-五十万'
    when ti_user_num > 10000 then '一万-十万'
    else '一万以下'
  end as ti_level,
  imp_note_num,
  bind_note_num,
  softad_imp_note_num,
  pos_imp_note_num,
  new_note_num,
  0 as cash_cost,
  0 as bind_cash_cost
from
  (
    select
      dtm as date_key,
      spu_id,
      -- spu_name,
      -- brand_id,
      -- brand_name,
      -- commercial_taxonomy_name1,
      -- commercial_code2,
      -- commercial_taxonomy_name2,
      -- commercial_code3,
      -- commercial_taxonomy_name3,
      -- commercial_code4,
      -- commercial_taxonomy_name4,
      sum(imp_num) as imp_num,
      sum(click_num) as click_num,
      sum(like_num) as like_num,
      sum(fav_num) as fav_num,
      sum(read_feed_num) as read_feed_num,
      sum(share_num) as share_num,
      sum(query_cnt) as query_cnt, --缺少截屏数
      sum(coalesce(feed_screenshot_num,0)+ coalesce(follow_feed_screenshot_num,0)+ coalesce(nearby_screenshot_num,0)+ coalesce(search_screenshot_num,0)+
      coalesce(activity_h5_screenshot_num,0)) as note_screenshot_cnt
    from
      redapp.app_ads_spu_user_interest_1d_di --存在1个spu对应多个类目
    where
      dtm = '{{ds_nodash}}'
    group by dtm,
      spu_id
      -- spu_name,
      -- brand_id,
      -- brand_name,
      -- commercial_taxonomy_name1,
      -- commercial_code2,
      -- commercial_taxonomy_name2,
      -- commercial_code3,
      -- commercial_taxonomy_name3,
      -- commercial_code4,
      -- commercial_taxonomy_name4
  ) t1
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
    group by 1,2,3,4,5,6,7,8,9,10,11
  ) t2 on t1.spu_id = t2.spu_id
  left join (
    select
      spu_id,
      count(
        case
          when imp_num > 0 then t3.note_id
          else null
        end
      ) as imp_note_num,
      count(distinct case when bind_type=2 then t3.note_id else null end) as bind_note_num,
      count(
        case
          when imp_num > 0
          and softad_score > 0.5 then t3.note_id
          else null
        end
      ) as softad_imp_note_num,
      count(
        case
          when imp_num > 0
          and nvl(positive_sentiment_num,0)/
          (nvl(positive_sentiment_num,0)+nvl(negative_sentiment_num,0)+nvl(neutral_sentiment_num,0))>0.5
          then t3.note_id else null end
      ) as pos_imp_note_num,
      count(
        case
          when substring(create_time, 1, 10) = '{{ds}}' then t3.note_id
          else null
        end
      ) as new_note_num
    from
      (
        select
          spu_id,
          note_id,
          create_time,
          bind_type,
          positive_sentiment_num,
          negative_sentiment_num,
          neutral_sentiment_num
        from
          ads_databank.dim_spu_note_df
        where
          dtm = '{{ds_nodash}}'
          --and bind_type=2 --人工绑定
      ) t3
      left join (
        -- 04.11新逻辑 --
        select
          a.discovery_id,
          max(algo_softad_socre) as softad_score
        from
          reddm.dm_soc_fake_recommend_discovery_day a
          join reddw.dw_soc_discovery_delta_7_day b on b.dtm = '{{ds_nodash}}'
          and a.discovery_id = b.discovery_id
        WHERE
          a.dtm = '{{ds_nodash}}'
          and algo_softad_socre >= 0.5
        group by
          1
      ) t4 on t4.discovery_id = t3.note_id
      left join --笔记流量互动
      (
        select
          discovery_id,
          sum(imp_num) as imp_num
        from
          reddm.dm_soc_discovery_engagement_day_inc
        where
          dtm = '{{ds_nodash}}'
        group by
          discovery_id
      ) t5 on t5.discovery_id = t3.note_id
    group by
      t3.spu_id
  ) t6 on t6.spu_id = t1.spu_id --30日aips人群
  left join (
    select
      spu_id,
      COUNT(distinct user_id) as ti_user_num
    from
      redapp.app_ads_spu_user_interest_30d_di
    where
      dtm = '{{ds_nodash}}'
      and interest_level in ('TI')
    group by
      spu_id
  ) aips on aips.spu_id = t1.spu_id

union all 
select '{{ds}}' as date_key,
  module,
  product,
  marketing_target,
  cost.spu_id,
  tt1.brand_id,
  tt1.brand_name,
  tt1.spu_name,
  tt1.commercial_taxonomy_name1,
  tt1.commercial_code2,
  tt1.commercial_taxonomy_name2,
  tt1.commercial_code3,
  tt1.commercial_taxonomy_name3,
  tt1.commercial_code4,
  tt1.commercial_taxonomy_name4,
  0 as imp_num,
  0 as click_num,
  0 as like_num,
  0 as fav_num,
  0 as read_feed_num,
  0 as share_num,
  0 as query_cnt,
  0 as note_screenshot_cnt,
  0 as ti_user_num,
  '其他' as ti_level,
  0 as imp_note_num,
  0 as bind_note_num,
  0 as softad_imp_note_num,
  0 as pos_imp_note_num,
  0 as new_note_num,
  sum(cash_cost) as cash_cost,
  sum(bind_cash_cost) as bind_cash_cost
from
   (
    select ----绑定spu的流水分摊 
      spu_id,
      case when module = '蒲公英' then '品合' else module end as module,
      product,
      marketing_target,
      cash_cost,
      0 as bind_cash_cost
    from
      redcdm.dm_ads_spu_product_cost_avg_1d_di
    where
      dtm = '{{ds_nodash}}'
    union all --人工绑定spu的流水加和 
    select
      spu_id,
      module,
      product,
      marketing_target,
      0 as cash_cost,
      cash_cost as bind_cash_cost
    from spu_cost
  ) cost 
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
  ) tt1 on tt1.spu_id = cost.spu_id
  group by  module,
    product,
    marketing_target,
    cost.spu_id,
    tt1.brand_id,
    tt1.brand_name,
    tt1.spu_name,
    tt1.commercial_taxonomy_name1,
    tt1.commercial_code2,
    tt1.commercial_taxonomy_name2,
    tt1.commercial_code3,
    tt1.commercial_taxonomy_name3,
    tt1.commercial_code4,
    tt1.commercial_taxonomy_name4
)detail