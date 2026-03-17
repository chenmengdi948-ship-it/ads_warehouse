--流水数据
select quarter,
  spu_id,
  spu_name,
  brand_id2 as brand_id,
  brand_name2 as brand_name,
  commercial_level,
  commercial_code,
  commercial_name,
  commercial_code1,
  commercial_taxonomy_name1,
  commercial_code2,
  commercial_taxonomy_name2,
  commercial_code3,
  commercial_taxonomy_name3,
  commercial_code4,
  commercial_taxonomy_name4,

  is_trd_mapping_spu,
  marketing_target,
  module,
  product,
  optimize_target,

  
  income_amt,
  cash_income_amt,
  deal_gmv,
  k_live_dgmv,
  b_live_dgmv,
  note_dgmv,
  
  ads_deal_gmv,
  offline_spu_name,
  offline_brand_name,
  is_mapping_spu,
  is_mapping_brand,
  '' as category,
  '' as segment,
  '' as sub_segment,
  0 as element,
   0 as  benefit,
   0 as  sale,
   0 as  unit
from 
    (select spu.*,
      coalesce(spu.brand_id,t3.brand_id) as brand_id2,
      coalesce(spu.brand_name,t3.brand_name) as brand_name2,
      offline_brand_name,
      case when t3.brand_id<>'' then 1 else 0 end as is_mapping_brand
    from 
      (select quarter,
        coalesce(t1.spu_id,t2.spu_id) as spu_id,
        coalesce(t1.spu_name,t2.spu_name) as spu_name,
        brand_id,
        
        commercial_level,
        commercial_code,
        commercial_name,
        commercial_code1,
        commercial_taxonomy_name1,
        commercial_code2,
        commercial_taxonomy_name2,
        commercial_code3,
        commercial_taxonomy_name3,
        commercial_code4,
        commercial_taxonomy_name4,
        brand_name,
        is_trd_mapping_spu,
        marketing_target,
        module,
        product,
        optimize_target,

        
        income_amt,
        cash_income_amt,
        deal_gmv,
        k_live_dgmv,
        b_live_dgmv,
        note_dgmv,
        
        ads_deal_gmv,
        offline_spu_name,
        case when t2.spu_id<>'' then 1 else 0 end as is_mapping_spu
      from
       (SELECT
        concat(year(date_key),'-','Q',ceil(month(date_key)/3)) as quarter,
        spu_id,
        spu_name,
        brand_id,
        
        commercial_level,
        commercial_code,
        commercial_name,
        commercial_code1,
        commercial_taxonomy_name1,
        commercial_code2,
        commercial_taxonomy_name2,
        commercial_code3,
        commercial_taxonomy_name3,
        commercial_code4,
        commercial_taxonomy_name4,
        brand_name,


        is_trd_mapping_spu,


        marketing_target,
        module,
        product,
        optimize_target,

        
        sum(income_amt) as income_amt,
        sum(cash_income_amt) as cash_income_amt,
        sum(deal_gmv) as deal_gmv,
        sum(k_live_dgmv) as k_live_dgmv,
        sum(b_live_dgmv) as b_live_dgmv,
        sum(note_dgmv) as note_dgmv,
        
        sum(ads_deal_gmv) as ads_deal_gmv
      FROM
        redapp.app_ads_industry_spu_mapping_metrics_detail_df
      WHERE
        dtm = '20250121'
        group by concat(year(date_key),'-','Q',ceil(month(date_key)/3)) ,
        spu_id,
        spu_name,
        brand_id,
        brand_name,
        commercial_level,
        commercial_code,
        commercial_name,
        commercial_code1,
        commercial_taxonomy_name1,
        commercial_code2,
        commercial_taxonomy_name2,
        commercial_code3,
        commercial_taxonomy_name3,
        commercial_code4,
        commercial_taxonomy_name4,
        is_trd_mapping_spu,


        marketing_target,
        module,
        product,
        optimize_target
        )t1 
        full  join 
        (SELECT
        concat_ws(',',collect_set(offline_spu_name)) as offline_spu_name,
        spu_id,
        concat_ws(',',collect_set(spu_name)) as spu_name
      FROM
        redods.ods_redoc2hive_ads_industry_spu_mapping_df
      WHERE
        dtm = '{{ds_nodash}}'
      group by spu_id
        )t2 
        on t1.spu_id = t2.spu_id
    )spu
    full join 
    (SELECT brand_id,
      concat_ws(',',collect_set(offline_brand_name)) as offline_brand_name,
      concat_ws(',',collect_set(brand_name)) as brand_name,
      
    FROM
      redods.ods_redoc2hive_ads_industry_brand_mapping_df
    WHERE
      dtm = '{{ds_nodash}}'
    group by brand_id
    )t3
    on spu.brand_id = t3.brand_id
  )detail

union all

--线下数据
select period as quarter,
    spu.spu_id,
    t5.spu_name,
    t3.brand_id,
    t5.brand_name,
    commercial_level,
    commercial_code,
    commercial_name,
    commercial_code1,
    commercial_taxonomy_name1,
    commercial_code2,
    commercial_taxonomy_name2,
    commercial_code3,
    commercial_taxonomy_name3,
    commercial_code4,
    commercial_taxonomy_name4,
    is_trd_mapping_spu,
    '' as marketing_target,
    '' as module,
    '' as product,
    '' as optimize_target,

    
    0 as income_amt,
    0 as cash_income_amt,
    0 as deal_gmv,
    0 as k_live_dgmv,
    0 as b_live_dgmv,
    0 as note_dgmv,
    
    0 as ads_deal_gmv,
    t4.offline_spu_name,
    t4.offline_brand_name,
    case when spu.spu_id<>'' then 1 else 0 end as is_mapping_spu,
    case when t3.brand_id<>'' then 1 else 0 end as is_mapping_brand,
    category,
    segment,
    sub_segment,
    element,
    benefit,
    sale,
    unit
  from
  (SELECT
    concat(year,'-',period) as period,

    offline_brand_name,
    category,
    segment,
    sub_segment,
    element,
    benefit,
    offline_spu_name,
    sale,
    unit
  FROM
    redods.ods_redoc2hive_ads_industry_offline_spu_brand_info_df
  WHERE
    dtm = '{{ds_nodash}}'
  )t4
  left join 
  (SELECT
    offline_spu_name,
    spu_id,
    spu_name
  FROM
    redods.ods_redoc2hive_ads_industry_spu_mapping_df
  WHERE
    dtm = '{{ds_nodash}}'

  )spu
  on spu.offline_spu_name = t4.offline_spu_name
  left join 
  (SELECT
    offline_brand_name,
    brand_name,
    brand_id
  FROM
    redods.ods_redoc2hive_ads_industry_brand_mapping_df
  WHERE
    dtm = '{{ds_nodash}}'
  )t3
  on t3.offline_brand_name = t4.offline_brand_name
  left join 
  (SELECT
    date_key,
    spu_id,
    spu_name,
    brand_id,
    commercial_level,
    commercial_code,
    commercial_name,
    commercial_code1,
    commercial_taxonomy_name1,
    commercial_code2,
    commercial_taxonomy_name2,
    commercial_code3,
    commercial_taxonomy_name3,
    commercial_code4,
    commercial_taxonomy_name4,
    brand_name,
    spu_type,
    mapping_first_industry_name,
    mapping_second_industry_name,
    is_brand_ka_spu,
    is_ads_ka_spu,
    is_trd_mapping_spu,
    ti_cash_income_amt,
    ti_cash_income_amt_30d,
    ti_cash_income_amt_1m,
    spu_deal_gmv_30d,
    spu_deal_gmv_1m,
    spu_deal_gmv,
    is_core_spu
  FROM
    redapp.app_ads_industry_spu_mapping_label_df
  WHERE
    dtm = '{{ds_nodash}}'
  )t5 
  on t5.spu_id = spu.spu_id




  CREATE EXTERNAL TABLE `redapp.app_ads_industry_spu_mapping_quarter_info_df`(
  `quarter` string COMMENT '季度', 
  `spu_id` bigint COMMENT 'spu id--统一对外$$以这个字段为准,等于main_spu_id', 
  `spu_name` string COMMENT 'spu名称$$spu_name', 
  `brand_id` bigint COMMENT '雅典娜品牌ID$$对应维表ads_databank.dim_brand_df的brand_id', 
  `brand_name` string COMMENT '品牌名$$对应维表ads_databank.dim_brand_df的brand_name', 
  `commercial_level` int COMMENT '类目级别$$类目级别', 
  `commercial_code` string COMMENT '类目ID$$对应维表ads_databank.dim_commercial_taxonomy_df的commercial_code', 
  `commercial_name` string COMMENT '类目名称$$类目名称', 
  `commercial_code1` string COMMENT '一级类目ID$$一级类目ID', 
  `commercial_taxonomy_name1` string COMMENT '一级类目名称$$一级类目名称', 
  `commercial_code2` string COMMENT '二级类目ID$$二级类目ID', 
  `commercial_taxonomy_name2` string COMMENT '二级类目名称$$二级类目名称', 
  `commercial_code3` string COMMENT '三级类目ID$$三级类目ID', 
  `commercial_taxonomy_name3` string COMMENT '三级类目名称$$三级类目名称', 
  `commercial_code4` string COMMENT '四级类目ID$$四级类目ID', 
  `commercial_taxonomy_name4` string COMMENT '四级类目名称$$四级类目名称', 
  
  
  `is_trd_mapping_spu` bigint COMMENT '是否有站内sku mapping关系', 
  
  `marketing_target` bigint COMMENT '计划营销目的$$营销诉求:3-商品销量 4-产品种草 6-品牌知名度 7-品牌意向 8-直播推广 9-客资收集 10-抢占关键词 11-抢占人群 12-加粉 13-行业商品推广\t', 
  `module` string COMMENT '产品线$$品牌、效果、薯条、品合', 
  `product` string COMMENT '二级产品线$$枚举值：品牌【开屏、品牌专区、搜索第三位、信息流GD、火焰话题】， 效果【搜索、信息流、视频内流】， 薯条【旧版竞价、新版加热、旧版加热、新版竞价】，品合【定制、共创、招募、新芽】', 
  `optimize_target` string COMMENT '优化目标$$种草通、非种草通（竞价有，其他为\'整体\'）', 
  
  `income_amt` double COMMENT '运营消耗$$单位-元', 
  `cash_income_amt` double COMMENT '现金消耗$$单位-元', 
  `deal_gmv` double COMMENT '站内dgmv', 
  `k_live_dgmv` double COMMENT 'k播dgmv', 
  `b_live_dgmv` double COMMENT '店播dgmv', 
  `note_dgmv` double COMMENT '笔记dgmv', 
  
  `ads_deal_gmv` double COMMENT '广告引导dealgmv-交易电商口径', 
  offline_spu_name string COMMENT '线下spu名称', 
  offline_brand_name  string COMMENT '线下spu名称', 
  is_mapping_spu  string COMMENT '线下spu名称', 
  is_mapping_brand  string COMMENT '线下spu名称', 
category  string COMMENT '线下spu名称', 
 segment  string COMMENT '线下spu名称', 
 sub_segment  string COMMENT '线下spu名称', 
 element  string COMMENT '线下spu名称', 
 benefit  string COMMENT '线下spu名称', 
 sale  bigint COMMENT '线下spu名称', 
 unit  bigint COMMENT '线下spu名称')
COMMENT '行业-广告*交易打通-商业化spu粒度广告和交易指标宽表'
PARTITIONED BY ( 
  `dtm` string COMMENT '-')

