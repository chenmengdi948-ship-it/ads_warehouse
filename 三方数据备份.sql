select 
   a.date_key
  ,a.note_id
  ,a.third_platform
  ,a.report_brand_user_id
  ,a.report_brand_user_name
  ,a.trans_ratio
  ,a.company_code
  ,a.company_name
  ,a.spu_id
  ,a.spu_layer
  ,a.direct_sales_dept1_name
  ,a.direct_sales_dept2_name
  ,a.direct_sales_dept3_name
  ,a.direct_sales_dept4_name
  ,a.direct_sales_dept5_name
  ,a.direct_sales_dept6_name
  ,a.direct_sales_name
  ,a.operator_name
  ,a.track_detail_name
  ,a.first_track_industry_dept_name
  ,a.second_track_industry_dept_name 
  ,a.track_industry_dept_group_name
  ,a.first_industry_name
  ,a.second_industry_name
  ,
    case
    when a.first_track_industry_dept_name in ('美妆','服饰潮流','奢品','美护') then '美奢服行业'
    when a.first_track_industry_dept_name in ('母婴','大健康','食品饮料','宠物','日百') then '快消行业'
    when a.first_track_industry_dept_name in ('3C家电','家居','家居家装','房地产') then '耐消行业'
    when a.first_track_industry_dept_name in ('互联网') then '互联网行业'
    when a.first_track_industry_dept_name in ('交通出行') then '交通出行行业'
    
    when a.first_track_industry_dept_name in ( '生活服务','行业团队其他','生态客户业务部','创作者商业化部','自闭环及其他') then first_track_industry_dept_name 
    else '其他' end as industry_group
  ,a.ads_income_amt
  ,a.bcoo_total_amt
  ,a.total_amt
  ,if((date_format(a.date_key, '%Y-%m-%d') between date_format(a.brief_begin_time, '%Y-%m-%d') and date_format(a.brief_end_time, '%Y-%m-%d') and a.third_platform in ('JINGDONG')) or (date_format(a.date_key, '%Y-%m-%d') between date_format(a.brief_start_time, '%Y-%m-%d') and date_format(a.brief_end_time, '%Y-%m-%d') and a.third_platform in ('TAOBAO')), max(a.bcoo_total_avg_amt) over(partition by a.note_id), 0) as bcoo_total_avg_amt
  ,a.ads_cash_income_amt
  ,a.ads_rtb_cash_income_amt
  ,a.ads_rtb_search_cash_income_amt
  ,a.ads_rtb_feed_cash_income_amt
  ,a.ads_rtb_video_feed_cash_income_amt
  ,a.ads_rtb_ecm_close_cash_income_amt
  ,a.ads_rtb_clue_cash_income_amt
  ,a.ads_rtb_interest_cash_income_amt
  ,a.ads_brand_cash_income_amt
  ,a.ads_chips_cash_income_amt
  ,a.total_imp_user_num
  ,a.ads_imp_user_num
  ,a.total_double_row_note_imp_user_num
  ,a.ads_double_row_note_imp_user_num
  ,a.total_double_row_note_click_user_num
  ,a.ads_double_row_note_click_user_num
  ,a.total_read_user_num
  ,a.total_click_user_num
  ,a.ads_click_user_num
  ,a.ads_rtb_search_click_user_num
  ,a.ads_rtb_feed_click_user_num
  ,a.ads_rtb_video_feed_click_user_num
  ,a.ads_third_active_user_num_15d / (cast(a.trans_ratio as int) / 100) as ads_third_active_user_num_15d
  ,a.ads_rtb_search_third_active_user_num_15d / (cast(a.trans_ratio as int) / 100) as ads_rtb_search_third_active_user_num_15d
  ,a.ads_rtb_feed_third_active_user_num_15d / (cast(a.trans_ratio as int) / 100) as ads_rtb_feed_third_active_user_num_15d
  ,a.ads_rtb_video_feed_third_active_user_num_15d / (cast(a.trans_ratio as int) / 100) as ads_rtb_video_feed_third_active_user_num_15d

  ,a.total_third_active_user_num_15d / (cast(a.trans_ratio as int) / 100) as total_third_active_user_num_15d
  ,a.ads_third_active_user_num_30d / (cast(a.trans_ratio as int) / 100) as ads_third_active_user_num_30d
  ,a.ads_rtb_search_third_active_user_num_30d / (cast(a.trans_ratio as int) / 100) as ads_rtb_search_third_active_user_num_30d
  ,a.ads_rtb_feed_third_active_user_num_30d / (cast(a.trans_ratio as int) / 100) as ads_rtb_feed_third_active_user_num_30d
  ,a.ads_rtb_video_feed_third_active_user_num_30d / (cast(a.trans_ratio as int) / 100) as ads_rtb_video_feed_third_active_user_num_30d

  ,a.total_third_active_user_num_30d / (cast(a.trans_ratio as int) / 100) as total_third_active_user_num_30d
  ,a.dtm
  ,  case when a.direct_sales_dept3_name in ('美奢潮流服饰行业','生活服务行业','生态中台部') then  a.direct_sales_dept5_name when  a.direct_sales_dept2_name ='行业团队' then  
  a.direct_sales_dept4_name
    else '其他' end as sales_dept
  ,b.brand_group_tag_name AS brand_group_tag_name
  ,b.is_core as is_core
  -- ,case when c.brand_account_id is not null then 1 else 0 end as is_old

  -- ,if(coalesce(d.brand_account_id, '') <> '', 1, 0) as is_23_brand_account_id
  ,if(date_key between '2023-01-01' and '2023-12-31' and  bcoo_total_amt > 0, 1, 0) as is_23_brand_account_id
  ,'1' as data_type
from bi_ads.app_ads_bcoo_third_note_attribution_metrics_1d_df_view a 
LEFT JOIN 
(
select
 brand_account_id
,brand_group_tag_name
,case when( (first_track_industry_dept_name ='美护'
and brand_group_tag_name  in 
('HBN','梦尔达','联合利华','欧莱雅集团','郑州永速集团','怀素集团','上海家化','水羊','永辉集团','宝洁','环亚集团','雅诗兰黛集团','资生堂','AMIRO 觅光','逸仙集团','欧诗漫','LVMH集团','乐金生活','Sisley法国希思黎','华熙生物','爱茉莉集团','贝泰妮集团','珀莱雅集团','汉高集团','玫瑰是玫瑰','拜尔斯道夫集团','由莱科技','皮尔法伯','高浪集团','伽蓝集团','丸美集团','山东福瑞达','LaboratoriosVinas 集团','溪木源','RED CHAMBER','宜格集团','强生','碧捷集团','HomeFacialPro','谷雨','雅顿集团','巨子生物','欧舒丹','PMPM','毛戈平MGP','丝塔芙','香奈儿CHANEL','若也','娇韵诗','AFU阿芙')
)or 
(first_track_industry_dept_name ='服饰潮流'
and brand_group_tag_name  in 
('小野和子','安踏','伊澈内衣','茉寻','特步','爱慕','幸棉','Victoria\'s Secret维多利亚的秘密','森马','绫致','慕裁','骆驼','快尚时装','李宁','Sweaty Betty Official','蕉下','轻奢女皇','PUMA','赫基集团','亚瑟士','汇洁集团','波司登','江南布衣','哥伦比亚','伯希和','汇美时尚','LOLA ROSE','有棵树','太平鸟','斯凯奇','雅瑞光学','enjoy it','百丽','觅橘','法趣服饰','阿迪达斯','HOKA','海澜之家','Inditex集团','威富','Ubras','lululemon','迅销集团','泰兰尼斯','361度','UGG','Under Armour','NewBalance','马克华菲','芬斯狄娜','ARSIS','MLB','七匹狼','蕉内','NIKE','奶糖派','匹克','雅戈尔','昂跑','sinsin','MAIA ACTIVE','歌莉娅','MOLYVIVI')
) or
(first_track_industry_dept_name ='奢品'
and brand_group_tag_name  in 
('FWRD','爱马仕','英奢','LVMH集团','卓翠坊翡翠','历峰集团','添锦','厂长手镯','开云集团','瑞表集团','潮宏基','蟋蟀珠宝','Longchamp','GRAFF','周大生 CHOW TAI SENG','Moncler盟可睐','锦美','周大福','PANDORA珠宝','FARFETCH发发奇','Tapestry','Chopard','PVH集团','娜玖','DR钻戒','HEFANG','周生生','白岚','梵誓ONESWEAR','MaxMara','香奈儿CHANEL','CAPRI集团','比斯特','Mytheresa','ARMANI阿玛尼','NET-A-PORTER颇特','Leysen莱绅通灵','戴比尔斯','六福珠宝','Prada','I-PRIMO','FERRAGAMO','TORY BURCH','TODS','BREITLING','VALENTINO','老庙黄金','柏丽德/APM','Burberry','CRD克徕帝')
)) then 1 else 0 end as is_core
,case 
when first_track_industry_dept_name in('美护') then
(case 
when  direct_sales_dept5_name in ('行业一部（美妆）') then '轻尘（美护）' 
-- when  direct_sales_dept5_name in ('行业二部（美妆）') then 'EL&海外小众（美妆）'
when  direct_sales_dept5_name in ('行业二部（美妆）') and direct_sales_dept6_name in ('美妆行业一组（二部）') then '柊镜（美护）'
when  direct_sales_dept5_name in ('行业二部（美妆）') and direct_sales_dept6_name in ('美妆行业二组（二部）') then '南烈（美护）'
when  direct_sales_dept5_name in ('行业三部（美妆）') then '金姆（美护）'
when  direct_sales_dept5_name in ('行业四部（美妆）') then '千月（美护）'
when  direct_sales_dept5_name in ('行业五部（美妆）') then '初景（美护）'
else '其他' end )
when first_track_industry_dept_name in('服饰潮流') then  
(case
when  direct_sales_dept5_name in ('行业一部（服饰潮流）') then '秀宁（服饰潮流）' 
when  direct_sales_dept5_name in ('行业二部（服饰潮流）') then '红心（服饰潮流）' 
when  direct_sales_dept5_name in ('行业三部（服饰潮流）') then '俊义（服饰潮流）' 
when  direct_sales_dept5_name in ('行业一部（奢品）') then '白水（服饰潮流）' 
when  direct_sales_dept5_name in ('行业二部（奢品）') then '若尘（服饰潮流）' 
else '其他' end )
when first_track_industry_dept_name in('奢品') then 
(case     
when  direct_sales_dept5_name in ('行业一部（奢品）') then '白水（奢品）'
when  direct_sales_dept5_name in ('行业二部（奢品）') then '若尘（奢品）'
when  direct_sales_dept5_name in ('行业一部（美妆）') then '轻尘（奢品）'
else '其他' end )
else direct_sales_dept5_name end  as direct_sales_dept5_name
from bi_ads.app_ads_insight_module_account_df
where dtm =(select max(dtm) from bi_ads.app_ads_insight_module_account_df)
and first_track_industry_dept_name in ('美护','服饰潮流','奢品')
group by 1,2,3,4
) b on a.report_brand_user_id=b.brand_account_id

-- left join 
-- (
-- select
--  brand_account_id
-- from bi_ads.dm_ads_industry_alliance_note_account_df
-- where dtm =(select max(dtm) from bi_ads.dm_ads_industry_alliance_note_account_df)
-- and date_key between '2023-01-01' and '2023-12-31'
-- and first_track_industry_dept_name in ('美护','服饰潮流','奢品')
-- and bcoo_income_amt>0 
-- and tag=1
-- group by 1
-- ) c  on a.report_brand_user_id=c.brand_account_id

-- left join 
-- (
-- select
--   brand_account_id
--  ,case 
--    when tag = 1 then 'TAOBAO' 
--    when tag = 2 then 'JINGDONG' 
--   end as third_platform
-- from bi_ads.dm_ads_industry_alliance_note_account_df
-- where dtm =(select max(dtm) from bi_ads.dm_ads_industry_alliance_note_account_df)
-- and date_key between '2023-01-01' and '2023-12-31'
-- and bcoo_income_amt>0 
-- and tag in (1, 2)
-- group by 1, 2
-- ) d  on a.report_brand_user_id = d.brand_account_id
-- and a.third_platform = d.third_platform


union all 


select 
   a.date_key
  ,a.note_id
  ,a.third_platform
  ,a.report_brand_user_id
  ,a.report_brand_user_name
  ,a.trans_ratio
  ,a.company_code
  ,a.company_name
  ,a.spu_id
  ,a.spu_layer
  ,a.direct_sales_dept1_name
  ,a.direct_sales_dept2_name
  ,a.direct_sales_dept3_name
  ,a.direct_sales_dept4_name
  ,a.direct_sales_dept5_name
  ,a.direct_sales_dept6_name
  ,a.direct_sales_name
  ,a.operator_name
  ,a.track_detail_name
  ,a.first_track_industry_dept_name
  ,a.second_track_industry_dept_name 
  ,'0' as track_industry_dept_group_name
  ,a.first_industry_name
  ,a.second_industry_name
  ,'0' as industry_group
  ,a.ads_income_amt
  ,a.bcoo_total_amt
  ,a.total_amt
  ,if((date_format(a.date_key, '%Y-%m-%d') between date_format(a.brief_begin_time, '%Y-%m-%d') and date_format(a.brief_end_time, '%Y-%m-%d') and a.third_platform in ('JINGDONG')) or (date_format(a.date_key, '%Y-%m-%d') between date_format(a.brief_start_time, '%Y-%m-%d') and date_format(a.brief_end_time, '%Y-%m-%d') and a.third_platform in ('TAOBAO')), max(a.bcoo_total_avg_amt) over(partition by a.note_id), 0) as bcoo_total_avg_amt
  ,a.ads_cash_income_amt
  ,a.ads_rtb_cash_income_amt
  ,a.ads_rtb_search_cash_income_amt
  ,a.ads_rtb_feed_cash_income_amt
  ,a.ads_rtb_video_feed_cash_income_amt
  ,a.ads_rtb_ecm_close_cash_income_amt
  ,a.ads_rtb_clue_cash_income_amt
  ,a.ads_rtb_interest_cash_income_amt
  ,a.ads_brand_cash_income_amt
  ,a.ads_chips_cash_income_amt
  ,a.total_imp_user_num
  ,a.ads_imp_user_num
  ,a.total_double_row_note_imp_user_num
  ,a.ads_double_row_note_imp_user_num
  ,a.total_double_row_note_click_user_num
  ,a.ads_double_row_note_click_user_num
  ,a.total_read_user_num
  ,a.total_click_user_num
  ,a.ads_click_user_num
  ,a.ads_rtb_search_click_user_num
  ,a.ads_rtb_feed_click_user_num
  ,a.ads_rtb_video_feed_click_user_num
  ,a.ads_third_active_user_num_15d / (cast(a.trans_ratio as int) / 100) as ads_third_active_user_num_15d
  ,a.ads_rtb_search_third_active_user_num_15d / (cast(a.trans_ratio as int) / 100) as ads_rtb_search_third_active_user_num_15d
  ,a.ads_rtb_feed_third_active_user_num_15d / (cast(a.trans_ratio as int) / 100) as ads_rtb_feed_third_active_user_num_15d
  ,a.ads_rtb_video_feed_third_active_user_num_15d / (cast(a.trans_ratio as int) / 100) as ads_rtb_video_feed_third_active_user_num_15d

  ,a.total_third_active_user_num_15d / (cast(a.trans_ratio as int) / 100) as total_third_active_user_num_15d
  ,a.ads_third_active_user_num_30d / (cast(a.trans_ratio as int) / 100) as ads_third_active_user_num_30d
  ,a.ads_rtb_search_third_active_user_num_30d / (cast(a.trans_ratio as int) / 100) as ads_rtb_search_third_active_user_num_30d
  ,a.ads_rtb_feed_third_active_user_num_30d / (cast(a.trans_ratio as int) / 100) as ads_rtb_feed_third_active_user_num_30d
  ,a.ads_rtb_video_feed_third_active_user_num_30d / (cast(a.trans_ratio as int) / 100) as ads_rtb_video_feed_third_active_user_num_30d

  ,a.total_third_active_user_num_30d / (cast(a.trans_ratio as int) / 100) as total_third_active_user_num_30d
  ,a.dtm
  ,  case when a.direct_sales_dept3_name in ('美奢潮流服饰行业','生活服务行业','生态中台部') then  a.direct_sales_dept5_name when  a.direct_sales_dept2_name ='行业团队' then  
  a.direct_sales_dept4_name
    else '其他' end as sales_dept
  ,b.brand_group_tag_name AS brand_group_tag_name
  ,b.is_core as is_core
  -- ,case when c.brand_account_id is not null then 1 else 0 end as is_old

  -- ,if(coalesce(d.brand_account_id, '') <> '', 1, 0) as is_23_brand_account_id
  ,if(date_key between '2023-01-01' and '2023-12-31' and  bcoo_total_amt>0, 1, 0) as is_23_brand_account_id
  ,'0' as data_type
from bi_ads.app_ads_bcoo_third_note_attribution_metrics_1d_df_view a 
LEFT JOIN 
(
select
 brand_account_id
,brand_group_tag_name
,case when( (first_track_industry_dept_name ='美护'
and brand_group_tag_name  in 
('HBN','梦尔达','联合利华','欧莱雅集团','郑州永速集团','怀素集团','上海家化','水羊','永辉集团','宝洁','环亚集团','雅诗兰黛集团','资生堂','AMIRO 觅光','逸仙集团','欧诗漫','LVMH集团','乐金生活','Sisley法国希思黎','华熙生物','爱茉莉集团','贝泰妮集团','珀莱雅集团','汉高集团','玫瑰是玫瑰','拜尔斯道夫集团','由莱科技','皮尔法伯','高浪集团','伽蓝集团','丸美集团','山东福瑞达','LaboratoriosVinas 集团','溪木源','RED CHAMBER','宜格集团','强生','碧捷集团','HomeFacialPro','谷雨','雅顿集团','巨子生物','欧舒丹','PMPM','毛戈平MGP','丝塔芙','香奈儿CHANEL','若也','娇韵诗','AFU阿芙')
)or 
(first_track_industry_dept_name ='服饰潮流'
and brand_group_tag_name  in 
('小野和子','安踏','伊澈内衣','茉寻','特步','爱慕','幸棉','Victoria\'s Secret维多利亚的秘密','森马','绫致','慕裁','骆驼','快尚时装','李宁','Sweaty Betty Official','蕉下','轻奢女皇','PUMA','赫基集团','亚瑟士','汇洁集团','波司登','江南布衣','哥伦比亚','伯希和','汇美时尚','LOLA ROSE','有棵树','太平鸟','斯凯奇','雅瑞光学','enjoy it','百丽','觅橘','法趣服饰','阿迪达斯','HOKA','海澜之家','Inditex集团','威富','Ubras','lululemon','迅销集团','泰兰尼斯','361度','UGG','Under Armour','NewBalance','马克华菲','芬斯狄娜','ARSIS','MLB','七匹狼','蕉内','NIKE','奶糖派','匹克','雅戈尔','昂跑','sinsin','MAIA ACTIVE','歌莉娅','MOLYVIVI')
) or
(first_track_industry_dept_name ='奢品'
and brand_group_tag_name  in 
('FWRD','爱马仕','英奢','LVMH集团','卓翠坊翡翠','历峰集团','添锦','厂长手镯','开云集团','瑞表集团','潮宏基','蟋蟀珠宝','Longchamp','GRAFF','周大生 CHOW TAI SENG','Moncler盟可睐','锦美','周大福','PANDORA珠宝','FARFETCH发发奇','Tapestry','Chopard','PVH集团','娜玖','DR钻戒','HEFANG','周生生','白岚','梵誓ONESWEAR','MaxMara','香奈儿CHANEL','CAPRI集团','比斯特','Mytheresa','ARMANI阿玛尼','NET-A-PORTER颇特','Leysen莱绅通灵','戴比尔斯','六福珠宝','Prada','I-PRIMO','FERRAGAMO','TORY BURCH','TODS','BREITLING','VALENTINO','老庙黄金','柏丽德/APM','Burberry','CRD克徕帝')
)) then 1 else 0 end as is_core
,case 
when first_track_industry_dept_name in('美护') then
(case 
when  direct_sales_dept5_name in ('行业一部（美妆）') then '轻尘（美护）' 
-- when  direct_sales_dept5_name in ('行业二部（美妆）') then 'EL&海外小众（美妆）'
when  direct_sales_dept5_name in ('行业二部（美妆）') and direct_sales_dept6_name in ('美妆行业一组（二部）') then '柊镜（美护）'
when  direct_sales_dept5_name in ('行业二部（美妆）') and direct_sales_dept6_name in ('美妆行业二组（二部）') then '南烈（美护）'
when  direct_sales_dept5_name in ('行业三部（美妆）') then '金姆（美护）'
when  direct_sales_dept5_name in ('行业四部（美妆）') then '千月（美护）'
when  direct_sales_dept5_name in ('行业五部（美妆）') then '初景（美护）'
else '其他' end )
when first_track_industry_dept_name in('服饰潮流') then  
(case
when  direct_sales_dept5_name in ('行业一部（服饰潮流）') then '秀宁（服饰潮流）' 
when  direct_sales_dept5_name in ('行业二部（服饰潮流）') then '红心（服饰潮流）' 
when  direct_sales_dept5_name in ('行业三部（服饰潮流）') then '俊义（服饰潮流）' 
when  direct_sales_dept5_name in ('行业一部（奢品）') then '白水（服饰潮流）' 
when  direct_sales_dept5_name in ('行业二部（奢品）') then '若尘（服饰潮流）' 
else '其他' end )
when first_track_industry_dept_name in('奢品') then 
(case     
when  direct_sales_dept5_name in ('行业一部（奢品）') then '白水（奢品）'
when  direct_sales_dept5_name in ('行业二部（奢品）') then '若尘（奢品）'
when  direct_sales_dept5_name in ('行业一部（美妆）') then '轻尘（奢品）'
else '其他' end )
else direct_sales_dept5_name end  as direct_sales_dept5_name
from bi_ads.app_ads_insight_module_account_df
where dtm =(select max(dtm) from bi_ads.app_ads_insight_module_account_df)
and first_track_industry_dept_name in ('美护','服饰潮流','奢品')
group by 1,2,3,4
) b on a.report_brand_user_id=b.brand_account_id

-- left join 
-- (
-- select
--  brand_account_id
-- from bi_ads.dm_ads_industry_alliance_note_account_df
-- where dtm =(select max(dtm) from bi_ads.dm_ads_industry_alliance_note_account_df)
-- and date_key between '2023-01-01' and '2023-12-31'
-- and first_track_industry_dept_name in ('美护','服饰潮流','奢品')
-- and bcoo_income_amt>0 
-- and tag=1
-- group by 1
-- ) c  on a.report_brand_user_id=c.brand_account_id

-- left join 
-- (
-- select
--   brand_account_id
--  ,case 
--    when tag = 1 then 'TAOBAO' 
--    when tag = 2 then 'JINGDONG' 
--   end as third_platform
-- from bi_ads.dm_ads_industry_alliance_note_account_df
-- where dtm =(select max(dtm) from bi_ads.dm_ads_industry_alliance_note_account_df)
-- and date_key between '2023-01-01' and '2023-12-31'
-- and bcoo_income_amt>0 
-- and tag in (1, 2)
-- group by 1, 2
-- ) d  on a.report_brand_user_id = d.brand_account_id
-- and a.third_platform = d.third_platform


