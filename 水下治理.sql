SET "kyuubi.spark.option.--conf spark.sql.files.maxPartitionBytes=500m"; 
SET "kyuubi.spark.option.--conf spark.sql.iceberg.read.split.target-size=536870912";
SET "kyuubi.spark.option.--conf spark.driver.memory=10g;"

drop table if exists temp.temp_app_ads_industry_marketing_note_detail_td_df_{{ds_nodash}};
create table temp.temp_app_ads_industry_marketing_note_detail_td_df_{{ds_nodash}} as

select 
a.note_id,
author_user_id,note_url,note_ads_type,title,content,taxonomy1,taxonomy2,taxonomy3,
brand_account_id,track_industry_dept_group_name,create_time,is_goods_note,
biji_type,
seller_id,
is_material_note,
is_brand,
is_kos,
is_bind
from
(SELECT note_id
FROM 
(
SELECT  note_id,
        split(label_info ,':')[0] AS tag_id
FROM shequ_risk_data.dwd_process_center_note_tag_df a lateral view explode(label) AS label_info
WHERE dtm = '{{ds_nodash}}'
) tt2
where tag_id in (50000418,50000419,1015483,1015482,50002277,50002278,50002279,50002276,50002280,50002281,50002282,50002284,50002283)
group by 1
) a 
--笔记信息
left join
(SELECT 
    note_id,author_user_id,note_url,note_ads_type,title,content,taxonomy1,taxonomy2,taxonomy3,
    brand_account_id,track_industry_dept_group_name,create_time,is_goods_note,
    case  when note_ads_type in ('KOS授权笔记', 'KOS发布未授权笔记') then 'KOS笔记'
      when note_ads_type not in ('UGC笔记', 'CPS笔记') then note_ads_type else '其他笔记' end as biji_type,
      is_material_note,
      is_brand,
      is_kos,
      is_bind
  FROM redcdm.dim_ads_note_extend_df
  WHERE dtm = '{{ds_nodash}}'
  ) b1 on a.note_id = b1.note_id

--是否有电商seller_id
left join
(select user_id,
    max(seller_id) as seller_id
    from redapp.app_ads_ecm_seller_core_metrics_1d_di a 
    where dtm='{{ds_nodash}}'
    and state in  (100,200,300)
    group by 1) c on b1.author_user_id = c.user_id

where substr(create_time,1,10) between f_getdate('{{ds}}',-29) and '{{ds}}';


---剔除企业号、KOS、蒲公英、广告素材、开店商家笔记、商品笔记

drop table if exists temp.temp_app_ads_industry_marketing_note_detail_td_df_{{ds_nodash}}_clean;
create table temp.temp_app_ads_industry_marketing_note_detail_td_df_{{ds_nodash}}_clean as
select 
*
from temp.temp_app_ads_industry_marketing_note_detail_td_df_{{ds_nodash}}
where is_material_note<>1 and is_brand<>1 and is_kos<>1 and is_bind<>1 and is_goods_note<>'true'
and seller_id is null

;

---发布营销感笔记的账号
drop table if exists temp.temp_app_ads_industry_marketing_note_detail_td_df_{{ds_nodash}}_user;
create table temp.temp_app_ads_industry_marketing_note_detail_td_df_{{ds_nodash}}_user as
select 
author_user_id,
count(distinct note_id) as marketing_note_cnt
from temp.temp_app_ads_industry_marketing_note_detail_td_df_{{ds_nodash}}_clean
group by 1

;
---社区营销感用户模型：剔除企业号、员工号、开店商家

drop table if exists temp.zhudi_marketing_user;
create table temp.zhudi_marketing_user as
select 
a.user_id 
from
(select -----社区营销感用户模型识别
    content_id as user_id
from shequ_risk_data.dwd_process_center_tag_record_all_df
where dtm = '{{ds_nodash}}'
and content_type = 'USER'
and tag_id_list rlike '1035073|1035127' --标签组合
group by 1
) a 

left join
--账号信息
(select 
  user_id
  from redapp.app_ads_industry_msg_user_metrics_di
  where dtm ='{{ds_nodash}}'
  and user_type in ('1-企业号','2-kos账号')
  group by 1
  ) b on a.user_id = b.user_id

--是否有电商seller_id
left join
(select user_id,
    max(seller_id) as seller_id
    from redapp.app_ads_ecm_seller_core_metrics_1d_di a 
    where dtm='{{ds_nodash}}'
    and state in  (100,200,300)
    group by 1) c on a.user_id = c.user_id

where b.user_id is null and c.user_id is null

group by 1

;
--评论区引导or直接导流的

drop table if exists temp.temp_app_ads_industry_sxdl_detail_td_df_{{ds_nodash}};
create table temp.temp_app_ads_industry_sxdl_detail_td_df_{{ds_nodash}} as
select 
comment_id, cmt, diversion_score,
cmt_contact_type,
case 
when diversion_label in ('zhijiedaoliu') then '1-直接导流'
when diversion_label in ('yindaodaoliu') then '2-引导导流'
end as daoliu_cmt_type
from
(
select 
  all_values['commentId'] as comment_id, 
  all_values['content'] as cmt,
  all_values['commentContactV4Result'] as cmt_contact_type,
  get_json_object(get_json_object(all_values['commentADMarketingServerV1'], '$.labelResponse.labelInfos[0].info'), '$.diversion_label') as diversion_label,
  get_json_object(get_json_object(all_values['commentADMarketingServerV1'], '$.labelResponse.labelInfos[0].info'), '$.diversion_score') as diversion_score,
  get_json_object(all_values['commentADMarketingServerV1'], '$.label') as market_label,
  get_json_object(all_values['commentADMarketingServerV1'], '$.score') as market_score
--  final_result.handle_results as res
from shequ_risk_data.dwd_risk_soc_audit_hit_log_hi
where scenario_id = 'commentV2'
  and dtm between f_getdate('{{ds_nodash}}',-29) and '{{ds_nodash}}'
  and all_values['commentADMarketingServerV1'] is not null
)
where (diversion_label in ('yindaodaoliu','zhijiedaoliu') and  diversion_score>0.7 and  market_label<>'qiugou');


--添加评论发布者ID

drop table if exists temp.temp_app_ads_industry_sxdl_detail_td_df_{{ds_nodash}}_user;
create table temp.temp_app_ads_industry_sxdl_detail_td_df_{{ds_nodash}}_user as

select a.*,
comment_user_id,
discovery_id,
discovery_user_id
from temp.temp_app_ads_industry_sxdl_detail_td_df_{{ds_nodash}} a  
left join 
(select 
id as comment_id,
comment_user_id,
discovery_id,
discovery_user_id
from reddw.dw_soc_discovery_comment_detail_day
where dtm = '{{ds_nodash}}'
and enabled = true
group by 1,2,3,4) b on a.comment_id = b.comment_id 

where cmt not like '%xhslink%';



select 
user_id,
1 as is_sxdl,
0 as is_zydl,
0 as is_plwd,
0 as is_sxyx,
1 as is_sxjy,
0 as is_chuzhi,
0 as is_tibao,
0 as is_liuliang_tiaokong
from redapp.app_ads_industry_msg_user_metrics_di
where dtm = '{{ds_nodash}}'
and
(user_type in ('1-企业号','2-kos账号','3-导流伪素人号', '4-翘客导流号')
or user_level = '专业号-个人')
and send_msg_cnt_30d>0
union all 
select 
user_id,
0 as is_sxdl,
1 as is_zydl,
0 as is_plwd,
0 as is_sxyx,
1 as is_sxjy,
0 as is_chuzhi,
0 as is_tibao,
0 as is_liuliang_tiaokong
from 
(select user_id
from shequ_algo_basic.top_note_diversion_tag_record_di
where dtm between f_getdate('{{ds_nodash}}',-29) and '{{ds_nodash}}'
group by 1
union all 
select 
user_id
from shequ_algo_basic.screenshot_userprofile_vlm_pred_di
where dtm between f_getdate('{{ds_nodash}}',-29) and '{{ds_nodash}}'
and pred_label = '导流'
group by 1
)a 
group by 1
union all 
--评论区引导or直接导流的
--全部评论外导账号
select comment_user_id as user_id,
0 as is_sxdl,
0 as is_zydl,
1 as is_plwd,
0 as is_sxyx,
1 as is_sxjy,
0 as is_chuzhi,
0 as is_tibao,
0 as is_liuliang_tiaokong
from
(select 
comment_user_id,
count(distinct comment_id) as daoliu_cmt_cnt,
collect_set(cmt) as content_list
from temp.temp_app_ads_industry_sxdl_detail_td_df_{{ds_nodash}}_user
group by 1) a
where daoliu_cmt_cnt>=20
group by 1
-- 铺水下营销感笔记的账号全集：营销感笔记 + 营销感账号合并
union all 
select 
user_id ,
0 as is_sxdl,
0 as is_zydl,
0 as is_plwd,
1 as is_sxyx,
1 as is_sxjy,
0 as is_chuzhi,
0 as is_tibao,
0 as is_liuliang_tiaokong
from
(select 
author_user_id as user_id 
from temp.temp_app_ads_industry_marketing_note_detail_td_df_{{ds_nodash}}_user
where marketing_note_cnt>=10
group by 1
union all 
select 
user_id
from temp.zhudi_marketing_user
group by 1
) a 
group by 1

union all
--导流治理名单
 select 
  a.content_id as user_id,
  0 as is_sxdl,
  0 as is_zydl,
  0 as is_plwd,
  0 as is_sxyx,
  0 as is_sxjy,
  1 as is_chuzhi,
  0 as is_tibao,
  0 as is_liuliang_tiaokong,
  max(cast(update_time as string)) as chuzhi_update_time, --最近处置更新日期
  collect_set(action_name) as chuzhi_action_name_list --当前生效处置动作标签
  from
    (select
    content_id,
    processor,
    max(update_time) as update_time
    from shequ_risk_data.dwd_process_center_tag_record_all_df
    where dtm = '{{ds_nodash}}'
    and regexp_like(tag_id_list,'50001946')
    and substring(create_time,1,10) between f_getdate('{{ds}}',-6) and '{{ds}}'
    group by 1,2
    ) a
    left join
    (select
    content_id,
    processor,
    action_name
    from shequ_risk_data.dwd_process_center_action_status_info_all_df
  where dtm = '{{ds_nodash}}'
    ) b on a.processor = b.processor and a.content_id = b.content_id
group by 1

union all 
select pro_user_id as user_id,
0 as is_sxdl,
0 as is_zydl,
0 as is_plwd,
0 as is_sxyx,
0 as is_sxjy,
0 as is_chuzhi,
1 as is_tibao,
0 as is_liuliang_tiaokong,
null as chuzhi_update_time, --最近处置更新日期
null as chuzhi_action_name_list --当前生效处置动作标签
 from ads_algo_feed.leads_ad_ym_dc_seasub_flow_guide_202506_df
where dtm between f_getdate( '{{ds_nodash}}',-6) and '{{ds_nodash}}'
group by 1
union all 
select 
  pro_user_id as user_id,
  0 as is_sxdl,
0 as is_zydl,
0 as is_plwd,
0 as is_sxyx,
0 as is_sxjy,
0 as is_chuzhi,
0 as is_tibao,
1 as is_liuliang_tiaokong,
null as chuzhi_update_time, --最近处置更新日期
null as chuzhi_action_name_list --当前生效处置动作标签
from
  ads_algo_feed.ym_pro_user_id_exp_stg_0423_df
where
  dtm = max_dtm('ads_algo_feed.ym_pro_user_id_exp_stg_0423_df')