drop table if exists temp.temp_app_ads_spu_metrics_detail_df360_{{ds_nodash}};
create table temp.temp_app_ads_spu_metrics_detail_df360_{{ds_nodash}}
select date_key,
  spu_id ,
  spu_name ,
  commercial_code ,
  commercial_name ,
  commercial_code1 ,
  commercial_taxonomy_name1 ,
  commercial_code2 ,
  commercial_taxonomy_name2 ,
  commercial_code3 ,
  commercial_taxonomy_name3 ,
  commercial_code4 ,
  commercial_taxonomy_name4,
  gmv as gmv,
  (gmv+1)/(gmv_last_30d+1)-1 as gmv_30diffratio,
  soc_read_feed_num as note_read_num,
  (soc_read_feed_num+1)/(soc_read_feed_num_last_30d+1)-1 as note_read_num_30diffratio,
  query_cnt  as search_num,
  (query_cnt+1)/(query_cnt_last_30d+1)-1 as search_num_30diffratio,
  (total_commercial_soc_read_feed_num+1)/(total_commercial_soc_read_feed_num_last_30d+1)-1 as commercial_read_num_30diffratio,
  (total_commercial_query_cnt+1)/(total_commercial_query_cnt_last_30d+1)-1 as commercial_search_num_30diffratio,
  nps as nps,
  aips_user_num as aips_crowd_level,
  (aips_user_num+1)/(aips_user_num_last_30d+1)-1 as aips_crowd_level_30diffratio,
  rtb_click_cnt/rtb_imp_cnt as ctr,
  rtb_income_amt/rtb_click_cnt as cpc,
  taobao_ad_third_active_user_num/taobao_ad_click_user_num as red_star_cvr,
  taobao_ad_ads_income_amt/taobao_ad_third_active_user_num as red_star_cpuv,
  chengben as i_ti_cost
from 
(SELECT date_key,
  spu_id ,
  spu_name ,
  commercial_code ,
  commercial_name ,
  commercial_code1 ,
  commercial_taxonomy_name1 ,
  commercial_code2 ,
  commercial_taxonomy_name2 ,
  commercial_code3 ,
  commercial_taxonomy_name3 ,
  commercial_code4 ,
  commercial_taxonomy_name4,
  SUM(((((coalesce(taobao_rgmv, 0) + coalesce(tianmao_rgmv, 0)) + coalesce(jd_rgmv, 0)) + coalesce(dou_rgmv, 0)) + coalesce(dgmv, 0))) AS gmv,
  SUM(((((coalesce(taobao_rgmv_last_30d, 0) + coalesce(tianmao_rgmv_last_30d, 0)) + coalesce(jd_rgmv_last_30d, 0)) + coalesce(dou_rgmv_last_30d, 0)) + coalesce(dgmv_last_30d, 0))) AS gmv_last_30d,
  SUM(soc_read_feed_num) AS soc_read_feed_num,
  SUM(soc_read_feed_num_last_30d) AS soc_read_feed_num_last_30d,
  SUM(query_cnt) AS query_cnt,
  SUM(query_cnt_last_30d) AS query_cnt_last_30d,
  SUM(total_commercial_soc_read_feed_num) AS total_commercial_soc_read_feed_num,
  SUM(total_commercial_soc_read_feed_num_last_30d) AS total_commercial_soc_read_feed_num_last_30d,
  SUM(total_commercial_query_cnt) AS total_commercial_query_cnt,
  SUM(total_commercial_query_cnt_last_30d) AS total_commercial_query_cnt_last_30d,
  SUM((((coalesce(a_user_num, 0) + coalesce(i_user_num, 0)) + coalesce(p_user_num, 0)) + coalesce(ti_user_num, 0))) AS aips_user_num,
  SUM((((coalesce(a_user_num_last_30d, 0) + coalesce(i_user_num_last_30d, 0)) + coalesce(p_user_num_last_30d, 0)) + coalesce(ti_user_num_last_30d, 0))) AS aips_user_num_last_30d,
  SUM(rtb_click_cnt) AS rtb_click_cnt,
  SUM(rtb_imp_cnt) AS rtb_imp_cnt,
  SUM(rtb_income_amt) AS rtb_income_amt,
  SUM(taobao_ad_third_active_user_num) AS taobao_ad_third_active_user_num,
  SUM(taobao_ad_click_user_num) AS taobao_ad_click_user_num,
  SUM(taobao_ad_ads_income_amt) AS taobao_ad_ads_income_amt,
  SUM((rtb_income_amt / new_ads_i_ti_user_num)) AS chengben,
  MAX(nps) AS nps
FROM redapp.app_ads_spu_metrics_detail_df
WHERE dtm = '{{ds_nodash}}'
GROUP BY date_key,
  spu_id,
  spu_name,
  commercial_code,
  commercial_name,
  commercial_code1,
  commercial_taxonomy_name1,
  commercial_code2,
  commercial_taxonomy_name2,
  commercial_code3,
  commercial_taxonomy_name3,
  commercial_code4,
  commercial_taxonomy_name4
  )base
;





select spu_id, spu_name, commercial_taxonomy_name1, integration_score, rank, 
    -- 指标详情
    soc_read_feed_num, dianosis_info_1, commercial_query_cnt, dianosis_info_2,
    -- 标记类目等级，方便业务做筛选
    1 as taxonomy_level
from
(
  select commercial_taxonomy_name1, spu_id, spu_name, soc_read_feed_num, dianosis_info_1, commercial_query_cnt, dianosis_info_2, integration_score, 
    row_number() over(partition by commercial_taxonomy_name1 order by integration_score desc) as rank
  from
  (
    select 
      spu_id, 
      spu_name,
      spu.commercial_taxonomy_name1,
      -- 指标的详情和对应的排名分位
      soc_read_feed_num,
      case
        when soc_read_feed_num > note_read_num_percent[5] then 'top1'
        when soc_read_feed_num > note_read_num_percent[4] then 'top1-5'
        when soc_read_feed_num > note_read_num_percent[3] then 'top5-10'
        when soc_read_feed_num > note_read_num_percent[2] then 'top10-20'
        when soc_read_feed_num > note_read_num_percent[1] then 'top20-50'
        else ''
      end as dianosis_info_1,
      commercial_query_cnt,
      case
        when commercial_query_cnt > commercial_query_cnt_percent[5] then 'top1'
        when commercial_query_cnt > commercial_query_cnt_percent[4] then 'top1-5'
        when commercial_query_cnt > commercial_query_cnt_percent[3] then 'top5-10'
        when commercial_query_cnt > commercial_query_cnt_percent[2] then 'top10-20'
        when commercial_query_cnt > commercial_query_cnt_percent[1] then 'top20-50'
        else ''
      end as dianosis_info_2,
      -- 这里就是完整的得分计算公式了，扩展指标数就行
      (
      search_num_ratio * 
        case
          when commercial_query_cnt > commercial_query_cnt_percent[5] then 100
          when commercial_query_cnt_percent[5] - commercial_query_cnt_percent[0] = 0 then 0
          when commercial_query_cnt < commercial_query_cnt_percent[0] then 0
          else commercial_query_cnt - commercial_query_cnt_percent[0] * 100 / commercial_query_cnt_percent[5] - commercial_query_cnt_percent[0]
        end
      +
      note_read_num_ratio * 
        case
          when soc_read_feed_num > note_read_num_percent[5] then 100
          when note_read_num_percent[5] - note_read_num_percent[0] = 0 then 0
          when soc_read_feed_num < note_read_num_percent[0] then 0
          else soc_read_feed_num - note_read_num_percent[0] * 100 / note_read_num_percent[5] - note_read_num_percent[0]
        end
       -- +
        -- GMV增速：建议计算公式的分子分母统一都+1以便可计算>=2的统一都直接赋值=100，剩下的再进行指数化计算
      ) as integration_score


select
  (select date_key,
        spu_id,
        spu_name,
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
        gmv,
        gmv_30diffratio,
        note_read_num,
        note_read_num_30diffratio,
        search_num,
        search_num_30diffratio,
        commercial_read_num_30diffratio,
        commercial_search_num_30diffratio,
        nps,
        aips_crowd_level,
        aips_crowd_level_30diffratio,
        ctr,
        cpc,
        red_star_cvr,
        red_star_cpuv,
        i_ti_cost,
  gmv_ratio * 
  case
    when gmv > gmv_percent[5] then 100
    when gmv_percent[5] - gmv_percent[0] = 0 then 0
    when gmv < gmv_percent[0] then 0
    else (gmv - gmv_percent[0]) * 100 / (gmv_percent[5] - gmv_percent[0])
  end as final_gmv,

gmv_30diffratio_ratio * 
  case
    when gmv_30diffratio > gmv_30diffratio_percent[5] then 100
    when gmv_30diffratio_percent[5] - gmv_30diffratio_percent[0] = 0 then 0
    when gmv_30diffratio < gmv_30diffratio_percent[0] then 0
    else (gmv_30diffratio - gmv_30diffratio_percent[0]) * 100 / (gmv_30diffratio_percent[5] - gmv_30diffratio_percent[0])
  end as final_gmv_30diffratio,

note_read_num_ratio * 
  case
    when note_read_num > note_read_num_percent[5] then 100
    when note_read_num_percent[5] - note_read_num_percent[0] = 0 then 0
    when note_read_num < note_read_num_percent[0] then 0
    else (note_read_num - note_read_num_percent[0]) * 100 / (note_read_num_percent[5] - note_read_num_percent[0])
  end as final_note_read_num,

note_read_num_30diffratio_ratio * 
  case
    when note_read_num_30diffratio > note_read_num_30diffratio_percent[5] then 100
    when note_read_num_30diffratio_percent[5] - note_read_num_30diffratio_percent[0] = 0 then 0
    when note_read_num_30diffratio < note_read_num_30diffratio_percent[0] then 0
    else (note_read_num_30diffratio - note_read_num_30diffratio_percent[0]) * 100 / (note_read_num_30diffratio_percent[5] - note_read_num_30diffratio_percent[0])
  end as final_note_read_num_30diffratio,

search_num_ratio * 
  case
    when search_num > search_num_percent[5] then 100
    when search_num_percent[5] - search_num_percent[0] = 0 then 0
    when search_num < search_num_percent[0] then 0
    else (search_num - search_num_percent[0]) * 100 / (search_num_percent[5] - search_num_percent[0])
  end as final_search_num,

search_num_30diffratio_ratio * 
  case
    when search_num_30diffratio > search_num_30diffratio_percent[5] then 100
    when search_num_30diffratio_percent[5] - search_num_30diffratio_percent[0] = 0 then 0
    when search_num_30diffratio < search_num_30diffratio_percent[0] then 0
    else (search_num_30diffratio - search_num_30diffratio_percent[0]) * 100 / (search_num_30diffratio_percent[5] - search_num_30diffratio_percent[0])
  end as final_search_num_30diffratio,

commercial_read_num_30diffratio_ratio * 
  case
    when commercial_read_num_30diffratio > commercial_read_num_30diffratio_percent[5] then 100
    when commercial_read_num_30diffratio_percent[5] - commercial_read_num_30diffratio_percent[0] = 0 then 0
    when commercial_read_num_30diffratio < commercial_read_num_30diffratio_percent[0] then 0
    else (commercial_read_num_30diffratio - commercial_read_num_30diffratio_percent[0]) * 100 / (commercial_read_num_30diffratio_percent[5] - commercial_read_num_30diffratio_percent[0])
  end as final_commercial_read_num_30diffratio,

commercial_search_num_30diffratio_ratio * 
  case
    when commercial_search_num_30diffratio > commercial_search_num_30diffratio_percent[5] then 100
    when commercial_search_num_30diffratio_percent[5] - commercial_search_num_30diffratio_percent[0] = 0 then 0
    when commercial_search_num_30diffratio < commercial_search_num_30diffratio_percent[0] then 0
    else (commercial_search_num_30diffratio - commercial_search_num_30diffratio_percent[0]) * 100 / (commercial_search_num_30diffratio_percent[5] - commercial_search_num_30diffratio_percent[0])
  end as final_commercial_search_num_30diffratio,

nps_ratio * 
  case
    when nps > nps_percent[5] then 100
    when nps_percent[5] - nps_percent[0] = 0 then 0
    when nps < nps_percent[0] then 0
    else (nps - nps_percent[0]) * 100 / (nps_percent[5] - nps_percent[0])
  end as final_nps,

aips_crowd_level_ratio * 
  case
    when aips_crowd_level > aips_crowd_level_percent[5] then 100
    when aips_crowd_level_percent[5] - aips_crowd_level_percent[0] = 0 then 0
    when aips_crowd_level < aips_crowd_level_percent[0] then 0
    else (aips_crowd_level - aips_crowd_level_percent[0]) * 100 / (aips_crowd_level_percent[5] - aips_crowd_level_percent[0])
  end as final_aips_crowd_level,

aips_crowd_level_30diffratio_ratio * 
  case
    when aips_crowd_level_30diffratio > aips_crowd_level_30diffratio_percent[5] then 100
    when aips_crowd_level_30diffratio_percent[5] - aips_crowd_level_30diffratio_percent[0] = 0 then 0
    when aips_crowd_level_30diffratio < aips_crowd_level_30diffratio_percent[0] then 0
    else (aips_crowd_level_30diffratio - aips_crowd_level_30diffratio_percent[0]) * 100 / (aips_crowd_level_30diffratio_percent[5] - aips_crowd_level_30diffratio_percent[0])
  end as final_aips_crowd_level_30diffratio,

ctr_ratio * 
  case
    when ctr > ctr_percent[5] then 100
    when ctr_percent[5] - ctr_percent[0] = 0 then 0
    when ctr < ctr_percent[0] then 0
    else (ctr - ctr_percent[0]) * 100 / (ctr_percent[5] - ctr_percent[0])
  end as final_ctr,

cpc_ratio * 
  case
    when cpc > cpc_percent[5] then 100
    when cpc_percent[5] - cpc_percent[0] = 0 then 0
    when cpc < cpc_percent[0] then 0
    else (cpc - cpc_percent[0]) * 100 / (cpc_percent[5] - cpc_percent[0])
  end as final_cpc,

red_star_cvr_ratio * 
  case
    when red_star_cvr > red_star_cvr_percent[5] then 100
    when red_star_cvr_percent[5] - red_star_cvr_percent[0] = 0 then 0
    when red_star_cvr < red_star_cvr_percent[0] then 0
    else (red_star_cvr - red_star_cvr_percent[0]) * 100 / (red_star_cvr_percent[5] - red_star_cvr_percent[0])
  end as final_red_star_cvr,

red_star_cpuv_ratio * 
  case
    when red_star_cpuv > red_star_cpuv_percent[5] then 100
    when red_star_cpuv_percent[5] - red_star_cpuv_percent[0] = 0 then 0
    when red_star_cpuv < red_star_cpuv_percent[0] then 0
    else (red_star_cpuv - red_star_cpuv_percent[0]) * 100 / (red_star_cpuv_percent[5] - red_star_cpuv_percent[0])
  end as final_red_star_cpuv,

i_ti_cost_ratio * 
  case
    when i_ti_cost > i_ti_cost_percent[5] then 100
    when i_ti_cost_percent[5] - i_ti_cost_percent[0] = 0 then 0
    when i_ti_cost < i_ti_cost_percent[0] then 0
    else (i_ti_cost - i_ti_cost_percent[0]) * 100 / (i_ti_cost_percent[5] - i_ti_cost_percent[0])
  end as final_i_ti_cost


    from
    (
      SELECT
        date_key,
        spu_id,
        spu_name,
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
        gmv,
        gmv_30diffratio,
        note_read_num,
        note_read_num_30diffratio,
        search_num,
        search_num_30diffratio,
        commercial_read_num_30diffratio,
        commercial_search_num_30diffratio,
        nps,
        aips_crowd_level,
        aips_crowd_level_30diffratio,
        ctr,
        cpc,
        red_star_cvr,
        red_star_cpuv,
        i_ti_cost
      from
        temp.temp_app_ads_spu_metrics_detail_df360_{{ds_nodash}}
    ) spu
    join
    (
        select date_key,
          commercial_taxonomy_name1,
          commercial_taxonomy_name2,
          commercial_taxonomy_name3,
          commercial_taxonomy_name4,
          commercial_level,
          gmv_percent,
          gmv_30diffratio_percent,
          note_read_num_percent,
          note_read_num_30diffratio_percent,
          search_num_percent,
          search_num_30diffratio_percent,
          commercial_read_num_30diffratio_percent,
          commercial_search_num_30diffratio_percent,
          nps_percent,
          aips_crowd_level_percent,
          aips_crowd_level_30diffratio_percent,
          ctr_percent,
          cpc_percent,
          red_star_cvr_percent,
          red_star_cpuv_percent,
          i_ti_cost_percent

        from
        (
        -- 计算每个指标的分位数，扩展到16个
        select grouping__id as commercial_level,
            date_key,
            coalesce(commercial_taxonomy_name1, 'all') as commercial_taxonomy_name1,
            coalesce(commercial_taxonomy_name2, 'all') as commercial_taxonomy_name2,
            coalesce(commercial_taxonomy_name3, 'all') as commercial_taxonomy_name3,
            coalesce(commercial_taxonomy_name4, 'all') as commercial_taxonomy_name4,
            percentile_approx(gmv, array(0.05, 0.5, 0.8, 0.9, 0.95, 0.99)) as gmv_percent,
            percentile_approx(gmv_30diffratio, array(0.05, 0.5, 0.8, 0.9, 0.95, 0.99)) as gmv_30diffratio_percent,
            percentile_approx(note_read_num, array(0.05, 0.5, 0.8, 0.9, 0.95, 0.99)) as note_read_num_percent,
            percentile_approx(note_read_num_30diffratio, array(0.05, 0.5, 0.8, 0.9, 0.95, 0.99)) as note_read_num_30diffratio_percent,
            percentile_approx(search_num, array(0.05, 0.5, 0.8, 0.9, 0.95, 0.99)) as search_num_percent,
            percentile_approx(search_num_30diffratio, array(0.05, 0.5, 0.8, 0.9, 0.95, 0.99)) as search_num_30diffratio_percent,
            percentile_approx(commercial_read_num_30diffratio, array(0.05, 0.5, 0.8, 0.9, 0.95, 0.99)) as commercial_read_num_30diffratio_percent,
            percentile_approx(commercial_search_num_30diffratio, array(0.05, 0.5, 0.8, 0.9, 0.95, 0.99)) as commercial_search_num_30diffratio_percent,
            percentile_approx(nps, array(0.05, 0.5, 0.8, 0.9, 0.95, 0.99)) as nps_percent,
            percentile_approx(aips_crowd_level, array(0.05, 0.5, 0.8, 0.9, 0.95, 0.99)) as aips_crowd_level_percent,
            percentile_approx(aips_crowd_level_30diffratio, array(0.05, 0.5, 0.8, 0.9, 0.95, 0.99)) as aips_crowd_level_30diffratio_percent,
            percentile_approx(ctr, array(0.05, 0.5, 0.8, 0.9, 0.95, 0.99)) as ctr_percent,
            percentile_approx(cpc, array(0.05, 0.5, 0.8, 0.9, 0.95, 0.99)) as cpc_percent,
            percentile_approx(red_star_cvr, array(0.05, 0.5, 0.8, 0.9, 0.95, 0.99)) as red_star_cvr_percent,
            percentile_approx(red_star_cpuv, array(0.05, 0.5, 0.8, 0.9, 0.95, 0.99)) as red_star_cpuv_percent,
            percentile_approx(i_ti_cost, array(0.05, 0.5, 0.8, 0.9, 0.95, 0.99)) as i_ti_cost_percent

        from
         temp.temp_app_ads_spu_metrics_detail_df360_{{ds_nodash}}
       
        group by date_key,
            commercial_taxonomy_name1,
            commercial_taxonomy_name2,
            commercial_taxonomy_name3,
            commercial_taxonomy_name4
        grouping sets(
           (date_key,commercial_taxonomy_name1) -- 行业整体
          ,(date_key,commercial_taxonomy_name2) -- 分行业
          ,(date_key,commercial_taxonomy_name3) -- 分行业、赛道
          ,(date_key,commercial_taxonomy_name4) -- 分行业、赛道
        )
      ) tax_percent
      join
      (
        -- 不同一级类目下，指标的系数表，后端每天从mysql导出到hive
        -- {"GMV":0.0625,"GMV_30diffRatio":0.0625,"note_read_num":0.0625,"note_read_num_30diffRatio":0.0625,"search_num":0.0625,"search_num_30diffRatio":0.0625,"commercial_read_num_30diffRatio":0.0625,"commercial_search_num_30diffRatio":0.0625,"NPS":0.0625,"AIPS_crowd_level":0.0625,"AIPS_crowd_level_30diffRatio":0.0625,"CTR":0.0625,"CPC":0.0625,"red_star_CVR":0.0625,"red_star_CPUV":0.0625,"i_Ti_cost":0.0625}
        select taxonomy1, 
          config_json,
         get_json_object(config_json, '$.GMV') as gmv_ratio,
        get_json_object(config_json, '$.GMV_30diffRatio') as gmv_30diffratio_ratio,
        get_json_object(config_json, '$.note_read_num') as note_read_num_ratio,
        get_json_object(config_json, '$.note_read_num_30diffRatio') as note_read_num_30diffratio_ratio,
        get_json_object(config_json, '$.search_num') as search_num_ratio,
        get_json_object(config_json, '$.search_num_30diffRatio') as search_num_30diffratio_ratio,
        get_json_object(config_json, '$.commercial_read_num_30diffRatio') as commercial_read_num_30diffratio_ratio,
        get_json_object(config_json, '$.commercial_search_num_30diffRatio') as commercial_search_num_30diffratio_ratio,
        get_json_object(config_json, '$.NPS') as nps_ratio,
        get_json_object(config_json, '$.AIPS_crowd_level') as aips_crowd_level_ratio,
        get_json_object(config_json, '$.AIPS_crowd_level_30diffRatio') as aips_crowd_level_30diffratio_ratio,
        get_json_object(config_json, '$.CTR') as ctr_ratio,
        get_json_object(config_json, '$.CPC') as cpc_ratio,
        get_json_object(config_json, '$.red_star_CVR') as red_star_cvr_ratio,
        get_json_object(config_json, '$.red_star_CPUV') as red_star_cpuv_ratio,
        get_json_object(config_json, '$.i_Ti_cost') as i_ti_cost_ratio

          -- 扩展16个
        from redods.activity_analyser_business_zhongcao_config_df
        where dtm = '{{ds_nodash}}'
      ) tax_index
      on tax_percent.commercial_taxonomy_name1 = tax_index.taxonomy1
    ) tax
    on spu.commercial_taxonomy_name1 = tax.commercial_taxonomy_name1
    and spu.date_key = tax.date_key
  ) spu_socre
) t
where rank <= 100
 
