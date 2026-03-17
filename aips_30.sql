--回刷脚本，直接读取30天分区数据
-- insert overwrite table redapp.app_ads_spu_user_interest_30d_di partition(dtm, commercial_code1)
-- select
--   user_id,
--   spu_id,
--   commercial_taxonomy_name1,
--   commercial_code2,
--   commercial_taxonomy_name2,
--   commercial_code3,
--   commercial_taxonomy_name3,
--   commercial_code4,
--   commercial_taxonomy_name4,
--   interest_level,
--   interest_level_map,
--   dtm,
--   commercial_code1
-- from
--   (
--     select
--       coalesce(t1.user_id, t2.user_id) as user_id,
--       coalesce(t1.spu_id, t2.spu_id) as spu_id,
--       coalesce(t1.commercial_taxonomy_name1,t2.commercial_taxonomy_name1) as commercial_taxonomy_name1,
--       coalesce(t1.commercial_code2, t2.commercial_code2) as commercial_code2,
--       coalesce(t1.commercial_taxonomy_name2,t2.commercial_taxonomy_name2) as commercial_taxonomy_name2,
--       coalesce(t1.commercial_code3, t2.commercial_code3) as commercial_code3,
--       coalesce(t1.commercial_taxonomy_name3,t2.commercial_taxonomy_name3) as commercial_taxonomy_name3,
--       coalesce(t1.commercial_code4, t2.commercial_code4) as commercial_code4,
--       coalesce(t1.commercial_taxonomy_name4,t2.commercial_taxonomy_name4) as commercial_taxonomy_name4,
--       case
--         when coalesce(t2.S_date, '') != '' then 'S'
--         when coalesce(t1.P_date, '') != '' then 'P'
--         when coalesce(t1.TI_date, '') != '' then 'TI'
--         when coalesce(t1.I_date, '') != '' then 'I'
--         when coalesce(t1.A_date, '') != '' then 'A'
--       end as interest_level,
--       str_to_map(concat('A:',t1.A_date,'&I:',t1.I_date,'&TI:',t1.TI_date,'&P:',t1.P_date,'&S:',coalesce(t2.S_date, '')),'&') as interest_level_map,
--       '{{ds_nodash}}' as dtm,
--       coalesce(t1.commercial_code1, t2.commercial_code1) as commercial_code1
--     from
--       (
--         select  /*+ mapjoin(t2) */
--           t1.user_id,
--           t1.spu_id,
--           t1.commercial_code1,
--           t1.commercial_taxonomy_name1,
--           t1.commercial_code2,
--           t1.commercial_taxonomy_name2,
--           t1.commercial_code3,
--           t1.commercial_taxonomy_name3,
--           t1.commercial_code4,
--           t1.commercial_taxonomy_name4,
--           if(t1.A_date > f_getdate('{{ds_nodash}}', -30),t1.A_date,'') as A_date,
--           if(t1.I_date > f_getdate('{{ds_nodash}}', -30),t1.I_date,'') as I_date,
--           if(t1.P_date > f_getdate('{{ds_nodash}}', -30),t1.P_date,'') as P_date,
--           if(t1.TI_date > f_getdate('{{ds_nodash}}', -30),t1.TI_date,'') as TI_date
--         from
--           (
--             select  /*+ mapjoin(t2) */
--               detail.user_id,
--               detail.spu_id,
--               detail.A_date,
--               detail.I_date,
--               detail.P_date,
--               detail.TI_date,
--               --detail.interest_level,
--               t2.commercial_code1,
--               t2.commercial_taxonomy_name1,
--               t2.commercial_code2,
--               t2.commercial_taxonomy_name2,
--               t2.commercial_code3,
--               t2.commercial_taxonomy_name3,
--               t2.commercial_code4,
--               t2.commercial_taxonomy_name4
--             from
--               (
--               select
--                 a.user_id,
--                 a.spu_id,
--                 --interest_level
--                 coalesce(MAX(case when a.interest_level='A' THEN  a.dtm else null end),'') as A_date,
--                 coalesce(MAX(case when a.interest_level='I' THEN  a.dtm else null end),'') as I_date,
--                 coalesce(MAX(case when a.interest_level='P' THEN  a.dtm else null end),'') as P_date,
--                 coalesce(MAX(case when a.interest_level='TI' THEN  a.dtm else null end),'') as TI_date
--               from
--                 (select user_id,
--                   spu_id,
--                   interest_level,
--                   dtm
--                 from  redapp.app_ads_spu_user_interest_1d_di 
--                 where
--                   dtm <= '{{ds_nodash}}'
--                   and dtm > f_getdate('{{ds_nodash}}', -30) --最近30日增量分区读取
--                   and interest_level in ('A', 'I','TI', 'P')
--                 )a
--                 inner join
--                 (select user_id
--                 from   reddw.dw_user_seq_id_offline_day 
--                 where
--                   dtm = greatest('{{ds_nodash}}','20230505')
--                 )b
--                 on a.user_id = b.user_id
--               group by a.user_id,
--                 a.spu_id
--               )detail
--             left join
--               ads_databank.dim_spu_df t2
--             on detail.spu_id = t2.spu_id
--             where 
--               t2.dtm = greatest('{{ds_nodash}}','20230505')
--           ) t1
--           
--       ) t1 
--       full outer join 
--       (
--         select /*+ mapjoin(t2) */
--           t1.user_id,
--           t1.spu_id,
--           t1.S_date,
--           t2.commercial_code1,
--           t2.commercial_taxonomy_name1,
--           t2.commercial_code2,
--           t2.commercial_taxonomy_name2,
--           t2.commercial_code3,
--           t2.commercial_taxonomy_name3,
--           t2.commercial_code4,
--           t2.commercial_taxonomy_name4
--         from
--         (
--           select
--             a.author_id as user_id,
--             a.spu_id,
--             max(date_format(a.create_time, 'yyyyMMdd')) as S_date
--           from
--             ads_databank.dim_spu_note_df a
--           inner join
--             reddw.dw_user_seq_id_offline_day b
--           on a.author_id = b.user_id
          -- left join
          -- (
          -- select
          --   note_id
          -- from
          --   ads_databank.dim_comment_spu_note_attribute_selling_point_emotional_df 
          -- where
          --   dtm = '{{ds_nodash}}'
          --   and (coalesce(positive_sentiment_word_num, 0) + coalesce(negative_sentiment_word_num, 0) + coalesce(neutral_sentiment_word_num, 0)) > 0
          -- group by 1
          -- )c
          -- on a.note_id = c.note_id
--           where
--             a.dtm = greatest('{{ds_nodash}}', '20230505')
--             and ((coalesce(a.positive_sentiment_num, 0) + coalesce(a.negative_sentiment_num, 0) + coalesce(a.neutral_sentiment_num, 0)) > 0  or c.note_id is not null)
--             and date_format(a.create_time, 'yyyyMMdd') > f_getdate('{{ds_nodash}}', -365)
--             and date_format(a.create_time, 'yyyyMMdd') <= '{{ds_nodash}}'
--             and b.dtm = greatest('{{ds_nodash}}','20230505')
--           group by 1,2
--         )t1
--         left join
--           ads_databank.dim_spu_df t2
--         on t1.spu_id = t2.spu_id
--         where t2.dtm = greatest('{{ds_nodash}}','20230505')
--       ) t2 
--       on t1.user_id = t2.user_id and t1.spu_id = t2.spu_id
--   ) total
-- where
--   interest_level is not null
-- ;

--daybyday
insert overwrite table redapp.app_ads_spu_user_interest_30d_di partition(dtm, commercial_code1)
select
  user_id,
  spu_id,
  commercial_taxonomy_name1,
  commercial_code2,
  commercial_taxonomy_name2,
  commercial_code3,
  commercial_taxonomy_name3,
  commercial_code4,
  commercial_taxonomy_name4,
  interest_level,
  interest_level_map,
  dtm,
  commercial_code1
from
  (
    select
      coalesce(t1.user_id, t2.user_id) as user_id,
      coalesce(t1.spu_id, t2.spu_id) as spu_id,
      coalesce(t1.commercial_taxonomy_name1,t2.commercial_taxonomy_name1) as commercial_taxonomy_name1,
      coalesce(t1.commercial_code2, t2.commercial_code2) as commercial_code2,
      coalesce(t1.commercial_taxonomy_name2,t2.commercial_taxonomy_name2) as commercial_taxonomy_name2,
      coalesce(t1.commercial_code3, t2.commercial_code3) as commercial_code3,
      coalesce(t1.commercial_taxonomy_name3,t2.commercial_taxonomy_name3) as commercial_taxonomy_name3,
      coalesce(t1.commercial_code4, t2.commercial_code4) as commercial_code4,
      coalesce(t1.commercial_taxonomy_name4,t2.commercial_taxonomy_name4) as commercial_taxonomy_name4,
      case
        when coalesce(t2.S_date, '') != '' then 'S'
        when coalesce(t1.P_date, '') != '' then 'P'
        when coalesce(t1.TI_date, '') != '' then 'TI'
        when coalesce(t1.I_date, '') != '' then 'I'
        when coalesce(t1.A_date, '') != '' then 'A'
      end as interest_level,
      str_to_map(concat('A:',t1.A_date,'&I:',t1.I_date,'&TI:',t1.TI_date,'&P:',t1.P_date,'&S:',coalesce(t2.S_date, '')),'&') as interest_level_map,
      '{{ds_nodash}}' as dtm,
      coalesce(t1.commercial_code1, t2.commercial_code1) as commercial_code1
    from
      (
        select  /*+ mapjoin(t2) */
          t1.user_id,
          t1.spu_id,
          t1.commercial_code1,
          t1.commercial_taxonomy_name1,
          t1.commercial_code2,
          t1.commercial_taxonomy_name2,
          t1.commercial_code3,
          t1.commercial_taxonomy_name3,
          t1.commercial_code4,
          t1.commercial_taxonomy_name4,
          case
            when t1.interest_level = 'A' then '{{ds_nodash}}'
            else if(t1.A_date > f_getdate('{{ds_nodash}}', -30),t1.A_date,'')
          end as A_date,
          case
            when t1.interest_level = 'I' then '{{ds_nodash}}'
            else if(t1.I_date > f_getdate('{{ds_nodash}}', -30),t1.I_date,'')
          end as I_date,
          case
            when t1.interest_level = 'TI' then '{{ds_nodash}}'
            else if(t1.TI_date > f_getdate('{{ds_nodash}}', -30),t1.TI_date,'')
          end as TI_date,
          case
            when t1.interest_level = 'P' then '{{ds_nodash}}'
            else if(t1.P_date > f_getdate('{{ds_nodash}}', -30),t1.P_date,'')
          end as P_date
        from
          (
            select  /*+ mapjoin(t2) */
              t1.user_id,
              t1.spu_id,
              t1.A_date,
              t1.I_date,
              t1.P_date,
              t1.TI_date,
              t1.interest_level,
              t2.commercial_code1,
              t2.commercial_taxonomy_name1,
              t2.commercial_code2,
              t2.commercial_taxonomy_name2,
              t2.commercial_code3,
              t2.commercial_taxonomy_name3,
              t2.commercial_code4,
              t2.commercial_taxonomy_name4
            from
              (
              select
                coalesce(t1.user_id, t2.user_id) as user_id,
                coalesce(t1.spu_id, t2.spu_id) as spu_id,
                coalesce(t1.interest_level_map ['A'], '') as A_date,
                coalesce(t1.interest_level_map ['I'], '') as I_date,
                coalesce(t1.interest_level_map ['P'], '') as P_date,
                coalesce(t1.interest_level_map ['TI'], '') as TI_date,
                t2.interest_level
              from
                (
                select
                  a.user_id,
                  a.spu_id,
                  a.interest_level_map
                from
                  redapp.app_ads_spu_user_interest_30d_di a
                inner join
                  reddw.dw_user_seq_id_offline_day b
                on a.user_id = b.user_id
                where
                  a.dtm = f_getdate('{{ds_nodash}}', -1)
                  and b.dtm = greatest('{{ds_nodash}}','20230505')
                )t1
              full outer join
                (
                select
                  a.user_id,
                  a.spu_id,
                  a.interest_level
                from
                  redapp.app_ads_spu_user_interest_1d_di a
                inner join
                  reddw.dw_user_seq_id_offline_day b
                on a.user_id = b.user_id
                where
                  a.dtm = '{{ds_nodash}}'
                  and a.interest_level in ('A', 'I','TI', 'P')
                  and b.dtm = greatest('{{ds_nodash}}','20230505')
                )t2
                on t1.user_id = t2.user_id and t1.spu_id = t2.spu_id
              )t1
            left join
              ads_databank.dim_spu_df t2
            on t1.spu_id = t2.spu_id
            where 
              t2.dtm = greatest('{{ds_nodash}}', '20230505')
          ) t1
          
      ) t1 
      full outer join 
      (
        select /*+ mapjoin(t2) */
          t1.user_id,
          t1.spu_id,
          t1.S_date,
          t2.commercial_code1,
          t2.commercial_taxonomy_name1,
          t2.commercial_code2,
          t2.commercial_taxonomy_name2,
          t2.commercial_code3,
          t2.commercial_taxonomy_name3,
          t2.commercial_code4,
          t2.commercial_taxonomy_name4
        from
        (
          select
            a.author_id as user_id,
            a.spu_id,
            max(date_format(a.create_time, 'yyyyMMdd')) as S_date
          from
            ads_databank.dim_spu_note_df a
          inner join
            reddw.dw_user_seq_id_offline_day b
          on a.author_id = b.user_id
          left join
          (
          select
            note_id
          from
            ads_databank.dim_comment_spu_note_attribute_selling_point_emotional_df 
          where
            dtm = greatest('{{ds_nodash}}', '20230505')
            and (coalesce(positive_sentiment_word_num, 0) + coalesce(negative_sentiment_word_num, 0) + coalesce(neutral_sentiment_word_num, 0)) > 0
          group by 1
          )c
          on a.note_id = c.note_id
          where
            a.dtm = greatest('{{ds_nodash}}', '20230505')
            and ((coalesce(a.positive_sentiment_num, 0) + coalesce(a.negative_sentiment_num, 0) + coalesce(a.neutral_sentiment_num, 0)) > 0  or c.note_id is not null)
            and date_format(a.create_time, 'yyyyMMdd') > f_getdate('{{ds_nodash}}', -365)
            and date_format(a.create_time, 'yyyyMMdd') <= '{{ds_nodash}}'
            and b.dtm = greatest('{{ds_nodash}}','20230505')
          group by 1,2
        )t1
        left join
          ads_databank.dim_spu_df t2
        on t1.spu_id = t2.spu_id
        where t2.dtm = greatest('{{ds_nodash}}','20230505')
      ) t2 
      on t1.user_id = t2.user_id and t1.spu_id = t2.spu_id
  ) total
where
  interest_level is not null
;
