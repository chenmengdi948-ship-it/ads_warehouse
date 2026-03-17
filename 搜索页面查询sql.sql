SELECT `t106`.`search_keyword` AS `d0`,
  SUM(`t106`.`cpc_cost`) AS `m0`,
  SUM(`t106`.`query_cnt`) AS `m1`,
  SUM(`t106`.`ads_query_cnt`) AS `m2`,
  SUM(`t106`.`ads_imp_cnt`) AS `m3`,
  SUM(`t106`.`ads_cpc_click_cnt`) AS `m4`,
  SUM(`t106`.`ads_cpc_imp_cnt`) AS `m5`,
  SUM(`t106`.`kengwei_imp_cnt`) AS `m6`
FROM `reddm`.`dm_ads_industry_search_keyword_1d_di` AS `t106`
WHERE (((STR_TO_DATE(CAST(`t106`.`dtm` AS CHAR(255)), '%Y%m%d') >= DATE('2023-08-26')) AND (STR_TO_DATE(CAST(`t106`.`dtm` AS CHAR(255)), '%Y%m%d') < DATE('2023-08-27'))) AND (`t106`.`commercial_taxonomy_name1` IN ('食品饮料')))
GROUP BY `t106`.`search_keyword`
ORDER BY (SUM(`t106`.`cpc_cost`) / 100) DESC
LIMIT 20



SELECT `__table__1`.`d0` AS `d0`,
  `__table__1`.`d1` AS `d1`,
  `__table__1`.`__measure__0` AS `m0`,
  `__table__1`.`__measure__1` AS `m1`,
  `__table__1`.`__measure__2` AS `m2`,
  `__table__1`.`__measure__3` AS `m3`,
  `__table__1`.`__measure__4` AS `m4`,
  `__table__1`.`__measure__5` AS `m5`,
  `__table__1`.`__measure__6` AS `m6`,
  `__table__2`.`__measure__7` AS `m7`,
  `__table__2`.`__measure__8` AS `m8`,
  `__table__2`.`__measure__9` AS `m9`,
  `__table__2`.`__measure__10` AS `m10`,
  `__table__2`.`__measure__11` AS `m11`,
  `__table__2`.`__measure__12` AS `m12`,
  `__table__2`.`__measure__13` AS `m13`,
  `__table__2`.`__measure__14` AS `m14`,
  `__table__2`.`__measure__15` AS `m15`
FROM (
  SELECT `t107`.`commercial_taxonomy_name1` AS `d0`,
    `t107`.`commercial_taxonomy_name2` AS `d1`,
    SUM(`t107`.`search_keyword_cnt`) AS `__measure__0`,
    COUNT(DISTINCT `t107`.`dtm`) AS `__measure__1`,
    SUM(`t107`.`ads_cpc_imp_search_keyword_cnt`) AS `__measure__2`,
    SUM(`t107`.`query_cnt`) AS `__measure__3`,
    SUM(`t107`.`cpc_cost_search_keyword_cnt`) AS `__measure__4`,
    SUM(`t107`.`ads_query_cnt`) AS `__measure__5`,
    SUM(`t107`.`ads_imp_cnt`) AS `__measure__6`
  FROM `redapp`.`app_ads_industry_searchword_commercial_taxonomy_1d_di` AS `t107`
  WHERE (((STR_TO_DATE(CAST(`t107`.`dtm` AS CHAR(255)), '%Y%m%d') >= DATE('2023-08-26')) AND (STR_TO_DATE(CAST(`t107`.`dtm` AS CHAR(255)), '%Y%m%d') < DATE('2023-08-27'))) AND (`t107`.`commercial_taxonomy_name1` IN ('食品饮料')))
  GROUP BY `t107`.`commercial_taxonomy_name1`,
    `t107`.`commercial_taxonomy_name2`
) AS `__table__1`
INNER JOIN (
  SELECT `__table__4`.`commercial_taxonomy_name1` AS `d0`,
    `__table__4`.`commercial_taxonomy_name2` AS `d1`,
    `__table__3`.`__measure__16` AS `__measure__7`,
    `__table__3`.`__measure__17` AS `__measure__8`,
    `__table__3`.`__measure__18` AS `__measure__9`,
    `__table__3`.`__measure__19` AS `__measure__10`,
    `__table__3`.`__measure__20` AS `__measure__11`,
    `__table__3`.`__measure__21` AS `__measure__12`,
    `__table__3`.`__measure__22` AS `__measure__13`,
    `__table__3`.`__measure__23` AS `__measure__14`,
    `__table__3`.`__measure__24` AS `__measure__15`
  FROM (
    SELECT `t107`.`commercial_taxonomy_name1` AS `commercial_taxonomy_name1`,
      `t107`.`commercial_taxonomy_name2` AS `commercial_taxonomy_name2`
    FROM `redapp`.`app_ads_industry_searchword_commercial_taxonomy_1d_di` AS `t107`
    WHERE (((STR_TO_DATE(CAST(`t107`.`dtm` AS CHAR(255)), '%Y%m%d') >= DATE('2023-08-26')) AND (STR_TO_DATE(CAST(`t107`.`dtm` AS CHAR(255)), '%Y%m%d') < DATE('2023-08-27'))) AND (`t107`.`commercial_taxonomy_name1` IN ('食品饮料')))
    GROUP BY `t107`.`commercial_taxonomy_name1`,
      `t107`.`commercial_taxonomy_name2`
  ) AS `__table__4`
  INNER JOIN (
    SELECT `t107`.`commercial_taxonomy_name1` AS `commercial_taxonomy_name1`,
      (SUM(`t107`.`search_keyword_cnt`) / COUNT(DISTINCT `t107`.`dtm`)) AS `__measure__16`,
      (SUM(`t107`.`ads_cpc_imp_search_keyword_cnt`) / 1) AS `__measure__17`,
      (SUM(`t107`.`query_cnt`) / 1) AS `__measure__18`,
      (SUM(`t107`.`cpc_cost_search_keyword_cnt`) / 1) AS `__measure__19`,
      (SUM(`t107`.`ads_query_cnt`) / SUM(`t107`.`query_cnt`)) AS `__measure__20`,
      (SUM(`t107`.`ads_imp_cnt`) / SUM(`t107`.`query_cnt`)) AS `__measure__21`,
      (SUM(`t107`.`ads_cpc_imp_search_keyword_cnt`) / SUM(`t107`.`search_keyword_cnt`)) AS `__measure__22`,
      (SUM(`t107`.`cpc_cost_search_keyword_cnt`) / SUM(`t107`.`search_keyword_cnt`)) AS `__measure__23`,
      SUM(`t107`.`query_cnt`) AS `__measure__24`
    FROM `redapp`.`app_ads_industry_searchword_commercial_taxonomy_1d_di` AS `t107`
    WHERE (((STR_TO_DATE(CAST(`t107`.`dtm` AS CHAR(255)), '%Y%m%d') >= DATE('2023-08-26')) AND (STR_TO_DATE(CAST(`t107`.`dtm` AS CHAR(255)), '%Y%m%d') < DATE('2023-08-27'))) AND (`t107`.`commercial_taxonomy_name1` IN ('食品饮料')))
    GROUP BY `t107`.`commercial_taxonomy_name1`
  ) AS `__table__3` ON (`__table__4`.`commercial_taxonomy_name1` <=> `__table__3`.`commercial_taxonomy_name1`)
) AS `__table__2` ON ((`__table__1`.`d0` <=> `__table__2`.`d0`) AND (`__table__1`.`d1` <=> `__table__2`.`d1`))
ORDER BY (`__table__1`.`__measure__0` / `__table__1`.`__measure__1`) DESC
LIMIT 10000