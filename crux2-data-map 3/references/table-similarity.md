---
name: crux2-data-map/table-similarity
description: 表相似度分析工作流——发现与目标表功能相似的其他表，从 Schema 重叠度、语义相似度、血缘关联度三个维度综合打分。当用户想找与某张表功能相似的替代表或关联表时读取本文档。
---

# 表相似度分析工作流（table-similarity）

**触发**：用户想了解与某张表功能相似的其他表。

**步骤**：
1. `table-detail --detail-types COLUMN` → 目标表字段列表
2. `search-tables` → 按目标表中文名/关键词搜索候选表（top-10）
3. `lineage --down-lvl 1` → 下游表（同下游的表可能相似）
4. 对 top-5 候选表分别执行 `table-detail --detail-types COLUMN` 获取字段列表

**LLM 综合判断**（0-100 分）：
- Schema 结构重叠度（字段名/类型的 Jaccard 相似度）
- 语义相似度（表名/中文名/注释的语义接近程度）
- 血缘关联度（共同上游或下游）

**输出格式**：
```markdown
| 排名 | 表名 | 相似度 | 相似点 | 差异点 |
|------|------|--------|--------|--------|
| 1 | reddw.ads_app_dau_di | 92 | 同为日活指标表，字段高度重叠 | 粒度不同（明细 vs 汇总） |
```
