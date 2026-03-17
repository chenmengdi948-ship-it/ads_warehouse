---
name: crux2-data-map/lineage-analysis
description: 血缘与影响分析工作流——查询表的上下游血缘关系并评估变更影响范围。当用户询问某张表的数据来源、下游依赖或需要评估表变更影响时读取本文档。
---

# 血缘与影响分析工作流（lineage-analysis）

**触发**：用户询问表的上下游关系、变更影响范围。

**步骤**：
1. 确认有 `qualifiedName`，若无则先调 `table-detail --detail-types BASIC` 获取
2. 调用 `lineage --up-lvl N --down-lvl N` 获取血缘图
3. 调用 `table-detail --detail-types TASK` 获取关联的生产任务列表（含 taskId 和 SQL 代码），分析数据加工逻辑
4. LLM 总结影响范围，重点：
   - 下游有哪些核心表/重要任务
   - 受影响的数据层级（ODS→DWD→DWS→ADS）
   - 关键任务的 SQL 加工逻辑（来自 TASK 返回），帮助判断变更的实际影响面
