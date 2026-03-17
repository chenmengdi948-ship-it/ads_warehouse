---
name: crux2-data-map/field-caliber
description: 字段口径分析工作流——分析字段的计算逻辑、业务含义和数据来源，支持两个字段的口径对比。当用户需要了解某字段的计算口径，或对比两个字段差异时读取本文档。
---

# 字段口径分析工作流（field-caliber）

**触发**：用户需了解某字段的计算口径，或对比两个字段。

**步骤**：
1. `table-detail --detail-types COLUMN` → 字段定义（注释、类型）
2. `table-detail --detail-types DDL` → 建表语句中的字段注释
3. `table-detail --detail-types TASK` → 获取生产任务列表（含 taskId 和 SQL 代码），从中分析字段的计算逻辑
4. `lineage --up-lvl 1` → 上游字段来源

**LLM 推理**（综合以上信息）：
- 输出：字段业务含义 + 计算公式/逻辑 + 数据来源 + 更新频率
- 重点：从 TASK 返回的 SQL 中定位目标字段的 SELECT/CASE/JOIN 逻辑，提取精确口径

**口径对比模式**：对两个字段分别执行上述流程，逐维度对比差异。
