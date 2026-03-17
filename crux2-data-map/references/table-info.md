---
name: crux2-data-map/table-info
description: 表基本信息工作流——查询已知表的 Schema、分区、DDL、数据预览、生产任务等详细信息。当用户明确指定表名并需要了解表结构或生产信息时读取本文档。
---

# 表基本信息工作流（table-info）

**触发**：用户明确指出某张表名，需了解其 Schema、分区、DDL 等。

**步骤**：
1. 调用 `table-detail` 获取所需信息（通过 `--detail-types` 按需选择）：
   - `--detail-types BASIC` → qualifiedName、cruxTableInfoId、负责人等基础信息
   - `--detail-types BASIC COLUMN` → 字段列表（含分区键 `[分区]`、废弃 `[已废弃]`、安全等级）
   - `--detail-types PARTITION` → 最新分区列表
   - `--detail-types DDL` → 建表语句
   - `--detail-types TASK` → 生产任务（含 SQL 代码）
   - `--detail-types TOPIC` → 关联专题
2. 如需查看数据样本：调用 `preview --table-id <cruxTableInfoId>`（cruxTableInfoId 从 BASIC 结果获取）
3. 如需查看任务 SQL：`table-detail --detail-types TASK` 返回的任务信息已包含 SQL 代码
