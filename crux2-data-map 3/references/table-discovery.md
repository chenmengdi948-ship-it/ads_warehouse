---
name: crux2-data-map/table-discovery
description: 智能找表工作流——通过自然语言描述找到匹配的数据表。当用户用业务语义或指标名称描述想找的表（如"找一张记录用户日活的表"）时读取本文档。
---

# 智能找表工作流（table-discovery）

**触发**：用户用自然语言描述想找的表或指标（如"找一张记录用户日活的表"）。

**关键约束**：
- 默认只推荐生产环境表（库名不含 `_dev` 后缀）；用户明确要求 dev 环境时除外
- 无关联生产任务的表需在结果中**显式标注警告**（⚠️ 无生产任务，可能已停更），不得默认推荐为首选
- 多张表均合适时**不自动选定**，须输出候选表对比信息（含库表名、中文名、负责人、生产任务状态、字段覆盖情况、推荐/注意说明），由用户决策

**参考 SOP**（按需灵活执行）：
```
Step 1 — 语义理解与关键词提取
  从用户意图提取 2-4 个搜索关键词，含业务同义词
  示例：「日活」→ ["日活", "dau", "daily_active", "活跃用户"]

Step 2 — 专题通道（优先）
  调用 search-topics 搜索匹配专题
  若有匹配，调用 topic-tables 获取专题内候选表

Step 3 — 通用搜索通道（与 Step 2 并行）
  search-tables（按表名）+ search-columns（按字段名），合并去重

Step 4 — 多维信号采集（对 top-5 候选表）
  调用 table-detail --detail-types BASIC TASK
  获取 isHighQuality / isCoreTable / queryCount / tasks

Step 5 — 多级排序
  1. 优质核心表优先：isHighQuality=true AND isCoreTable=true
  2. 来自官方专题优先：fromTopic=true
  3. 有生产任务优先：tasks 非空
  4. 查询次数降序：queryCount 越高越靠前
```

**输出格式**：
```markdown
## 找表结果

| 排名 | 库表名 | 中文名 | 负责人 | 生产任务 | 推荐/注意 |
|------|--------|--------|--------|----------|-----------|
| 1 | reddw.dws_user_dau_di | 用户日活明细表 | 张三 | ✅ 有 | 优质核心表，来自精选专题，查询次数最高 |
| 2 | reddm.dm_user_dau_di | 用户日活聚合表 | 李四 | ✅ 有 | 聚合粒度，字段较少 |
| 3 | reddw.ods_user_active_di | 用户活跃原始表 | — | ⚠️ 无 | 可能已停更，谨慎使用 |

> 以上有多张候选表，请告知您需要哪张，或说明更具体的需求（如需要哪些字段），我来帮您进一步确认。
```
