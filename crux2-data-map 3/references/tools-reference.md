---
name: crux2-data-map/tools-reference
description: 原子工具的完整参数说明与 Markdown 输出示例，覆盖搜索、表信息、血缘、专题四大类工具。当需要查询某个工具的具体参数、默认值或输出格式时读取本文档。
---

# 数据地图 Tools 文档

> 本文档描述 `scripts/tools.py` 提供的原子工具。  
> 默认输出为 **Markdown 格式**（LLM 友好），追加 `--raw` 参数可切换为原始 JSON。

---

## 通用约定

| 项目 | 说明 |
|------|------|
| 默认 catalog | `hive` |
| 默认 cluster | `ALIYUN_2` |
| qualifiedName 格式 | `{catalog}@@{dbName}@@{tableName}@@{clusterName}` |
| 鉴权 | SSO Token / 环境变量 / `.cookie` 文件 |
| 输出格式 | 默认 Markdown，`--raw` 切换为 JSON |

---

## 搜索类

### 1. search-tables — 模糊搜索表

```bash
python tools.py search-tables [选项]
```

| 参数 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `--search-key` | str | — | 搜索关键词（表名/中文名模糊匹配） |
| `--catalog` | str | hive | 数据源 |
| `--cluster` | str | — | 集群名 |
| `--db` | str | — | 数据库名 |
| `--table` | str | — | 表名精确过滤 |
| `--core-table` | flag | — | 只看核心表 |
| `--high-quality` | flag | — | 只看优质表 |
| `--owner` | str | — | 负责人邮箱 |
| `--page` | int | 1 | 页码 |
| `--size` | int | 10 | 每页条数 |
| `--raw` | flag | — | 输出原始 JSON |

**后端 API**：`POST /tools/searchTable`

**Markdown 输出示例**：
```markdown
## 搜索结果（共 125 条，当前第 1 页，显示 3 条）

| # | 库表名 | 中文名 | 负责人 | 查询次数 | tableId | 标签 |
|---|--------|--------|--------|----------|---------|------|
| 1 | `reddw.dws_user_dau_di` | 用户日活明细表 | 张三 | 12,450 | 1234 | **[优质表]** **[核心表]** |
| 2 | `reddw.ads_app_dau_di` | App日活汇总表 | 李四 | 8,230 | 5678 | **[优质表]** |
| 3 | `reddw.dwd_user_login_di` | 用户登录日志表 | 王五 | 3,100 | 9012 | |
```

---

### 2. search-columns — 模糊搜索字段

```bash
python tools.py search-columns [选项]
```

参数同 `search-tables`（不含 `--core-table`、`--high-quality`、`--owner`）。

**后端 API**：`POST /tools/searchColumn`

**Markdown 输出示例**：
```markdown
## 字段搜索结果（共 42 条，当前第 1 页，显示 3 条）

| # | 字段名 | 类型 | 中文名 | 注释 | 所属表 |
|---|--------|------|--------|------|--------|
| 1 | `dau` | BIGINT | 日活用户数 | 当日去重活跃用户数 | `reddw.dws_user_dau_di` |
| 2 | `dau_7d` | BIGINT | 近7日DAU | 近7日日均活跃用户数 | `reddw.ads_app_dau_di` |
| 3 | `is_dau` | INT | 是否日活 | 当日是否为活跃用户 0/1 | `reddw.dwd_user_login_di` |
```

---

## 表信息类

### 3. table-detail — 表详情（统一接口）

> 一个命令覆盖原来的 table-detail / table-schema / table-partitions / table-ddl / task-link / table-topics，通过 `--detail-types` 按需查询。

```bash
# 查全部（默认）
python tools.py table-detail --db reddw --table dws_user_dau_di

# 只查基础信息 + 字段
python tools.py table-detail --db reddw --table dws_user_dau_di --detail-types BASIC COLUMN

# 只查 DDL
python tools.py table-detail --db reddw --table dws_user_dau_di --detail-types DDL

# 查分区列表
python tools.py table-detail --db reddw --table dws_user_dau_di --detail-types PARTITION

# 查生产任务
python tools.py table-detail --db reddw --table dws_user_dau_di --detail-types TASK

# 查关联专题
python tools.py table-detail --db reddw --table dws_user_dau_di --detail-types TOPIC
```

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `--catalog` | str | 否（默认 hive） | 数据源 |
| `--cluster` | str | 否（默认 ALIYUN_2） | 集群名 |
| `--db` | str | **是** | 数据库名 |
| `--table` | str | **是** | 表名 |
| `--detail-types` | str... | 否 | 要查询的类型（默认全部）：`BASIC` `COLUMN` `PARTITION` `TASK` `TOPIC` `DDL` |
| `--raw` | flag | 否 | 输出原始 JSON |

**后端 API**：`POST /tools/tableDetail`

**`--detail-types` 可选值说明**：

| 值 | 说明 |
|----|------|
| `BASIC` | 基础信息（表名、中文名、负责人、生命周期等） |
| `COLUMN` | 字段列表（含分区键标记 `[分区]`、废弃标记 `[已废弃]`、主键标记 `[主键]`、安全等级） |
| `PARTITION` | 分区列表（分页，默认最新10条） |
| `TASK` | 关联的生产任务（含任务 ID、名称、SQL 代码） |
| `TOPIC` | 关联的数据专题（分页） |
| `DDL` | 建表语句 |

**Markdown 输出示例**（全类型）：
```markdown
## 表详情: reddw.dws_user_dau_di

- **中文名**: 用户日活明细表
- **qualifiedName**: `hive@@reddw@@dws_user_dau_di@@ALIYUN_2`
- **cruxTableInfoId**: 1234
- **表类型**: MANAGED_TABLE
- **负责人**: 张三 (zhangsan@xiaohongshu.com)
- **分区字段**: dtm
- **生命周期**: 90 天
- **标签**: **[优质表]** **[核心表]**
- **表说明**: 用户维度日活明细，按天分区。

### 字段列表（共 5 个）

| # | 字段名 | 类型 | 中文名 | 注释 | 安全级别 | 标注 |
|---|--------|------|--------|------|----------|------|
| 1 | `user_id` | STRING | 用户ID | 用户唯一标识 | L2 | |
| 2 | `dau` | BIGINT | 日活用户数 | 当日去重活跃用户数 | L1 | |
| 3 | `dtm` | STRING | 日期分区 | 格式 yyyyMMdd | L1 | **[分区]** |

### 分区列表（共 365 条，当前第 1 页，显示 3 条）

| # | 分区 | 文件数 | 大小 | 创建时间 |
|---|------|--------|------|----------|
| 1 | `dtm=20260309` | 128 | 2.3 GB | 2026-03-09 08:30:00 |

### 生产任务

| # | 任务ID | 任务名称 | 优先级 | 链接 |
|---|--------|----------|--------|------|
| 1 | 3891 | dws_user_dau_di_task | HIGH | [链接](https://dataverse.xxxx) |

### 关联专题（共 2 条，当前第 1 页，显示 2 条）

| # | 专题ID | 专题名称 | 描述 |
|---|--------|----------|------|
| 1 | 12 | 用户增长数据专题 | 包含 DAU/MAU/留存等核心指标表 |

### DDL

\```sql
CREATE TABLE reddw.dws_user_dau_di (
  user_id STRING COMMENT '用户唯一标识',
  dau BIGINT COMMENT '当日去重活跃用户数',
  dtm STRING COMMENT '日期分区 yyyyMMdd'
)
PARTITIONED BY (dtm STRING)
STORED AS PARQUET;
\```
```

---

### 4. preview — 数据预览

> 直接调用单次接口，无需额外鉴权步骤。默认 Markdown 模式下：数据 > 4KB 时落盘并截断输出；`--raw` 模式直接输出原始 JSON。

```bash
python tools.py preview --table-id <cruxTableInfoId> [--size 10]
```

| 参数 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `--table-id` | int | **是** | cruxTableInfoId（从 table-detail BASIC 结果中获取） |
| `--size` | int | 10 | 预览行数（最大 50） |
| `--raw` | flag | — | 输出原始 JSON |

**后端 API**：`POST /tools/preview`（响应为原始 JSON 字符串，不包裹在 BaseHttpResponse 中）

**Markdown 输出示例**：
```markdown
## 数据预览: cruxTableInfoId=1234（3 行）

| user_id | dau | platform | dtm |
|---------|-----|----------|-----|
| u_001 | 1 | ios | 20260309 |
| u_002 | 1 | android | 20260309 |
| u_003 | 1 | web | 20260309 |
```

---

## 血缘类

### 5. lineage — 血缘图模式

```bash
python tools.py lineage --qualified-name "hive@@reddw@@dws_user_dau_di@@ALIYUN_2" \
  --up-lvl 1 --down-lvl 2
# 或拆分参数：
python tools.py lineage --catalog hive --db reddw --table dws_user_dau_di --cluster ALIYUN_2 \
  --up-lvl 1 --down-lvl 1
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `--qualified-name` | str | `catalog@@db@@table@@cluster`（优先） |
| `--catalog/--db/--table/--cluster` | str | 拆分参数（不传 qualified-name 时使用） |
| `--up-lvl` | int | 上游层数（默认 0） |
| `--down-lvl` | int | 下游层数（默认 0） |

**后端 API**：`GET /tools/tableLineage`

---

## 专题类

### 6. search-topics — 搜索专题

```bash
python tools.py search-topics [--search-key "用户增长"] [--page 1] [--size 10]
```

**后端 API**：`POST /tools/searchTopic`

---

### 7. topic-detail — 专题详情

```bash
python tools.py topic-detail --topic-id <cruxTopicInfoId> [--increase]
```

| 参数 | 说明 |
|------|------|
| `--topic-id` | 专题 ID |
| `--increase` | 是否增加浏览计数（默认否） |

**后端 API**：`GET /tools/topicDetail`

---

### 8. topic-tables — 专题内表列表

```bash
python tools.py topic-tables --topic-id <cruxTopicInfoId> [--search-key "dau"]
```

**后端 API**：`POST /tools/topicDirectorySearch`

---

## Tool 与后端 API 映射总表

| Tool | 后端 API |
|------|----------|
| `search-tables` | `POST /tools/searchTable` |
| `search-columns` | `POST /tools/searchColumn` |
| `table-detail` | `POST /tools/tableDetail`（统一，支持 BASIC/COLUMN/PARTITION/TASK/TOPIC/DDL） |
| `preview` | `POST /tools/preview` |
| `lineage` | `GET /tools/tableLineage` |
| `search-topics` | `POST /tools/searchTopic` |
| `topic-detail` | `GET /tools/topicDetail` |
| `topic-tables` | `POST /tools/topicDirectorySearch` |
