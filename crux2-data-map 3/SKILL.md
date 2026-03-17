---
name: crux2-data-map
description: 查询小红书大数据平台数据地图（Crux2）的表资产信息，支持：按关键词/字段/业务语义找表（专题优先+多维排序）、查表 Schema/分区/DDL/数据预览/生产任务、血缘与影响分析、字段口径分析与对比、表适用性评估、表相似度分析。当用户需要查找表、了解表结构、查看血缘、分析字段口径、写 SQL 前做数据调研时使用。
metadata:
  author: 数据平台
  version: "1.1"
  tags: 数据地图,元数据,Crux2
---

# 数据地图 Skill（crux2-data-map）

数据地图（Crux2）提供大数据平台的元数据管理与查询能力。本 Skill 通过 `scripts/tools.py` 调用 10 个原子工具，覆盖从"找表"到"评估表适用性"的完整数据调研链路。

## 使用前提

**脚本调用方式**（所有工具统一入口）：
```bash
python ./scripts/tools.py <subcommand> [options]
```

**鉴权**（优先级从高到低）：
1. **SSO Token（推荐）**：运行 `python ./scripts/sso_auth.py` 完成浏览器登录后自动缓存到 `~/.sso/.redInfo`，后续请求自动构造 Cookie，无需手动复制
2. 环境变量 `CRUX2_DATA_MAP_COOKIE` → 直接使用环境变量中的 Cookie 值
3. `assets/.cookie` → Cookie 文件（执行 `set-cookie` 命令写入）
4. 内网无鉴权直接访问

**鉴权失败处理**：

脚本遇到 401 时会在 **stdout** 输出可操作的恢复指引（退出码 `2`），Agent 应读取输出并按以下任一方式恢复：

- 方式 A（推荐）：运行 `python ./scripts/sso_auth.py`，浏览器登录后自动缓存 Token，后续请求自动鉴权
- 方式 B（自动）：调用 `data-fe-common-sso` skill 获取 Cookie
- 方式 C（手动）：提示用户从浏览器开发者工具复制 Cookie
- 方式 B/C 获取 Cookie 后执行下面命令
```bash
python ./scripts/tools.py set-cookie "<cookie>"
```

## 子 Skill 速查

| 子 Skill | 触发场景 | 核心工具 |
|----------|----------|----------|
| **智能找表** | 用自然语言描述想找的表/指标 | `search-topics` → `topic-tables` → `search-tables` → `table-detail --detail-types BASIC`（排序：优质核心表 > 专题来源 > 查询次数） |
| **表基本信息** | 明确表名，需了解 Schema/分区/DDL | `table-detail`（可通过 `--detail-types` 按需查询 BASIC/COLUMN/PARTITION/DDL/TASK/TOPIC） |
| **血缘分析** | 查上下游关系、变更影响范围 | `lineage` |
| **字段口径** | 了解字段计算逻辑，或对比两个字段 | `table-detail --detail-types COLUMN DDL TASK` + `lineage` |
| **表适用性** | 评估候选表是否满足业务需求 | `table-detail --detail-types BASIC COLUMN PARTITION` |
| **表相似度** | 发现功能相似的其他表 | `table-detail --detail-types COLUMN` + `search-tables` + `lineage` |

## 详细文档（按需读取）

根据当前任务场景，按需读取对应的工作流文档：

| 场景 | 读取文件 |
|------|----------|
| 用自然语言找表 | [references/table-discovery.md](references/table-discovery.md) |
| 查看已知表的详情/Schema/DDL | [references/table-info.md](references/table-info.md) |
| 查血缘/分析影响范围 | [references/lineage-analysis.md](references/lineage-analysis.md) |
| 分析字段计算口径 | [references/field-caliber.md](references/field-caliber.md) |
| 评估表是否满足业务需求 | [references/table-fitness.md](references/table-fitness.md) |
| 发现功能相似的表 | [references/table-similarity.md](references/table-similarity.md) |
| 查询工具参数与输出格式 | [references/tools-reference.md](references/tools-reference.md) |

## 关键约束

- 血缘查询优先使用 `qualifiedName`，格式：`{catalog}@@{dbName}@@{tableName}@@{cluster}`（也支持 `--catalog/--db/--table/--cluster` 拆分参数拼接）
- `preview` 需要 `cruxTableInfoId`，从 `table-detail --detail-types BASIC` 结果的 `basicInfo.cruxTableInfoId` 获取
- 用户只提供 `db.table` 格式时，默认补充 `--catalog hive --cluster ALIYUN_2`
- Schema 超过 50 个字段时，只向 LLM 展示分区字段 + 注释含业务关键词的字段
- **所有表名、字段名、库名等信息必须来自工具返回结果**，禁止凭记忆或推断自行构造，未经工具确认的表/字段不得出现在 SQL 或结论中
- **生成 SQL 前，若目标表含分区字段，必须先执行 `table-detail --detail-types PARTITION` 查询可用分区**，用实际存在的分区值作为过滤条件，不得使用占位符或假设值

## 工具快速参考

```bash
python tools.py search-tables --search-key "dau" --size 10
python tools.py search-columns --search-key "user_id" --db reddw
python tools.py table-detail --db reddw --table dws_user_dau_di                        # 查全部
python tools.py table-detail --db reddw --table dws_user_dau_di --detail-types BASIC COLUMN
python tools.py table-detail --db reddw --table dws_user_dau_di --detail-types DDL
python tools.py table-detail --db reddw --table dws_user_dau_di --detail-types PARTITION
python tools.py table-detail --db reddw --table dws_user_dau_di --detail-types TASK
python tools.py table-detail --db reddw --table dws_user_dau_di --detail-types TOPIC
python tools.py preview --table-id 1234 --size 10
python tools.py lineage --qualified-name "hive@@reddw@@dws_user_dau_di@@ALIYUN_2" --up-lvl 1 --down-lvl 2
python tools.py search-topics --search-key "用户增长"
python tools.py topic-detail --topic-id 42
python tools.py topic-tables --topic-id 42
python tools.py set-cookie "<cookie>"
```
