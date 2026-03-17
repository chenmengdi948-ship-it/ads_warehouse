"""数据地图 Tool 层：原子工具 + Markdown 格式化 + CLI 入口

用法示例：
  python tools.py search-tables --search-key dau --size 5
  python tools.py table-detail --catalog hive --cluster ALIYUN_2 --db reddw --table dws_user_dau_di
  python tools.py table-detail --db reddw --table dws_user_dau_di --detail-types BASIC COLUMN
  python tools.py preview --table-id 1234 --size 10
  python tools.py lineage --qualified-name "hive@@reddw@@dws_user_dau_di@@ALIYUN_2" --up-lvl 1 --down-lvl 1
"""
import json
import math
import os
import sys
import argparse

from data_map_api import api_get, api_post, get_data, save_cookie, COOKIE_FILE, RESOURCES_DIR, NoCookieError

PREVIEW_SIZE_MAX = 50
PREVIEW_SIZE_DEFAULT = 10
PREVIEW_BIG_THRESHOLD_BYTES = 4 * 1024  # 4KB
PREVIEW_TRUNCATE_ROWS = 10


# ────────────────────────────────────────────────────
# 工具函数
# ────────────────────────────────────────────────────

def _fmt_size(n: int | str | None) -> str:
    """把字节数格式化为人类可读大小"""
    if n is None or n == "":
        return ""
    try:
        n = int(n)
    except (ValueError, TypeError):
        return str(n)
    if n >= 1024 ** 3:
        return f"{n / 1024 ** 3:.1f} GB"
    if n >= 1024 ** 2:
        return f"{n / 1024 ** 2:.1f} MB"
    if n >= 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n} B"


def _tag_label(is_high_quality: bool, is_core_table: bool) -> str:
    tags = []
    if is_high_quality:
        tags.append("**[优质表]**")
    if is_core_table:
        tags.append("**[核心表]**")
    return " ".join(tags)


def _page_summary(total: int, page: int, per_page: int) -> str:
    return f"共 {total} 条，当前第 {page} 页，显示 {per_page} 条"


def _md_table(headers: list[str], rows: list[list]) -> str:
    """生成 Markdown 表格"""
    sep = "|".join(["---"] * len(headers))
    header_row = "| " + " | ".join(headers) + " |"
    sep_row = "| " + sep + " |"
    data_rows = ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return "\n".join([header_row, sep_row] + data_rows)


# ────────────────────────────────────────────────────
# 1. search_tables
# ────────────────────────────────────────────────────

def search_tables(
    search_key=None, catalog="hive", cluster_name=None, db_name=None,
    table_name=None, core_table=None, is_high_quality=None, owner_id=None, env="PROD",
    page=1, size=10,
) -> dict:
    """模糊搜索表"""
    body = {"page": page, "size": size, "catalog": catalog}
    if search_key:
        body["searchKey"] = search_key
    if cluster_name:
        body["clusterName"] = cluster_name
    if db_name:
        body["dbName"] = db_name
    if table_name:
        body["tableName"] = table_name
    if core_table is not None:
        body["coreTable"] = core_table
    if is_high_quality is not None:
        body["isHighQuality"] = is_high_quality
    if owner_id:
        body["ownerId"] = owner_id
    if env:
        body["env"] = env
    return api_post("/tools/searchTable", body)


def fmt_search_tables(resp: dict, page: int = 1) -> str:
    data = get_data(resp)
    content = data.get("content", [])
    total = data.get("totalElements", 0)
    if not content:
        return f"> 未找到表（共 {total} 条）"

    rows = []
    for i, t in enumerate(content, 1):
        tag = _tag_label(t.get("isHighQuality", False), t.get("coreTable", False))
        query_count = t.get("queryCount") or 0
        table_id = t.get("cruxTableInfoId", "")
        rows.append([
            i,
            f"`{t.get('dbTableName', '')}`",
            t.get("tableChineseName", "") or "—",
            t.get("ownerName", "") or "—",
            f"{query_count:,}",
            table_id,
            tag,
        ])

    output = [
        f"## 搜索结果（{_page_summary(total, page, len(content))}）",
        "",
        _md_table(["#", "库表名", "中文名", "负责人", "查询次数", "tableId", "标签"], rows),
    ]
    return "\n".join(output)


# ────────────────────────────────────────────────────
# 2. search_columns
# ────────────────────────────────────────────────────

def search_columns(
    search_key=None, catalog="hive", cluster_name=None, db_name=None,
    table_name=None, page=1, size=10,
) -> dict:
    """模糊搜索字段"""
    body = {
        "page": page,
        "size": size,
        "catalog": catalog,
        "searchOrderType": "ORDER_BY_CREATE_TIME",
        "searchPageType": "COLUMN",
    }
    if search_key:
        body["searchKey"] = search_key
    if cluster_name:
        body["clusterName"] = cluster_name
    if db_name:
        body["dbName"] = db_name
    if table_name:
        body["tableName"] = table_name
    return api_post("/tools/searchColumn", body)


def fmt_search_columns(resp: dict, page: int = 1) -> str:
    data = get_data(resp)
    content = data.get("content", [])
    total = data.get("totalElements", 0)
    if not content:
        return f"> 未找到字段（共 {total} 条）"

    rows = []
    for i, c in enumerate(content, 1):
        table_label = f"{c.get('dbName', '')}.{c.get('tableName', '')}"
        rows.append([
            i,
            f"`{c.get('columnName', '')}`",
            c.get("dataType", "") or "—",
            c.get("columnChineseName", "") or "—",
            c.get("columnComment", "") or "—",
            f"`{table_label}`",
        ])

    output = [
        f"## 字段搜索结果（{_page_summary(total, page, len(content))}）",
        "",
        _md_table(["#", "字段名", "类型", "中文名", "注释", "所属表"], rows),
    ]
    return "\n".join(output)


# ────────────────────────────────────────────────────
# 3. get_table_detail（统一接口，6 合 1）
# ────────────────────────────────────────────────────

_ALL_DETAIL_TYPES = ["BASIC", "COLUMN", "PARTITION", "TASK", "TOPIC", "DDL"]


def get_table_detail(
    catalog: str,
    cluster: str,
    database: str,
    table_name: str,
    detail_types: list[str] | None = None,
) -> dict:
    """获取表详情（统一接口），按需查询不同维度的信息。

    Args:
        catalog: 数据源类型，如 "hive"、"mysql"、"clickhouse"
        cluster: 集群名，如 "ALIYUN_2"
        database: 数据库名，如 "reddw"
        table_name: 表名，如 "dws_user_dau_di"
        detail_types: 要查询的详情类型列表，可选值：
            - "BASIC"     : 基础信息（表名、中文名、负责人、生命周期等）
            - "COLUMN"    : 字段信息（含分区字段标记 isPartitionKey、安全等级、废弃标记 isDiscarded）
            - "PARTITION" : 分区列表（分页结构）
            - "TASK"      : 关联的生产任务（含任务 ID、名称、SQL 代码）
            - "TOPIC"     : 关联的数据专题（分页结构）
            - "DDL"       : 建表语句
            默认 None 时查询全部类型。

    Returns:
        API 原始响应 dict，data 字段结构为 AgentToolsDetailResponse：
        {
            "basicInfo": {              # detailTypes 含 BASIC 时返回
                "qualifiedName": "...",
                "cruxTableInfoId": 123,
                "catalog": "hive",
                "clusterName": "ALIYUN_2",
                "dbName": "reddw",
                "tableName": "...",
                "tableChineseName": "...",
                "comment": "...",
                "desc": "...",
                "ownerName": "...",
                "ownerId": "...",
                "tableType": "...",
                "isHighQuality": true,
                "coreTable": true,
                "lifeCycle": 90,
                "dtm": "ds"
            },
            "columns": [                # detailTypes 含 COLUMN 时返回
                {
                    "columnName": "...",
                    "dataType": "...",
                    "columnChineseName": "...",
                    "columnComment": "...",
                    "isPrimary": false,
                    "isPartitionKey": false,
                    "securityLevel": "L2",
                    "privacyType": "...",
                    "isDiscarded": false
                }
            ],
            "partitions": {             # detailTypes 含 PARTITION 时返回（分页结构）
                "page": 1, "size": 10,
                "content": [...],
                "totalElements": 100
            },
            "tasks": [...],             # detailTypes 含 TASK 时返回
            "topics": {                 # detailTypes 含 TOPIC 时返回（分页结构）
                "page": 1, "size": 10,
                "content": [...],
                "totalElements": 10
            },
            "ddl": "CREATE TABLE ..."   # detailTypes 含 DDL 时返回
        }
    """
    body = {
        "catalog": catalog,
        "clusterName": cluster,
        "dbName": database,
        "tableName": table_name,
        "detailTypes": detail_types or _ALL_DETAIL_TYPES,
    }
    return api_post("/tools/tableDetail", body)


def fmt_table_detail(resp: dict) -> str:
    """将 get_table_detail 的响应格式化为 Markdown。

    自动检测 data 中包含哪些部分（basicInfo / columns / partitions / tasks / topics / ddl），
    按顺序渲染对应的 Markdown 段落。
    """
    d = get_data(resp)
    if not d:
        return "> 表详情为空"

    sections = []

    # ── BASIC ──
    basic = d.get("basicInfo")
    if basic:
        db_name = basic.get("dbName", "")
        tbl_name = basic.get("tableName", "")
        db_table = basic.get("dbTableName") or f"{db_name}.{tbl_name}"
        tag = _tag_label(basic.get("isHighQuality", False), basic.get("coreTable", False))
        owner = f"{basic.get('ownerName', '') or '—'} ({basic.get('ownerId', '') or '—'})"
        desc = basic.get("desc") or basic.get("comment") or "—"
        lines = [
            f"## 表详情: {db_table}",
            "",
            f"- **中文名**: {basic.get('tableChineseName', '') or '—'}",
            f"- **qualifiedName**: `{basic.get('qualifiedName', '')}`",
            f"- **cruxTableInfoId**: {basic.get('cruxTableInfoId', '')}",
            f"- **表类型**: {basic.get('tableType', '') or '—'}",
            f"- **负责人**: {owner}",
            f"- **分区字段**: {basic.get('dtm', '') or '无'}",
            f"- **生命周期**: {basic.get('lifeCycle') or '—'} 天",
        ]
        if tag:
            lines.append(f"- **标签**: {tag}")
        lines.append(f"- **表说明**: {desc}")
        sections.append("\n".join(lines))

    # ── COLUMN ──
    columns = d.get("columns")
    if columns is not None:
        rows = []
        for i, c in enumerate(columns, 1):
            tags = []
            if c.get("isPartitionKey"):
                tags.append("**[分区]**")
            if c.get("isDiscarded"):
                tags.append("**[已废弃]**")
            if c.get("isPrimary"):
                tags.append("**[主键]**")
            tag_str = " ".join(tags)
            sec_level = c.get("securityLevel") or ""
            rows.append([
                i,
                f"`{c.get('columnName', '')}`",
                c.get("dataType", "") or "—",
                c.get("columnChineseName", "") or "—",
                c.get("columnComment", "") or "—",
                sec_level or "—",
                tag_str,
            ])
        col_section = [
            f"### 字段列表（共 {len(columns)} 个）",
            "",
            _md_table(["#", "字段名", "类型", "中文名", "注释", "安全级别", "标注"], rows),
        ]
        sections.append("\n".join(col_section))

    # ── PARTITION ──
    partitions = d.get("partitions")
    if partitions is not None:
        content = partitions.get("content", [])
        total = partitions.get("totalElements", 0)
        page = partitions.get("page", 1)
        if not content:
            sections.append(f"> 无分区数据（共 {total} 条）")
        else:
            rows = []
            for i, p in enumerate(content, 1):
                size_str = _fmt_size(p.get("totalSize"))
                rows.append([
                    i,
                    f"`{p.get('dtm', '')}`",
                    p.get("numFile", "—"),
                    size_str or "—",
                    p.get("createTime", "") or "—",
                ])
            part_section = [
                f"### 分区列表（{_page_summary(total, page, len(content))}）",
                "",
                _md_table(["#", "分区", "文件数", "大小", "创建时间"], rows),
            ]
            sections.append("\n".join(part_section))

    # ── TASK ──
    tasks = d.get("tasks")
    if tasks is not None:
        if not tasks:
            sections.append("> 该表暂无关联的生产任务")
        else:
            rows = []
            for i, t in enumerate(tasks, 1):
                task_url = t.get("link") or t.get("taskUrl") or t.get("url") or "—"
                rows.append([
                    i,
                    t.get("taskId", "") or "—",
                    t.get("task") or t.get("taskName", "") or "—",
                    t.get("priority", "") or "—",
                    f"[链接]({task_url})" if task_url != "—" else "—",
                ])
            task_section = [
                "### 生产任务",
                "",
                _md_table(["#", "任务ID", "任务名称", "优先级", "链接"], rows),
            ]
            for i, t in enumerate(tasks, 1):
                code = t.get("code", "").strip()
                if code:
                    task_name = t.get("task") or t.get("taskName") or f"任务{i}"
                    task_section += [
                        "",
                        f"#### 任务{i} SQL（{task_name}）",
                        "",
                        "```sql",
                        code,
                        "```",
                    ]
            sections.append("\n".join(task_section))

    # ── TOPIC ──
    topics = d.get("topics")
    if topics is not None:
        content = topics.get("content", [])
        total = topics.get("totalElements", 0)
        page = topics.get("page", 1)
        if not content:
            sections.append("> 该表未关联任何数据专题")
        else:
            rows = []
            for i, t in enumerate(content, 1):
                rows.append([
                    i,
                    t.get("cruxTopicInfoId", "") or "—",
                    t.get("topicName", "") or "—",
                    t.get("topicDesc", "") or "—",
                ])
            topic_section = [
                f"### 关联专题（{_page_summary(total, page, len(content))}）",
                "",
                _md_table(["#", "专题ID", "专题名称", "描述"], rows),
            ]
            sections.append("\n".join(topic_section))

    # ── DDL ──
    ddl = d.get("ddl")
    if ddl is not None:
        sections.append(f"### DDL\n\n```sql\n{ddl}\n```")

    return "\n\n".join(sections) if sections else "> 无详情数据"


# ────────────────────────────────────────────────────
# 4. preview_data（单次调用，响应为原始 JSON 字符串）
# ────────────────────────────────────────────────────

def _fetch_preview_raw(crux_table_info_id: int, size: int) -> str:
    """调用 /tools/preview，返回原始 JSON 字符串（接口不包裹在 BaseHttpResponse 中）"""
    import requests as _requests
    from data_map_api import BASE_URL, _headers
    body = {"cruxTableInfoId": crux_table_info_id, "size": min(size, PREVIEW_SIZE_MAX)}
    resp = _requests.post(
        f"{BASE_URL}/tools/preview",
        headers=_headers("application/json;charset=UTF-8"),
        json=body,
        timeout=30,
    )
    from data_map_api import _check_auth_error
    _check_auth_error(resp)
    return resp.text


def preview_data(crux_table_info_id: int, size: int = PREVIEW_SIZE_DEFAULT) -> tuple[str, str | None]:
    """预览数据。返回 (markdown_text, saved_file_path_or_None)"""
    size = min(size, PREVIEW_SIZE_MAX)
    raw_str = _fetch_preview_raw(crux_table_info_id, size)
    byte_len = len(raw_str.encode("utf-8"))

    rows_data: list[dict] = []
    try:
        parsed = json.loads(raw_str)
        if isinstance(parsed, list) and parsed:
            rows_data = parsed
    except Exception:
        pass

    def _to_md_table(rows: list[dict]) -> str:
        if not rows:
            return "> 无数据"
        headers = list(rows[0].keys())
        md_rows = [[str(r.get(h, "")) for h in headers] for r in rows]
        return _md_table(headers, md_rows)

    if byte_len <= PREVIEW_BIG_THRESHOLD_BYTES:
        md = f"## 数据预览: cruxTableInfoId={crux_table_info_id}（{len(rows_data)} 行）\n\n{_to_md_table(rows_data)}"
        return md, None
    else:
        os.makedirs(RESOURCES_DIR, exist_ok=True)
        path = os.path.join(RESOURCES_DIR, f".tmp_preview_{crux_table_info_id}.json")
        with open(path, "w", encoding="utf-8") as f:
            f.write(raw_str)
        truncated = rows_data[:PREVIEW_TRUNCATE_ROWS] if rows_data else []
        kb = byte_len / 1024
        trunc_md = _to_md_table(truncated)
        md_lines = [
            f"## 数据预览: cruxTableInfoId={crux_table_info_id}（截断，共 {len(rows_data)} 行）",
            "",
            trunc_md,
            "",
            f"> 数据量较大（{kb:.1f} KB），完整结果已写入: `{path}`",
        ]
        return "\n".join(md_lines), path


def preview_data_raw(crux_table_info_id: int, size: int = PREVIEW_SIZE_DEFAULT) -> str:
    """预览数据的 raw 模式：返回原始 JSON 字符串，不做 Markdown 截断/落盘"""
    size = min(size, PREVIEW_SIZE_MAX)
    return _fetch_preview_raw(crux_table_info_id, size)



# ────────────────────────────────────────────────────
# 6. get_lineage_chain
# ────────────────────────────────────────────────────

def build_qualified_name(catalog: str, db_name: str, table_name: str, cluster_name: str) -> str:
    """拼接 qualifiedName，格式：catalog@@database@@tableName@@cluster"""
    return f"{catalog}@@{db_name}@@{table_name}@@{cluster_name}"


def get_lineage_chain(qualified_name: str, up_lvl: int = 0, down_lvl: int = 0) -> dict:
    """获取表上下游血缘（图模式）"""
    params = {"qualifiedName": qualified_name, "upLvl": up_lvl, "downLvl": down_lvl}
    return api_get("/tools/tableLineage", params)


def fmt_lineage_chain(resp: dict, up_lvl: int = 0, down_lvl: int = 0) -> str:
    data = get_data(resp)
    if not data:
        return "> 无血缘数据"

    # API 返回两种结构：
    #   A) 嵌套结构：单个根节点含 directUpList / directDownList
    #   B) 扁平列表：含 direction + level 字段
    items = data if isinstance(data, list) else [data]
    root = items[0] if items else {}

    # 尝试结构 A：嵌套的 directUpList / directDownList
    direct_up = root.get("directUpList") or []
    direct_down = root.get("directDownList") or []

    # 过滤掉自引用（表自身出现在自己的上下游中）
    root_qn = root.get("qualifiedName", "")
    direct_up = [n for n in direct_up if n.get("qualifiedName") != root_qn]
    direct_down = [n for n in direct_down if n.get("qualifiedName") != root_qn]

    if direct_up or direct_down:
        # 结构 A：嵌套格式
        table_name = root.get("table") or root.get("name") or "目标表"
        if "@@" in table_name:
            table_name = table_name.split("@@")[2]

        def _section_nested(title: str, nodes: list) -> str:
            rows = []
            for i, n in enumerate(nodes, 1):
                depth = n.get("depth", 1)
                db_table = n.get("table") or n.get("name", "—")
                direct_up_num = n.get("directUpNum", 0)
                direct_down_num = n.get("directDownNum", 0)
                cluster = n.get("clusterName") or n.get("cluster", "")
                rows.append([i, depth, f"`{db_table}`", cluster,
                             direct_up_num, direct_down_num])
            return f"### {title}\n\n{_md_table(['#', '层级', '库表名', '集群', '上游数', '下游数'], rows)}"

        sections = [f"## 血缘: {table_name}（上游 {up_lvl} 层，下游 {down_lvl} 层）", ""]
        if direct_up:
            sections.append(_section_nested(f"上游表（{len(direct_up)} 张）", direct_up))
        if direct_down:
            sections.append(_section_nested(f"下游表（{len(direct_down)} 张）", direct_down))
        return "\n\n".join(sections)

    # 尝试结构 B：扁平列表含 direction + level
    upstream = [x for x in items if x.get("direction") in ("UP", "up", "upstream") and x.get("level", 0) > 0]
    downstream = [x for x in items if x.get("direction") in ("DOWN", "down", "downstream") or x.get("level", 0) < 0]

    if upstream or downstream:
        def _section_flat(title: str, nodes: list) -> str:
            rows = []
            for i, n in enumerate(nodes, 1):
                lvl = abs(n.get("level", 0))
                db_table = n.get("dbTableName") or n.get("tableName", "—")
                chinese = n.get("tableChineseName", "") or "—"
                task_type = n.get("taskType", "") or "—"
                task_id = n.get("taskId", "") or "—"
                task_url = n.get("taskUrl") or n.get("url") or ""
                task_link = f"[{task_id}]({task_url})" if task_url else str(task_id)
                rows.append([i, lvl, f"`{db_table}`", chinese, task_type, task_link])
            return f"### {title}\n\n{_md_table(['#', '层级', '库表名', '中文名', '任务类型', '任务'], rows)}"

        qn_part = root.get("qualifiedName", "")
        table_name = qn_part.split("@@")[2] if "@@" in qn_part else "目标表"

        sections = [f"## 血缘: {table_name}（上游 {up_lvl} 层，下游 {down_lvl} 层）", ""]
        if upstream:
            sections.append(_section_flat("上游表", upstream))
        if downstream:
            sections.append(_section_flat("下游表", downstream))
        return "\n\n".join(sections)

    # 兜底：原样输出
    return f"## 血缘数据（原始）\n\n```json\n{json.dumps(data, ensure_ascii=False, indent=2)[:5000]}\n```"


# ────────────────────────────────────────────────────
# 7. search_topics
# ────────────────────────────────────────────────────

def search_topics(search_key=None, page=1, size=10) -> dict:
    """搜索数据专题"""
    body = {"page": page, "size": size}
    if search_key:
        body["searchKey"] = search_key
    return api_post("/tools/searchTopic", body)


def fmt_search_topics(resp: dict, page: int = 1) -> str:
    data = get_data(resp)
    content = data.get("content", [])
    total = data.get("totalElements", 0)
    if not content:
        return f"> 未找到专题（共 {total} 条）"

    rows = []
    for i, t in enumerate(content, 1):
        rows.append([
            i,
            t.get("cruxTopicInfoId", "") or "—",
            t.get("topicName", "") or "—",
            t.get("topicDesc", "") or "—",
            t.get("ownerName", "") or "—",
        ])

    output = [
        f"## 专题搜索结果（{_page_summary(total, page, len(content))}）",
        "",
        _md_table(["#", "专题ID", "专题名称", "描述", "负责人"], rows),
    ]
    return "\n".join(output)


# ────────────────────────────────────────────────────
# 9. get_topic_detail
# ────────────────────────────────────────────────────

def get_topic_detail(crux_topic_info_id: int, increase: bool = False) -> dict:
    """获取专题详情"""
    params = {"cruxTopicInfoId": crux_topic_info_id}
    if increase:
        params["increase"] = "true"
    return api_get("/tools/topicDetail", params)


def fmt_topic_detail(resp: dict) -> str:
    d = get_data(resp)
    if not d:
        return "> 专题不存在"
    lines = [
        f"## 专题: {d.get('topicName', '')}",
        "",
        f"- **专题ID**: {d.get('cruxTopicInfoId', '')}",
        f"- **描述**: {d.get('topicDesc', '') or '—'}",
        f"- **负责人**: {d.get('ownerName', '') or '—'}",
        f"- **创建时间**: {d.get('createTime', '') or '—'}",
    ]
    return "\n".join(lines)


# ────────────────────────────────────────────────────
# 10. get_topic_tables
# ────────────────────────────────────────────────────

def get_topic_tables(crux_topic_info_id: int, search_key=None) -> dict:
    """获取专题内的表列表"""
    body = {"cruxTopicInfoId": crux_topic_info_id, "withTable": True}
    if search_key:
        body["searchKey"] = search_key
    return api_post("/tools/topicDirectorySearch", body)


def fmt_topic_tables(resp: dict) -> str:
    data = get_data(resp)
    if not data:
        return "> 该专题无表数据"

    # API 返回树形结构，需要递归遍历 children 收集所有表
    all_tables = []
    path_stack = []  # 用于记录目录路径

    def traverse(node: dict, depth: int = 0):
        """递归遍历树形结构，收集所有表"""
        if not node:
            return

        # 如果是表（cruxTableInfoId 不为 null）
        if node.get("cruxTableInfoId"):
            directory_path = " > ".join(path_stack) if path_stack else "—"
            all_tables.append({
                **node,
                "_directory": directory_path,
            })
        # 如果有子节点，继续遍历
        elif node.get("children"):
            # 如果当前节点是目录（有 name 且不是表），加入路径
            if node.get("name") and not node.get("cruxTableInfoId"):
                path_stack.append(node["name"])
            
            for child in node["children"]:
                traverse(child, depth + 1)
            
            # 回溯时移除路径
            if node.get("name") and not node.get("cruxTableInfoId"):
                path_stack.pop()

    traverse(data)

    if not all_tables:
        return "> 该专题无表数据"

    rows = []
    for i, t in enumerate(all_tables, 1):
        tag = _tag_label(t.get("isHighQuality", False), t.get("coreTable", False))
        rows.append([
            i,
            t.get("_directory", "—"),
            f"`{t.get('dbTableName', '') or t.get('tableName', '—')}`",
            t.get("tableChineseName", "") or "—",
            t.get("ownerName", "") or "—",
            tag,
        ])

    output = [
        f"## 专题内表列表（共 {len(all_tables)} 张表）",
        "",
        _md_table(["#", "目录路径", "库表名", "中文名", "负责人", "标签"], rows),
    ]
    return "\n".join(output)



# ────────────────────────────────────────────────────
# CLI 入口
# ────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="数据地图 Tool 层 CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # ── search-tables ──
    p = sub.add_parser("search-tables", help="模糊搜索表")
    p.add_argument("--search-key", default=None)
    p.add_argument("--catalog", default="hive")
    p.add_argument("--cluster", default=None, dest="cluster_name")
    p.add_argument("--db", default=None, dest="db_name")
    p.add_argument("--table", default=None, dest="table_name")
    p.add_argument("--core-table", action="store_true", default=None, dest="core_table")
    p.add_argument("--high-quality", action="store_true", default=None, dest="is_high_quality")
    p.add_argument("--owner", default=None, dest="owner_id")
    p.add_argument("--env", default="PROD", dest="env")
    p.add_argument("--page", type=int, default=1)
    p.add_argument("--size", type=int, default=10)
    p.add_argument("--raw", action="store_true")

    # ── search-columns ──
    p = sub.add_parser("search-columns", help="模糊搜索字段")
    p.add_argument("--search-key", default=None)
    p.add_argument("--catalog", default="hive")
    p.add_argument("--cluster", default=None, dest="cluster_name")
    p.add_argument("--db", default=None, dest="db_name")
    p.add_argument("--table", default=None, dest="table_name")
    p.add_argument("--page", type=int, default=1)
    p.add_argument("--size", type=int, default=10)
    p.add_argument("--raw", action="store_true")

    # ── table-detail ──
    p = sub.add_parser(
        "table-detail",
        help="表详情（统一接口，可按需查询 BASIC/COLUMN/PARTITION/TASK/TOPIC/DDL）",
    )
    p.add_argument("--catalog", default="hive")
    p.add_argument("--cluster", default="ALIYUN_2", dest="cluster_name")
    p.add_argument("--db", required=True, dest="db_name")
    p.add_argument("--table", required=True, dest="table_name")
    p.add_argument(
        "--detail-types",
        nargs="+",
        default=None,
        dest="detail_types",
        metavar="TYPE",
        help="要查询的类型，可多选：BASIC COLUMN PARTITION TASK TOPIC DDL（默认全部）",
    )
    p.add_argument("--raw", action="store_true")

    # ── preview ──
    p = sub.add_parser("preview", help="数据预览（默认10行，最多50行）")
    p.add_argument("--table-id", type=int, required=True, dest="table_id")
    p.add_argument("--size", type=int, default=PREVIEW_SIZE_DEFAULT)
    p.add_argument("--raw", action="store_true")

    # ── lineage ──
    p = sub.add_parser("lineage", help="血缘图模式（优先 qualifiedName）")
    p.add_argument(
        "--qualified-name", default=None, dest="qualified_name",
        help="格式: catalog@@dbName@@tableName@@cluster",
    )
    p.add_argument("--catalog", default=None)
    p.add_argument("--db", default=None, dest="db_name")
    p.add_argument("--table", default=None, dest="table_name")
    p.add_argument("--cluster", default=None, dest="cluster_name")
    p.add_argument("--up-lvl", type=int, default=0)
    p.add_argument("--down-lvl", type=int, default=0)
    p.add_argument("--raw", action="store_true")

    # ── search-topics ──
    p = sub.add_parser("search-topics", help="搜索数据专题")
    p.add_argument("--search-key", default=None)
    p.add_argument("--page", type=int, default=1)
    p.add_argument("--size", type=int, default=10)
    p.add_argument("--raw", action="store_true")

    # ── topic-detail ──
    p = sub.add_parser("topic-detail", help="专题详情")
    p.add_argument("--topic-id", type=int, required=True, dest="topic_id")
    p.add_argument("--increase", action="store_true", default=False,
                   help="是否增加浏览计数")
    p.add_argument("--raw", action="store_true")

    # ── topic-tables ──
    p = sub.add_parser("topic-tables", help="专题内的表列表")
    p.add_argument("--topic-id", type=int, required=True, dest="topic_id")
    p.add_argument("--search-key", default=None)
    p.add_argument("--raw", action="store_true")

    # ── set-cookie ──
    p = sub.add_parser("set-cookie", help="写入 Cookie 到 assets/.cookie（供 SSO skill 或手动调用）")
    p.add_argument("cookie", help="Cookie 字符串")

    args = parser.parse_args()

    import requests as _requests
    try:
        if args.command == "search-tables":
            core = True if args.core_table else None
            hq = True if args.is_high_quality else None
            r = search_tables(
                search_key=args.search_key, catalog=args.catalog,
                cluster_name=args.cluster_name, db_name=args.db_name,
                table_name=args.table_name, core_table=core,
                is_high_quality=hq, owner_id=args.owner_id,
                env=args.env,
                page=args.page, size=args.size,
            )
            print(json.dumps(r, ensure_ascii=False, indent=2) if args.raw else fmt_search_tables(r, args.page))

        elif args.command == "search-columns":
            r = search_columns(
                search_key=args.search_key, catalog=args.catalog,
                cluster_name=args.cluster_name, db_name=args.db_name,
                table_name=args.table_name, page=args.page, size=args.size,
            )
            print(json.dumps(r, ensure_ascii=False, indent=2) if args.raw else fmt_search_columns(r, args.page))

        elif args.command == "table-detail":
            r = get_table_detail(
                args.catalog, args.cluster_name, args.db_name, args.table_name,
                detail_types=args.detail_types,
            )
            print(json.dumps(r, ensure_ascii=False, indent=2) if args.raw else fmt_table_detail(r))

        elif args.command == "preview":
            if args.raw:
                raw_str = preview_data_raw(args.table_id, args.size)
                try:
                    parsed = json.loads(raw_str)
                    print(json.dumps(parsed, ensure_ascii=False, indent=2))
                except Exception:
                    print(raw_str)
            else:
                md, _ = preview_data(args.table_id, args.size)
                print(md)

        elif args.command == "lineage":
            qn = args.qualified_name
            if not qn:
                if args.catalog and args.db_name and args.table_name and args.cluster_name:
                    qn = build_qualified_name(args.catalog, args.db_name, args.table_name, args.cluster_name)
                else:
                    print("错误：请提供 --qualified-name 或 --catalog/--db/--table/--cluster", file=sys.stderr)
                    sys.exit(1)
            r = get_lineage_chain(qn, up_lvl=args.up_lvl, down_lvl=args.down_lvl)
            print(json.dumps(r, ensure_ascii=False, indent=2) if args.raw else fmt_lineage_chain(r, args.up_lvl, args.down_lvl))

        elif args.command == "search-topics":
            r = search_topics(search_key=args.search_key, page=args.page, size=args.size)
            print(json.dumps(r, ensure_ascii=False, indent=2) if args.raw else fmt_search_topics(r, args.page))

        elif args.command == "topic-detail":
            r = get_topic_detail(args.topic_id, increase=args.increase)
            print(json.dumps(r, ensure_ascii=False, indent=2) if args.raw else fmt_topic_detail(r))

        elif args.command == "topic-tables":
            r = get_topic_tables(args.topic_id, search_key=args.search_key)
            print(json.dumps(r, ensure_ascii=False, indent=2) if args.raw else fmt_topic_tables(r))

        elif args.command == "set-cookie":
            save_cookie(args.cookie)

    except NoCookieError as e:
        print(f"[鉴权失败] {e}")
        print()
        print("请通过以下任一方式提供 Cookie 后重试：")
        print("  方式 A（自动）：调用 data-fe-common-sso skill 获取 Cookie，")
        print(f"    然后执行: python tools.py set-cookie \"<cookie>\"")
        print("  方式 B（手动）：让用户从浏览器开发者工具复制 Cookie，")
        print(f"    然后执行: python tools.py set-cookie \"<cookie>\"")
        sys.exit(2)
    except _requests.HTTPError as e:
        print(f"HTTP 错误: {e}", file=sys.stderr)
        if e.response is not None:
            print(e.response.text[:500], file=sys.stderr)
        sys.exit(1)
    except RuntimeError as e:
        print(f"错误: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
