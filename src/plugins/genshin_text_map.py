import re
import sqlite3
import urllib.parse
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, TypedDict

import httpx
from data.search_index import (
    SEARCH_DOCS_TABLE,
    SEARCH_FTS_TABLE,
    SOURCE_TABLES,
    TABLE_SCOPES,
    SearchStage,
    build_search_stages,
    configure_connection,
    normalize_storage_text,
)
from nonebot import on_regex
from nonebot.adapters.onebot.v11 import Event

# === 配置 ===
DB_PATH = Path(__file__).parent / "data" / "genshin_text.db"
WIKI_API = "https://wiki.biligame.com/ys/api.php"
TRUNCATE_LEN = 60
PAGE_LIMIT = 5
MAX_WIKI_DESC_LEN = 200
MAX_DETAIL_LEN = 1000


class SearchDocument(TypedDict):
    id: str
    cn: str
    jp: str


class SearchResult(SearchDocument, total=False):
    source: str


class WikiResult(TypedDict):
    t: str
    d: str
    url: str


WikiSearchResponse = WikiResult | Literal["ERROR"] | None


@dataclass(frozen=True)
class SendContext:
    cmd: str = ""
    kw: str = ""
    page: int = 1


class DB:
    def conn(self) -> sqlite3.Connection:
        return configure_connection(sqlite3.connect(DB_PATH, check_same_thread=False))

    def get_by_id(self, text_id: str) -> SearchResult | None:
        if not DB_PATH.exists():
            return None

        with self.conn() as conn:
            cursor = conn.cursor()
            for table_name, config in SOURCE_TABLES.items():
                key_col = config["key_col"]
                row = cursor.execute(
                    f"SELECT {key_col}, chs, jp FROM {table_name} WHERE {key_col} = ?",
                    (text_id,),
                ).fetchone()
                if row:
                    return {
                        "id": str(row[0]),
                        "cn": normalize_storage_text(row[1]),
                        "jp": normalize_storage_text(row[2]),
                        "source": config["source_name"],
                    }
        return None

    def search(
        self,
        col: str,
        kw: str,
        table: str = "all",
        page: int = 1,
    ) -> tuple[list[SearchResult], bool]:
        if not DB_PATH.exists():
            return [], False

        keyword = kw.strip()
        if not keyword:
            return [], False

        source_tables = TABLE_SCOPES.get(table, TABLE_SCOPES["text_map"])
        stages = build_search_stages(col, keyword)
        if not stages:
            return [], False

        fetch_limit = PAGE_LIMIT + 1
        offset = max(page - 1, 0) * PAGE_LIMIT

        try:
            with self.conn() as conn:
                cursor = conn.cursor()
                if not self._has_search_index(cursor):
                    return (
                        [
                            {
                                "id": "ERROR",
                                "cn": "搜索索引不存在，请先运行导入脚本重建数据库。",
                                "jp": "",
                                "source": "error",
                            }
                        ],
                        False,
                    )

                rows = self._query_ranked_docs(
                    cursor=cursor,
                    stages=stages,
                    source_tables=source_tables,
                    limit=fetch_limit,
                    offset=offset,
                )

                has_next = len(rows) > PAGE_LIMIT
                if has_next:
                    rows = rows[:PAGE_LIMIT]

                results = []
                for row in rows:
                    doc = self._fetch_source_doc(
                        cursor,
                        row["source_table"],
                        row["source_key"],
                    )
                    if not doc:
                        continue
                    results.append(
                        {
                            "id": doc["id"],
                            "cn": safe_truncate(doc["cn"], TRUNCATE_LEN),
                            "jp": safe_truncate(doc["jp"], TRUNCATE_LEN),
                            "source": row["source_tag"],
                        }
                    )

                return results, has_next
        except (sqlite3.DatabaseError, sqlite3.OperationalError) as exc:
            return (
                [{"id": "ERROR", "cn": f"髞呵ｯｯ: {exc}", "jp": "", "source": "error"}],
                False,
            )

    def _has_search_index(self, cursor: sqlite3.Cursor) -> bool:
        tables = {
            row[0]
            for row in cursor.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type IN ('table', 'view')
                """
            ).fetchall()
        }
        return SEARCH_DOCS_TABLE in tables and SEARCH_FTS_TABLE in tables

    def _query_ranked_docs(
        self,
        cursor: sqlite3.Cursor,
        stages: Sequence[SearchStage],
        source_tables: tuple[str, ...],
        limit: int,
        offset: int,
    ) -> list[sqlite3.Row]:
        source_filter = ", ".join("?" for _ in source_tables)
        stage_sql: list[str] = []
        params: list[object] = []

        for stage in stages:
            stage_sql.append(
                f"""
                SELECT
                    docs.doc_id,
                    docs.source_table,
                    docs.source_key,
                    docs.source_tag,
                    ? AS rank_bucket,
                    bm25({SEARCH_FTS_TABLE}) AS score
                FROM {SEARCH_FTS_TABLE}
                JOIN {SEARCH_DOCS_TABLE} AS docs
                    ON docs.doc_id = {SEARCH_FTS_TABLE}.rowid
                WHERE docs.source_table IN ({source_filter})
                  AND {SEARCH_FTS_TABLE} MATCH ?
                """
            )
            params.append(stage.rank)
            params.extend(source_tables)
            params.append(stage.match_query)

        sql = f"""
        WITH ranked AS (
            {" UNION ALL ".join(stage_sql)}
        ),
        dedup AS (
            SELECT
                doc_id,
                source_table,
                source_key,
                source_tag,
                MIN(rank_bucket) AS rank_bucket,
                MIN(score) AS score
            FROM ranked
            GROUP BY doc_id, source_table, source_key, source_tag
        )
        SELECT doc_id, source_table, source_key, source_tag
        FROM dedup
        ORDER BY rank_bucket, score, source_tag, source_key
        LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        return cursor.execute(sql, params).fetchall()

    def _fetch_source_doc(
        self,
        cursor: sqlite3.Cursor,
        table_name: str,
        source_key: str,
    ) -> SearchDocument | None:
        config = SOURCE_TABLES.get(table_name)
        if config is None:
            return None

        key_col = config["key_col"]
        row = cursor.execute(
            f"SELECT {key_col}, chs, jp FROM {table_name} WHERE {key_col} = ?",
            (source_key,),
        ).fetchone()
        if row is None:
            return None

        return {
            "id": str(row[0]),
            "cn": normalize_storage_text(row[1]),
            "jp": normalize_storage_text(row[2]),
        }


db = DB()


def safe_truncate(text: str, limit: int) -> str:
    normalized = normalize_storage_text(text)
    if len(normalized) <= limit:
        return normalized
    return normalized[:limit] + "..."


def clean_html(raw_html: str) -> str:
    if not raw_html:
        return ""
    text = re.sub(r"<(script|style).*?>.*?</\1>", "", raw_html, flags=re.DOTALL)
    text = re.sub(r"<br\s*/?>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    return re.sub(r"\s+", " ", text).strip()


async def search_wiki(kw: str) -> WikiSearchResponse:
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            search_res = await client.get(
                WIKI_API,
                params={
                    "action": "query",
                    "list": "search",
                    "srsearch": kw,
                    "format": "json",
                },
            )
            search_res.raise_for_status()
            search_data = search_res.json()
            if not search_data["query"]["search"]:
                return None

            title = search_data["query"]["search"][0]["title"]
            parse_res = await client.get(
                WIKI_API,
                params={
                    "action": "parse",
                    "page": title,
                    "prop": "text",
                    "format": "json",
                    "redirects": True,
                },
            )
            parse_res.raise_for_status()
            parse_data = parse_res.json()
            if "error" in parse_data:
                return None

            html = parse_data["parse"]["text"]["*"]
            final_title = parse_data["parse"]["title"]
            desc = ""
            bot_match = re.search(
                r'<div[^>]*class="[^"]*wiki-bot[^"]*"[^>]*>(.*?)</div>',
                html,
                re.DOTALL,
            )
            if bot_match:
                desc = clean_html(bot_match.group(1))
                if len(desc) > MAX_WIKI_DESC_LEN:
                    desc = desc[:MAX_WIKI_DESC_LEN] + "..."

            safe_title = urllib.parse.quote(final_title)
        except (httpx.HTTPError, KeyError, TypeError, ValueError):
            return "ERROR"
        else:
            return {
                "t": final_title,
                "d": desc,
                "url": f"https://wiki.biligame.com/ys/{safe_title}",
            }


def parse_cmd_args(text: str, prefix: str) -> tuple[str | None, int]:
    raw = re.sub(f"^{prefix}\\s*", "", text).strip()
    if not raw:
        return None, 1
    match = re.search(r"^(.*)\s+(\d+)$", raw)
    if match:
        return match.group(1), int(match.group(2))
    return raw, 1


help_cmd = on_regex(r"^#help\s*$", priority=5, block=True)
jp_cmd = on_regex(r"^#jp\s*(.+)$", priority=5, block=True)
cn_cmd = on_regex(r"^#cn\s*(.+)$", priority=5, block=True)
srjp_cmd = on_regex(r"^#srjp\s*(.+)$", priority=5, block=True)
srcn_cmd = on_regex(r"^#srcn\s*(.+)$", priority=5, block=True)
id_cmd = on_regex(r"^#id\s*(.+)$", priority=5, block=True)
wiki_cmd = on_regex(r"^#wiki\s*(.+)$", priority=5, block=True)
read_cmd = on_regex(r"^#read\s*(.+)$", priority=5, block=True)
sub_cmd = on_regex(r"^#sub\s*(.+)$", priority=5, block=True)


@help_cmd.handle()
async def _(_event: Event):
    msg = """🤖 原神文本查询助手（FTS版）
━━━━━━━━━━━━━━
🔍 基础搜索
• #jp <关键词> [页码]
• #cn <关键词> [页码]

📚 分类搜索
• #read <关键词> : 仅搜书籍
• #sub <关键词>  : 仅搜字幕

🔢 其他
• #id <ID/文件名> : 查全文
• #wiki <关键词> : 查Wiki
━━━━━━━━━━━━━━
🚄 星铁文本搜索
• #srjp <关键词> [页码]
• #srcn <关键词> [页码]

💡 提示
• #jp 默认支持日文汉字与假名互查
• 结果按“原文精确 → 读音精确 → 前缀匹配”排序
"""
    await help_cmd.finish(msg)


@jp_cmd.handle()
async def _(event: Event):
    kw, page = parse_cmd_args(event.get_plaintext().strip(), "#jp")
    if not kw:
        return
    res, has_next = db.search("jp", kw, table="all", page=page)
    await send(
        jp_cmd,
        res,
        has_next=has_next,
        context=SendContext(cmd="#jp", kw=kw, page=page),
    )


@cn_cmd.handle()
async def _(event: Event):
    kw, page = parse_cmd_args(event.get_plaintext().strip(), "#cn")
    if not kw:
        return
    res, has_next = db.search("chs", kw, table="all", page=page)
    await send(
        cn_cmd,
        res,
        has_next=has_next,
        context=SendContext(cmd="#cn", kw=kw, page=page),
    )


@srjp_cmd.handle()
async def _(event: Event):
    kw, page = parse_cmd_args(event.get_plaintext().strip(), "#srjp")
    if not kw:
        return
    res, has_next = db.search("jp", kw, table="sr_text_map", page=page)
    await send(
        srjp_cmd,
        res,
        has_next=has_next,
        context=SendContext(cmd="#srjp", kw=kw, page=page),
    )


@srcn_cmd.handle()
async def _(event: Event):
    kw, page = parse_cmd_args(event.get_plaintext().strip(), "#srcn")
    if not kw:
        return
    res, has_next = db.search("chs", kw, table="sr_text_map", page=page)
    await send(
        srcn_cmd,
        res,
        has_next=has_next,
        context=SendContext(cmd="#srcn", kw=kw, page=page),
    )


@read_cmd.handle()
async def _(event: Event):
    kw, page = parse_cmd_args(event.get_plaintext().strip(), "#read")
    if not kw:
        return
    res, has_next = db.search("chs", kw, table="readable", page=page)
    await send(
        read_cmd,
        res,
        has_next=has_next,
        context=SendContext(cmd="#read", kw=kw, page=page),
    )


@sub_cmd.handle()
async def _(event: Event):
    kw, page = parse_cmd_args(event.get_plaintext().strip(), "#sub")
    if not kw:
        return
    res, has_next = db.search("chs", kw, table="subtitle", page=page)
    await send(
        sub_cmd,
        res,
        has_next=has_next,
        context=SendContext(cmd="#sub", kw=kw, page=page),
    )


@wiki_cmd.handle()
async def _(event: Event):
    kw = re.sub(r"^#wiki\s*", "", event.get_plaintext().strip()).strip()
    await wiki_cmd.send(f"🌐 Wiki: {kw}")
    res = await search_wiki(kw)
    if res is None:
        await wiki_cmd.finish("Wiki 未收录")
        return
    if res == "ERROR":
        await wiki_cmd.finish("网络超时")
        return
    msg = f"📖 {res['t']}"
    if res["d"]:
        msg += f"\n{res['d']}"
    msg += f"\n🔗 {res['url']}"
    await wiki_cmd.finish(msg)


@id_cmd.handle()
async def _(event: Event):
    raw_id = re.sub(r"^#id\s*", "", event.get_plaintext().strip()).strip()
    res = db.get_by_id(raw_id)
    if not res:
        await id_cmd.finish(f"未在任何库中找到 ID: {raw_id}")
        return

    source = res.get("source", "未知来源")
    id_label = "📄 文件名" if source in ["书籍文档", "剧情字幕"] else "🆔 ID"
    msg_jp = (
        res["jp"][:MAX_DETAIL_LEN] + "..."
        if len(res["jp"]) > MAX_DETAIL_LEN
        else res["jp"]
    )
    msg_cn = (
        res["cn"][:MAX_DETAIL_LEN] + "..."
        if len(res["cn"]) > MAX_DETAIL_LEN
        else res["cn"]
    )

    msg = (
        f"📁 来源: {source}\n{id_label}: {res['id']}\n"
        f"🇯🇵 JP:\n{msg_jp}\n{'-' * 15}\n🇨🇳 CN:\n{msg_cn}"
    )
    await id_cmd.finish(msg)


async def send(
    matcher: Any,
    res: Sequence[SearchResult],
    *,
    has_next: bool,
    context: SendContext,
) -> None:
    page = context.page
    if not res:
        if page > 1:
            await matcher.finish(f"没有更多结果了 (当前第 {page} 页)")
        else:
            await matcher.finish("未找到匹配项")
        return

    if res[0]["id"] == "ERROR":
        await matcher.finish(f"⚠️ {res[0]['cn']}")
        return

    msg_list = [f"🔍 结果 (第 {page} 页):"]

    for item in res:
        source = item.get("source", "map")
        if source == "read":
            icon = "📄"
        elif source == "sub":
            icon = "🎬"
        else:
            icon = "🆔"
        msg_list.append(
            f"{icon} {item['id']}\n🇯🇵 {item['jp']}\n🇨🇳 {item['cn']}\n{'=' * 10}"
        )

    if has_next:
        msg_list.append(f"👉 下一页: {context.cmd} {context.kw} {page + 1}")
    else:
        msg_list.append("🏁 (已显示全部)")

    msg_list.append("💡 全文: #id <ID>")
    await matcher.finish("\n".join(msg_list))
