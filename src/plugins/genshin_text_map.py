import re
import sqlite3
import urllib.parse
from pathlib import Path

import httpx
from nonebot import on_regex
from nonebot.adapters.onebot.v11 import Bot, Event

# === 配置 ===
DB_PATH = Path(__file__).parent / "data" / "genshin_text.db"
WIKI_API = "https://wiki.biligame.com/ys/api.php"
TRUNCATE_LEN = 60
PAGE_LIMIT = 5  # 每页显示数量

class DB:
    def conn(self):
        c = sqlite3.connect(DB_PATH, check_same_thread=False)
        # 开启 WAL 模式和缓存优化，大幅提升读取性能
        c.execute("PRAGMA journal_mode=WAL;")
        c.execute("PRAGMA cache_size=-2000;") # 使用约 2MB 缓存

        return c

    def get_by_id(self, text_id, table="text_map", id_col="id"):
        """获取全文"""
        if not DB_PATH.exists(): return None
        tables_to_check = [
            ("text_map", "id", "普通文本"),
            ("readable", "filename", "书籍文档"),
            ("subtitle", "filename", "剧情字幕")
        ]
        with self.conn() as c:
            cur = c.cursor()
            for t_name, col_name, source_name in tables_to_check:
                cur.execute(f"SELECT {col_name}, chs, jp FROM {t_name} WHERE {col_name} = ?", (text_id,))
                row = cur.fetchone()
                if row:
                    return {"id": row[0], "cn": row[1], "jp": row[2], "source": source_name}
        return None

    def search(self, col, kw, table="all", page=1):
        """
        极速搜索模式
        返回: (results, has_next)
        """
        if not DB_PATH.exists(): return [], False

        kw = kw.strip()
        offset = (page - 1) * PAGE_LIMIT

        # 优化策略：尝试多获取 1 条数据 (PAGE_LIMIT + 1)
        # 如果能获取到，说明有下一页；否则说明到了末尾。
        # 这样就不需要运行昂贵的 COUNT(*) 查询了。
        fetch_limit = PAGE_LIMIT + 1

        if table == "all":
            targets = [("text_map", "id", "map"), ("readable", "filename", "read"), ("subtitle", "filename", "sub")]
        elif table == "readable":
            targets = [("readable", "filename", "read")]
        elif table == "subtitle":
            targets = [("subtitle", "filename", "sub")]
        else:
            targets = [("text_map", "id", "map")]

        try:
            with self.conn() as c:
                cur = c.cursor()
                queries = []
                params = []

                kw_lower = kw.lower()
                kw_like = f"%{kw}%"
                kw_compact = kw.replace(" ", "")
                kw_compact_lower = kw_compact.lower()
                fuzzy_pattern = None
                if len(kw_compact) > 1:
                    fuzzy_pattern = f"%{'%'.join(kw_compact)}%"

                for t_name, id_col, source in targets:
                    if fuzzy_pattern:
                        q = (
                            f"SELECT {id_col} as id, chs, jp, '{source}' as source, "
                            f"CASE "
                            f"WHEN LOWER({col}) = ? OR LOWER(REPLACE({col}, ' ', '')) = ? THEN 0 "
                            f"WHEN LOWER({col}) LIKE LOWER(?) THEN 1 "
                            f"ELSE 2 END AS match_rank FROM {t_name} "
                            f"WHERE LOWER({col}) LIKE LOWER(?) OR LOWER(REPLACE({col}, ' ', '')) LIKE LOWER(?)"
                        )
                        queries.append(q)
                        params.extend([kw_lower, kw_compact_lower, kw_like, kw_like, fuzzy_pattern])
                    else:
                        q = (
                            f"SELECT {id_col} as id, chs, jp, '{source}' as source, "
                            f"CASE WHEN LOWER({col}) = ? THEN 0 ELSE 1 END AS match_rank FROM {t_name} "
                            f"WHERE LOWER({col}) LIKE LOWER(?)"
                        )
                        queries.append(q)
                        params.extend([kw_lower, kw_like])

                full_sql = " UNION ALL ".join(queries)
                full_sql = f"SELECT id, chs, jp, source FROM ({full_sql}) ORDER BY match_rank, id LIMIT ? OFFSET ?"
                params.append(fetch_limit)
                params.append(offset)

                cur.execute(full_sql, params)

                raw_results = cur.fetchall()
                results = []

                # 判断是否有下一页
                has_next = False
                if len(raw_results) > PAGE_LIMIT:
                    has_next = True
                    # 切片，只保留前 PAGE_LIMIT 条
                    raw_results = raw_results[:PAGE_LIMIT]

                for r in raw_results:
                    cn_text = r[1] if r[1] else ""
                    jp_text = r[2] if r[2] else ""
                    if len(cn_text) > TRUNCATE_LEN: cn_text = cn_text[:TRUNCATE_LEN] + "..."
                    if len(jp_text) > TRUNCATE_LEN: jp_text = jp_text[:TRUNCATE_LEN] + "..."
                    results.append({"id": r[0], "cn": cn_text, "jp": jp_text, "source": r[3]})

                return results, has_next

        except Exception as e:
            return [{"id":"ERROR", "cn":f"错误: {e}", "jp":"", "source":"error"}], False

db = DB()

def clean_html(raw_html):
    if not raw_html: return ""
    text = re.sub(r'<(script|style).*?>.*?</\1>', '', raw_html, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    return re.sub(r'\s+', ' ', text).strip()

async def search_wiki(kw):
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            search_res = await client.get(WIKI_API, params={"action":"query","list":"search","srsearch":kw,"format":"json"})
            search_data = search_res.json()
            if not search_data["query"]["search"]: return None

            title = search_data["query"]["search"][0]["title"]
            parse_res = await client.get(WIKI_API, params={"action": "parse", "page": title, "prop": "text", "format": "json", "redirects": True})
            parse_data = parse_res.json()
            if "error" in parse_data: return None

            html = parse_data["parse"]["text"]["*"]
            final_title = parse_data["parse"]["title"]
            desc = ""
            bot_match = re.search(r'<div[^>]*class="[^"]*wiki-bot[^"]*"[^>]*>(.*?)</div>', html, re.DOTALL)
            if bot_match:
                desc = clean_html(bot_match.group(1))
                if len(desc) > 200: desc = desc[:200] + "..."

            safe_title = urllib.parse.quote(final_title)
            return {"t": final_title, "d": desc, "url": f"https://wiki.biligame.com/ys/{safe_title}"}
        except: return "ERROR"

def parse_cmd_args(text, prefix):
    raw = re.sub(f"^{prefix}\s*", "", text).strip()
    if not raw: return None, 1
    match = re.search(r'^(.*)\s+(\d+)$', raw)
    if match: return match.group(1), int(match.group(2))
    return raw, 1

# === 指令 ===
help_cmd = on_regex(r"^#help\s*$", priority=5, block=True)
jp_cmd = on_regex(r"^#jp\s*(.+)$", priority=5, block=True)
cn_cmd = on_regex(r"^#cn\s*(.+)$", priority=5, block=True)
id_cmd = on_regex(r"^#id\s*(.+)$", priority=5, block=True)
wiki_cmd = on_regex(r"^#wiki\s*(.+)$", priority=5, block=True)
read_cmd = on_regex(r"^#read\s*(.+)$", priority=5, block=True)
sub_cmd = on_regex(r"^#sub\s*(.+)$", priority=5, block=True)

@help_cmd.handle()
async def _(event: Event):
    msg = """🤖 原神文本查询助手 (极速版)
━━━━━━━━━━━━━━
🔍 基础搜索（支持模糊匹配，精确匹配优先）
• #jp <关键词> [页码]
• #cn <关键词> [页码]

📚 分类搜索
• #read <关键词> : 仅搜书籍
• #sub <关键词>  : 仅搜剧情

🔢 其他
• #id <ID/文件名> : 查全文
• #wiki <关键词> : 查Wiki

💡 提示
• 结果按“精确匹配 → 普通匹配 → 模糊匹配”排序
• 为了提升速度，不再显示总条数
"""
    await help_cmd.finish(msg)

@jp_cmd.handle()
async def _(event: Event):
    kw, page = parse_cmd_args(event.get_plaintext().strip(), "#jp")
    if not kw: return
    res, has_next = db.search("jp", kw, table="all", page=page)
    await send(jp_cmd, res, has_next, cmd="#jp", kw=kw, page=page)

@cn_cmd.handle()
async def _(event: Event):
    kw, page = parse_cmd_args(event.get_plaintext().strip(), "#cn")
    if not kw: return
    res, has_next = db.search("chs", kw, table="all", page=page)
    await send(cn_cmd, res, has_next, cmd="#cn", kw=kw, page=page)

@read_cmd.handle()
async def _(event: Event):
    kw, page = parse_cmd_args(event.get_plaintext().strip(), "#read")
    if not kw: return
    res, has_next = db.search("chs", kw, table="readable", page=page)
    await send(read_cmd, res, has_next, cmd="#read", kw=kw, page=page)

@sub_cmd.handle()
async def _(event: Event):
    kw, page = parse_cmd_args(event.get_plaintext().strip(), "#sub")
    if not kw: return
    res, has_next = db.search("chs", kw, table="subtitle", page=page)
    await send(sub_cmd, res, has_next, cmd="#sub", kw=kw, page=page)

@wiki_cmd.handle()
async def _(event: Event):
    kw = re.sub(r"^#wiki\s*", "", event.get_plaintext().strip()).strip()
    await wiki_cmd.send(f"🌐 Wiki: {kw}")
    res = await search_wiki(kw)
    if not res: await wiki_cmd.finish("Wiki 未收录")
    elif res == "ERROR": await wiki_cmd.finish("网络超时")
    msg = f"📖 {res['t']}"
    if res['d']: msg += f"\n{res['d']}"
    msg += f"\n🔗 {res['url']}"
    await wiki_cmd.finish(msg)

@id_cmd.handle()
async def _(event: Event):
    raw_id = re.sub(r"^#id\s*", "", event.get_plaintext().strip()).strip()
    res = db.get_by_id(raw_id)
    if not res: await id_cmd.finish(f"未在任何库中找到 ID: {raw_id}")

    source = res.get('source', '未知来源')
    id_label = "📄 文件名" if source in ["书籍文档", "剧情字幕"] else "🆔 ID"
    msg_jp = res['jp'][:1000] + "..." if len(res['jp']) > 1000 else res['jp']
    msg_cn = res['cn'][:1000] + "..." if len(res['cn']) > 1000 else res['cn']

    msg = f"📁 来源: {source}\n{id_label}: {res['id']}\n" \
          f"🇯🇵 JP:\n{msg_jp}\n{'-'*15}\n🇨🇳 CN:\n{msg_cn}"
    await id_cmd.finish(msg)

async def send(matcher, res, has_next, cmd="", kw="", page=1):
    if not res:
        if page > 1: await matcher.finish(f"没有更多结果了 (当前第 {page} 页)")
        else: await matcher.finish("未找到匹配项")

    if res[0]['id'] == "ERROR":
        await matcher.finish(f"⚠️ {res[0]['cn']}")
        return

    msg_list = [f"🔍 结果 (第 {page} 页):"]

    for i in res:
        source = i.get('source', 'map')
        if source == 'read': icon = "📄"
        elif source == 'sub': icon = "🎬"
        else: icon = "🆔"
        msg_list.append(f"{icon} {i['id']}\n🇯🇵 {i['jp']}\n🇨🇳 {i['cn']}\n{'='*10}")

    if has_next:
        msg_list.append(f"👉 下一页: {cmd} {kw} {page+1}")
    else:
        msg_list.append("🏁 (已显示全部)")

    msg_list.append("💡 全文: #id <ID>")

    await matcher.finish("\n".join(msg_list))
