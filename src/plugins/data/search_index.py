import json
import re
import sqlite3
import time
import unicodedata
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from functools import lru_cache
from hashlib import blake2b
from importlib import import_module
from pathlib import Path
from typing import Any

SOURCE_TABLES = {
    "text_map": {
        "key_col": "id",
        "source_tag": "map",
        "source_name": "普通文本",
    },
    "sr_text_map": {
        "key_col": "id",
        "source_tag": "srmap",
        "source_name": "星铁文本",
    },
    "readable": {
        "key_col": "filename",
        "source_tag": "read",
        "source_name": "书籍文档",
    },
    "subtitle": {
        "key_col": "filename",
        "source_tag": "sub",
        "source_name": "剧情字幕",
    },
}

TABLE_SCOPES = {
    "all": ("text_map", "readable", "subtitle"),
    "text_map": ("text_map",),
    "sr_text_map": ("sr_text_map",),
    "readable": ("readable",),
    "subtitle": ("subtitle",),
}

SEARCH_DOCS_TABLE = "search_docs"
SEARCH_FTS_TABLE = "search_fts"
SEARCH_TERM_CACHE_TABLE = "search_term_cache"
DEFAULT_PERSIST_TERM_CACHE = False
READING_OVERRIDE_PATH = Path(__file__).with_name("jp_reading_overrides.json")
INVISIBLE_CHAR_CODEPOINTS = "\ufeff\u200b\u200c\u200d\u200e\u200f\u2060"
INVISIBLE_CHARS = dict.fromkeys(map(ord, INVISIBLE_CHAR_CODEPOINTS), None)
TOKEN_RE = re.compile(r"\S+")
LATIN_RE = re.compile(r"[A-Za-z0-9_]+")
NON_TOKEN_CHARS_RE = re.compile(r"""["'`]+""")
JP_OR_CN_RE = re.compile(r"[\u3040-\u30ff\u3400-\u9fff]+")
HIRAGANA_RE = re.compile(r"[\u3041-\u3096]+")
KATAKANA_RE = re.compile(r"[\u30a1-\u30f6]+")
REPLACEMENT_CHAR = "\ufffd"
MAX_INDEX_TOKENS = 4000
MAX_QUERY_TOKENS = 8
MIN_ROWS_FOR_EMPTY_DATASET_CHECK = 20
MAX_EMPTY_ROW_RATIO = 0.98
REINDEX_FETCH_BATCH_SIZE = 10000
REINDEX_INSERT_BATCH_SIZE = 10000
TERM_CACHE_SIZE = 300000
KATAKANA_START = 0x30A1
KATAKANA_END = 0x30F6
KATAKANA_TO_HIRAGANA_OFFSET = 0x60
MIN_BIGRAM_SOURCE_LENGTH = 2

SearchRow = tuple[str, str | None, str | None]
ReindexProgressCallback = Callable[[str, int, int, float], None]
DocInsertRow = tuple[int, str, str, str]
FtsInsertRow = tuple[int, str, str, str]
CacheInsertRow = tuple[str, str, str, str, str, str, str, str]


class CorruptedChineseTextError(ValueError):
    def __init__(self, label: str, empty_rows: int, total_rows: int) -> None:
        message = (
            f"{label} Chinese text is unexpectedly empty "
            f"({empty_rows}/{total_rows} rows); "
            "stop importing to avoid persisting corrupted content."
        )
        super().__init__(message)


@dataclass(frozen=True)
class SearchStage:
    rank: int
    match_query: str


@dataclass(frozen=True)
class IndexedTerms:
    chs_digest: str
    jp_digest: str
    chs_terms: str
    jp_terms: str
    jp_reading_terms: str
    cache_needs_update: bool

    def has_terms(self) -> bool:
        return bool(self.chs_terms or self.jp_terms or self.jp_reading_terms)


@dataclass
class ReindexContext:
    write_cursor: sqlite3.Cursor
    doc_rows: list[DocInsertRow]
    fts_rows: list[FtsInsertRow]
    cache_rows: list[CacheInsertRow]
    persist_term_cache: bool
    fetch_batch_size: int
    insert_batch_size: int
    overrides_digest: str
    cached_chinese_terms: Callable[[str], str]
    cached_japanese_terms: Callable[[str], tuple[str, str]]


def configure_connection(conn: sqlite3.Connection) -> sqlite3.Connection:
    conn.text_factory = str
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA cache_size=-2000;")
    conn.execute("PRAGMA busy_timeout=3000;")
    return conn


def normalize_storage_text(text: str | None) -> str:
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKC", text).translate(INVISIBLE_CHARS)
    return normalized.replace("\r\n", "\n").replace("\r", "\n").strip()


def normalize_search_text(text: str | None) -> str:
    normalized = normalize_storage_text(text)
    return re.sub(r"\s+", " ", normalized).strip()


def read_utf8_text(path: Path) -> str:
    return normalize_storage_text(path.read_text(encoding="utf-8-sig"))


def load_json_utf8(path: Path) -> dict[str, str]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def contains_suspicious_text(text: str) -> bool:
    return REPLACEMENT_CHAR in text


def split_clean_rows(
    rows: Sequence[SearchRow],
) -> tuple[list[SearchRow], list[str]]:
    clean_rows: list[SearchRow] = []
    rejected_ids: list[str] = []
    for source_key, chs_text, jp_text in rows:
        normalized_chs = normalize_storage_text(chs_text)
        normalized_jp = normalize_storage_text(jp_text)
        if (
            contains_suspicious_text(normalized_chs)
            or contains_suspicious_text(normalized_jp)
        ):
            rejected_ids.append(str(source_key))
            continue
        clean_rows.append((str(source_key), chs_text, jp_text))
    return clean_rows, rejected_ids


def validate_chinese_rows(
    rows: Sequence[SearchRow],
    label: str,
) -> None:
    if not rows:
        return

    empty_rows = 0
    for _, chs_text, _ in rows:
        normalized = normalize_storage_text(chs_text)
        if not normalized:
            empty_rows += 1

    empty_ratio = empty_rows / len(rows)
    if (
        len(rows) >= MIN_ROWS_FOR_EMPTY_DATASET_CHECK
        and empty_ratio >= MAX_EMPTY_ROW_RATIO
    ):
        raise CorruptedChineseTextError(
            label=label,
            empty_rows=empty_rows,
            total_rows=len(rows),
        )


def ensure_search_schema(
    conn: sqlite3.Connection,
    *,
    persist_term_cache: bool = DEFAULT_PERSIST_TERM_CACHE,
) -> None:
    cursor = conn.cursor()
    cursor.execute("DROP INDEX IF EXISTS idx_tm_chs")
    cursor.execute("DROP INDEX IF EXISTS idx_tm_jp")
    cursor.execute("DROP INDEX IF EXISTS idx_sr_tm_chs")
    cursor.execute("DROP INDEX IF EXISTS idx_sr_tm_jp")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SEARCH_DOCS_TABLE} (
            doc_id INTEGER PRIMARY KEY,
            source_table TEXT NOT NULL,
            source_key TEXT NOT NULL,
            source_tag TEXT NOT NULL
        )
        """
    )
    cursor.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_{SEARCH_DOCS_TABLE}_source
        ON {SEARCH_DOCS_TABLE}(source_table, source_key)
        """
    )
    cursor.execute(
        f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS {SEARCH_FTS_TABLE}
        USING fts5(
            chs_terms,
            jp_terms,
            jp_reading_terms,
            tokenize='unicode61 remove_diacritics 0',
            prefix='2 3',
            content='',
            columnsize=0
        )
        """
    )
    if persist_term_cache:
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SEARCH_TERM_CACHE_TABLE} (
                source_table TEXT NOT NULL,
                source_key TEXT NOT NULL,
                chs_digest TEXT NOT NULL,
                jp_digest TEXT NOT NULL,
                overrides_digest TEXT NOT NULL,
                chs_terms TEXT NOT NULL,
                jp_terms TEXT NOT NULL,
                jp_reading_terms TEXT NOT NULL,
                PRIMARY KEY (source_table, source_key)
            )
            """
        )
    else:
        cursor.execute(f"DROP TABLE IF EXISTS {SEARCH_TERM_CACHE_TABLE}")
    conn.commit()


def _clear_search_index(write_cursor: sqlite3.Cursor) -> None:
    write_cursor.execute(f"DELETE FROM {SEARCH_DOCS_TABLE}")
    write_cursor.execute(
        f"INSERT INTO {SEARCH_FTS_TABLE}({SEARCH_FTS_TABLE}) VALUES('delete-all')"
    )


def _build_term_caches(
    overrides: dict[str, str],
) -> tuple[Callable[[str], str], Callable[[str], tuple[str, str]]]:
    @lru_cache(maxsize=TERM_CACHE_SIZE)
    def cached_chinese_terms(text: str) -> str:
        return build_chinese_terms(text)

    @lru_cache(maxsize=TERM_CACHE_SIZE)
    def cached_japanese_terms(text: str) -> tuple[str, str]:
        return build_japanese_terms(text, overrides)

    return cached_chinese_terms, cached_japanese_terms


def _flush_reindex_batches(
    write_cursor: sqlite3.Cursor,
    doc_rows: list[DocInsertRow],
    fts_rows: list[FtsInsertRow],
    cache_rows: list[CacheInsertRow],
    *,
    persist_term_cache: bool,
) -> None:
    if not doc_rows and not cache_rows:
        return

    if doc_rows:
        write_cursor.executemany(
            f"""
            INSERT INTO {SEARCH_DOCS_TABLE}(
                doc_id, source_table, source_key, source_tag
            )
            VALUES (?, ?, ?, ?)
            """,
            doc_rows,
        )
        write_cursor.executemany(
            f"""
            INSERT INTO {SEARCH_FTS_TABLE}(
                rowid,
                chs_terms,
                jp_terms,
                jp_reading_terms
            )
            VALUES (?, ?, ?, ?)
            """,
            fts_rows,
        )

    if persist_term_cache and cache_rows:
        write_cursor.executemany(
            f"""
            INSERT INTO {SEARCH_TERM_CACHE_TABLE}(
                source_table,
                source_key,
                chs_digest,
                jp_digest,
                overrides_digest,
                chs_terms,
                jp_terms,
                jp_reading_terms
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_table, source_key) DO UPDATE SET
                chs_digest = excluded.chs_digest,
                jp_digest = excluded.jp_digest,
                overrides_digest = excluded.overrides_digest,
                chs_terms = excluded.chs_terms,
                jp_terms = excluded.jp_terms,
                jp_reading_terms = excluded.jp_reading_terms
            """,
            cache_rows,
        )

    doc_rows.clear()
    fts_rows.clear()
    cache_rows.clear()


def _iter_source_rows(
    conn: sqlite3.Connection,
    table_name: str,
    key_col: str,
    fetch_batch_size: int,
    *,
    persist_term_cache: bool,
) -> Iterable[sqlite3.Row]:
    read_cursor = conn.cursor()
    read_cursor.arraysize = fetch_batch_size
    if persist_term_cache:
        read_cursor.execute(
            f"""
            SELECT
                CAST(src.{key_col} AS TEXT) AS source_key,
                src.chs,
                src.jp,
                cache.chs_digest,
                cache.jp_digest,
                cache.overrides_digest,
                cache.chs_terms,
                cache.jp_terms,
                cache.jp_reading_terms
            FROM {table_name} AS src
            LEFT JOIN {SEARCH_TERM_CACHE_TABLE} AS cache
                ON cache.source_table = ?
               AND cache.source_key = CAST(src.{key_col} AS TEXT)
            """,
            (table_name,),
        )
    else:
        read_cursor.execute(
            f"""
            SELECT
                CAST(src.{key_col} AS TEXT) AS source_key,
                src.chs,
                src.jp,
                NULL AS chs_digest,
                NULL AS jp_digest,
                NULL AS overrides_digest,
                NULL AS chs_terms,
                NULL AS jp_terms,
                NULL AS jp_reading_terms
            FROM {table_name} AS src
            """
        )

    while True:
        batch = read_cursor.fetchmany(fetch_batch_size)
        if not batch:
            return
        yield from batch


def _cache_row_matches(
    row: sqlite3.Row,
    indexed_terms: IndexedTerms,
    overrides_digest: str,
) -> bool:
    return (
        row["chs_digest"] == indexed_terms.chs_digest
        and row["jp_digest"] == indexed_terms.jp_digest
        and row["overrides_digest"] == overrides_digest
        and row["chs_terms"] == indexed_terms.chs_terms
        and row["jp_terms"] == indexed_terms.jp_terms
        and row["jp_reading_terms"] == indexed_terms.jp_reading_terms
    )


def _resolve_chinese_terms(
    row: sqlite3.Row,
    chs_text: str,
    chs_digest: str,
    cached_chinese_terms: Callable[[str], str],
) -> tuple[str, bool]:
    chs_cache_fresh = row["chs_digest"] == chs_digest
    if chs_cache_fresh:
        return row["chs_terms"], True
    if not chs_text:
        return "", False
    return cached_chinese_terms(chs_text), False


def _resolve_japanese_terms(
    row: sqlite3.Row,
    jp_text: str,
    jp_digest: str,
    overrides_digest: str,
    cached_japanese_terms: Callable[[str], tuple[str, str]],
) -> tuple[str, str, bool]:
    jp_cache_fresh = (
        row["jp_digest"] == jp_digest
        and row["overrides_digest"] == overrides_digest
    )
    if jp_cache_fresh:
        return row["jp_terms"], row["jp_reading_terms"], True
    if not jp_text:
        return "", "", False
    jp_terms, jp_reading_terms = cached_japanese_terms(jp_text)
    return jp_terms, jp_reading_terms, False


def _build_indexed_terms(
    row: sqlite3.Row,
    *,
    overrides_digest: str,
    cached_chinese_terms: Callable[[str], str],
    cached_japanese_terms: Callable[[str], tuple[str, str]],
) -> IndexedTerms:
    chs_text = normalize_storage_text(row["chs"])
    jp_text = normalize_storage_text(row["jp"])
    chs_digest = digest_text(chs_text)
    jp_digest = digest_text(jp_text)

    if contains_suspicious_text(chs_text) or contains_suspicious_text(jp_text):
        empty_terms = IndexedTerms(
            chs_digest=chs_digest,
            jp_digest=jp_digest,
            chs_terms="",
            jp_terms="",
            jp_reading_terms="",
            cache_needs_update=False,
        )
        return IndexedTerms(
            chs_digest=empty_terms.chs_digest,
            jp_digest=empty_terms.jp_digest,
            chs_terms=empty_terms.chs_terms,
            jp_terms=empty_terms.jp_terms,
            jp_reading_terms=empty_terms.jp_reading_terms,
            cache_needs_update=not _cache_row_matches(
                row, empty_terms, overrides_digest
            ),
        )

    chs_terms, chs_cache_fresh = _resolve_chinese_terms(
        row, chs_text, chs_digest, cached_chinese_terms
    )
    jp_terms, jp_reading_terms, jp_cache_fresh = _resolve_japanese_terms(
        row,
        jp_text,
        jp_digest,
        overrides_digest,
        cached_japanese_terms,
    )
    return IndexedTerms(
        chs_digest=chs_digest,
        jp_digest=jp_digest,
        chs_terms=chs_terms,
        jp_terms=jp_terms,
        jp_reading_terms=jp_reading_terms,
        cache_needs_update=not (chs_cache_fresh and jp_cache_fresh),
    )


def _append_cache_row(
    cache_rows: list[CacheInsertRow],
    *,
    table_name: str,
    source_key: str,
    overrides_digest: str,
    indexed_terms: IndexedTerms,
) -> None:
    cache_rows.append(
        (
            table_name,
            source_key,
            indexed_terms.chs_digest,
            indexed_terms.jp_digest,
            overrides_digest,
            indexed_terms.chs_terms,
            indexed_terms.jp_terms,
            indexed_terms.jp_reading_terms,
        )
    )


def _reindex_source_table(
    conn: sqlite3.Connection,
    table_name: str,
    config: dict[str, str],
    context: ReindexContext,
    next_doc_id: int,
) -> tuple[int, int, int]:
    rows_read = 0
    rows_indexed = 0

    for row in _iter_source_rows(
        conn,
        table_name,
        config["key_col"],
        context.fetch_batch_size,
        persist_term_cache=context.persist_term_cache,
    ):
        rows_read += 1
        source_key = str(row["source_key"])
        indexed_terms = _build_indexed_terms(
            row,
            overrides_digest=context.overrides_digest,
            cached_chinese_terms=context.cached_chinese_terms,
            cached_japanese_terms=context.cached_japanese_terms,
        )

        if context.persist_term_cache and indexed_terms.cache_needs_update:
            _append_cache_row(
                context.cache_rows,
                table_name=table_name,
                source_key=source_key,
                overrides_digest=context.overrides_digest,
                indexed_terms=indexed_terms,
            )

        if not indexed_terms.has_terms():
            continue

        context.doc_rows.append(
            (next_doc_id, table_name, source_key, config["source_tag"])
        )
        context.fts_rows.append(
            (
                next_doc_id,
                indexed_terms.chs_terms,
                indexed_terms.jp_terms,
                indexed_terms.jp_reading_terms,
            )
        )
        next_doc_id += 1
        rows_indexed += 1

        if (
            len(context.doc_rows) >= context.insert_batch_size
            or len(context.cache_rows) >= context.insert_batch_size
        ):
            _flush_reindex_batches(
                context.write_cursor,
                context.doc_rows,
                context.fts_rows,
                context.cache_rows,
                persist_term_cache=context.persist_term_cache,
            )

    return next_doc_id, rows_read, rows_indexed


def rebuild_search_index(
    conn: sqlite3.Connection,
    *,
    persist_term_cache: bool = DEFAULT_PERSIST_TERM_CACHE,
    fetch_batch_size: int = REINDEX_FETCH_BATCH_SIZE,
    insert_batch_size: int = REINDEX_INSERT_BATCH_SIZE,
    progress_callback: ReindexProgressCallback | None = None,
) -> int:
    ensure_search_schema(conn, persist_term_cache=persist_term_cache)
    write_cursor = conn.cursor()
    _clear_search_index(write_cursor)

    overrides = load_reading_overrides()
    overrides_digest = compute_overrides_digest(overrides)
    cached_chinese_terms, cached_japanese_terms = _build_term_caches(overrides)
    doc_rows: list[DocInsertRow] = []
    fts_rows: list[FtsInsertRow] = []
    cache_rows: list[CacheInsertRow] = []
    context = ReindexContext(
        write_cursor=write_cursor,
        doc_rows=doc_rows,
        fts_rows=fts_rows,
        cache_rows=cache_rows,
        persist_term_cache=persist_term_cache,
        fetch_batch_size=fetch_batch_size,
        insert_batch_size=insert_batch_size,
        overrides_digest=overrides_digest,
        cached_chinese_terms=cached_chinese_terms,
        cached_japanese_terms=cached_japanese_terms,
    )
    doc_id = 1
    total_indexed = 0

    for table_name, config in SOURCE_TABLES.items():
        table_started_at = time.perf_counter()
        doc_id, rows_read, rows_indexed = _reindex_source_table(
            conn, table_name, config, context, doc_id
        )
        total_indexed += rows_indexed

        if progress_callback is not None:
            progress_callback(
                table_name,
                rows_read,
                rows_indexed,
                time.perf_counter() - table_started_at,
            )

    _flush_reindex_batches(
        write_cursor,
        doc_rows,
        fts_rows,
        cache_rows,
        persist_term_cache=persist_term_cache,
    )
    if total_indexed:
        write_cursor.execute(
            f"INSERT INTO {SEARCH_FTS_TABLE}({SEARCH_FTS_TABLE}) VALUES('optimize')"
        )

    conn.commit()
    return total_indexed


def build_search_stages(search_column: str, keyword: str) -> list[SearchStage]:
    if search_column == "chs":
        tokens = tokenize_chinese(keyword, limit=MAX_QUERY_TOKENS)
        return _build_term_stages("chs_terms", tokens, base_rank=0)

    if search_column == "jp":
        surface_tokens, reading_tokens = tokenize_japanese(
            keyword,
            overrides=load_reading_overrides(),
            limit=MAX_QUERY_TOKENS,
        )
        stages: list[SearchStage] = []
        stages.extend(_build_term_stages("jp_terms", surface_tokens, base_rank=0))
        stages.extend(
            _build_term_stages("jp_reading_terms", reading_tokens, base_rank=2)
        )
        return _dedupe_stages(stages)

    return []


def build_chinese_terms(text: str) -> str:
    return " ".join(tokenize_chinese(text, limit=MAX_INDEX_TOKENS))


def build_japanese_terms(text: str, overrides: dict[str, str]) -> tuple[str, str]:
    surface_tokens, reading_tokens = tokenize_japanese(
        text,
        overrides=overrides,
        limit=MAX_INDEX_TOKENS,
    )
    return " ".join(surface_tokens), " ".join(reading_tokens)


def tokenize_chinese(text: str, limit: int) -> list[str]:
    normalized = normalize_search_text(text)
    if not normalized:
        return []

    tokens: list[str] = []
    jieba_module = _load_jieba()
    if jieba_module is not None:
        tokens.extend(jieba_module.lcut_for_search(normalized))
    else:
        tokens.extend(_fallback_cjk_tokens(normalized))

    tokens.extend(LATIN_RE.findall(normalized.lower()))
    return _clean_tokens(tokens, limit=limit)


def tokenize_japanese(
    text: str,
    overrides: dict[str, str],
    limit: int,
) -> tuple[list[str], list[str]]:
    normalized = normalize_search_text(text)
    if not normalized:
        return [], []

    surface_tokens: list[str] = []
    reading_tokens: list[str] = []

    phrase_override = overrides.get(normalized)
    if phrase_override:
        reading_tokens.extend(_split_override_terms(phrase_override))

    sudachi = _load_sudachi()
    if sudachi is None:
        fallback_tokens = _fallback_cjk_tokens(normalized)
        surface_tokens.extend(fallback_tokens)
        reading_tokens.extend(
            katakana_to_hiragana(token)
            for token in fallback_tokens
            if is_kana(token)
        )
        return (
            _clean_tokens(surface_tokens, limit=limit),
            _clean_tokens(reading_tokens, limit=limit),
        )

    tokenizer_obj, split_mode = sudachi
    for morpheme in tokenizer_obj.tokenize(normalized, split_mode):
        surface = normalize_search_text(morpheme.surface())
        dictionary_form = morpheme.dictionary_form()
        if dictionary_form == "*":
            dictionary_form = surface
        else:
            dictionary_form = normalize_search_text(dictionary_form)

        surface_tokens.extend([surface, dictionary_form])

        override = overrides.get(surface) or overrides.get(dictionary_form)
        if override:
            reading_tokens.extend(_split_override_terms(override))
            continue

        reading_form = morpheme.reading_form()
        if reading_form and reading_form != "*":
            normalized_reading = normalize_search_text(reading_form)
            reading_tokens.append(katakana_to_hiragana(normalized_reading))
        elif is_kana(surface):
            reading_tokens.append(katakana_to_hiragana(surface))

    return (
        _clean_tokens(surface_tokens, limit=limit),
        _clean_tokens(reading_tokens, limit=limit),
    )


def is_kana(text: str) -> bool:
    return bool(text) and all(
        HIRAGANA_RE.fullmatch(char) or KATAKANA_RE.fullmatch(char) for char in text
    )


def katakana_to_hiragana(text: str) -> str:
    chars: list[str] = []
    for char in text:
        code = ord(char)
        if KATAKANA_START <= code <= KATAKANA_END:
            chars.append(chr(code - KATAKANA_TO_HIRAGANA_OFFSET))
        else:
            chars.append(char)
    return "".join(chars)


@lru_cache(maxsize=1)
def load_reading_overrides() -> dict[str, str]:
    if not READING_OVERRIDE_PATH.exists():
        return {}

    data = json.loads(READING_OVERRIDE_PATH.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return {}

    normalized: dict[str, str] = {}
    for key, value in data.items():
        if not isinstance(key, str) or not isinstance(value, str):
            continue
        normalized_key = normalize_search_text(key)
        normalized_value = normalize_search_text(value)
        if normalized_key and normalized_value:
            normalized[normalized_key] = normalized_value
    return normalized


def compute_overrides_digest(overrides: dict[str, str]) -> str:
    payload = json.dumps(
        overrides,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":")
    )
    return digest_text(payload)


def digest_text(text: str) -> str:
    return blake2b(text.encode("utf-8"), digest_size=16).hexdigest()


def _build_term_stages(
    column: str,
    tokens: list[str],
    base_rank: int,
) -> list[SearchStage]:
    if not tokens:
        return []

    exact_query = _build_match_query(column, tokens, prefix=False)
    prefix_query = _build_match_query(column, tokens, prefix=True)
    stages = [SearchStage(rank=base_rank, match_query=exact_query)]
    if prefix_query != exact_query:
        stages.append(SearchStage(rank=base_rank + 1, match_query=prefix_query))
    return stages


def _build_match_query(column: str, tokens: list[str], *, prefix: bool) -> str:
    terms = []
    for token in tokens:
        escaped = token.replace('"', '""')
        if prefix:
            terms.append(f'{column}:"{escaped}"*')
        else:
            terms.append(f'{column}:"{escaped}"')
    return " AND ".join(terms)


def _dedupe_stages(stages: Iterable[SearchStage]) -> list[SearchStage]:
    seen: set[str] = set()
    unique_stages: list[SearchStage] = []
    for stage in stages:
        if not stage.match_query or stage.match_query in seen:
            continue
        unique_stages.append(stage)
        seen.add(stage.match_query)
    return unique_stages


def _clean_tokens(tokens: Iterable[str], limit: int) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for raw_token in tokens:
        token = normalize_search_text(raw_token)
        token = NON_TOKEN_CHARS_RE.sub("", token)
        if not token:
            continue
        if len(token) == 1 and not is_kana(token) and not _is_cjk(token):
            continue
        if token in seen:
            continue
        seen.add(token)
        cleaned.append(token)
        if len(cleaned) >= limit:
            break
    return cleaned


def _fallback_cjk_tokens(text: str) -> list[str]:
    tokens: list[str] = []
    for match in JP_OR_CN_RE.finditer(text):
        chunk = match.group(0)
        if not chunk:
            continue
        tokens.append(chunk)
        if len(chunk) > MIN_BIGRAM_SOURCE_LENGTH and not is_kana(chunk):
            tokens.extend(chunk[i : i + 2] for i in range(len(chunk) - 1))
    return tokens


def _split_override_terms(text: str) -> list[str]:
    return [katakana_to_hiragana(token) for token in TOKEN_RE.findall(text)]


def _is_cjk(text: str) -> bool:
    return all("\u3400" <= char <= "\u9fff" for char in text)


@lru_cache(maxsize=1)
def _load_jieba() -> Any | None:
    try:
        return import_module("jieba")
    except ImportError:
        return None


@lru_cache(maxsize=1)
def _load_sudachi() -> tuple[Any, Any] | None:
    try:
        dictionary = import_module("sudachipy.dictionary")
        tokenizer = import_module("sudachipy.tokenizer")
    except ImportError:
        return None

    tokenizer_obj = dictionary.Dictionary().create()
    return tokenizer_obj, tokenizer.Tokenizer.SplitMode.B
