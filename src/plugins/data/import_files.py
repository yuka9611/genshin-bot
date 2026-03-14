import argparse
import os
import re
import sqlite3
import time
from collections.abc import Callable
from pathlib import Path

try:
    from search_index import (
        DEFAULT_PERSIST_TERM_CACHE,
        SOURCE_TABLES,
        configure_connection,
        ensure_search_schema,
        load_json_utf8,
        read_utf8_text,
        rebuild_search_index,
        split_clean_rows,
        validate_chinese_rows,
    )
except ImportError:
    try:
        from data.search_index import (
            DEFAULT_PERSIST_TERM_CACHE,
            SOURCE_TABLES,
            configure_connection,
            ensure_search_schema,
            load_json_utf8,
            read_utf8_text,
            rebuild_search_index,
            split_clean_rows,
            validate_chinese_rows,
        )
    except ImportError:
        from .search_index import (
            DEFAULT_PERSIST_TERM_CACHE,
            SOURCE_TABLES,
            configure_connection,
            ensure_search_schema,
            load_json_utf8,
            read_utf8_text,
            rebuild_search_index,
            split_clean_rows,
            validate_chinese_rows,
        )

BASE_DIR = Path(__file__).parent
DB_NAME = "genshin_text.db"
DB_PATH = BASE_DIR / DB_NAME
ANIME_GAME_DATA_DIR = Path(
    os.getenv("ANIME_GAME_DATA_DIR", r"C:\Users\yuka9\Downloads\AnimeGameData")
)
TEXTMAP_DIR = ANIME_GAME_DATA_DIR / "TextMap"
READABLE_DIR = ANIME_GAME_DATA_DIR / "Readable"
SUBTITLE_DIR = ANIME_GAME_DATA_DIR / "Subtitle"
TURN_BASED_GAME_DATA_DIR = Path(
    os.getenv("TURN_BASED_GAME_DATA_DIR", r"C:\Users\yuka9\Downloads\turnbasedgamedata")
)
SR_TEXTMAP_DIR = TURN_BASED_GAME_DATA_DIR / "TextMap"

TEXTMAP_CHS = "TextMapCHS.json"
TEXTMAP_JP = "TextMapJP.json"
REJECTED_ID_PREVIEW_LIMIT = 5


def get_db() -> sqlite3.Connection:
    return configure_connection(sqlite3.connect(DB_PATH))


def init_db() -> None:
    print(f"Preparing database: {DB_NAME}")  # noqa: T201
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS text_map (
                id TEXT PRIMARY KEY,
                chs TEXT,
                jp TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS sr_text_map (
                id TEXT PRIMARY KEY,
                chs TEXT,
                jp TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS readable (
                filename TEXT PRIMARY KEY,
                chs TEXT,
                jp TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS subtitle (
                filename TEXT PRIMARY KEY,
                chs TEXT,
                jp TEXT
            )
            """
        )
        ensure_search_schema(conn, persist_term_cache=DEFAULT_PERSIST_TERM_CACHE)
        conn.commit()


def compact_database() -> None:
    if not DB_PATH.exists():
        return

    print("[INFO] Compacting database file ...")  # noqa: T201
    with get_db() as conn:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("VACUUM")
    print("[OK] Database compacted.")  # noqa: T201


def clean_text_content(text: str, lang: str) -> str:
    if not text:
        return ""

    text = re.sub(
        r"\{M#(.*?)\}\{F#(.*?)\}",
        lambda match: f"{{{match.group(1)}/{match.group(2)}}}",
        text,
    )
    text = re.sub(
        r"\{F#(.*?)\}\{M#(.*?)\}",
        lambda match: f"{{{match.group(2)}/{match.group(1)}}}",
        text,
    )

    def replace_sexpro(match: re.Match[str]) -> str:
        return f"{{{match.group(2)}/{match.group(3)}}}"

    text = re.sub(r"\{(.*?)AVATAR#SEXPRO\[(.*?)\|(.*?)\]\}", replace_sexpro, text)
    wanderer = "流浪者" if lang == "CHS" else "放浪者"
    traveler = "旅行者" if lang == "CHS" else "旅人"
    re.sub(r"\{REALNAME\[ID\(1\)\|HOSTONLY\(true\)\]\}", wanderer, text)
    re.sub(r"\{NICKNAME\}", traveler, text)
    return text


def clean_srt(content: str) -> str:
    text = re.sub(
        r"\d{2}:\d{2}:\d{2},\d{3} --> \d{2}:\d{2}:\d{2},\d{3}.*",
        "",
        content,
    )
    text = re.sub(r"^\d+\s*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"<[^>]+>", "", text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def prepare_folders() -> bool:
    required_paths = [
        TEXTMAP_DIR / TEXTMAP_CHS,
        TEXTMAP_DIR / TEXTMAP_JP,
        SR_TEXTMAP_DIR / TEXTMAP_CHS,
        SR_TEXTMAP_DIR / TEXTMAP_JP,
        READABLE_DIR / "CHS",
        READABLE_DIR / "JP",
        SUBTITLE_DIR / "CHS",
        SUBTITLE_DIR / "JP",
    ]
    missing_paths = [path for path in required_paths if not path.exists()]
    if not missing_paths:
        return True

    print("\n[ERROR] Missing required source paths:")  # noqa: T201
    for source_path in missing_paths:
        print(f"  - {source_path}")  # noqa: T201
    return False


def process_textmap_to_table(
    chs_path: Path,
    jp_path: Path,
    table_name: str,
    label: str,
) -> None:
    if not chs_path.exists() or not jp_path.exists():
        print(f"\n[WARN] {label} TextMap JSON not found, skip.")  # noqa: T201
        return

    print(f"\n[INFO] Importing {label} TextMap ...")  # noqa: T201
    start_time = time.time()

    try:
        chs_data = load_json_utf8(chs_path)
        jp_data = load_json_utf8(jp_path)
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        print(f"[ERROR] Failed to load {label} TextMap: {exc}")  # noqa: T201
        return

    rows: list[tuple[str, str | None, str | None]] = []
    all_ids = set(chs_data.keys()) | set(jp_data.keys())
    for text_id in all_ids:
        chs_text = chs_data.get(text_id)
        jp_text = jp_data.get(text_id)
        cleaned_chs = clean_text_content(str(chs_text), "CHS") if chs_text else None
        cleaned_jp = clean_text_content(str(jp_text), "JP") if jp_text else None
        if cleaned_chs is not None or cleaned_jp is not None:
            rows.append((str(text_id), cleaned_chs, cleaned_jp))

    rows, rejected_ids = split_clean_rows(rows)
    if rejected_ids:
        preview = ", ".join(rejected_ids[:REJECTED_ID_PREVIEW_LIMIT])
        suffix = (
            " ..."
            if len(rejected_ids) > REJECTED_ID_PREVIEW_LIMIT
            else ""
        )
        print(  # noqa: T201
            "[WARN] "
            f"{label} skipped {len(rejected_ids)} rows with "
            f"replacement characters: {preview}{suffix}"
        )
    if not rows:
        print(f"[ERROR] {label} has no clean TextMap rows to import.")  # noqa: T201
        return

    try:
        validate_chinese_rows(rows, label)
    except ValueError as exc:
        print(f"[ERROR] {exc}")  # noqa: T201
        return

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {table_name}")
        cursor.executemany(
            f"INSERT INTO {table_name} (id, chs, jp) VALUES (?, ?, ?)",
            rows,
        )
        conn.commit()

    duration = time.time() - start_time
    print(  # noqa: T201
        f"[OK] {label} TextMap imported: {len(rows)} rows ({duration:.2f}s)"
    )


def process_textmap() -> None:
    process_textmap_to_table(
        TEXTMAP_DIR / TEXTMAP_CHS,
        TEXTMAP_DIR / TEXTMAP_JP,
        "text_map",
        "Genshin",
    )


def process_sr_textmap() -> None:
    process_textmap_to_table(
        SR_TEXTMAP_DIR / TEXTMAP_CHS,
        SR_TEXTMAP_DIR / TEXTMAP_JP,
        "sr_text_map",
        "StarRail",
    )


def resolve_localized_file(
    jp_dir: Path,
    chs_file: Path,
    base_name: str,
    extension: str,
) -> Path:
    candidates = (
        jp_dir / f"{base_name}_JP.{extension}",
        jp_dir / f"{base_name}.{extension}",
        jp_dir / chs_file.name,
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[-1]


def read_category_text(
    path: Path,
    language: str,
    cleaner: Callable[[str], str] | None = None,
) -> tuple[str, bool]:
    if not path.exists():
        return "", False

    try:
        text = read_utf8_text(path)
    except (OSError, UnicodeDecodeError) as exc:
        print(  # noqa: T201
            f"[WARN] Skip unreadable {language} file {path.name}: {exc}"
        )
        return "", False

    if cleaner and text:
        text = cleaner(text)
    return text, True


def process_category(
    category: str,
    extension: str,
    table_name: str,
    cleaner: Callable[[str], str] | None = None,
) -> None:
    source_dir = {
        "Readable": READABLE_DIR,
        "Subtitle": SUBTITLE_DIR,
    }.get(category, ANIME_GAME_DATA_DIR / category)
    chs_dir = source_dir / "CHS"
    jp_dir = source_dir / "JP"

    if not chs_dir.exists():
        return

    chs_files = list(chs_dir.glob(f"*.{extension}"))
    if not chs_files:
        print(f"\n[INFO] Skip {category}: no files found.")  # noqa: T201
        return

    print(f"\n[INFO] Importing {category}: {len(chs_files)} files ...")  # noqa: T201
    rows: list[tuple[str, str, str]] = []
    matched_count = 0

    for chs_file in chs_files:
        raw_stem = chs_file.stem
        base_name = raw_stem.removesuffix("_CHS")
        jp_file = resolve_localized_file(jp_dir, chs_file, base_name, extension)
        chs_text, _ = read_category_text(chs_file, "CHS", cleaner)
        jp_text, has_jp_file = read_category_text(jp_file, "JP", cleaner)
        if has_jp_file:
            matched_count += 1

        rows.append(
            (
                base_name,
                chs_text or "",
                jp_text or "",
            )
        )

    try:
        validate_chinese_rows(rows, category)
    except ValueError as exc:
        print(f"[ERROR] {exc}")  # noqa: T201
        return

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.executemany(
            f"INSERT OR REPLACE INTO {table_name} (filename, chs, jp) VALUES (?, ?, ?)",
            rows,
        )
        conn.commit()

    print(  # noqa: T201
        f"[OK] {category} imported: {len(rows)} rows, matched files: {matched_count}"
    )


def print_sources() -> None:
    print(f"[INFO] Source (Genshin): {ANIME_GAME_DATA_DIR}")  # noqa: T201
    print(f"[INFO] Source (StarRail): {TURN_BASED_GAME_DATA_DIR}")  # noqa: T201


def print_db_size() -> None:
    if DB_PATH.exists():
        size_mb = DB_PATH.stat().st_size / (1024 * 1024)
        print(f"[INFO] Generated file: {DB_NAME} ({size_mb:.2f} MB)")  # noqa: T201


def get_missing_source_tables(conn: sqlite3.Connection) -> list[str]:
    cursor = conn.cursor()
    existing_tables = {
        row[0]
        for row in cursor.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type IN ('table', 'view')
            """
        ).fetchall()
    }
    return [
        table_name for table_name in SOURCE_TABLES if table_name not in existing_tables
    ]


def log_reindex_progress(
    table_name: str,
    rows_read: int,
    rows_indexed: int,
    duration: float,
) -> None:
    print(  # noqa: T201
        f"[INFO] Reindexed {table_name}: "
        f"scanned {rows_read} rows, indexed {rows_indexed} rows ({duration:.2f}s)"
    )


def run_import() -> int:
    print_sources()
    if not prepare_folders():
        return 1

    init_db()
    process_textmap()
    process_sr_textmap()
    process_category("Readable", "txt", "readable")
    process_category("Subtitle", "srt", "subtitle", cleaner=clean_srt)
    compact_database()
    print_db_size()
    return 0


def run_reindex() -> int:
    if not DB_PATH.exists():
        print(f"[ERROR] Database file not found: {DB_PATH}")  # noqa: T201
        return 1

    with get_db() as conn:
        missing_tables = get_missing_source_tables(conn)
        if missing_tables:
            print("[ERROR] Missing source tables for reindex:")  # noqa: T201
            for table_name in missing_tables:
                print(f"  - {table_name}")  # noqa: T201
            return 1

        started_at = time.perf_counter()
        indexed_docs = rebuild_search_index(
            conn, progress_callback=log_reindex_progress
        )
        duration = time.perf_counter() - started_at

    print(  # noqa: T201
        f"\n[OK] FTS index rebuilt: {indexed_docs} docs ({duration:.2f}s)"
    )
    compact_database()
    print_db_size()
    return 0


def run_all() -> int:
    import_exit_code = run_import()
    if import_exit_code != 0:
        return import_exit_code
    return run_reindex()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import source data and rebuild FTS.")
    parser.add_argument(
        "--pause",
        action="store_true",
        help="Wait for Enter before exiting.",
    )
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("all", help="Import all source data and rebuild the FTS.")
    subparsers.add_parser("import", help="Import source data only.")
    subparsers.add_parser("reindex", help="Rebuild the FTS index from the existing DB.")
    return parser


def main(argv: list[str] | None = None) -> int:
    print("=== Genshin Data Import Tool ===")  # noqa: T201
    parser = build_parser()
    args = parser.parse_args(argv)
    command = args.command or "all"

    if command == "all":
        exit_code = run_all()
    elif command == "import":
        exit_code = run_import()
    else:
        exit_code = run_reindex()

    if args.pause:
        input("\nPress Enter to exit...")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
