import sqlite3
import re
import os
import time
import json
from pathlib import Path

# === 設定エリア ===
BASE_DIR = Path(__file__).parent
DB_NAME = "genshin_text.db"
DB_PATH = BASE_DIR / DB_NAME
ANIME_GAME_DATA_DIR = Path(os.getenv("ANIME_GAME_DATA_DIR", r"C:\Users\yuka9\Downloads\AnimeGameData"))
TEXTMAP_DIR = ANIME_GAME_DATA_DIR / "TextMap"
READABLE_DIR = ANIME_GAME_DATA_DIR / "Readable"
SUBTITLE_DIR = ANIME_GAME_DATA_DIR / "Subtitle"
TURN_BASED_GAME_DATA_DIR = Path(os.getenv("TURN_BASED_GAME_DATA_DIR", r"C:\Users\yuka9\Downloads\turnbasedgamedata"))
SR_TEXTMAP_DIR = TURN_BASED_GAME_DATA_DIR / "TextMap"

# TextMapのファイル名
TEXTMAP_CHS = "TextMapCHS.json"
TEXTMAP_JP = "TextMapJP.json"

# === データベース処理 ===

def get_db():
    return sqlite3.connect(DB_PATH)

def init_db():
    """データベースとテーブルの初期化"""
    print(f"📂 データベースを準備中: {DB_NAME}")
    
    with get_db() as conn:
        c = conn.cursor()
        
        # 1. TextMapテーブル
        c.execute('''CREATE TABLE IF NOT EXISTS text_map (
                        id TEXT PRIMARY KEY, 
                        chs TEXT, 
                        jp TEXT
                    )''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_tm_chs ON text_map(chs)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_tm_jp ON text_map(jp)')
        c.execute('''CREATE TABLE IF NOT EXISTS sr_text_map (
                        id TEXT PRIMARY KEY,
                        chs TEXT,
                        jp TEXT
                    )''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_sr_tm_chs ON sr_text_map(chs)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_sr_tm_jp ON sr_text_map(jp)')

        # 2. 書籍用テーブル
        c.execute('''CREATE TABLE IF NOT EXISTS readable (
                        filename TEXT PRIMARY KEY,
                        chs TEXT,
                        jp TEXT
                    )''')
        
        # 3. 字幕用テーブル
        c.execute('''CREATE TABLE IF NOT EXISTS subtitle (
                        filename TEXT PRIMARY KEY,
                        chs TEXT,
                        jp TEXT
                    )''')
        conn.commit()

# === テキスト処理ロジック ===

def clean_text_content(text, lang):
    """プレースホルダー処理"""
    if not text: return ""

    # {M#男}{F#女} -> {男/女}
    text = re.sub(r'\{M#(.*?)\}\{F#(.*?)\}', lambda m: f"{{{m.group(1)}/{m.group(2)}}}", text)
    text = re.sub(r'\{F#(.*?)\}\{M#(.*?)\}', lambda m: f"{{{m.group(2)}/{m.group(1)}}}", text)

    # {AVATAR#SEXPRO...}
    def replace_sexpro(match):
        return f"{{{match.group(2)}/{match.group(3)}}}"
    text = re.sub(r'\{(.*?)AVATAR#SEXPRO\[(.*?)\|(.*?)\]\}', replace_sexpro, text)

    # 放浪者・旅人
    wanderer = "流浪者" if lang == 'CHS' else "放浪者"
    traveler = "旅行者" if lang == 'CHS' else "旅人"
    text = re.sub(r"\{REALNAME\[ID\(1\)\|HOSTONLY\(true\)\]\}", wanderer, text)
    text = re.sub(r"\{NICKNAME\}", traveler, text)

    return text

def clean_srt(content):
    """SRTファイルからタイムコードとインデックスを削除"""
    # タイムコード行削除
    text = re.sub(r'\d{2}:\d{2}:\d{2},\d{3} --> \d{2}:\d{2}:\d{2},\d{3}.*', '', content)
    # 数字だけの行（インデックス）削除
    text = re.sub(r'^\d+\s*$', '', text, flags=re.MULTILINE)
    # タグ削除
    text = re.sub(r'<[^>]+>', '', text)
    # 余分な改行を整理
    return re.sub(r'\n{3,}', '\n\n', text).strip()

def prepare_folders():
    """Check whether required AnimeGameData paths exist."""
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
    missing_paths = [p for p in required_paths if not p.exists()]
    if missing_paths:
        print("\n[ERROR] Missing required source paths:")
        for source_path in missing_paths:
            print(f"  - {source_path}")
        return False
    return True

# === TextMap Processing ===

def process_textmap_to_table(chs_path, jp_path, table_name, label):
    if not chs_path.exists() or not jp_path.exists():
        print(f"\n[WARN] {label} TextMap JSON not found, skip.")
        return

    print(f"\n[INFO] Importing {label} TextMap ...")
    start_time = time.time()

    try:
        with open(chs_path, "r", encoding="utf-8") as f:
            chs_data = json.load(f)
        with open(jp_path, "r", encoding="utf-8") as f:
            jp_data = json.load(f)
    except Exception as e:
        print(f"[ERROR] Failed to load {label} TextMap: {e}")
        return

    rows = []
    all_ids = set(chs_data.keys()) | set(jp_data.keys())
    for tid in all_ids:
        c = chs_data.get(tid)
        j = jp_data.get(tid)

        if c:
            c = clean_text_content(c, "CHS")
        if j:
            j = clean_text_content(j, "JP")

        if c or j:
            rows.append((tid, c, j))

    with get_db() as conn:
        c = conn.cursor()
        c.execute(f"DELETE FROM {table_name}")
        c.executemany(f"INSERT INTO {table_name} (id, chs, jp) VALUES (?, ?, ?)", rows)
        conn.commit()

    print(f"[OK] {label} TextMap imported: {len(rows)} rows ({time.time() - start_time:.2f}s)")


def process_textmap():
    process_textmap_to_table(
        TEXTMAP_DIR / TEXTMAP_CHS,
        TEXTMAP_DIR / TEXTMAP_JP,
        "text_map",
        "Genshin"
    )


def process_sr_textmap():
    process_textmap_to_table(
        SR_TEXTMAP_DIR / TEXTMAP_CHS,
        SR_TEXTMAP_DIR / TEXTMAP_JP,
        "sr_text_map",
        "StarRail"
    )


def process_category(category, ext, table_name, cleaner=None):
    source_map = {
        "Readable": READABLE_DIR,
        "Subtitle": SUBTITLE_DIR
    }
    source_dir = source_map.get(category)
    if source_dir is None:
        source_dir = ANIME_GAME_DATA_DIR / category
    chs_dir = source_dir / "CHS"
    jp_dir = source_dir / "JP"
    
    if not chs_dir.exists(): return
    chs_files = list(chs_dir.glob(f"*.{ext}"))
    if not chs_files:
        print(f"\nℹ️  スキップ: {category} (ファイルなし)")
        return

    print(f"\n🚀 {category} の処理を開始 ({len(chs_files)} ファイル)...")
    data_list = []
    matched_count = 0
    
    for chs_file in chs_files:
        raw_stem = chs_file.stem  # 拡張子なしのファイル名 (例: story_CHS)
        
        # === ファイル名マッチングロジック (修正版) ===
        # 1. 共通ID (Base Name) を決定
        if raw_stem.endswith("_CHS"):
            base_name = raw_stem[:-4] # "_CHS" を削除 (例: story)
        else:
            base_name = raw_stem      # (例: book)

        # 2. 日本語ファイルを探す
        # 優先: base_name + "_JP" (例: story_JP.srt)
        jp_file = jp_dir / f"{base_name}_JP.{ext}"
        
        # フォールバック1: base_name そのまま (例: book.txt)
        if not jp_file.exists():
            jp_file = jp_dir / f"{base_name}.{ext}"

        # フォールバック2: 元のファイル名のまま (例: rare_case.txt)
        if not jp_file.exists():
            jp_file = jp_dir / chs_file.name

        # --- 読み込み処理 ---
        try:
            with open(chs_file, 'r', encoding='utf-8', errors='ignore') as f:
                chs_text = f.read()
                if cleaner: chs_text = cleaner(chs_text)
        except: chs_text = ""

        jp_text = ""
        if jp_file.exists():
            try:
                with open(jp_file, 'r', encoding='utf-8', errors='ignore') as f:
                    jp_text = f.read()
                    if cleaner: jp_text = cleaner(jp_text)
                matched_count += 1
            except: pass
        
        # base_name (_CHSなし) をIDとして保存
        data_list.append((base_name, chs_text, jp_text))

    if data_list:
        with get_db() as conn:
            c = conn.cursor()
            c.executemany(f"INSERT OR REPLACE INTO {table_name} (filename, chs, jp) VALUES (?, ?, ?)", data_list)
            conn.commit()
        print(f"✅ {category}完了: {len(data_list)} 件 (ペア成立: {matched_count})")

def main():
    print("=== Genshin Data Import Tool ===")
    print(f"[INFO] Source (Genshin): {ANIME_GAME_DATA_DIR}")
    print(f"[INFO] Source (StarRail): {TURN_BASED_GAME_DATA_DIR}")
    if not prepare_folders():
        return
    init_db()
    
    # 1. TextMap
    process_textmap()
    # 1.5 StarRail TextMap
    process_sr_textmap()
    # 2. 書籍 (txt) - ファイル名マッチング自動判別
    process_category("Readable", "txt", "readable", cleaner=None)
    # 3. 字幕 (srt) - 時間軸除去 + _CHS削除マッチング
    process_category("Subtitle", "srt", "subtitle", cleaner=clean_srt)
    
    print("\n🎉 すべての処理が完了しました！")
    if DB_PATH.exists():
        print(f"生成ファイル: {DB_NAME} ({DB_PATH.stat().st_size / (1024*1024):.2f} MB)")
        print("👉 このファイルをサーバーの src/plugins/data/ にアップロードしてください。")
    input("\nEnterキーを押して終了...")

if __name__ == "__main__":
    main()
