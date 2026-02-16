"""
 r/kdramas POPULARITY PIPELINE 
==================================================================

THE GOAL:
- We have big Reddit files (posts + comments).
- We want to count how many times each drama is mentioned each WEEK.
- People use nicknames and acronyms (CLOY, 2521, DOTS, WLFKBJ...).
- Some words are dangerous because they are normal English (life, last, family...).
- We want good accuracy so our dashboard is trustworthy.
- We want to keep auto-acronym generation BUT we specifically DO NOT want "days", "omg", "end", "low" counted.

OUTPUT (an Excel file):
1) weekly_pivot  : rows = drama titles, columns = ISO week (YYYY-W##), values = mentions
2) weekly_long   : long-format table (title, week, mentions)
3) ranking_total : total mentions per drama

MEMORY SAFETY:
- We stream JSONL line-by-line (no full file in RAM).
- Safe for an 8GB laptop.
"""

# =============================
# 1) IMPORT LIBRARIES
# =============================
# - re: helps with text patterns and word-boundaries
# - datetime: turns timestamps into week labels
# - defaultdict: counts things easily
# - pandas: builds tables and saves Excel
# - orjson: reads JSON lines very fast
# - ahocorasick: super fast multi-string matching (thousands of titles at once)
# - tqdm: progress bar so you know it’s working
import re
from collections import defaultdict
from datetime import datetime, UTC
import pandas as pd
import orjson
import ahocorasick
from tqdm import tqdm

# =============================
# 2) FILE PATHS (EDIT IF NEEDED)
# =============================

# These are where your data files live on your computer.
POSTS_PATH = r"C:\Users\nowsh\Downloads\Spring 2026\r_kdramas_posts.jsonl"
COMMENTS_PATH = r"C:\Users\nowsh\Downloads\Spring 2026\r_kdramas_comments.jsonl"

#kdrama titles downloaded from wikipedia and deleted unnecessary lines or words manually
WIKI_TITLES_XLSX = r"korean_dramas_wikipedia.xlsx"

# This is what we will create at the end.
OUTPUT_FILE = r"C:\Users\nowsh\Documents\kdramas-dashboard\STRICT_weekly_kdrama_mentions_test.xlsx"


# =============================
# 3) STRICT DISAMBIGUATION SETTINGS
# =============================
# Some drama titles are also normal English words (life, last, family).
# People say "last week" or "family drama" and we don't want to count those as drama titles.
# So we only count ambiguous titles when we have strong evidence.

# Words that suggest the person is talking about a drama/show
CONTEXT_WORDS = {
    "drama", "kdrama", "series", "show",
    "episode", "ep", "ost",
    "watch", "watching", "watched", "rewatch", "rewatching",
    "binge", "binged",
    "cast", "actor", "actress", "starring", "lead",
    "netflix", "tvn", "jtbc", "kbs", "sbs", "mbc", "ena", "ocn",
    "finale", "ending", "plot", "character",
    "rating", "ratings", "aired", "airing", "chemistry","peak", "favorite", "favourite", "slow", "pacing", "issue"
}

# Titles that are ALSO common English words (very risky)
AMBIGUOUS_TITLES_EXTRA = {
    "life", "romance", "signal", "voice",
    "time", "item",  "hometown",
    "last", "family", "times", "remember", "wanted",
    "friends", "run", "short", "live", "lovers", "enemies"
}

# Genre words are dangerous because people ask for recommendations
GENRE_TITLES = {"romance", "comedy", "thriller", "fantasy", "mystery", "horror", "action", "drama","timeslip", "melodrama"}

# HARD BLOCK titles:
# Some titles are too ambiguous to ever count reliably (they show up constantly in English).
# In strict precision mode, we sacrifice recall to protect accuracy.
HARD_BLOCK_TITLES = {"last", "family", "times", "remember", "wanted"}

# Common phrases that often cause false positives for those ambiguous titles
NEGATIVE_PHRASES = {
    "last week", "last night", "last episode", "last scene", "last time","family drama", "family issues", "family show",
    "lost interest", "lost my", "wanted to", "wanted it", "wanted a",
    "loved it", "love it", "liked it", "like it",
    "remember when", "remember that", "remember this", "hated it", "at the time", "all the time","multiple time", "happy ending", "hea", "thank you"
}
# Regex helper: what counts as a "word character"
WORD_CHAR = re.compile(r"\w")

# =============================
# 4) LOAD CANONICAL DRAMA TITLES
# =============================
 
# We load the official drama titles from your Excel list (Wikipedia extraction).
print("Loading drama titles from Excel...")
df_titles = pd.read_excel(WIKI_TITLES_XLSX)
# Keep original title for acronym generation
df_titles["title_original"] = df_titles["title"].astype(str)

# Clean titles: lowercase, trim, and normalize spaces
df_titles["title_clean"] = (
    df_titles["title"]
    .astype(str)
    .str.lower()
    .str.strip()
    .str.replace(r"\s+", " ", regex=True)
)

canonical_titles = df_titles["title_clean"].dropna().unique().tolist()
canonical_set = set(canonical_titles)
print(f"Loaded {len(canonical_titles)} canonical titles.")

# =============================
# 5) FIND NESTED TITLES (SUBSTRING PROBLEM)
# =============================
 
# Example: "hometown" is inside "hometown cha-cha-cha".
# If someone writes the full long title, we only want to count the long one.
# Also, if they write only the short one, it's risky → require strong evidence.
nested_titles = set()
for t1 in canonical_titles:
    for t2 in canonical_titles:
        if t1 != t2 and t1 in t2:
            nested_titles.add(t1)
            break

print(f"Detected {len(nested_titles)} nested titles (substring titles).")

# =============================
# 6) AUTO-AMBIGUOUS TITLES (ONE WORD)
# =============================

# Single-word titles are usually risky in strict mode.
ambiguous_auto = {t for t in canonical_titles if len(t.split()) == 1}
print(f"Detected {len(ambiguous_auto)} auto-ambiguous titles (single-word).")





# Manual aliases (safe-ish ones)
#These are the shorthand people *really* use.
MANUAL_ALIASES_RAW = {
    "2521": "twenty-five twenty-one",
    "25/21": "twenty-five twenty-one",
    "25-21": "twenty-five twenty-one",
    "wlfkbj": "weightlifting fairy kim bok-joo",
    "wlf": "weightlifting fairy kim bok-joo",
    "sh**ting stars": "shooting stars",
    "swag": "weightlifting fairy kim bok-joo",
    "kim bok joo": "weightlifting fairy kim bok-joo",
    "kbj": "weightlifting fairy kim bok-joo",
    "weight lifting": "weightlifting fairy kim bok-joo",
    "weightlifting fairy": "weightlifting fairy kim bok-joo",
    "W: Two Worlds": "w",
    "W: Two Worlds Apart": "w",
    "hometown ccc":"hometown cha-cha-cha",
    "hometown cha cha cha":"hometown cha-cha-cha",
    "hometown chachacha":"hometown cha-cha-cha",
    "in your brilliant season": "in your radiant season",
    "goblin": "guardian: the lonely and great god",
    "bride of habaek":"the bride of habaek",
    "rookie historian": "rookie historian goo hae-ryung",
    "jealousy incarnate": "don't dare to dream",
    "School 2015": "who are you: school 2015",
    "Strong girl do bong soon": "strong girl bong-soon",
    "Strong woman do bong soon": "strong girl bong-soon",
    "minmin": "strong girl bong-soon",
    "bongbong": "strong girl bong-soon",
    "ahn min-hyuk": "strong girl bong-soon",
    "swdbs": "strong girl bong-soon",
    "jealousy incarnate": "don't dare to dream",
    "idol i": "i dol i",
    "moon lovers": "moon lovers: scarlet heart ryeo",
    "scarlet heart": "moon lovers: scarlet heart ryeo",
    "bon appetit your majesty": "bon appétit, your majesty",
    "bon apetit your majesty": "bon appétit, your majesty",
    "it's ok to not be ok": "it's okay to not be okay",
    "its ok to not be ok": "it's okay to not be okay"
}

# =============================
# 7) BUILD ALIAS MAP (DEDUP SAFE VERSION)
# =============================

print("Building alias map...")

from collections import defaultdict

def normalize_text(s: str) -> str:
    """Make text consistent."""
    s = (s or "").lower().strip()
    s = re.sub(r"\s+", " ", s)
    return s

def normalize_title_variant(s: str) -> str:
    """Normalize hyphens to spaces (bok-joo vs bok joo)."""
    s = normalize_text(s)
    s = s.replace("-", " ")
    s = re.sub(r"\s+", " ", s)
    return s

def resolve_to_canonical(name: str) -> str:
    """
    Manual alias values should match canonical titles from Excel.
    If not exact, try hyphen-normalized match.
    """
    n1 = normalize_text(name)
    if n1 in canonical_set:
        return n1
    n2 = normalize_title_variant(name)
    if n2 in canonical_set:
        return n2
    return n1  # fallback


def make_acronym(title: str) -> str:
    """
    Simple fandom-style acronym:
    Take first letter of EVERY word (no stopword removal).
    """
    words = normalize_text(title).split()
    ac = "".join(w[0] for w in words if w and w[0].isalpha())
    return ac.lower()


# ----------------------------------
# Collect ALL possible alias sources
# ----------------------------------

alias_sources = defaultdict(list)

# 7a) Canonical titles (clean → clean)
for t in canonical_titles:
    alias_sources[normalize_text(t)].append(t)

# 7b) Auto acronyms (generate from ORIGINAL title, map to CLEAN title)
for _, row in df_titles.iterrows():
    original_title = row["title_original"]
    clean_title = row["title_clean"]

    ac = make_acronym(original_title)

    if len(ac) >= 3:
        alias_sources[ac].append(clean_title)

# 7c) Manual aliases
for alias, canon in MANUAL_ALIASES_RAW.items():
    alias_norm = normalize_text(alias)
    canon_resolved = resolve_to_canonical(canon)
    alias_sources[alias_norm].append(canon_resolved)

# 7d) Remove blacklist aliases
BLACKLIST_ALIASES = {"days", "end", "way", "omg","for","has","low","mom"}

for bad in BLACKLIST_ALIASES:
    if bad in alias_sources:
        del alias_sources[bad]

# ----------------------------------
# 7e) Final dedup & conflict handling
# ----------------------------------

alias_map = {}
duplicate_same = []
duplicate_conflict = []

for alias, canon_list in alias_sources.items():

    unique_canons = set(canon_list)

    if len(unique_canons) == 1:
        # SAFE duplicate (same mapping repeated)
        alias_map[alias] = list(unique_canons)[0]

        if len(canon_list) > 1:
            duplicate_same.append(alias)

    else:
        # Conflict: same alias maps to multiple dramas
        duplicate_conflict.append((alias, list(unique_canons)))

print(f"Final aliases used: {len(alias_map)}")
print(f"Safe duplicate aliases collapsed: {len(duplicate_same)}")
print(f"Removed conflicting aliases: {len(duplicate_conflict)}")

if duplicate_conflict:
    print("\n⚠ Conflicting aliases removed (first few shown):")
    for a, c in duplicate_conflict[:5]:
        print(f" - {a} → {c}")


# =============================
# 8) BUILD FAST MATCHER (AHO-CORASICK)
# =============================

print("Building Aho-Corasick automaton...")

automaton = ahocorasick.Automaton()

for alias, canonical in alias_map.items():
    automaton.add_word(alias, (alias, canonical))

automaton.make_automaton()

print("Automaton built successfully.")

# =============================
# 9) WORD BOUNDARY + CONTEXT CHECKS
# =============================
def valid_boundary(text: str, start: int, end: int) -> bool:
    """
    - Ensure we matched a whole word, not part of another word.
    - Example: 'kingdom' should not match inside 'kingdoms'.
    """
    left_ok = (start == 0) or (not WORD_CHAR.match(text[start - 1]))
    right_ok = (end == len(text) - 1) or (not WORD_CHAR.match(text[end + 1]))
    return left_ok and right_ok

def has_context(text: str, start: int, end: int, window: int = 80) -> bool:
    """
    - Look around the match.
    - If we see show words like 'episode' or 'kdrama', it’s more likely real.
    """
    L = max(0, start - window)
    R = min(len(text), end + window + 1)
    snippet = text[L:R]
    return any(w in snippet for w in CONTEXT_WORDS)

# =============================
# 10) STRICT FILTER (THE ACCURACY BRAIN)
# =============================
def strict_filter(text: str, canonical: str, start: int, end: int) -> bool:
    """
    Decide if a match is REAL.
    We are strict:
    - Hard-block hopeless titles (last/family/times/remember/wanted).
    - Block known negative phrases like "last week", "loved it".
    - Genre words like "romance" get special rule.
    - Nested and ambiguous titles require strong evidence.
    """

    # 0) Hard block titles that are too ambiguous to trust
    if canonical in HARD_BLOCK_TITLES:
        return False


    # --------------------------------------------------
    # 🔥 SPECIAL SAFE EXCEPTIONS (ADDED — NOTHING REMOVED)
    # --------------------------------------------------

    # Special handling for W (extremely ambiguous single letter)
    if canonical == "w":
        strong_w_signals = [
            "w two worlds",
            "w: two worlds",
            "two worlds",
            "lee jong suk",
            "han hyo joo",
            "hhj",
            "2016",
            "mbc"
        ]
        if not any(sig in text for sig in strong_w_signals):
            return False
        return True
    if canonical == "tomorrow":
        strong_tomorrow_signals = [
            "rowoon",
            "mbc",
            "2022"
        ]
        if not any(sig in text for sig in strong_tomorrow_signals):
            return False
        return True

    # Special handling for Happiness
    if canonical == "happiness":
        # Must still have show context nearby
        #if not has_context(text, start, end):
         #   return False
        return True
    if canonical == "abyss":
        return True
    if canonical == "mouse":
        return True
    if canonical == "vincenzo":
        return True
    if canonical == "vigilante":
        return True
    if canonical == "vagabond":
        return True
    if canonical == "moving":
        return True
    # --------------------------------------------------
    # ORIGINAL LOGIC CONTINUES UNCHANGED
    # --------------------------------------------------

    # 1) If negative phrase exists, reject (prevents "last week", "loved it", etc.)
    for phrase in NEGATIVE_PHRASES:
        if phrase in text:
            return False


    # 2) Genre words
    if canonical in GENRE_TITLES:
        if canonical == "romance":
            if "2002" not in text:
                return False
        else:
            if not has_context(text, start, end):
                return False


    # 3) Nested titles (like hometown)
    if canonical in nested_titles:
        if not has_context(text, start, end):
            return False


    # 4) Ambiguous titles need VERY strong evidence
    if canonical in ambiguous_auto or canonical in AMBIGUOUS_TITLES_EXTRA:

        # Require show-context
        if not has_context(text, start, end):
            return False

        strong_signals = [
            f"{canonical} (",
            f"{canonical} 20",
            f"{canonical} kdrama",
            f"{canonical} series",
            f"{canonical} netflix",
            f"{canonical} tvn",
            f"{canonical} jtbc",
            f"{canonical} kbs",
            f"{canonical} sbs",
            f"{canonical} mbc",
        ]

        if not any(sig in text for sig in strong_signals):
            return False

    return True

# =============================
# 11) EXTRACT MATCHES (LONGEST MATCH WINS + DEDUP)
# =============================
def extract_matches(text: str) -> set[str]:
    """
 
    - Find all candidate matches.
    - Filter them strictly.
    - If a short match overlaps a longer match, keep the longer one.
    - Count each title at most once per post/comment.
    """
    candidates = []

    for end_idx, (alias, canonical) in automaton.iter(text):
        start_idx = end_idx - len(alias) + 1

        if not valid_boundary(text, start_idx, end_idx):
            continue

        if not strict_filter(text, canonical, start_idx, end_idx):
            continue

        candidates.append((start_idx, end_idx, canonical, len(alias)))

    if not candidates:
        return set()

    # Longest alias first
    candidates.sort(key=lambda x: (x[3], x[1] - x[0]), reverse=True)

    selected = []
    occupied = set()

    for start, end, canonical, _alen in candidates:
        if any(i in occupied for i in range(start, end + 1)):
            continue
        selected.append(canonical)
        for i in range(start, end + 1):
            occupied.add(i)

    return set(selected)

# =============================
# 12) WEEK KEY HELPER (FIXED)
# =============================
from datetime import datetime, UTC

def iso_week_key(ts: int) -> str:
    """Convert timestamp to ISO week string like 2024-W07."""
    dt = datetime.fromtimestamp(ts, UTC)
    year, week, _ = dt.isocalendar()
    return f"{year}-W{week:02d}"

# =============================
# 12B) MONTH KEY HELPER (FIXED)
# =============================
def iso_month_key(ts: int) -> str:
    """
    Convert timestamp to monthly key like 2024-07.
    """
    dt = datetime.fromtimestamp(ts, UTC)
    return f"{dt.year}-{dt.month:02d}"

# =============================
# 13) PROCESS POSTS THEN COMMENTS (STREAMING)
# =============================
weekly_counts = defaultdict(int)
monthly_counts = defaultdict(int)

def process_posts(path: str):
    print(f"\nProcessing POSTS: {path}")
    with open(path, "rb") as f:
        for line in tqdm(f, desc="posts"):
            obj = orjson.loads(line)
            ts = obj.get("created_utc")
            if not ts:
                continue

            title = obj.get("title") or ""
            selftext = obj.get("selftext") or ""
            text = normalize_text(title + " " + selftext)
            if not text:
                continue

            wk = iso_week_key(ts)
            month = iso_month_key(ts)
            
            matched = extract_matches(text)
            
            for t in matched:
                weekly_counts[(t, wk)] += 1
                monthly_counts[(t, month)] += 1

def process_comments(path: str):
    print(f"\nProcessing COMMENTS: {path}")
    with open(path, "rb") as f:
        for line in tqdm(f, desc="comments"):
            obj = orjson.loads(line)
            ts = obj.get("created_utc")
            if not ts:
                continue

            body = obj.get("body") or ""
            text = normalize_text(body)
            if not text:
                continue

            wk = iso_week_key(ts)
            month = iso_month_key(ts)
            
            matched = extract_matches(text)
            
            for t in matched:
                weekly_counts[(t, wk)] += 1
                monthly_counts[(t, month)] += 1


# Run in the order you requested: posts first
process_posts(POSTS_PATH)
process_comments(COMMENTS_PATH)

# =============================
# 14) BUILD OUTPUT TABLES + SAVE EXCEL
# =============================
print("\nBuilding output tables...")

rows = [{"title": t, "week": w, "mentions": c} for (t, w), c in weekly_counts.items()]
df_long = pd.DataFrame(rows)

if df_long.empty:
    print("No matches found. Check your titles file and alias list.")
    raise SystemExit(0)

# Pivot: rows = titles, columns = weeks
df_pivot = df_long.pivot_table(
    index="title",
    columns="week",
    values="mentions",
    fill_value=0
)

# Add TOTAL popularity
df_pivot["TOTAL"] = df_pivot.sum(axis=1)
df_pivot = df_pivot.sort_values("TOTAL", ascending=False)

# Ranking table
df_rank = (
    df_long.groupby("title", as_index=False)["mentions"]
    .sum()
    .sort_values("mentions", ascending=False)
)
# =============================
# MONTHLY TABLES (NEW)
# =============================
print("Building monthly tables...")

monthly_rows = [
    {"title": t, "month": m, "mentions": c}
    for (t, m), c in monthly_counts.items()
]

df_month_long = pd.DataFrame(monthly_rows)

# Pivot: rows = titles, columns = month
df_month_pivot = df_month_long.pivot_table(
    index="title",
    columns="month",
    values="mentions",
    fill_value=0
)

# Add TOTAL popularity (monthly aggregation)
df_month_pivot["TOTAL"] = df_month_pivot.sum(axis=1)
df_month_pivot = df_month_pivot.sort_values("TOTAL", ascending=False)

# Monthly ranking
df_month_rank = (
    df_month_long.groupby("title", as_index=False)["mentions"]
    .sum()
    .sort_values("mentions", ascending=False)
)

with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:

    # Weekly sheets
    df_pivot.to_excel(writer, sheet_name="weekly_pivot")
    df_long.sort_values(["title", "week"]).to_excel(writer, sheet_name="weekly_long", index=False)
    df_rank.to_excel(writer, sheet_name="ranking_total", index=False)

    # Monthly sheets (NEW)
    df_month_pivot.to_excel(writer, sheet_name="monthly_pivot")
    df_month_long.sort_values(["title", "month"]).to_excel(writer, sheet_name="monthly_long", index=False)
    df_month_rank.to_excel(writer, sheet_name="ranking_monthly_total", index=False)


print("\n" + "="*60)
print("✅ PIPELINE COMPLETE")
print("="*60)
print(f"📁 Saved Excel File: {OUTPUT_FILE}")

# Basic dataset stats
print("\n--- DATA SUMMARY ---")
print(f"Canonical titles loaded: {len(canonical_titles)}")
print(f"Total aliases used: {len(alias_map)} (with blacklist applied)")
print(f"Nested titles detected: {len(nested_titles)}")
print(f"Auto-ambiguous (single-word) titles: {len(ambiguous_auto)}")

# Weekly stats
print("\n--- WEEKLY ANALYSIS ---")
print(f"Total (title, week) pairs counted: {len(weekly_counts)}")
print(f"Unique weeks detected: {len(set(w for (_, w) in weekly_counts.keys()))}")
print(f"Unique dramas mentioned (weekly): {len(df_rank)}")

# Monthly stats
print("\n--- MONTHLY ANALYSIS ---")
print(f"Total (title, month) pairs counted: {len(monthly_counts)}")
print(f"Unique months detected: {len(set(m for (_, m) in monthly_counts.keys()))}")
print(f"Unique dramas mentioned (monthly): {len(df_month_rank)}")

# Top 5 sanity check
print("\n--- TOP 5 DRAMAS (WEEKLY TOTAL) ---")
print(df_rank.head(30).to_string(index=False))
print("\n All done successfully.")
