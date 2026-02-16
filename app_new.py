# ============================================================
# 📺 K-DRAMA REDDIT POPULARITY DASHBOARD
# ============================================================

import streamlit as st
import pandas as pd
import plotly.express as px
import re

# ------------------------------------------------------------
# PAGE SETTINGS
# ------------------------------------------------------------
st.set_page_config(
    page_title="K-Drama Reddit Popularity",
    layout="wide"
)

st.title("📺 r/kdramas Popularity Dashboard")
st.markdown(
    "This dashboard analyzes Reddit mention trends of Korean dramas. "
    "Release year analysis is based on Wikipedia data. This is not perfect. It only relies on text mentions, not image or GIF used in post or comments without mentioning names."
)

# ------------------------------------------------------------
# HELPER — EXTRACT START YEAR
# ------------------------------------------------------------
def extract_start_year(year_value):
    if pd.isna(year_value):
        return None

    year_str = str(year_value)
    year_str = year_str.replace("–", "-")  # normalize dash

    match = re.search(r"(19|20)\d{2}", year_str)
    if match:
        return int(match.group())

    return None


# ------------------------------------------------------------
# LOAD DATA
# ------------------------------------------------------------
@st.cache_data
def load_data():

    # Reddit mention outputs
    weekly_long = pd.read_excel(
        "STRICT_weekly_kdrama_mentions_test.xlsx",
        sheet_name="weekly_long"
    )

    weekly_rank = pd.read_excel(
        "STRICT_weekly_kdrama_mentions_test.xlsx",
        sheet_name="ranking_total"
    )

    monthly_long = pd.read_excel(
        "STRICT_weekly_kdrama_mentions_test.xlsx",
        sheet_name="monthly_long"
    )

    # Wikipedia master list
    wiki_df = pd.read_excel("korean_dramas_wikipedia.xlsx")

    # Clean titles
# ------------------------------------------------------------
# CLEAN YEAR COLUMN (extract start year)
# ------------------------------------------------------------
    wiki_df["start_year"] = (
        wiki_df["year"]
        .astype(str)
        .str.extract(r"(\d{4})")   # take first 4-digit year
        .astype(float)
    )

    # Lowercase title for merging consistency
    wiki_df["title_clean"] = (
        wiki_df["title"]
        .astype(str)
        .str.lower()
        .str.strip()
    )

# ------------------------------------------------------------
# MANUAL YEAR OVERRIDES 
# ------------------------------------------------------------

    MANUAL_YEAR_OVERRIDE = {
        "mimi": 2014,
    }

    for title, forced_year in MANUAL_YEAR_OVERRIDE.items():
        wiki_df.loc[
            wiki_df["title_clean"] == title,
            "start_year"
        ] = forced_year

# ------------------------------------------------------------
# REMOVE DUPLICATES — KEEP MOST RECENT VERSION
# ------------------------------------------------------------

    wiki_df = (
        wiki_df
        .sort_values("start_year", ascending=False)
        .drop_duplicates(subset=["title_clean"], keep="first")
    )


    return weekly_long, weekly_rank, monthly_long, wiki_df


weekly_long, weekly_rank, monthly_long, wiki_df = load_data()

# Merge release year into ranking
rank_with_year = weekly_rank.merge(
    wiki_df[["title_clean", "start_year"]],
    left_on="title",
    right_on="title_clean",
    how="left"
).drop(columns=["title_clean"])


# ============================================================
# 🔥 PART 1 — TOP N TOTAL POPULARITY
# ============================================================

st.header("🔥 Top Popular Dramas (Total Mentions)")

top_n = st.number_input(
    "Select Top N (1–200)",
    min_value=1,
    max_value=200,
    value=10
)

top_df = weekly_rank.head(top_n)

fig_bar = px.bar(
    top_df,
    x="mentions",
    y="title",
    orientation="h",
    title=f"Top {top_n} Dramas by Total Mentions"
)

fig_bar.update_layout(
    yaxis=dict(autorange="reversed"),
    height=900
)

st.plotly_chart(fig_bar, use_container_width=True)

st.markdown("---")


# ============================================================
# 📈 PART 2 — MULTI-DRAMA TREND COMPARISON
# ============================================================

st.header("📈 Multi-Drama Trend Comparison")

trend_type = st.radio(
    "View Trend By",
    options=["Weekly", "Monthly"],
    horizontal=True
)

if trend_type == "Weekly":
    trend_df = weekly_long.copy()
    trend_df["date"] = pd.to_datetime(
        trend_df["week"] + "-1",
        format="%G-W%V-%u"
    )
else:
    trend_df = monthly_long.copy()
    trend_df["date"] = pd.to_datetime(
        trend_df["month"] + "-01"
    )

min_date = trend_df["date"].min()
max_date = trend_df["date"].max()

date_range = st.date_input(
    "Select Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

if isinstance(date_range, tuple):
    start_date, end_date = date_range
else:
    start_date = end_date = date_range

trend_df = trend_df[
    (trend_df["date"] >= pd.to_datetime(start_date)) &
    (trend_df["date"] <= pd.to_datetime(end_date))
]

selected_titles = st.multiselect(
    "Select up to 10 Dramas to Compare",
    options=sorted(trend_df["title"].unique()),
    max_selections=10
)

if selected_titles:

    filtered_df = trend_df[
        trend_df["title"].isin(selected_titles)
    ]

    fig_line = px.line(
        filtered_df,
        x="date",
        y="mentions",
        color="title",
        markers=True,
        title=f"{trend_type} Mention Trend Comparison"
    )

    fig_line.update_layout(height=700)

    st.plotly_chart(fig_line, use_container_width=True)

else:
    st.info("Select up to 10 dramas to display comparison.")

st.markdown("---")


# ============================================================
# 🔎 PART 3 — SINGLE DRAMA DEEP DIVE
# ============================================================

st.header("🔎 Single Drama Deep Dive")

selected_drama = st.selectbox(
    "Search & Select a Drama",
    options=sorted(weekly_rank["title"].unique())
)

rank_position = weekly_rank.reset_index(drop=True)
rank_position["rank"] = rank_position.index + 1

rank_value = rank_position[
    rank_position["title"] == selected_drama
]["rank"].values

if len(rank_value) > 0:
    st.metric("Overall Rank (Total Mentions)", int(rank_value[0]))

# Weekly trend
weekly_single = weekly_long[
    weekly_long["title"] == selected_drama
].copy()

weekly_single["date"] = pd.to_datetime(
    weekly_single["week"] + "-1",
    format="%G-W%V-%u"
)

fig_week = px.line(
    weekly_single,
    x="date",
    y="mentions",
    markers=True,
    title=f"Weekly Mentions of {selected_drama}"
)

st.plotly_chart(fig_week, use_container_width=True)

# Monthly trend
monthly_single = monthly_long[
    monthly_long["title"] == selected_drama
].copy()

monthly_single["date"] = pd.to_datetime(
    monthly_single["month"] + "-01"
)

fig_month = px.line(
    monthly_single,
    x="date",
    y="mentions",
    markers=True,
    title=f"Monthly Mentions of {selected_drama}"
)

st.plotly_chart(fig_month, use_container_width=True)

st.markdown("---")


# ============================================================
# 🏆 PART 4 — TOP DRAMAS IN A SPECIFIC WEEK OR MONTH
# ============================================================

st.header("🏆 Top Dramas in a Selected Timeframe")

timeframe_type = st.radio(
    "Select Timeframe Type",
    ["Weekly", "Monthly"],
    horizontal=True
)

if timeframe_type == "Weekly":
    temp_df = weekly_long
    time_column = "week"
else:
    temp_df = monthly_long
    time_column = "month"

selected_period = st.selectbox(
    f"Select {timeframe_type}",
    sorted(temp_df[time_column].unique())
)

top_k = st.slider("Select Top K", 1, 10, 5)

period_df = temp_df[temp_df[time_column] == selected_period]

rank_period = (
    period_df.groupby("title", as_index=False)["mentions"]
    .sum()
    .sort_values("mentions", ascending=False)
    .head(top_k)
)

fig_period = px.bar(
    rank_period,
    x="mentions",
    y="title",
    orientation="h",
    title=f"Top {top_k} Dramas in {selected_period}"
)

fig_period.update_layout(yaxis=dict(autorange="reversed"))
st.plotly_chart(fig_period, use_container_width=True)

st.markdown("---")


# ============================================================
# 🎬 PART 5 — TOP DRAMAS BY RELEASE YEAR
# ============================================================

st.header("🎬 Top Dramas by Release Year")

available_years = sorted(
    rank_with_year["start_year"].dropna().unique()
)

selected_year = st.selectbox(
    "Select Release Year",
    available_years
)

top_year_k = st.radio(
    "Show Top",
    [10, 20],
    horizontal=True
)

year_subset = (
    rank_with_year[
        rank_with_year["start_year"] == selected_year
    ]
    .sort_values("mentions", ascending=False)
    .head(top_year_k)
)

if not year_subset.empty:

    fig_year = px.bar(
        year_subset,
        x="mentions",
        y="title",
        orientation="h",
        title=f"Top {top_year_k} Dramas Released in {selected_year}"
    )

    fig_year.update_layout(
        yaxis=dict(autorange="reversed"),
        height=700
    )

    st.plotly_chart(fig_year, use_container_width=True)

else:
    st.info("No Reddit data available for this release year.")
