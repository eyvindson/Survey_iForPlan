from __future__ import annotations

import os
import secrets
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import streamlit as st
except ModuleNotFoundError:  # Allows non-UI tests to import this module.
    st = None

try:
    import plotly.express as px
    import plotly.graph_objects as go
except ModuleNotFoundError:  # Allows config/export tests without UI extras installed.
    px = None
    go = None


BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"
DB_PATH = Path(os.environ.get("SURVEY_DB_PATH", BASE_DIR / "survey.db"))
EXPORT_DIR = BASE_DIR / "exports"

ADMIN_PASSCODE = os.environ.get("SURVEY_ADMIN_PASSCODE", "admin")
APP_BASE_URL = os.environ.get("APP_BASE_URL", "").rstrip("/")

RATING_QUESTIONS = [
    {
        "question_id": "importance",
        "short_label": "Importance",
        "label": "How important is this as a CSF in your region?",
        "help": "1 = least important, 5 = most important",
    },
    {
        "question_id": "feasibility",
        "short_label": "Feasibility",
        "label": "How feasible is this in your region?",
        "help": "1 = least feasible, 5 = most feasible",
    },
    {
        "question_id": "area_scale",
        "short_label": "Implementation area scale",
        "label": "On how large areas could this CSF be implemented?",
        "help": (
            "1 = stand level; 2 = small management unit; 3 = medium management "
            "unit; 4 = landscape-scale forest areas; 5 = regional/national level"
        ),
    },
]

EFFECT_LABELS = {
    "positive": "Positive",
    "neutral": "Neutral",
    "negative": "Negative",
}
EFFECT_OPTIONS = ["Positive", "Neutral", "Negative"]
RATING_OPTIONS = ["Skip", "1", "2", "3", "4", "5"]
EFFECT_SCORE = {"negative": -1, "neutral": 0, "positive": 1}
EFFECT_COLORS = {
    "positive": "#16803c",
    "neutral": "#8a94a6",
    "negative": "#bf3f36",
    "skipped": "#d0d5dd",
}


@dataclass(frozen=True)
class SurveyConfig:
    options: pd.DataFrame
    services: pd.DataFrame


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalize_token(token: str) -> str:
    return token.strip()


def response_key(question_id: str, service_id: str | None = None) -> str:
    if service_id:
        return f"{question_id}:{service_id}"
    return question_id


def load_config(config_dir: Path = CONFIG_DIR) -> SurveyConfig:
    options_path = config_dir / "management_options.csv"
    services_path = config_dir / "ecosystem_services.csv"
    options = pd.read_csv(options_path)
    services = pd.read_csv(services_path)

    required_option_cols = {"option_id", "sort_order", "csf_category", "management_option"}
    required_service_cols = {"service_id", "sort_order", "service_group", "service_label"}
    missing_option_cols = required_option_cols.difference(options.columns)
    missing_service_cols = required_service_cols.difference(services.columns)
    if missing_option_cols:
        raise ValueError(f"Missing option columns: {sorted(missing_option_cols)}")
    if missing_service_cols:
        raise ValueError(f"Missing service columns: {sorted(missing_service_cols)}")
    if options["option_id"].duplicated().any():
        duplicates = options.loc[options["option_id"].duplicated(), "option_id"].tolist()
        raise ValueError(f"Duplicate option_id values: {duplicates}")
    if services["service_id"].duplicated().any():
        duplicates = services.loc[services["service_id"].duplicated(), "service_id"].tolist()
        raise ValueError(f"Duplicate service_id values: {duplicates}")

    options = options.sort_values("sort_order").reset_index(drop=True)
    services = services.sort_values("sort_order").reset_index(drop=True)
    return SurveyConfig(options=options, services=services)


def connect_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(db_path: Path = DB_PATH, config_dir: Path = CONFIG_DIR) -> None:
    conn = connect_db(db_path)
    now = utc_now()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS experts (
            token TEXT PRIMARY KEY,
            name TEXT NOT NULL DEFAULT '',
            email TEXT NOT NULL DEFAULT '',
            region TEXT NOT NULL DEFAULT '',
            role TEXT NOT NULL DEFAULT '',
            organization TEXT NOT NULL DEFAULT '',
            consent_at TEXT,
            completed_at TEXT,
            archived INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS responses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token TEXT NOT NULL,
            option_id TEXT NOT NULL,
            response_key TEXT NOT NULL,
            question_id TEXT NOT NULL,
            service_id TEXT,
            response_value TEXT,
            skipped INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            UNIQUE(token, option_id, response_key),
            FOREIGN KEY(token) REFERENCES experts(token)
        );
        """
    )

    experts_path = config_dir / "experts.csv"
    if experts_path.exists():
        experts = pd.read_csv(experts_path).fillna("")
        for row in experts.to_dict("records"):
            token = normalize_token(str(row.get("token", "")))
            if not token:
                continue
            conn.execute(
                """
                INSERT OR IGNORE INTO experts (
                    token, name, email, region, role, organization, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    token,
                    str(row.get("name", "")),
                    str(row.get("email", "")),
                    str(row.get("region", "")),
                    str(row.get("role", "")),
                    str(row.get("organization", "")),
                    now,
                    now,
                ),
            )
    conn.commit()
    conn.close()


def fetch_expert(conn: sqlite3.Connection, token: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM experts WHERE token = ?", (token,)).fetchone()
    return dict(row) if row else None


def fetch_experts(conn: sqlite3.Connection) -> pd.DataFrame:
    rows = conn.execute("SELECT * FROM experts ORDER BY created_at DESC").fetchall()
    return pd.DataFrame([dict(row) for row in rows])


def fetch_responses(conn: sqlite3.Connection, token: str | None = None) -> pd.DataFrame:
    if token:
        rows = conn.execute(
            "SELECT * FROM responses WHERE token = ? ORDER BY updated_at DESC", (token,)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM responses ORDER BY updated_at DESC").fetchall()
    return pd.DataFrame([dict(row) for row in rows])


def save_expert_profile(conn: sqlite3.Connection, token: str, profile: dict[str, str]) -> None:
    conn.execute(
        """
        UPDATE experts
        SET name = ?, email = ?, region = ?, role = ?, organization = ?, updated_at = ?
        WHERE token = ?
        """,
        (
            profile.get("name", ""),
            profile.get("email", ""),
            profile.get("region", ""),
            profile.get("role", ""),
            profile.get("organization", ""),
            utc_now(),
            token,
        ),
    )
    conn.commit()


def mark_consent(conn: sqlite3.Connection, token: str) -> None:
    conn.execute(
        "UPDATE experts SET consent_at = COALESCE(consent_at, ?), updated_at = ? WHERE token = ?",
        (utc_now(), utc_now(), token),
    )
    conn.commit()


def mark_completed(conn: sqlite3.Connection, token: str) -> None:
    conn.execute(
        "UPDATE experts SET completed_at = ?, updated_at = ? WHERE token = ?",
        (utc_now(), utc_now(), token),
    )
    conn.commit()


def add_expert(
    conn: sqlite3.Connection,
    name: str,
    email: str,
    region: str,
    role: str,
    organization: str,
    token: str | None = None,
) -> str:
    invite_token = normalize_token(token or secrets.token_urlsafe(12))
    now = utc_now()
    conn.execute(
        """
        INSERT INTO experts (token, name, email, region, role, organization, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (invite_token, name, email, region, role, organization, now, now),
    )
    conn.commit()
    return invite_token


def upsert_response(
    conn: sqlite3.Connection,
    token: str,
    option_id: str,
    question_id: str,
    service_id: str | None,
    answer: str | int | None,
) -> None:
    if answer in (None, ""):
        return
    skipped = 1 if str(answer).lower() == "skip" else 0
    value = None if skipped else str(answer).lower()
    key = response_key(question_id, service_id)
    conn.execute(
        """
        INSERT INTO responses (
            token, option_id, response_key, question_id, service_id,
            response_value, skipped, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(token, option_id, response_key) DO UPDATE SET
            response_value = excluded.response_value,
            skipped = excluded.skipped,
            updated_at = excluded.updated_at
        """,
        (token, option_id, key, question_id, service_id, value, skipped, utc_now()),
    )


def response_lookup(responses: pd.DataFrame) -> dict[tuple[str, str], dict[str, Any]]:
    if responses.empty:
        return {}
    return {
        (row["option_id"], row["response_key"]): row
        for row in responses.to_dict("records")
    }


def widget_value_from_saved(
    saved: dict[tuple[str, str], dict[str, Any]], option_id: str, key: str
) -> str | None:
    row = saved.get((option_id, key))
    if not row:
        return None
    if int(row.get("skipped", 0)):
        return "Skip"
    value = row.get("response_value")
    if value is None:
        return None
    if value in EFFECT_LABELS:
        return EFFECT_LABELS[value]
    return str(value)


def expected_response_keys(services: pd.DataFrame) -> list[str]:
    keys = [question["question_id"] for question in RATING_QUESTIONS]
    keys.extend(response_key("effect", service_id) for service_id in services["service_id"])
    return keys


def completion_status(
    options: pd.DataFrame, services: pd.DataFrame, responses: pd.DataFrame
) -> pd.DataFrame:
    saved = response_lookup(responses)
    expected = expected_response_keys(services)
    rows = []
    for option in options.to_dict("records"):
        answered = 0
        for key in expected:
            row = saved.get((option["option_id"], key))
            if row and (row.get("response_value") is not None or int(row.get("skipped", 0)) == 1):
                answered += 1
        rows.append(
            {
                "option_id": option["option_id"],
                "csf_category": option["csf_category"],
                "management_option": option["management_option"],
                "answered": answered,
                "total": len(expected),
                "complete": answered == len(expected),
            }
        )
    return pd.DataFrame(rows)


def completion_for_all_experts(
    conn: sqlite3.Connection, options: pd.DataFrame, services: pd.DataFrame
) -> pd.DataFrame:
    experts = fetch_experts(conn)
    if experts.empty:
        return pd.DataFrame()
    rows = []
    total_questions = len(options) * len(expected_response_keys(services))
    for expert in experts.to_dict("records"):
        responses = fetch_responses(conn, expert["token"])
        answered = len(responses)
        per_option = completion_status(options, services, responses)
        rows.append(
            {
                **expert,
                "answered_questions": answered,
                "total_questions": total_questions,
                "progress": answered / total_questions if total_questions else 0,
                "completed_options": int(per_option["complete"].sum()),
                "total_options": len(options),
                "submitted": bool(expert.get("completed_at")),
            }
        )
    return pd.DataFrame(rows)


def build_tidy_export(
    conn: sqlite3.Connection, options: pd.DataFrame, services: pd.DataFrame
) -> pd.DataFrame:
    responses = fetch_responses(conn)
    if responses.empty:
        return pd.DataFrame(
            columns=[
                "token",
                "name",
                "email",
                "region",
                "role",
                "organization",
                "completed_at",
                "csf_category",
                "option_id",
                "management_option",
                "question_id",
                "service_id",
                "service_group",
                "service_label",
                "answer",
                "skipped",
                "updated_at",
            ]
        )
    responses = responses.rename(columns={"updated_at": "response_updated_at"})
    experts = fetch_experts(conn)
    tidy = responses.merge(experts, on="token", how="left")
    tidy = tidy.merge(options, on="option_id", how="left")
    tidy = tidy.merge(services, on="service_id", how="left")
    tidy["answer"] = tidy.apply(
        lambda row: "skipped" if int(row["skipped"]) else row["response_value"], axis=1
    )
    columns = [
        "token",
        "name",
        "email",
        "region",
        "role",
        "organization",
        "completed_at",
        "csf_category",
        "option_id",
        "management_option",
        "question_id",
        "service_id",
        "service_group",
        "service_label",
        "answer",
        "skipped",
        "response_updated_at",
    ]
    tidy = tidy[columns].rename(columns={"response_updated_at": "updated_at"})
    return tidy.sort_values(["token", "csf_category", "option_id", "question_id"])


def build_matrix_export(
    conn: sqlite3.Connection, options: pd.DataFrame, services: pd.DataFrame
) -> pd.DataFrame:
    experts = fetch_experts(conn)
    response_rows = fetch_responses(conn)
    responses = {
        (row["token"], row["option_id"], row["response_key"]): row
        for row in response_rows.to_dict("records")
    } if not response_rows.empty else {}
    rating_labels = {question["question_id"]: question["short_label"] for question in RATING_QUESTIONS}

    rows = []
    for expert in experts.to_dict("records") if not experts.empty else []:
        for option in options.to_dict("records"):
            row = {
                "token": expert["token"],
                "name": expert["name"],
                "email": expert["email"],
                "region": expert["region"],
                "role": expert["role"],
                "organization": expert["organization"],
                "completed_at": expert["completed_at"],
                "CSF category": option["csf_category"],
                "Management option": option["management_option"],
            }
            for question_id, label in rating_labels.items():
                saved = responses.get((expert["token"], option["option_id"], question_id))
                row[label] = export_answer(saved)
            for service in services.to_dict("records"):
                key = response_key("effect", service["service_id"])
                saved = responses.get((expert["token"], option["option_id"], key))
                row[service["service_label"]] = export_answer(saved)
            rows.append(row)
    return pd.DataFrame(rows)


def export_answer(saved: dict[str, Any] | None) -> str:
    if not saved:
        return ""
    if int(saved.get("skipped", 0)):
        return "skipped"
    return saved.get("response_value") or ""


def aggregate_effect_summary(
    tidy: pd.DataFrame, services: pd.DataFrame, options: pd.DataFrame
) -> pd.DataFrame:
    if tidy.empty:
        return pd.DataFrame()
    effects = tidy[tidy["question_id"] == "effect"].copy()
    if effects.empty:
        return pd.DataFrame()
    effects["answer"] = effects["answer"].fillna("missing")
    grouped = (
        effects.groupby(
            [
                "csf_category",
                "option_id",
                "management_option",
                "service_id",
                "service_group",
                "service_label",
                "answer",
            ],
            dropna=False,
        )
        .size()
        .reset_index(name="count")
    )
    totals = grouped.groupby(["option_id", "service_id"])["count"].transform("sum")
    grouped["share"] = grouped["count"] / totals
    return grouped.sort_values(["csf_category", "option_id", "service_id", "answer"])


def excel_bytes(conn: sqlite3.Connection, options: pd.DataFrame, services: pd.DataFrame) -> bytes:
    tidy = build_tidy_export(conn, options, services)
    matrix = build_matrix_export(conn, options, services)
    summary = aggregate_effect_summary(tidy, services, options)
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        matrix.to_excel(writer, sheet_name="matrix", index=False)
        tidy.to_excel(writer, sheet_name="tidy", index=False)
        summary.to_excel(writer, sheet_name="effect_summary", index=False)
    return buffer.getvalue()


def write_exports(conn: sqlite3.Connection, options: pd.DataFrame, services: pd.DataFrame) -> dict[str, Path]:
    EXPORT_DIR.mkdir(exist_ok=True)
    tidy = build_tidy_export(conn, options, services)
    matrix = build_matrix_export(conn, options, services)
    summary = aggregate_effect_summary(tidy, services, options)
    paths = {
        "tidy": EXPORT_DIR / "responses_tidy.csv",
        "matrix": EXPORT_DIR / "responses_matrix.xlsx",
        "summary": EXPORT_DIR / "aggregate_summary.csv",
    }
    tidy.to_csv(paths["tidy"], index=False)
    summary.to_csv(paths["summary"], index=False)
    with pd.ExcelWriter(paths["matrix"], engine="openpyxl") as writer:
        matrix.to_excel(writer, sheet_name="matrix", index=False)
        tidy.to_excel(writer, sheet_name="tidy", index=False)
        summary.to_excel(writer, sheet_name="effect_summary", index=False)
    return paths


def require_streamlit() -> None:
    if st is None:
        raise RuntimeError("Streamlit is not installed. Run `pip install -r requirements.txt`.")


def inject_css() -> None:
    st.markdown(
        """
        <style>
        .block-container {padding-top: 2.6rem; max-width: 1260px;}
        div[data-testid="stMetric"] {
            background: #f7f9fb;
            border: 1px solid #e5e7eb;
            border-radius: 8px;
            padding: 0.8rem 0.9rem;
        }
        .survey-title {
            font-size: 1.9rem;
            font-weight: 720;
            color: #101828;
            margin: 0 0 0.15rem 0;
        }
        .category-pill {
            display: inline-block;
            padding: 0.35rem 0.7rem;
            border-radius: 999px;
            background: #e9f4ef;
            color: #175c3c;
            font-size: 0.8rem;
            line-height: 1.35;
            margin: 0.25rem 0 0.9rem 0;
            max-width: 100%;
        }
        .effect-legend span {
            display: inline-block;
            margin-right: 0.7rem;
            font-size: 0.85rem;
        }
        .dot {
            width: 0.7rem;
            height: 0.7rem;
            border-radius: 999px;
            display: inline-block;
            margin-right: 0.25rem;
            vertical-align: -0.05rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_effect_legend() -> None:
    st.markdown(
        """
        <div class="effect-legend">
            <span><i class="dot" style="background:#16803c"></i>Positive</span>
            <span><i class="dot" style="background:#8a94a6"></i>Neutral</span>
            <span><i class="dot" style="background:#bf3f36"></i>Negative</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def invite_link(token: str) -> str:
    suffix = f"?token={token}"
    return f"{APP_BASE_URL}{suffix}" if APP_BASE_URL else suffix


def run_survey(conn: sqlite3.Connection, config: SurveyConfig) -> None:
    query_token = st.query_params.get("token", "")
    token = normalize_token(st.session_state.get("active_token", query_token))

    if not token:
        render_token_entry(conn)
        return

    expert = fetch_expert(conn, token)
    if not expert:
        st.session_state.pop("active_token", None)
        st.error("That invite token was not found. Please check the link or token spelling.")
        render_token_entry(conn)
        return

    st.session_state["active_token"] = token
    if st.query_params.get("token") != token:
        st.query_params["token"] = token

    if not expert.get("consent_at"):
        render_intro(conn, expert)
        return

    if expert.get("completed_at"):
        render_personal_summary(conn, config, expert, locked=True)
        return

    render_survey_card(conn, config, expert)


def render_token_entry(conn: sqlite3.Connection) -> None:
    st.markdown('<p class="survey-title">Forest CSF preference survey</p>', unsafe_allow_html=True)
    st.caption("Enter the invite token provided by the study team.")
    token = st.text_input("Invite token", placeholder="example: demo-expert")
    if st.button("Open survey", type="primary"):
        token = normalize_token(token)
        if fetch_expert(conn, token):
            st.session_state["active_token"] = token
            st.query_params["token"] = token
            st.rerun()
        st.error("Token not found.")


def render_intro(conn: sqlite3.Connection, expert: dict[str, Any]) -> None:
    st.markdown('<p class="survey-title">Before you begin</p>', unsafe_allow_html=True)
    st.caption("Confirm your details and consent to participate.")
    with st.form("expert_intro"):
        col1, col2 = st.columns(2)
        with col1:
            name = st.text_input("Name", value=expert.get("name", ""))
            email = st.text_input("Email", value=expert.get("email", ""))
            region = st.text_input("Region", value=expert.get("region", ""))
        with col2:
            role = st.text_input("Role", value=expert.get("role", ""))
            organization = st.text_input("Organization", value=expert.get("organization", ""))
        consent = st.checkbox(
            "I consent to participate and understand my responses will be used for research analysis."
        )
        submitted = st.form_submit_button("Start survey", type="primary")

    if submitted:
        if not name.strip() or not region.strip() or not role.strip():
            st.error("Please provide at least your name, region, and role.")
            return
        if not consent:
            st.error("Consent is required before starting the survey.")
            return
        save_expert_profile(
            conn,
            expert["token"],
            {
                "name": name,
                "email": email,
                "region": region,
                "role": role,
                "organization": organization,
            },
        )
        mark_consent(conn, expert["token"])
        st.rerun()


def render_progress_sidebar(
    config: SurveyConfig, status: pd.DataFrame, expert: dict[str, Any], current_index: int
) -> None:
    st.sidebar.subheader("Survey progress")
    total_complete = int(status["complete"].sum())
    st.sidebar.progress(total_complete / len(status), text=f"{total_complete}/{len(status)} options complete")
    st.sidebar.caption(f"Respondent: {expert.get('name') or expert['token']}")

    for category, rows in status.groupby("csf_category", sort=False):
        complete = int(rows["complete"].sum())
        st.sidebar.write(f"**{category}**")
        st.sidebar.progress(complete / len(rows), text=f"{complete}/{len(rows)}")

    st.sidebar.divider()
    jump_labels = [
        f"{i + 1}. {row['management_option']}"
        for i, row in config.options.iterrows()
    ]
    selected = st.sidebar.selectbox(
        "Jump to option",
        options=list(range(len(jump_labels))),
        index=current_index,
        format_func=lambda idx: jump_labels[idx],
    )
    if selected != current_index:
        st.session_state[f"current_option_{expert['token']}"] = selected
        st.rerun()


def first_incomplete_index(status: pd.DataFrame) -> int:
    incomplete = status.index[~status["complete"]].tolist()
    return int(incomplete[0]) if incomplete else 0


def render_survey_card(conn: sqlite3.Connection, config: SurveyConfig, expert: dict[str, Any]) -> None:
    responses = fetch_responses(conn, expert["token"])
    status = completion_status(config.options, config.services, responses)
    session_key = f"current_option_{expert['token']}"
    if session_key not in st.session_state:
        st.session_state[session_key] = first_incomplete_index(status)
    current_index = int(st.session_state[session_key])
    current_index = max(0, min(current_index, len(config.options) - 1))
    render_progress_sidebar(config, status, expert, current_index)

    option = config.options.iloc[current_index].to_dict()
    saved = response_lookup(responses)
    st.markdown(f'<div class="category-pill">{option["csf_category"]}</div>', unsafe_allow_html=True)
    st.markdown(f'<p class="survey-title">{option["management_option"]}</p>', unsafe_allow_html=True)
    st.caption(f"Management option {current_index + 1} of {len(config.options)}")
    st.progress(
        (current_index + 1) / len(config.options),
        text=f"Card {current_index + 1} of {len(config.options)}",
    )
    render_effect_legend()

    form_id = f"card_form_{expert['token']}_{option['option_id']}"
    with st.form(form_id):
        st.subheader("CSF implementation ratings")
        rating_answers: dict[str, str] = {}
        rating_cols = st.columns(3)
        for col, question in zip(rating_cols, RATING_QUESTIONS):
            key = question["question_id"]
            current = widget_value_from_saved(saved, option["option_id"], key)
            index = RATING_OPTIONS.index(current) if current in RATING_OPTIONS else None
            with col:
                rating_answers[key] = st.radio(
                    question["label"],
                    RATING_OPTIONS,
                    index=index,
                    horizontal=True,
                    help=question["help"],
                    key=f"{form_id}_{key}",
                )

        effect_answers: dict[str, str] = {}
        for group_name, group_services in config.services.groupby("service_group", sort=False):
            st.subheader(f"{group_name} service effects")
            columns = st.columns(2 if len(group_services) <= 6 else 3)
            for idx, service in enumerate(group_services.to_dict("records")):
                key = response_key("effect", service["service_id"])
                current = widget_value_from_saved(saved, option["option_id"], key)
                index = EFFECT_OPTIONS.index(current) if current in EFFECT_OPTIONS else None
                with columns[idx % len(columns)]:
                    effect_answers[service["service_id"]] = st.radio(
                        service["service_label"],
                        EFFECT_OPTIONS,
                        index=index,
                        horizontal=True,
                        key=f"{form_id}_{service['service_id']}",
                    )

        col1, col2, col3, col4 = st.columns([1.1, 1.1, 1.2, 1.6])
        save_only = col1.form_submit_button("Save")
        prev_clicked = col2.form_submit_button("Save and previous")
        next_clicked = col3.form_submit_button("Save and next", type="primary")
        final_clicked = col4.form_submit_button("Save and submit survey")

    if save_only or prev_clicked or next_clicked or final_clicked:
        for question_id, answer in rating_answers.items():
            upsert_response(conn, expert["token"], option["option_id"], question_id, None, answer)
        for service_id, answer in effect_answers.items():
            upsert_response(conn, expert["token"], option["option_id"], "effect", service_id, answer)
        conn.commit()

        updated_status = completion_status(config.options, config.services, fetch_responses(conn, expert["token"]))
        if final_clicked:
            if bool(updated_status["complete"].all()):
                mark_completed(conn, expert["token"])
                st.success("Survey submitted. Thank you.")
            else:
                missing = int((~updated_status["complete"]).sum())
                st.warning(f"{missing} management options still need answers or explicit skips.")
                st.session_state[session_key] = first_incomplete_index(updated_status)
            st.rerun()

        if prev_clicked:
            st.session_state[session_key] = max(0, current_index - 1)
        elif next_clicked:
            st.session_state[session_key] = min(len(config.options) - 1, current_index + 1)
        st.toast("Responses saved.")
        st.rerun()


def render_personal_summary(
    conn: sqlite3.Connection, config: SurveyConfig, expert: dict[str, Any], locked: bool
) -> None:
    st.markdown('<p class="survey-title">Your submitted summary</p>', unsafe_allow_html=True)
    if locked:
        st.info("Your final submission has been recorded. Contact the study team if edits are needed.")
    responses = fetch_responses(conn, expert["token"])
    status = completion_status(config.options, config.services, responses)
    col1, col2, col3 = st.columns(3)
    col1.metric("Completed options", f"{int(status['complete'].sum())}/{len(status)}")
    col2.metric("Answered or skipped items", f"{int(status['answered'].sum())}/{int(status['total'].sum())}")
    skipped = int((responses["skipped"] == 1).sum()) if not responses.empty else 0
    col3.metric("Explicit skips", skipped)

    if responses.empty:
        st.warning("No responses found yet.")
        return
    render_bubble_plot(conn, config, expert["token"])
    render_effect_heatmap(conn, config, expert["token"], title="Your ecosystem service effect map")


def render_bubble_plot(conn: sqlite3.Connection, config: SurveyConfig, token: str) -> None:
    tidy = build_tidy_export(conn, config.options, config.services)
    person = tidy[(tidy["token"] == token) & (tidy["question_id"].isin(["importance", "feasibility", "area_scale"]))]
    if person.empty or px is None:
        return
    pivot = (
        person.pivot_table(
            index=["option_id", "management_option", "csf_category"],
            columns="question_id",
            values="answer",
            aggfunc="first",
        )
        .reset_index()
    )
    for col in ["importance", "feasibility", "area_scale"]:
        pivot[col] = pd.to_numeric(pivot.get(col), errors="coerce")
    plot_df = pivot.dropna(subset=["importance", "feasibility"])
    if plot_df.empty:
        return
    st.subheader("Importance and feasibility")
    fig = px.scatter(
        plot_df,
        x="feasibility",
        y="importance",
        size=plot_df["area_scale"].fillna(1),
        color="csf_category",
        hover_name="management_option",
        range_x=[0.5, 5.5],
        range_y=[0.5, 5.5],
        labels={
            "feasibility": "Feasibility",
            "importance": "Importance",
            "csf_category": "CSF category",
            "size": "Implementation area scale",
        },
    )
    fig.update_layout(height=440, margin=dict(l=10, r=10, t=20, b=10))
    st.plotly_chart(fig, use_container_width=True)


def render_effect_heatmap(
    conn: sqlite3.Connection, config: SurveyConfig, token: str | None = None, title: str = "Effect map"
) -> None:
    if go is None:
        return
    tidy = build_tidy_export(conn, config.options, config.services)
    effects = tidy[tidy["question_id"] == "effect"].copy()
    if token:
        effects = effects[effects["token"] == token]
    if effects.empty:
        return
    if token:
        effects["score"] = effects["answer"].map(EFFECT_SCORE)
        matrix = effects.pivot_table(
            index="management_option", columns="service_label", values="score", aggfunc="first"
        )
        hover = effects.pivot_table(
            index="management_option", columns="service_label", values="answer", aggfunc="first"
        )
    else:
        dominant = (
            effects.groupby(["management_option", "service_label", "answer"])
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
            .drop_duplicates(["management_option", "service_label"])
        )
        dominant["score"] = dominant["answer"].map(EFFECT_SCORE)
        matrix = dominant.pivot_table(
            index="management_option", columns="service_label", values="score", aggfunc="first"
        )
        hover = dominant.pivot_table(
            index="management_option", columns="service_label", values="answer", aggfunc="first"
        )

    if matrix.empty:
        return
    ordered_options = config.options["management_option"].tolist()
    ordered_services = config.services["service_label"].tolist()
    matrix = matrix.reindex(index=ordered_options, columns=ordered_services)
    hover = hover.reindex(index=ordered_options, columns=ordered_services).fillna("missing")
    st.subheader(title)
    fig = go.Figure(
        data=go.Heatmap(
            z=matrix.values,
            x=matrix.columns,
            y=matrix.index,
            text=hover.values,
            hovertemplate="<b>%{y}</b><br>%{x}<br>Effect: %{text}<extra></extra>",
            zmin=-1,
            zmax=1,
            colorscale=[
                [0.0, "#bf3f36"],
                [0.49, "#edf0f5"],
                [0.57, "#d69e2e"],
                [1.0, "#16803c"],
            ],
            colorbar=dict(title="Effect"),
        )
    )
    fig.update_layout(
        height=780,
        margin=dict(l=10, r=10, t=20, b=10),
        xaxis=dict(tickangle=-45, tickfont=dict(size=10)),
        yaxis=dict(tickfont=dict(size=10)),
    )
    st.plotly_chart(fig, use_container_width=True)


def run_admin(conn: sqlite3.Connection, config: SurveyConfig) -> None:
    if not st.session_state.get("admin_ok"):
        st.markdown('<p class="survey-title">Admin access</p>', unsafe_allow_html=True)
        passcode = st.text_input("Admin passcode", type="password")
        if st.button("Open dashboard", type="primary"):
            if passcode == ADMIN_PASSCODE:
                st.session_state["admin_ok"] = True
                st.rerun()
            st.error("Incorrect passcode.")
        return

    st.markdown('<p class="survey-title">Study dashboard</p>', unsafe_allow_html=True)
    tab_dashboard, tab_invites, tab_exports = st.tabs(["Dashboard", "Invite tokens", "Exports"])
    with tab_dashboard:
        render_admin_dashboard(conn, config)
    with tab_invites:
        render_invites_admin(conn)
    with tab_exports:
        render_exports_admin(conn, config)


def render_admin_dashboard(conn: sqlite3.Connection, config: SurveyConfig) -> None:
    experts = completion_for_all_experts(conn, config.options, config.services)
    if experts.empty:
        st.warning("No experts are configured.")
        return
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Experts", len(experts))
    c2.metric("Submitted", int(experts["submitted"].sum()))
    c3.metric("Average progress", f"{experts['progress'].mean() * 100:.0f}%")
    c4.metric("Options", len(config.options))

    filters = st.columns(3)
    regions = ["All"] + sorted([x for x in experts["region"].dropna().unique() if x])
    roles = ["All"] + sorted([x for x in experts["role"].dropna().unique() if x])
    categories = ["All"] + config.options["csf_category"].drop_duplicates().tolist()
    region = filters[0].selectbox("Region", regions)
    role = filters[1].selectbox("Role", roles)
    category = filters[2].selectbox("CSF category", categories)

    filtered_experts = experts.copy()
    if region != "All":
        filtered_experts = filtered_experts[filtered_experts["region"] == region]
    if role != "All":
        filtered_experts = filtered_experts[filtered_experts["role"] == role]

    if px is not None:
        progress_fig = px.bar(
            filtered_experts.sort_values("progress"),
            x="progress",
            y="name",
            color="submitted",
            orientation="h",
            hover_data=["region", "role", "completed_options", "total_options"],
            labels={"progress": "Progress", "name": "Expert", "submitted": "Submitted"},
        )
        progress_fig.update_xaxes(tickformat=".0%")
        progress_fig.update_layout(height=360, margin=dict(l=10, r=10, t=20, b=10))
        st.plotly_chart(progress_fig, use_container_width=True)

    tidy = build_tidy_export(conn, config.options, config.services)
    if tidy.empty:
        st.info("No responses have been saved yet.")
        return
    if region != "All":
        tidy = tidy[tidy["region"] == region]
    if role != "All":
        tidy = tidy[tidy["role"] == role]
    if category != "All":
        tidy = tidy[tidy["csf_category"] == category]

    render_effect_heatmap_from_tidy(tidy, config, "Dominant ecosystem service effect")
    render_consensus_view(tidy)
    with st.expander("Raw tidy responses"):
        st.dataframe(tidy, use_container_width=True, hide_index=True)


def render_effect_heatmap_from_tidy(tidy: pd.DataFrame, config: SurveyConfig, title: str) -> None:
    if go is None:
        return
    effects = tidy[tidy["question_id"] == "effect"].copy()
    if effects.empty:
        return
    dominant = (
        effects.groupby(["management_option", "service_label", "answer"])
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .drop_duplicates(["management_option", "service_label"])
    )
    dominant["score"] = dominant["answer"].map(EFFECT_SCORE)
    matrix = dominant.pivot_table(
        index="management_option", columns="service_label", values="score", aggfunc="first"
    )
    hover = dominant.pivot_table(
        index="management_option", columns="service_label", values="answer", aggfunc="first"
    ).fillna("missing")
    if matrix.empty:
        return
    ordered_options = [x for x in config.options["management_option"].tolist() if x in matrix.index]
    ordered_services = [x for x in config.services["service_label"].tolist() if x in matrix.columns]
    matrix = matrix.reindex(index=ordered_options, columns=ordered_services)
    hover = hover.reindex(index=ordered_options, columns=ordered_services)
    st.subheader(title)
    fig = go.Figure(
        data=go.Heatmap(
            z=matrix.values,
            x=matrix.columns,
            y=matrix.index,
            text=hover.values,
            hovertemplate="<b>%{y}</b><br>%{x}<br>Dominant: %{text}<extra></extra>",
            zmin=-1,
            zmax=1,
            colorscale=[
                [0.0, "#bf3f36"],
                [0.49, "#edf0f5"],
                [0.57, "#d69e2e"],
                [1.0, "#16803c"],
            ],
        )
    )
    fig.update_layout(
        height=740,
        margin=dict(l=10, r=10, t=20, b=10),
        xaxis=dict(tickangle=-45, tickfont=dict(size=10)),
        yaxis=dict(tickfont=dict(size=10)),
    )
    st.plotly_chart(fig, use_container_width=True)


def render_consensus_view(tidy: pd.DataFrame) -> None:
    if px is None:
        return
    effects = tidy[tidy["question_id"] == "effect"].copy()
    if effects.empty:
        return
    effects["answer"] = effects["answer"].fillna("missing")
    counts = (
        effects.groupby(["service_group", "answer"])
        .size()
        .reset_index(name="count")
    )
    totals = counts.groupby("service_group")["count"].transform("sum")
    counts["share"] = counts["count"] / totals
    st.subheader("Consensus and uncertainty by service group")
    fig = px.bar(
        counts,
        x="service_group",
        y="share",
        color="answer",
        text=counts["share"].map(lambda x: f"{x:.0%}"),
        color_discrete_map={
            "positive": EFFECT_COLORS["positive"],
            "neutral": EFFECT_COLORS["neutral"],
            "negative": EFFECT_COLORS["negative"],
            "skipped": EFFECT_COLORS["skipped"],
            "missing": "#edf0f5",
        },
        labels={"service_group": "Service group", "share": "Share", "answer": "Answer"},
    )
    fig.update_yaxes(tickformat=".0%")
    fig.update_layout(height=380, margin=dict(l=10, r=10, t=20, b=10), barmode="stack")
    st.plotly_chart(fig, use_container_width=True)


def render_invites_admin(conn: sqlite3.Connection) -> None:
    st.subheader("Create invite token")
    with st.form("add_expert"):
        col1, col2 = st.columns(2)
        with col1:
            name = st.text_input("Name")
            email = st.text_input("Email")
            region = st.text_input("Region")
        with col2:
            role = st.text_input("Role")
            organization = st.text_input("Organization")
            manual_token = st.text_input("Custom token (optional)")
        submitted = st.form_submit_button("Create invite", type="primary")
    if submitted:
        if not name.strip() or not region.strip() or not role.strip():
            st.error("Name, region, and role are required.")
        else:
            try:
                token = add_expert(conn, name, email, region, role, organization, manual_token or None)
                st.success(f"Invite created: {invite_link(token)}")
            except sqlite3.IntegrityError:
                st.error("That token already exists. Use a different custom token.")

    experts = fetch_experts(conn)
    if not experts.empty:
        display = experts.copy()
        display["invite_link"] = display["token"].map(invite_link)
        st.dataframe(
            display[
                [
                    "name",
                    "email",
                    "region",
                    "role",
                    "organization",
                    "token",
                    "invite_link",
                    "consent_at",
                    "completed_at",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )


def render_exports_admin(conn: sqlite3.Connection, config: SurveyConfig) -> None:
    tidy = build_tidy_export(conn, config.options, config.services)
    matrix = build_matrix_export(conn, config.options, config.services)
    st.subheader("Download data")
    st.download_button(
        "Download tidy CSV",
        tidy.to_csv(index=False).encode("utf-8"),
        file_name="responses_tidy.csv",
        mime="text/csv",
    )
    st.download_button(
        "Download Excel workbook",
        excel_bytes(conn, config.options, config.services),
        file_name="responses_matrix.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    if st.button("Write export files to exports/", type="primary"):
        paths = write_exports(conn, config.options, config.services)
        st.success(
            "Wrote "
            + ", ".join(f"{name}: {path.relative_to(BASE_DIR)}" for name, path in paths.items())
        )
    with st.expander("Matrix preview"):
        st.dataframe(matrix, use_container_width=True, hide_index=True)
    with st.expander("Tidy preview"):
        st.dataframe(tidy, use_container_width=True, hide_index=True)


def main() -> None:
    require_streamlit()
    st.set_page_config(
        page_title="Forest CSF Preference Survey",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    inject_css()
    config = load_config()
    init_db()
    conn = connect_db()
    mode = st.sidebar.radio("Mode", ["Survey", "Admin"], horizontal=True)
    st.sidebar.divider()
    if mode == "Survey":
        run_survey(conn, config)
    else:
        run_admin(conn, config)
    conn.close()


if __name__ == "__main__":
    main()
