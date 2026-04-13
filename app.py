"""Operator dashboard — Redis + Postgres with Streamlit fragments (periodic refresh)."""

from __future__ import annotations

import os
from datetime import timedelta

import psycopg2
import redis
import streamlit as st
from psycopg2.extras import RealDictCursor

STREAM_KEY = os.getenv("STREAM_KEY", "stream:approved:intents")


def _pg_conn():
    host = os.getenv("POSTGRES_HOST")
    if not host:
        return None
    return psycopg2.connect(
        host=host,
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "agent"),
        password=os.getenv("POSTGRES_PASSWORD", "change-me"),
        dbname=os.getenv("POSTGRES_DB", "agent_mesh"),
        connect_timeout=5,
    )


st.set_page_config(page_title="Agent Mesh", layout="wide")
st.title("Agent Mesh — control plane")


@st.fragment(run_every=timedelta(seconds=4))
def panel_redis() -> None:
    redis_host = os.getenv("REDIS_HOST", "localhost")
    try:
        r = redis.Redis(host=redis_host, port=6379, decode_responses=True)
        r.ping()
        st.success(f"Redis OK ({redis_host})")

        hb_key = "execution:heartbeat"
        ttl = r.ttl(hb_key)
        val = r.get(hb_key)
        ttl_note = (
            "no such key"
            if ttl == -2
            else ("no expiry" if ttl == -1 else f"{ttl} s")
        )
        st.caption(f"`{hb_key}` TTL: {ttl_note}")
        if val is not None:
            st.metric("execution:heartbeat (raw)", val[:80] + ("…" if len(val) > 80 else ""))

        if r.exists(STREAM_KEY):
            st.metric("Stream XLEN", r.xlen(STREAM_KEY), help=STREAM_KEY)
        else:
            st.caption(f"No `{STREAM_KEY}` yet")

        legacy = "approved:intents"
        if r.exists(legacy):
            st.metric("Legacy list", r.llen(legacy))
    except Exception as e:
        st.error(f"Redis: {e}")


@st.fragment(run_every=timedelta(seconds=8))
def panel_postgres() -> None:
    st.subheader("execution_events (read-only)")
    conn = _pg_conn()
    if conn is None:
        st.caption("Set POSTGRES_HOST (and credentials) to show DB stats.")
        return
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT count(*) AS n FROM execution_events")
                n = cur.fetchone()["n"]
                st.metric("Rows in execution_events", f"{n:,}")
                cur.execute(
                    """
                    SELECT id, intent_id, trace_id, status, created_at
                    FROM execution_events
                    ORDER BY id DESC
                    LIMIT 8
                    """
                )
                rows = cur.fetchall()
        if rows:
            st.dataframe(rows, hide_index=True, use_container_width=True)
        else:
            st.caption("No rows yet.")
    except Exception as e:
        st.warning(f"Postgres: {e}")


panel_redis()
panel_postgres()

st.caption(
    "Metrics: scrape execution service `:9090/metrics` (Prometheus). "
    "Fragments refresh independently (Streamlit ≥ 1.33)."
)
