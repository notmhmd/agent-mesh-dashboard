"""Operator dashboard — Redis + Postgres with Streamlit fragments (periodic refresh)."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta

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


@st.fragment(run_every=timedelta(seconds=5))
def panel_flow_health() -> None:
    """Strategist → signal → execution visibility (cursors + approximate lag)."""
    st.subheader("Pipeline health (insight → signal → execution)")
    redis_host = os.getenv("REDIS_HOST", "localhost")
    try:
        r = redis.Redis(host=redis_host, port=6379, decode_responses=True)
        r.ping()
    except Exception as e:
        st.warning(f"Redis: {e}")
        return

    sig_key = "signal_agent:last_mi_id"
    learn_key = "learning_agent:last_exec_id"
    sig_cur = r.get(sig_key)
    learn_cur = r.get(learn_key)
    last_mi_redis = r.get("mesh:mi:last_id")
    last_notify = r.get("mesh:mi:last_notify_unix")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("signal cursor (`last_mi_id`)", sig_cur or "—")
    with c2:
        st.metric("learning cursor (`last_exec_id`)", learn_cur or "—")
    with c3:
        st.metric("last notify MI id", last_mi_redis or "—")
    with c4:
        if last_notify:
            try:
                ts = float(last_notify)
                ago = datetime.now(UTC).timestamp() - ts
                st.metric("seconds since MI notify", f"{ago:.1f}")
            except ValueError:
                st.caption("mesh:mi:last_notify_unix unreadable")
        else:
            st.caption("No `mesh:mi:last_notify_unix` (strategist not publishing yet)")

    conn = _pg_conn()
    if conn is None:
        st.caption("Set POSTGRES_HOST for backlog + lag columns.")
        return

    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT COALESCE(MAX(id), 0)::bigint AS max_id FROM market_intelligence")
                max_mi = int(cur.fetchone()["max_id"])
                cur.execute("SELECT COALESCE(MAX(id), 0)::bigint AS max_id FROM execution_events")
                max_ex = int(cur.fetchone()["max_id"])

        pending = None
        if sig_cur is not None and str(sig_cur).isdigit():
            pending = max(0, max_mi - int(sig_cur))

        b1, b2, b3 = st.columns(3)
        with b1:
            st.metric("max market_intelligence.id", max_mi)
        with b2:
            st.metric("MI backlog vs signal cursor", pending if pending is not None else "n/a")
        with b3:
            if learn_cur is not None and str(learn_cur).isdigit() and max_ex >= 0:
                lp = max(0, max_ex - int(learn_cur))
                st.metric("exec backlog vs learning cursor", lp)
            else:
                st.caption("Learning cursor n/a")

        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT ee.id AS exec_id, ee.trace_id, ee.status,
                           ee.created_at AS execution_at,
                           mi.id AS mi_id, mi.created_at AS mi_at,
                           EXTRACT(EPOCH FROM (ee.created_at - mi.created_at)) AS lag_sec
                    FROM execution_events ee
                    INNER JOIN market_intelligence mi
                      ON ee.trace_id = 'mesh-mi-' || mi.id::text
                    ORDER BY ee.id DESC
                    LIMIT 8
                    """
                )
                lag_rows = cur.fetchall()
        if lag_rows:
            st.caption("Matched rows: `trace_id` = `mesh-mi-{market_intelligence.id}` (signal path).")
            st.dataframe(lag_rows, hide_index=True, use_container_width=True)
        else:
            st.caption(
                "No matched insight→execution rows yet (need signal-agent intents with `mesh-mi-*` traces)."
            )
    except Exception as e:
        st.warning(f"Pipeline health query: {e}")


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


@st.fragment(run_every=timedelta(seconds=10))
def panel_llm() -> None:
    st.subheader("LLM strategist (LiteLLM — not on execution hot path)")
    redis_host = os.getenv("REDIS_HOST", "localhost")
    try:
        r = redis.Redis(host=redis_host, port=6379, decode_responses=True)
        raw = r.get("strategist:latest")
        if raw:
            try:
                st.json(json.loads(raw))
            except json.JSONDecodeError:
                st.code(raw)
        else:
            st.caption("No `strategist:latest` yet — run `docker compose --profile llm up -d strategist` with API keys.")
    except Exception as e:
        st.warning(f"Redis LLM cache: {e}")

    conn = _pg_conn()
    if conn is None:
        return
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, regime, sentiment_score, regime_multiplier, left(summary, 200) AS summary,
                           source, created_at
                    FROM market_intelligence
                    ORDER BY id DESC
                    LIMIT 5
                    """
                )
                rows = cur.fetchall()
        if rows:
            st.dataframe(rows, hide_index=True, use_container_width=True)
        else:
            st.caption("No rows in market_intelligence yet.")
    except Exception as e:
        st.caption(f"market_intelligence: {e}")


panel_redis()
panel_flow_health()
panel_postgres()
panel_llm()

st.caption(
    "Metrics: scrape execution service `:9090/metrics` (Prometheus). "
    "Fragments refresh independently (Streamlit ≥ 1.33)."
)
