"""Operator dashboard — Redis Streams + heartbeat (matches agent-mesh-execution)."""

import os
import time

import redis
import streamlit as st

STREAM_KEY = os.getenv("STREAM_KEY", "stream:approved:intents")

st.set_page_config(page_title="Agent Mesh", layout="wide")
st.title("Agent Mesh — control plane")

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
        ts_age = None
        try:
            ts = float(str(val).strip())
            if ts > 1e12:
                ts /= 1000.0
            if ts > 1e9:
                ts_age = time.time() - ts
        except (TypeError, ValueError):
            pass
        if ts_age is not None:
            st.metric("Heartbeat age (s)", f"{max(0, ts_age):.0f}")
            st.caption(f"Raw value: {val!r}")
        else:
            st.metric("execution:heartbeat", val)
    else:
        st.caption("No value at execution:heartbeat")

    if r.exists(STREAM_KEY):
        st.metric("Stream length (XLEN)", r.xlen(STREAM_KEY), help=STREAM_KEY)
    else:
        st.caption(f"No stream `{STREAM_KEY}` yet (publisher will create on first XADD)")

    legacy = "approved:intents"
    if r.exists(legacy):
        st.metric("Legacy list LLEN", r.llen(legacy), help="deprecated; use Streams")

except Exception as e:
    st.error(f"Redis: {e}")

st.caption("Refresh: Rerun from the ⋮ menu. For auto-refresh, use Streamlit `run_on_save` or an external reverse proxy.")

st.info("Postgres metrics: connect read-only from Grafana or extend this app with `psycopg2`.")
