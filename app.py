"""Operator dashboard — realtime-ish via Streamlit auto-refresh."""

import os
import time

import redis
import streamlit as st

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

    intents_key = "approved:intents"
    if r.exists(intents_key):
        st.metric("approved:intents", r.llen(intents_key), help="LLEN")

except Exception as e:
    st.error(f"Redis: {e}")

st.caption("Refresh: rerun the app (⋮ menu → Rerun) or enable auto-refresh if configured.")

st.info("Add Postgres queries for trades, signals, and heartbeats. See agent-mesh-infra for wiring.")
