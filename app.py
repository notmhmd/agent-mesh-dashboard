"""Operator dashboard — realtime-ish via Streamlit auto-refresh."""

import os

import redis
import streamlit as st

st.set_page_config(page_title="Agent Mesh", layout="wide")
st.title("Agent Mesh — control plane")

redis_host = os.getenv("REDIS_HOST", "localhost")
try:
    r = redis.Redis(host=redis_host, port=6379, decode_responses=True)
    r.ping()
    st.success("Redis OK")
except Exception as e:
    st.error(f"Redis: {e}")

st.info("Add Postgres queries for trades, signals, and heartbeats. See agent-mesh-infra for wiring.")
