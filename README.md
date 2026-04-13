# agent-mesh-dashboard

Streamlit operator UI for Agent Mesh (positions, signals, health).

## Run locally

```bash
pip install -r requirements.txt
REDIS_HOST=localhost streamlit run app.py
```

## Docker

```bash
docker build -t agent-mesh-dashboard .
docker run -p 8501:8501 -e REDIS_HOST=host.docker.internal agent-mesh-dashboard
```

Deployed behind **Caddy** via `agent-mesh-infra`.
