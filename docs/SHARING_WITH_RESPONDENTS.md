# Sharing The Survey With Respondents

The firewall message means the respondent's workplace is blocking plain HTTP traffic, direct IP traffic, or a non-standard port. That is a network policy decision, not a bug in the Streamlit app.

Do not try to bypass the workplace firewall. Use one of the deployment patterns below, or ask the respondent's IT team to allow the approved survey URL.

## Recommended Options

### 1. Host on an HTTPS Streamlit URL

For a quick pilot, deploy from GitHub to Streamlit Community Cloud. Respondents get an HTTPS URL like:

```text
https://your-survey-name.streamlit.app
```

This is usually easier for work computers than:

```text
http://195.148.31.109:8866
```

Important: the current app stores responses in SQLite. For real data collection, use a deployment with persistent storage, or export/download responses frequently from the admin screen.

### 2. Self-host With HTTPS On A Domain

If you have a server, run Streamlit internally on port `8866` and put HTTPS in front of it on standard port `443`. The full setup is in [SELF_HOST_HTTPS.md](SELF_HOST_HTTPS.md).

Example with Docker:

```bash
cp .env.example .env
nano .env
docker compose -f docker-compose.https.yml up -d --build
```

Then configure a reverse proxy. With Caddy, copy `deployment/Caddyfile.example`, replace `survey.example.org` with your domain, and run Caddy on the server:

```caddyfile
survey.example.org {
    reverse_proxy 127.0.0.1:8866
}
```

Caddy will serve the public site through HTTPS and forward traffic to Streamlit locally.

### 3. Ask IT To Allowlist The Survey

If the respondent's organization still blocks the HTTPS site, send their IT team:

- The final HTTPS URL.
- The purpose of the survey.
- Confirmation that it uses HTTPS.
- The expected respondent group and study period.

Avoid asking them to allow `http://IP:8866`; many organizations block that by design.

## Local Testing

If the friend only needs to test the app locally, they can run:

```bash
streamlit run app.py
```

and open:

```text
http://localhost:8866
```

This keeps the app on their own computer. It is not suitable for central data collection unless they send you the resulting `survey.db` or exported files.
