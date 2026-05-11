# Self-Host The Survey With HTTPS

This guide deploys the Streamlit survey at a normal HTTPS URL like:

```text
https://survey.example.org
```

It uses Docker Compose for the app and Caddy for automatic HTTPS. Caddy listens on standard web ports `80` and `443`, then forwards traffic internally to Streamlit on port `8866`.

## 1. What You Need

- A Linux server or VM with a public IP address.
- A domain or subdomain you control, for example `survey.example.org`.
- DNS access so you can point the domain to the server.
- Ports `80` and `443` open on the server firewall/security group.
- Docker Engine and Docker Compose plugin installed.

## 2. Point DNS To The Server

Create a DNS `A` record:

```text
survey.example.org -> your.server.ip.address
```

If your server has IPv6, you can also add an `AAAA` record.

Wait until DNS resolves before starting HTTPS. You can check from your laptop:

```bash
dig survey.example.org
```

## 3. Copy The Project To The Server

On the server, place this project in a directory such as:

```bash
/opt/csf-survey
```

Then enter the project directory:

```bash
cd /opt/csf-survey
```

## 4. Configure Environment Values

Copy the template and edit it:

```bash
cp .env.example .env
nano .env
```

Set:

```text
SURVEY_DOMAIN=survey.example.org
APP_BASE_URL=https://survey.example.org
SURVEY_ADMIN_PASSCODE=a-long-random-admin-passcode
```

Do not commit `.env` if it contains the real admin passcode.

## 5. Start The HTTPS Deployment

Run:

```bash
docker compose -f docker-compose.https.yml up -d --build
```

Watch logs:

```bash
docker compose -f docker-compose.https.yml logs -f
```

When Caddy has successfully obtained a certificate, open:

```text
https://survey.example.org
```

## 6. Create Invite Links

Open Admin mode in the app and use the admin passcode from `.env`.

Invite links will use `APP_BASE_URL`, so generated links should look like:

```text
https://survey.example.org?token=demo-expert
```

## 7. Updating The App

After changing code or config:

```bash
docker compose -f docker-compose.https.yml up -d --build
```

The SQLite database is stored in the Docker volume `survey_data`, so rebuilding the image does not erase responses.

## 8. Backups

Use the admin screen to download exports frequently.

For a server-side SQLite backup, stop writes briefly and copy the Docker volume data, or use:

```bash
docker compose -f docker-compose.https.yml exec survey python - <<'PY'
import sqlite3
src = sqlite3.connect("/data/survey.db")
dst = sqlite3.connect("/data/survey-backup.db")
src.backup(dst)
dst.close()
src.close()
PY
docker compose -f docker-compose.https.yml cp survey:/data/survey-backup.db ./survey-backup.db
```

## 9. Common Problems

- **Caddy cannot get a certificate:** confirm DNS points to the server and ports `80`/`443` are open.
- **The site works locally but not for colleagues:** ask IT to allowlist the final `https://survey.example.org` URL.
- **Admin links show the wrong host:** check `APP_BASE_URL` in `.env` and restart Compose.
- **Port 8866 is not public:** that is expected. In this deployment, only Caddy exposes ports `80` and `443`.

