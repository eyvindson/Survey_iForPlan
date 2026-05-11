# Online Preference Data Collection Tool

This is a Streamlit application for collecting named expert preferences about climate-smart forestry management options and their expected ecosystem service effects.

## What It Does

- Presents one management option at a time in a guided survey.
- Captures importance, feasibility, implementation area scale, and positive/neutral/negative ecosystem service effects.
- Supports named expert invite tokens with resume/edit before final submission.
- Stores responses in SQLite.
- Exports tidy CSV and Excel-compatible matrix data.
- Includes respondent summaries and admin dashboards.

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

Open the app and use one of the seeded demo tokens:

- `demo-expert`
- `regional-planner`
- `forest-manager`

For a production deployment, set an admin passcode:

```bash
export SURVEY_ADMIN_PASSCODE="replace-this"
streamlit run app.py
```

The included Streamlit config opens the app on port `8866` by default.

## Configuration

Editable survey files are in `config/`:

- `management_options.csv` controls CSF categories and management options.
- `ecosystem_services.csv` controls service labels and service groups.
- `experts.csv` seeds initial named invite tokens.

The app creates `survey.db` on first run. Runtime export files can be written to `exports/` from the admin screen.

## Deployment Notes

Streamlit Community Cloud, an institutional VM, or a containerized deployment will work for v1. For a public study, set:

- `SURVEY_ADMIN_PASSCODE` for admin access.
- `SURVEY_DB_PATH` if the SQLite file should live on persistent mounted storage.
- `APP_BASE_URL` so generated invite links use the public URL.

If respondents see a company firewall block, share the app through an HTTPS URL on a normal domain instead of `http://IP-address:8866`. See [docs/SHARING_WITH_RESPONDENTS.md](docs/SHARING_WITH_RESPONDENTS.md).

For a production-style Docker + HTTPS setup, see [docs/SELF_HOST_HTTPS.md](docs/SELF_HOST_HTTPS.md).

## Testing

```bash
pytest
python3 -m py_compile app.py
```
