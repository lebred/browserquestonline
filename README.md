# BrowserQuest Online

Public repository for the standalone BrowserQuest game stack.

## Included Files
- `game.html`: main browser game frontend
- `game-wiki.html`: in-game wiki page
- `game_app.py`: isolated FastAPI backend for `/game`, `/wiki`, and `/api/game/*`
- `requirements.txt`: Python dependencies

## Local Run
1. `python3 -m venv .venv`
2. `source .venv/bin/activate`
3. `pip install -r requirements.txt`
4. `uvicorn game_app:app --host 0.0.0.0 --port 8000`

Open:
- `http://localhost:8000/game`
- `http://localhost:8000/wiki`

## Environment Variables
- `BQ_APP_ROOT` (default `/srv/browserquest`)
- `BQ_DB_PATH` (default `$BQ_APP_ROOT/security.db`)
- `BQ_AUTH_USERS_TABLE` (default `auth_users`)
- `BQ_AUTH_SESSIONS_TABLE` (default `auth_sessions`)
- `BQ_AUTH_COOKIE` (default `bq_session`)
- `BQ_AUTH_START_PATH` (default `/api/auth/google/start`)

## Notes
- Keep credentials and secrets in `.env` only.
- Do not commit production database files.
