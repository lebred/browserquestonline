from fastapi import FastAPI, HTTPException, Body, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse

import json
import os
import re
import secrets
import sqlite3
import hmac
from datetime import datetime, timezone, timedelta
from pathlib import Path
from hashlib import sha256
from urllib.parse import quote
from zoneinfo import ZoneInfo

APP_ROOT = Path(os.getenv('BQ_APP_ROOT', '/srv/browserquest'))
STATIC_DIR = APP_ROOT / 'static'
SECURITY_DB = Path(os.getenv('BQ_DB_PATH', str(APP_ROOT / 'security.db')))
AUTH_USERS_TABLE = os.getenv('BQ_AUTH_USERS_TABLE', 'auth_users').strip() or 'auth_users'
AUTH_SESSIONS_TABLE = os.getenv('BQ_AUTH_SESSIONS_TABLE', 'auth_sessions').strip() or 'auth_sessions'
AUTH_COOKIE_NAME = os.getenv('BQ_AUTH_COOKIE', 'bq_session').strip() or 'bq_session'
AUTH_START_PATH = os.getenv('BQ_AUTH_START_PATH', '/api/auth/google/start').strip() or '/api/auth/google/start'
GAME_TZ = os.getenv('BQ_GAME_TIMEZONE', 'America/Montreal').strip() or 'America/Montreal'

app = FastAPI(title='BrowserQuest Online API')
app.mount('/static', StaticFiles(directory=str(STATIC_DIR)), name='static')

_SEC_DB_SCHEMA_READY = False


def _sec_secret() -> str:
    return (os.getenv('LAB_SESSION_SECRET', '') or '').strip() or 'change-me-lab-session-secret'


def _new_sid() -> str:
    token = secrets.token_urlsafe(20)
    sig = hmac.new(_sec_secret().encode('utf-8'), token.encode('utf-8'), sha256).hexdigest()[:16]
    return f'{token}.{sig}'


def _sid_valid(sid: str) -> bool:
    if not sid or '.' not in sid:
        return False
    token, sig = sid.rsplit('.', 1)
    good = hmac.new(_sec_secret().encode('utf-8'), token.encode('utf-8'), sha256).hexdigest()[:16]
    return hmac.compare_digest(sig, good)


def _auth_cookie_name() -> str:
    return AUTH_COOKIE_NAME


def _auth_session_max_age_sec() -> int:
    return 60 * 60 * 24 * 14


def _game_today_key() -> str:
    try:
        return datetime.now(ZoneInfo(GAME_TZ)).strftime('%Y-%m-%d')
    except Exception:
        return datetime.now(timezone.utc).strftime('%Y-%m-%d')


def _game_track_daily_kills(cur: sqlite3.Cursor, user_key: str, display_name: str, previous_kills: int, new_kills: int) -> None:
    delta = max(0, int(new_kills) - int(previous_kills))
    if delta <= 0:
        return
    day_key = _game_today_key()
    now_iso = datetime.now(timezone.utc).isoformat()
    cur.execute(
        '''
        INSERT INTO game_daily_kills(day_key, user_key, display_name, kills, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(day_key, user_key) DO UPDATE SET
            display_name=excluded.display_name,
            kills=game_daily_kills.kills + excluded.kills,
            updated_at=excluded.updated_at
        ''',
        (day_key, user_key, display_name[:120], delta, now_iso),
    )


def _sec_db() -> sqlite3.Connection:
    global _SEC_DB_SCHEMA_READY
    SECURITY_DB.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(SECURITY_DB), timeout=20.0, check_same_thread=False)
    con.row_factory = sqlite3.Row
    try:
        con.execute('PRAGMA busy_timeout=20000')
        con.execute('PRAGMA journal_mode=WAL')
        con.execute('PRAGMA synchronous=NORMAL')
    except Exception:
        pass
    if _SEC_DB_SCHEMA_READY:
        return con
    cur = con.cursor()
    cur.execute(
        f'''
        CREATE TABLE IF NOT EXISTS {AUTH_USERS_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            full_name TEXT NOT NULL DEFAULT '',
            avatar_url TEXT NOT NULL DEFAULT '',
            locale_pref TEXT NOT NULL DEFAULT 'fr',
            plan TEXT NOT NULL DEFAULT 'free',
            source TEXT NOT NULL DEFAULT 'google',
            role TEXT NOT NULL DEFAULT 'user',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        '''
    )
    cur.execute(
        f'''
        CREATE TABLE IF NOT EXISTS {AUTH_SESSIONS_TABLE} (
            sid TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            ip TEXT NOT NULL DEFAULT '',
            FOREIGN KEY(user_id) REFERENCES {AUTH_USERS_TABLE}(id)
        )
        '''
    )
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS game_profiles (
            user_key TEXT PRIMARY KEY,
            display_name TEXT NOT NULL DEFAULT '',
            level INTEGER NOT NULL DEFAULT 1,
            gold INTEGER NOT NULL DEFAULT 0,
            max_floor INTEGER NOT NULL DEFAULT 0,
            kills INTEGER NOT NULL DEFAULT 0,
            quests_done INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        '''
    )
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS game_saves (
            user_key TEXT PRIMARY KEY,
            save_json TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL
        )
        '''
    )
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS game_presence (
            user_key TEXT PRIMARY KEY,
            display_name TEXT NOT NULL DEFAULT '',
            zone TEXT NOT NULL DEFAULT 'village',
            floor INTEGER NOT NULL DEFAULT 0,
            x REAL NOT NULL DEFAULT 0,
            y REAL NOT NULL DEFAULT 0,
            hp INTEGER NOT NULL DEFAULT 100,
            lvl INTEGER NOT NULL DEFAULT 1,
            attack_at INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        )
        '''
    )
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS game_active_sessions (
            user_key TEXT PRIMARY KEY,
            client_session TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        )
        '''
    )
    cur.execute('PRAGMA table_info(game_presence)')
    gp_cols = {str(r[1]) for r in cur.fetchall()}
    if 'attack_at' not in gp_cols:
        cur.execute('ALTER TABLE game_presence ADD COLUMN attack_at INTEGER NOT NULL DEFAULT 0')
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS game_chat_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_key TEXT NOT NULL,
            display_name TEXT NOT NULL DEFAULT '',
            message TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL
        )
        '''
    )
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS game_guest_links (
            code TEXT PRIMARY KEY,
            guest_key TEXT NOT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            used_at TEXT NOT NULL DEFAULT '',
            used_by_user_key TEXT NOT NULL DEFAULT ''
        )
        '''
    )
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS game_daily_kills (
            day_key TEXT NOT NULL,
            user_key TEXT NOT NULL,
            display_name TEXT NOT NULL DEFAULT '',
            kills INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(day_key, user_key)
        )
        '''
    )
    cur.execute('CREATE INDEX IF NOT EXISTS idx_game_profiles_rank ON game_profiles(level DESC, gold DESC, max_floor DESC, updated_at DESC)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_game_chat_created ON game_chat_messages(created_at DESC, id DESC)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_game_guest_links_guest ON game_guest_links(guest_key, expires_at DESC)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_game_active_sessions_updated ON game_active_sessions(updated_at DESC)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_game_daily_kills_day ON game_daily_kills(day_key, kills DESC, updated_at DESC)')
    con.commit()
    _SEC_DB_SCHEMA_READY = True
    return con


def _auth_session_user(sid: str) -> dict | None:
    if not _sid_valid(sid):
        return None
    con = _sec_db()
    cur = con.cursor()
    cur.execute(
        f'''
        SELECT s.sid, s.user_id, s.created_at, s.last_seen, s.ip,
               u.email, u.full_name, u.avatar_url, u.locale_pref, u.plan, u.role, u.source
        FROM {AUTH_SESSIONS_TABLE} s
        JOIN {AUTH_USERS_TABLE} u ON u.id = s.user_id
        WHERE s.sid=?
        ''',
        (sid,),
    )
    row = cur.fetchone()
    if not row:
        con.close()
        return None
    try:
        last = datetime.fromisoformat(str(row['last_seen']))
    except Exception:
        cur.execute(f'DELETE FROM {AUTH_SESSIONS_TABLE} WHERE sid=?', (sid,))
        con.commit()
        con.close()
        return None
    if (datetime.now(timezone.utc) - last).total_seconds() > _auth_session_max_age_sec():
        cur.execute(f'DELETE FROM {AUTH_SESSIONS_TABLE} WHERE sid=?', (sid,))
        con.commit()
        con.close()
        return None
    cur.execute(f'UPDATE {AUTH_SESSIONS_TABLE} SET last_seen=? WHERE sid=?', (datetime.now(timezone.utc).isoformat(), sid))
    con.commit()
    con.close()
    return dict(row)


def _game_identity(request: Request, guest_id: str | None = None) -> tuple[str, str, bool]:
    def _clean_text(raw: str, max_len: int = 120) -> str:
        s = re.sub(r'\s+', ' ', str(raw or '').strip())[:max_len]
        return s

    bad_words = {
        'fuck', 'fucking', 'shit', 'bitch', 'asshole', 'cunt',
        'tabarnak', 'osti', 'calisse', 'criss', 'encule', 'enculé', 'pute', 'salope', 'merde',
    }

    def _contains_bad_words(txt: str) -> bool:
        t = _clean_text(txt, 300).lower()
        t = re.sub(r'[^a-z0-9àâçéèêëîïôûùüÿñæœ]+', '', t)
        for w in bad_words:
            ww = re.sub(r'[^a-z0-9àâçéèêëîïôûùüÿñæœ]+', '', w.lower())
            if ww and ww in t:
                return True
        return False

    def _safe_display_name(name: str) -> str:
        n = _clean_text(name, 120) or 'Player'
        if _contains_bad_words(n):
            return 'Player'
        return n

    sid = request.cookies.get(_auth_cookie_name(), '')
    user = _auth_session_user(sid)
    if user and user.get('user_id'):
        name = _safe_display_name(str(user.get('full_name') or user.get('email') or f"user-{user['user_id']}"))
        return (f"user:{int(user['user_id'])}", name[:120], True)
    gid = re.sub(r'[^a-zA-Z0-9_-]', '', str(guest_id or '').strip())[:80]
    if not gid:
        gid = 'guest'
    gname = _safe_display_name(f'Guest-{gid[:8]}')
    return (f'guest:{gid}', gname, False)


def _game_client_session(payload: dict | None) -> str:
    raw = str((payload or {}).get('client_session') or '').strip()
    raw = re.sub(r'[^a-zA-Z0-9._:-]', '', raw)[:120]
    if raw:
        return raw
    return f'legacy-{secrets.token_hex(6)}'


def _game_touch_active_session(cur: sqlite3.Cursor, user_key: str, client_session: str, *, force_claim: bool = False, ttl_sec: int = 120) -> bool:
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    cutoff = (now - timedelta(seconds=max(30, int(ttl_sec)))).isoformat()
    cur.execute('DELETE FROM game_active_sessions WHERE updated_at < ?', (cutoff,))
    cur.execute('SELECT client_session FROM game_active_sessions WHERE user_key=? LIMIT 1', (user_key,))
    row = cur.fetchone()
    if row:
        existing = str(row['client_session'] or '').strip()
        if existing and existing != client_session and not bool(force_claim):
            return False
    cur.execute(
        '''
        INSERT INTO game_active_sessions(user_key, client_session, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(user_key) DO UPDATE SET
            client_session=excluded.client_session,
            updated_at=excluded.updated_at
        ''',
        (user_key, client_session, now_iso),
    )
    return True


@app.get('/healthz')
def healthz():
    return {'ok': True, 'service': 'browserquest-game'}


@app.get('/api/game/auth/google/start')
def game_auth_google_start(next_path: str = '/game'):
    p = str(next_path or '/game').strip()
    if not p.startswith('/'):
        p = '/game'
    if p.startswith('/api/'):
        p = '/game'
    sep = '&' if '?' in AUTH_START_PATH else '?'
    target = f"{AUTH_START_PATH}{sep}next_path={quote(p, safe='/')}"
    return RedirectResponse(url=target, status_code=307)


@app.get('/game')
def game_page():
    return FileResponse(str(STATIC_DIR / 'game.html'))


@app.get('/wiki')
def game_wiki_page():
    return FileResponse(str(STATIC_DIR / 'game-wiki.html'))


@app.get('/api/game/me')
def game_me(request: Request, guest_id: str = ''):
    user_key, display_name, logged = _game_identity(request, guest_id=guest_id)
    return {'ok': True, 'logged_in': bool(logged), 'user_key': user_key, 'display_name': display_name}


@app.get('/api/game/save')
def game_save_get(request: Request, guest_id: str = ''):
    user_key, display_name, logged = _game_identity(request, guest_id=guest_id)
    con = _sec_db()
    cur = con.cursor()
    cur.execute('SELECT save_json, updated_at FROM game_saves WHERE user_key=? LIMIT 1', (user_key,))
    row = cur.fetchone()
    con.close()
    if not row:
        return {'ok': True, 'logged_in': bool(logged), 'user_key': user_key, 'display_name': display_name, 'save': None}
    try:
        save_obj = json.loads(str(row['save_json'] or '{}'))
    except Exception:
        save_obj = None
    return {'ok': True, 'logged_in': bool(logged), 'user_key': user_key, 'display_name': display_name, 'save': save_obj, 'updated_at': str(row['updated_at'] or '')}


@app.post('/api/game/save')
def game_save_set(request: Request, payload: dict = Body(...)):
    guest_id = str(payload.get('guest_id') or '')
    user_key, display_name, _logged = _game_identity(request, guest_id=guest_id)
    client_session = _game_client_session(payload)
    force_claim = bool(payload.get('force_claim'))
    save = payload.get('save') if isinstance(payload.get('save'), dict) else {}
    now_iso = datetime.now(timezone.utc).isoformat()
    save_json = json.dumps(save, ensure_ascii=False)[:300000]
    profile = payload.get('profile') if isinstance(payload.get('profile'), dict) else {}
    lvl = max(1, min(int(profile.get('level') or 1), 999))
    gold = max(0, min(int(profile.get('gold') or 0), 10_000_000))
    max_floor = max(0, min(int(profile.get('max_floor') or 0), 999))
    kills = max(0, min(int(profile.get('kills') or 0), 10_000_000))
    quests_done = max(0, min(int(profile.get('quests_done') or 0), 1000))

    def _save_score(obj: dict | None) -> tuple[int, int, int]:
        if not isinstance(obj, dict):
            return (0, 0, 0)
        p = obj.get('player') if isinstance(obj.get('player'), dict) else {}
        m = obj.get('metrics') if isinstance(obj.get('metrics'), dict) else {}
        return (int(p.get('lvl') or 1), int(m.get('max_floor') or 0), int(p.get('gold') or 0))

    con = _sec_db()
    cur = con.cursor()
    if not _game_touch_active_session(cur, user_key, client_session, force_claim=force_claim, ttl_sec=120):
        con.commit(); con.close()
        return {'ok': True, 'saved_at': now_iso, 'user_key': user_key, 'conflict': True, 'skipped': True, 'reason': 'active_elsewhere'}

    cur.execute('SELECT save_json FROM game_saves WHERE user_key=? LIMIT 1', (user_key,))
    existing = cur.fetchone()
    existing_obj = {}
    if existing:
        try:
            existing_obj = json.loads(str(existing['save_json'] or '{}'))
        except Exception:
            existing_obj = {}
    if existing and _save_score(save) < _save_score(existing_obj):
        con.close()
        return {'ok': True, 'saved_at': now_iso, 'user_key': user_key, 'skipped': True, 'reason': 'lower_progress_than_existing'}

    cur.execute(
        '''
        INSERT INTO game_saves(user_key, save_json, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(user_key) DO UPDATE SET
            save_json=excluded.save_json,
            updated_at=excluded.updated_at
        ''',
        (user_key, save_json, now_iso),
    )
    cur.execute('SELECT kills FROM game_profiles WHERE user_key=? LIMIT 1', (user_key,))
    prev_row = cur.fetchone()
    previous_kills = int((prev_row['kills'] if prev_row and prev_row['kills'] is not None else 0) or 0)
    cur.execute(
        '''
        INSERT INTO game_profiles(user_key, display_name, level, gold, max_floor, kills, quests_done, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_key) DO UPDATE SET
            display_name=excluded.display_name,
            level=excluded.level,
            gold=excluded.gold,
            max_floor=excluded.max_floor,
            kills=excluded.kills,
            quests_done=excluded.quests_done,
            updated_at=excluded.updated_at
        ''',
        (user_key, display_name, lvl, gold, max_floor, kills, quests_done, now_iso, now_iso),
    )
    _game_track_daily_kills(cur, user_key, display_name, previous_kills, kills)
    con.commit(); con.close()
    return {'ok': True, 'saved_at': now_iso, 'user_key': user_key}


@app.post('/api/game/claim-guest')
def game_claim_guest_save(request: Request, payload: dict = Body(...)):
    sid = request.cookies.get(_auth_cookie_name(), '')
    user = _auth_session_user(sid)
    if not user or not user.get('user_id'):
        raise HTTPException(status_code=401, detail='Login required')
    guest_id = re.sub(r'[^a-zA-Z0-9_-]', '', str(payload.get('guest_id') or '').strip())[:80]
    if not guest_id:
        raise HTTPException(status_code=400, detail='guest_id required')

    guest_key = f'guest:{guest_id}'
    user_key = f"user:{int(user['user_id'])}"
    display_name = str(user.get('full_name') or user.get('email') or f"user-{int(user['user_id'])}")[:120]

    con = _sec_db(); cur = con.cursor()

    def _score(obj: dict | None) -> tuple[int, int, int]:
        if not isinstance(obj, dict):
            return (0, 0, 0)
        p = obj.get('player') if isinstance(obj.get('player'), dict) else {}
        m = obj.get('metrics') if isinstance(obj.get('metrics'), dict) else {}
        return (int(p.get('lvl') or 1), int(m.get('max_floor') or 0), int(p.get('gold') or 0))

    cur.execute('SELECT save_json FROM game_saves WHERE user_key=? LIMIT 1', (user_key,)); user_row = cur.fetchone()
    cur.execute('SELECT save_json FROM game_saves WHERE user_key=? LIMIT 1', (guest_key,)); guest_row = cur.fetchone()
    if not guest_row:
        con.close(); return {'ok': True, 'claimed': False, 'reason': 'guest_save_not_found'}

    try: guest_save = json.loads(str(guest_row['save_json'] or '{}'))
    except Exception: guest_save = {}
    try: user_save = json.loads(str(user_row['save_json'] or '{}')) if user_row else {}
    except Exception: user_save = {}

    if _score(guest_save) <= _score(user_save):
        cur.execute('DELETE FROM game_saves WHERE user_key=?', (guest_key,))
        cur.execute('DELETE FROM game_profiles WHERE user_key=?', (guest_key,))
        con.commit(); con.close()
        return {'ok': True, 'claimed': False, 'reason': 'user_save_newer_or_equal'}

    now_iso = datetime.now(timezone.utc).isoformat()
    save_json = json.dumps(guest_save, ensure_ascii=False)[:300000]
    pp = guest_save.get('player') if isinstance(guest_save.get('player'), dict) else {}
    mm = guest_save.get('metrics') if isinstance(guest_save.get('metrics'), dict) else {}
    qd = int(guest_save.get('questsDone') or 0)
    lvl = max(1, min(int(pp.get('lvl') or 1), 999))
    gold = max(0, min(int(pp.get('gold') or 0), 10_000_000))
    max_floor = max(0, min(int(mm.get('max_floor') or 0), 999))
    kills = max(0, min(int(mm.get('kills') or 0), 10_000_000))
    quests_done = max(0, min(qd, 1000))
    cur.execute(
        '''
        INSERT INTO game_saves(user_key, save_json, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(user_key) DO UPDATE SET
            save_json=excluded.save_json,
            updated_at=excluded.updated_at
        ''',
        (user_key, save_json, now_iso),
    )
    cur.execute('SELECT kills FROM game_profiles WHERE user_key=? LIMIT 1', (user_key,))
    prev_row = cur.fetchone()
    previous_kills = int((prev_row['kills'] if prev_row and prev_row['kills'] is not None else 0) or 0)
    cur.execute(
        '''
        INSERT INTO game_profiles(user_key, display_name, level, gold, max_floor, kills, quests_done, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_key) DO UPDATE SET
            display_name=excluded.display_name,
            level=excluded.level,
            gold=excluded.gold,
            max_floor=excluded.max_floor,
            kills=excluded.kills,
            quests_done=excluded.quests_done,
            updated_at=excluded.updated_at
        ''',
        (user_key, display_name, lvl, gold, max_floor, kills, quests_done, now_iso, now_iso),
    )
    _game_track_daily_kills(cur, user_key, display_name, previous_kills, kills)
    cur.execute('DELETE FROM game_saves WHERE user_key=?', (guest_key,))
    cur.execute('DELETE FROM game_profiles WHERE user_key=?', (guest_key,))
    con.commit(); con.close()
    return {'ok': True, 'claimed': True}


@app.post('/api/game/link/create')
def game_create_guest_link(payload: dict = Body(...)):
    guest_id = re.sub(r'[^a-zA-Z0-9_-]', '', str(payload.get('guest_id') or '').strip())[:80]
    if not guest_id:
        raise HTTPException(status_code=400, detail='guest_id required')
    guest_key = f'guest:{guest_id}'
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    expires_at = (now + timedelta(hours=48)).isoformat()
    code = secrets.token_urlsafe(18)
    con = _sec_db(); cur = con.cursor()
    cur.execute('SELECT 1 FROM game_saves WHERE user_key=? LIMIT 1', (guest_key,))
    if not cur.fetchone():
        con.close(); return {'ok': True, 'created': False, 'reason': 'guest_save_not_found'}
    cur.execute('INSERT INTO game_guest_links(code, guest_key, created_at, expires_at, used_at, used_by_user_key) VALUES (?, ?, ?, ?, "", "")', (code, guest_key, now_iso, expires_at))
    con.commit(); con.close()
    return {'ok': True, 'created': True, 'code': code, 'expires_at': expires_at}


@app.post('/api/game/link/claim')
def game_claim_guest_link(request: Request, payload: dict = Body(...)):
    sid = request.cookies.get(_auth_cookie_name(), '')
    user = _auth_session_user(sid)
    if not user or not user.get('user_id'):
        raise HTTPException(status_code=401, detail='Login required')
    code = re.sub(r'[^a-zA-Z0-9_-]', '', str(payload.get('code') or '').strip())[:120]
    if not code:
        raise HTTPException(status_code=400, detail='code required')
    user_key = f"user:{int(user['user_id'])}"
    display_name = str(user.get('full_name') or user.get('email') or f"user-{int(user['user_id'])}")[:120]
    now_iso = datetime.now(timezone.utc).isoformat()

    con = _sec_db(); cur = con.cursor()
    cur.execute('SELECT code, guest_key, expires_at, used_at FROM game_guest_links WHERE code=? LIMIT 1', (code,))
    row = cur.fetchone()
    if not row:
        con.close(); return {'ok': True, 'claimed': False, 'reason': 'invalid_code'}
    if str(row['used_at'] or '').strip():
        con.close(); return {'ok': True, 'claimed': False, 'reason': 'already_used'}
    if str(row['expires_at'] or '') < now_iso:
        cur.execute('DELETE FROM game_guest_links WHERE code=?', (code,)); con.commit(); con.close()
        return {'ok': True, 'claimed': False, 'reason': 'expired'}

    guest_key = str(row['guest_key'] or '').strip()
    if not guest_key.startswith('guest:'):
        con.close(); return {'ok': True, 'claimed': False, 'reason': 'invalid_link'}

    cur.execute('SELECT save_json FROM game_saves WHERE user_key=? LIMIT 1', (user_key,)); user_row = cur.fetchone()
    cur.execute('SELECT save_json FROM game_saves WHERE user_key=? LIMIT 1', (guest_key,)); guest_row = cur.fetchone()
    if not guest_row:
        cur.execute('UPDATE game_guest_links SET used_at=?, used_by_user_key=? WHERE code=?', (now_iso, user_key, code))
        con.commit(); con.close(); return {'ok': True, 'claimed': False, 'reason': 'guest_save_not_found'}

    def _score(obj: dict | None) -> tuple[int, int, int]:
        if not isinstance(obj, dict):
            return (0, 0, 0)
        p = obj.get('player') if isinstance(obj.get('player'), dict) else {}
        m = obj.get('metrics') if isinstance(obj.get('metrics'), dict) else {}
        return (int(p.get('lvl') or 1), int(m.get('max_floor') or 0), int(p.get('gold') or 0))

    try: guest_save = json.loads(str(guest_row['save_json'] or '{}'))
    except Exception: guest_save = {}
    try: user_save = json.loads(str(user_row['save_json'] or '{}')) if user_row else {}
    except Exception: user_save = {}

    claimed = False
    if _score(guest_save) > _score(user_save):
        save_json = json.dumps(guest_save, ensure_ascii=False)[:300000]
        pp = guest_save.get('player') if isinstance(guest_save.get('player'), dict) else {}
        mm = guest_save.get('metrics') if isinstance(guest_save.get('metrics'), dict) else {}
        qd = int(guest_save.get('questsDone') or 0)
        lvl = max(1, min(int(pp.get('lvl') or 1), 999))
        gold = max(0, min(int(pp.get('gold') or 0), 10_000_000))
        max_floor = max(0, min(int(mm.get('max_floor') or 0), 999))
        kills = max(0, min(int(mm.get('kills') or 0), 10_000_000))
        quests_done = max(0, min(qd, 1000))
        cur.execute(
            '''
            INSERT INTO game_saves(user_key, save_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(user_key) DO UPDATE SET
                save_json=excluded.save_json,
                updated_at=excluded.updated_at
            ''',
            (user_key, save_json, now_iso),
        )
        cur.execute('SELECT kills FROM game_profiles WHERE user_key=? LIMIT 1', (user_key,))
        prev_row = cur.fetchone()
        previous_kills = int((prev_row['kills'] if prev_row and prev_row['kills'] is not None else 0) or 0)
        cur.execute(
            '''
            INSERT INTO game_profiles(user_key, display_name, level, gold, max_floor, kills, quests_done, updated_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_key) DO UPDATE SET
                display_name=excluded.display_name,
                level=excluded.level,
                gold=excluded.gold,
                max_floor=excluded.max_floor,
                kills=excluded.kills,
                quests_done=excluded.quests_done,
                updated_at=excluded.updated_at
            ''',
            (user_key, display_name, lvl, gold, max_floor, kills, quests_done, now_iso, now_iso),
        )
        _game_track_daily_kills(cur, user_key, display_name, previous_kills, kills)
        claimed = True

    cur.execute('DELETE FROM game_saves WHERE user_key=?', (guest_key,))
    cur.execute('DELETE FROM game_profiles WHERE user_key=?', (guest_key,))
    cur.execute('UPDATE game_guest_links SET used_at=?, used_by_user_key=? WHERE code=?', (now_iso, user_key, code))
    con.commit(); con.close()
    return {'ok': True, 'claimed': bool(claimed), 'reason': 'ok'}


@app.post('/api/game/presence/update')
def game_presence_update(request: Request, payload: dict = Body(...)):
    guest_id = str(payload.get('guest_id') or '')
    user_key, display_name, _logged = _game_identity(request, guest_id=guest_id)
    client_session = _game_client_session(payload)
    force_claim = bool(payload.get('force_claim'))
    zone = str(payload.get('zone') or 'village')[:40]
    floor = max(0, min(int(payload.get('floor') or 0), 999))
    x = float(payload.get('x') or 0.0)
    y = float(payload.get('y') or 0.0)
    hp = max(0, min(int(payload.get('hp') or 0), 99999))
    lvl = max(1, min(int(payload.get('lvl') or 1), 999))
    attack_at = max(0, int(payload.get('attack_at') or 0))
    now_iso = datetime.now(timezone.utc).isoformat()
    con = _sec_db(); cur = con.cursor()
    if not _game_touch_active_session(cur, user_key, client_session, force_claim=force_claim, ttl_sec=120):
        con.commit(); con.close(); return {'ok': False, 'conflict': True, 'reason': 'active_elsewhere'}
    cur.execute(
        '''
        INSERT INTO game_presence(user_key, display_name, zone, floor, x, y, hp, lvl, attack_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_key) DO UPDATE SET
            display_name=excluded.display_name,
            zone=excluded.zone,
            floor=excluded.floor,
            x=excluded.x,
            y=excluded.y,
            hp=excluded.hp,
            lvl=excluded.lvl,
            attack_at=excluded.attack_at,
            updated_at=excluded.updated_at
        ''',
        (user_key, display_name, zone, floor, x, y, hp, lvl, attack_at, now_iso),
    )
    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=120)).isoformat()
    cur.execute('DELETE FROM game_presence WHERE updated_at < ?', (cutoff,))
    con.commit(); con.close()
    return {'ok': True}


@app.get('/api/game/presence/list')
def game_presence_list(request: Request, guest_id: str = '', zone: str = 'village', floor: int = 0, limit: int = 60):
    user_key, _display_name, _logged = _game_identity(request, guest_id=guest_id)
    lim = max(1, min(int(limit or 60), 200))
    z = str(zone or 'village')[:40]
    fl = max(0, min(int(floor or 0), 999))
    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=120)).isoformat()
    con = _sec_db(); cur = con.cursor()
    cur.execute('DELETE FROM game_presence WHERE updated_at < ?', (cutoff,))
    cur.execute('SELECT COUNT(1) AS c FROM game_presence')
    total_online = int((cur.fetchone() or {'c': 0})['c'] or 0)
    cur.execute(
        '''
        SELECT user_key, display_name, zone, floor, x, y, hp, lvl, attack_at, updated_at
        FROM game_presence
        WHERE zone=? AND floor=? AND user_key<>?
        ORDER BY updated_at DESC
        LIMIT ?
        ''',
        (z, fl, user_key, lim),
    )
    rows = [dict(r) for r in cur.fetchall()]
    con.commit(); con.close()
    return {'ok': True, 'items': rows, 'total_online': total_online}


@app.post('/api/game/chat/send')
def game_chat_send(request: Request, payload: dict = Body(...)):
    guest_id = str(payload.get('guest_id') or '')
    user_key, display_name, _logged = _game_identity(request, guest_id=guest_id)
    msg = str(payload.get('message') or '').strip()
    if not msg:
        raise HTTPException(status_code=400, detail='message required')
    msg = re.sub(r'\s+', ' ', msg)[:280]
    bad_words = ['fuck', 'fucking', 'shit', 'bitch', 'asshole', 'cunt', 'tabarnak', 'osti', 'calisse', 'criss', 'encule', 'enculé', 'pute', 'salope', 'merde']
    for w in bad_words:
        msg = re.sub(re.escape(w), '*' * len(w), msg, flags=re.IGNORECASE)
    now_iso = datetime.now(timezone.utc).isoformat()
    con = _sec_db(); cur = con.cursor()
    cur.execute('INSERT INTO game_chat_messages(user_key, display_name, message, created_at) VALUES (?, ?, ?, ?)', (user_key, display_name[:120], msg, now_iso))
    con.commit(); con.close()
    return {'ok': True}


@app.get('/api/game/chat/list')
def game_chat_list(limit: int = 80):
    lim = max(1, min(int(limit or 80), 300))
    con = _sec_db(); cur = con.cursor()
    cur.execute('SELECT id, display_name, message, created_at FROM game_chat_messages ORDER BY id DESC LIMIT ?', (lim,))
    rows = [dict(r) for r in cur.fetchall()]
    con.close(); rows.reverse()
    return {'ok': True, 'items': rows}


@app.get('/api/game/leaderboard')
def game_leaderboard(limit: int = 20):
    lim = max(1, min(int(limit or 20), 200))
    con = _sec_db(); cur = con.cursor()
    cur.execute(
        '''
        SELECT user_key, display_name, level, gold, max_floor, kills, quests_done, updated_at
        FROM game_profiles
        ORDER BY level DESC, max_floor DESC, gold DESC, kills DESC, updated_at DESC
        LIMIT ?
        ''',
        (lim,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return {'ok': True, 'items': rows}


@app.get('/api/game/daily-leaderboard')
def game_daily_leaderboard(limit: int = 20):
    lim = max(1, min(int(limit or 20), 200))
    day_key = _game_today_key()
    con = _sec_db()
    cur = con.cursor()
    cur.execute(
        '''
        SELECT user_key, display_name, kills, updated_at
        FROM game_daily_kills
        WHERE day_key=?
        ORDER BY kills DESC, updated_at DESC
        LIMIT ?
        ''',
        (day_key, lim),
    )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return {'ok': True, 'day': day_key, 'items': rows}


@app.get('/api/game/wiki')
def game_wiki_api():
    quests = [
        {'id': 'q_rat_hunt', 'title': 'Rat Hunter', 'goal': 'Éliminer 5 rats dans le village', 'target': 5, 'metric': 'rat_kills', 'reward': {'gold': 25, 'xp': 20}},
        {'id': 'q_floor_two', 'title': 'First Descent', 'goal': "Atteindre l'étage 2 du donjon", 'target': 2, 'metric': 'max_floor', 'reward': {'gold': 40, 'xp': 35}},
        {'id': 'q_potions', 'title': 'Potion Collector', 'goal': 'Trouver 3 potions', 'target': 3, 'metric': 'potions_found', 'reward': {'gold': 20, 'xp': 20}},
    ]
    return {'ok': True, 'quests': quests}
