from fastapi import FastAPI, HTTPException, Body, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse

import json
import os
import re
import secrets
import sqlite3
import hmac
import random
import time
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
MAX_GUEST_IDS_PER_IP_PER_DAY = max(5, int(os.getenv('BQ_MAX_GUEST_IDS_PER_IP_PER_DAY', '40') or '40'))
GAME_RENAME_COST = max(1, int(os.getenv('BQ_GAME_RENAME_COST', '10000') or '10000'))
GAME_ADMIN_EMAIL = (os.getenv('BQ_ADMIN_EMAIL', 'matduke@gmail.com') or 'matduke@gmail.com').strip().lower()
GAME_ADMIN_SECRET_PATH = (os.getenv('BQ_ADMIN_SECRET_PATH', '/ops-bq-7f4k2') or '/ops-bq-7f4k2').strip() or '/ops-bq-7f4k2'
GAME_BAD_WORDS = {
    'fuck', 'fucking', 'shit', 'bitch', 'asshole', 'cunt',
    'tabarnak', 'osti', 'calisse', 'criss', 'encule', 'enculé', 'pute', 'salope', 'merde',
}
BLOG_ARTICLE_FILES = {
    'meilleurs-jeux-browser-2026': 'game-blog-a1.html',
    'browserquest-online-guide-complet': 'game-blog-a2.html',
    'browserquest-online-mobile-vs-pc': 'game-blog-a3.html',
    'progression-floor-1-a-20-browserquest': 'game-blog-a4.html',
    'floor-21-plus-endgame-browserquest': 'game-blog-a5.html',
    'objets-raretes-et-loot-browserquest': 'game-blog-a6.html',
    'boss-titans-et-archons-strategie-browserquest': 'game-blog-a7.html',
    'pourquoi-browserquest-online-est-addictif': 'game-blog-a8.html',
}
GAME_VERSION = os.getenv('BQ_GAME_VERSION', '0.21.5').strip() or '0.21.5'

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


def _contains_bad_words(txt: str) -> bool:
    t = re.sub(r'[^a-z0-9àâçéèêëîïôûùüÿñæœ]+', '', str(txt or '').strip().lower())
    if not t:
        return False
    for w in GAME_BAD_WORDS:
        ww = re.sub(r'[^a-z0-9àâçéèêëîïôûùüÿñæœ]+', '', str(w).lower())
        if ww and ww in t:
            return True
    return False


def _safe_display_name(raw: str, default: str = 'Player') -> str:
    n = re.sub(r'\s+', ' ', str(raw or '').strip())[:120]
    if not n:
        return default
    if _contains_bad_words(n):
        return default
    return n


def _normalize_legacy_guest_id(request: Request, gid: str) -> str:
    g = str(gid or '').strip()
    if g and g.lower() not in {'guest', 'default', 'null', 'undefined'}:
        return g
    ip = ''
    try:
        ip = str(request.client.host or '')
    except Exception:
        ip = ''
    ua = str(request.headers.get('user-agent') or '')
    seed = f'{ip}|{ua}|legacy-guest-fix-v1'
    return f'legacy_{sha256(seed.encode("utf-8")).hexdigest()[:16]}'


def _validate_player_name(raw: str) -> tuple[bool, str]:
    n = re.sub(r'\s+', ' ', str(raw or '').strip())[:32]
    if not re.match(r'^[A-Za-zÀ-ÖØ-öø-ÿ0-9 _-]{3,18}$', n or ''):
        return (False, '')
    if _contains_bad_words(n):
        return (False, '')
    # anti-spam: reject too many repeated chars
    if re.search(r'(.)\1{4,}', n):
        return (False, '')
    return (True, n)


def _request_ip(request: Request) -> str:
    xff = str(request.headers.get('x-forwarded-for') or '').strip()
    if xff:
        return xff.split(',')[0].strip()
    xr = str(request.headers.get('x-real-ip') or '').strip()
    if xr:
        return xr
    return str(request.client.host if request.client else '').strip()


def _enforce_guest_identity_quota(request: Request, cur: sqlite3.Cursor, user_key: str) -> None:
    if not str(user_key or '').startswith('guest:'):
        return
    ip = _request_ip(request)
    if not ip:
        return
    day_key = _game_today_key()
    now_iso = datetime.now(timezone.utc).isoformat()
    cur.execute(
        '''
        INSERT INTO game_guest_ip_daily(day_key, ip, guest_key, first_seen_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(day_key, ip, guest_key) DO UPDATE SET
            updated_at=excluded.updated_at
        ''',
        (day_key, ip, str(user_key), now_iso, now_iso),
    )
    cur.execute('SELECT COUNT(1) AS c FROM game_guest_ip_daily WHERE day_key=? AND ip=?', (day_key, ip))
    count = int((cur.fetchone() or {'c': 0})['c'] or 0)
    if count > MAX_GUEST_IDS_PER_IP_PER_DAY:
        raise HTTPException(status_code=429, detail='Too many guest identities from this IP today')


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
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS game_floor_instances (
            instance_key TEXT PRIMARY KEY,
            zone TEXT NOT NULL DEFAULT 'village',
            floor INTEGER NOT NULL DEFAULT 0,
            state_json TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL
        )
        '''
    )
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS game_guest_ip_daily (
            day_key TEXT NOT NULL,
            ip TEXT NOT NULL,
            guest_key TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(day_key, ip, guest_key)
        )
        '''
    )
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS game_retired_guests (
            guest_key TEXT PRIMARY KEY,
            retired_to_user_key TEXT NOT NULL DEFAULT '',
            retired_at TEXT NOT NULL
        )
        '''
    )
    cur.execute('CREATE INDEX IF NOT EXISTS idx_game_profiles_rank ON game_profiles(level DESC, gold DESC, max_floor DESC, updated_at DESC)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_game_chat_created ON game_chat_messages(created_at DESC, id DESC)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_game_guest_links_guest ON game_guest_links(guest_key, expires_at DESC)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_game_active_sessions_updated ON game_active_sessions(updated_at DESC)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_game_daily_kills_day ON game_daily_kills(day_key, kills DESC, updated_at DESC)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_game_guest_ip_daily_day_ip ON game_guest_ip_daily(day_key, ip, updated_at DESC)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_game_floor_instances_zone_floor ON game_floor_instances(zone, floor, updated_at DESC)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_game_retired_guests_time ON game_retired_guests(retired_at DESC)')
    # Purge legacy shared guest artifacts created before strict guest_id handling.
    cur.execute("DELETE FROM game_profiles WHERE user_key='guest:guest' OR lower(display_name)='guest-guest'")
    cur.execute("DELETE FROM game_saves WHERE user_key='guest:guest'")
    cur.execute("DELETE FROM game_presence WHERE user_key='guest:guest'")
    cur.execute("DELETE FROM game_active_sessions WHERE user_key='guest:guest'")
    cur.execute("DELETE FROM game_daily_kills WHERE user_key='guest:guest'")
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


def _game_identity(request: Request, guest_id: str | None = None, *, strict_guest: bool = False) -> tuple[str, str, bool]:
    sid = request.cookies.get(_auth_cookie_name(), '')
    user = _auth_session_user(sid)
    if user and user.get('user_id'):
        user_key = f"user:{int(user['user_id'])}"
        fallback = _safe_display_name(str(user.get('full_name') or user.get('email') or f"user-{user['user_id']}"))
        name = fallback
        try:
            con = _sec_db()
            cur = con.cursor()
            cur.execute('SELECT display_name FROM game_profiles WHERE user_key=? LIMIT 1', (user_key,))
            row = cur.fetchone()
            con.close()
            if row and str(row['display_name'] or '').strip():
                name = _safe_display_name(str(row['display_name'] or ''), default=fallback)
        except Exception:
            name = fallback
        return (user_key, name[:120], True)
    gid = re.sub(r'[^a-zA-Z0-9_-]', '', str(guest_id or '').strip())[:80]
    if not gid:
        if strict_guest:
            raise HTTPException(status_code=400, detail='guest_id required')
        gid = 'guest'
    gid = _normalize_legacy_guest_id(request, gid)[:80]
    gname = _safe_display_name(f'Guest-{gid[:8]}')
    return (f'guest:{gid}', gname, False)


def _require_game_admin(request: Request) -> dict:
    sid = request.cookies.get(_auth_cookie_name(), '')
    user = _auth_session_user(sid)
    if not (user and user.get('user_id')):
        raise HTTPException(status_code=401, detail='login required')
    email = str(user.get('email') or '').strip().lower()
    if not email or email != GAME_ADMIN_EMAIL:
        raise HTTPException(status_code=403, detail='forbidden')
    return user


def _game_client_session(payload: dict | None) -> str:
    raw = str((payload or {}).get('client_session') or '').strip()
    raw = re.sub(r'[^a-zA-Z0-9._:-]', '', raw)[:120]
    if raw:
        return raw
    return f'legacy-{secrets.token_hex(6)}'


def _is_retired_guest(cur: sqlite3.Cursor, user_key: str) -> bool:
    if not str(user_key or '').startswith('guest:'):
        return False
    cur.execute('SELECT 1 FROM game_retired_guests WHERE guest_key=? LIMIT 1', (str(user_key),))
    return bool(cur.fetchone())


def _retire_guest_key(cur: sqlite3.Cursor, guest_key: str, user_key: str, now_iso: str) -> None:
    if not str(guest_key or '').startswith('guest:'):
        return
    cur.execute(
        '''
        INSERT INTO game_retired_guests(guest_key, retired_to_user_key, retired_at)
        VALUES (?, ?, ?)
        ON CONFLICT(guest_key) DO UPDATE SET
            retired_to_user_key=excluded.retired_to_user_key,
            retired_at=excluded.retired_at
        ''',
        (str(guest_key), str(user_key), str(now_iso)),
    )


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


def _instance_key(zone: str, floor: int) -> str:
    z = str(zone or 'village').strip().lower()[:40]
    f = max(0, min(int(floor or 0), 999))
    return f'{z}:{f}'


def _floor_tier(floor: int) -> int:
    f = max(1, int(floor or 1))
    return max(0, (f - 1) // 5)


def _instance_target_count(zone: str, floor: int) -> int:
    if str(zone) != 'dungeon':
        return 0
    t = _floor_tier(floor)
    return min(14 + int(floor) * 2 + (t * 3), 44)


def _instance_map_size(floor: int) -> tuple[int, int]:
    f = max(1, int(floor or 1))
    base = 62
    if f >= 11:
        base = int(round(62 + min(44, (f - 1) * 2.2)))
    return (base, base)


def _instance_enemy(rng: random.Random, floor: int, idx: int, w: int, h: int) -> dict:
    tier = _floor_tier(floor)
    spike = 1.0 + tier * 0.55
    base = int((18 + floor * 5) * spike)
    kind = 'slime'
    hp = base
    atk = int((3 + floor) * (1 + tier * 0.4))
    xp = int((8 + floor * 2) * (1 + tier * 0.3))
    gold = int((2 + rng.randrange(0, 5 + floor)) * (1 + tier * 0.22))

    mini_boss_chance = 0.08 + tier * 0.04 + (0.12 if (floor % 5) == 0 else 0.0)
    if rng.random() < mini_boss_chance:
        if floor >= 12 and rng.random() < 0.05:
            kind = 'titan'
            hp = int(base * 12.6)
            atk += 20 + int(floor * 1.15)
            xp = int(xp * 7.8)
            gold += 95 + rng.randrange(0, 120)
        elif tier >= 2 and rng.random() < 0.5:
            kind = 'dragon'
            hp = int(base * 4.6)
            atk += 7 + int(floor / 2)
            xp = int(xp * 3.1)
            gold += 24 + rng.randrange(0, 30)
        else:
            kind = 'ogre'
            hp = int(base * 3.5)
            atk += 5 + int(floor / 3)
            xp = int(xp * 2.5)
            gold += 14 + rng.randrange(0, 18)
    elif rng.random() < 0.12:
        kind = 'brute'
        hp = int(base * 2.5)
        atk += 2 + int(floor / 2)
        xp = int(xp * 2.0)
        gold += 8 + rng.randrange(0, 12)

    element = ''
    if kind == 'dragon':
        variants = ['fire', 'frost', 'shadow']
        element = variants[max(0, int(floor)) % len(variants)]

    ex = 6 + rng.randrange(0, max(8, w - 12))
    ey = 6 + rng.randrange(0, max(8, h - 12))
    eid = f'dng:{int(floor)}:m{int(idx)}'
    return {
        'id': eid,
        'x': float(ex) + 0.5,
        'y': float(ey) + 0.5,
        'hp': int(max(1, hp)),
        'hpMax': int(max(1, hp)),
        'atk': int(max(1, atk)),
        'xp': int(max(1, xp)),
        'gold': int(max(0, gold)),
        'kind': str(kind),
        'element': str(element),
        'facing': 'down',
    }


def _instance_default_state(zone: str, floor: int) -> dict:
    z = str(zone or 'village').strip().lower()
    fl = max(0, min(int(floor or 0), 999))
    if z != 'dungeon' or fl <= 0:
        return {'zone': z, 'floor': fl, 'enemies': [], 'next_respawn_ts': float(time.time() + 6.0)}

    w, h = _instance_map_size(fl)
    seed_src = f'{z}:{fl}:seed-v1'.encode('utf-8')
    seed = int.from_bytes(sha256(seed_src).digest()[:8], 'big', signed=False)
    rng = random.Random(seed)
    count = _instance_target_count(z, fl)
    enemies = [_instance_enemy(rng, fl, i, w, h) for i in range(count)]
    return {'zone': z, 'floor': fl, 'enemies': enemies, 'next_respawn_ts': float(time.time() + 4.0)}


def _instance_load(cur: sqlite3.Cursor, zone: str, floor: int) -> dict:
    z = str(zone or 'village').strip().lower()[:40]
    fl = max(0, min(int(floor or 0), 999))
    key = _instance_key(z, fl)
    cur.execute('SELECT state_json FROM game_floor_instances WHERE instance_key=? LIMIT 1', (key,))
    row = cur.fetchone()
    if not row:
        state = _instance_default_state(z, fl)
        now_iso = datetime.now(timezone.utc).isoformat()
        cur.execute(
            '''
            INSERT INTO game_floor_instances(instance_key, zone, floor, state_json, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(instance_key) DO UPDATE SET
                zone=excluded.zone,
                floor=excluded.floor,
                state_json=excluded.state_json,
                updated_at=excluded.updated_at
            ''',
            (key, z, fl, json.dumps(state, ensure_ascii=False)[:1200000], now_iso),
        )
        return state
    try:
        state = json.loads(str(row['state_json'] or '{}'))
        if not isinstance(state, dict):
            raise ValueError('bad state')
    except Exception:
        state = _instance_default_state(z, fl)
    state['zone'] = z
    state['floor'] = fl
    if not isinstance(state.get('enemies'), list):
        state['enemies'] = []
    return state


def _instance_save(cur: sqlite3.Cursor, zone: str, floor: int, state: dict) -> None:
    z = str(zone or 'village').strip().lower()[:40]
    fl = max(0, min(int(floor or 0), 999))
    key = _instance_key(z, fl)
    now_iso = datetime.now(timezone.utc).isoformat()
    cur.execute(
        '''
        INSERT INTO game_floor_instances(instance_key, zone, floor, state_json, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(instance_key) DO UPDATE SET
            zone=excluded.zone,
            floor=excluded.floor,
            state_json=excluded.state_json,
            updated_at=excluded.updated_at
        ''',
        (key, z, fl, json.dumps(state, ensure_ascii=False)[:1200000], now_iso),
    )


def _instance_clean_and_respawn(state: dict) -> dict:
    z = str(state.get('zone') or 'village').strip().lower()[:40]
    fl = max(0, min(int(state.get('floor') or 0), 999))
    enemies_raw = state.get('enemies') if isinstance(state.get('enemies'), list) else []
    enemies: list[dict] = []
    by_id: set[str] = set()
    for e in enemies_raw[:240]:
        if not isinstance(e, dict):
            continue
        eid = str(e.get('id') or '').strip()[:120]
        if not eid or eid in by_id:
            continue
        by_id.add(eid)
        try:
            hp_max = max(1, int(e.get('hpMax') or e.get('hp') or 1))
            hp = max(0, min(hp_max, int(e.get('hp') or hp_max)))
            if hp <= 0:
                continue
            enemies.append(
                {
                    'id': eid,
                    'x': float(e.get('x') or 0.0),
                    'y': float(e.get('y') or 0.0),
                    'hp': hp,
                    'hpMax': hp_max,
                    'atk': max(1, int(e.get('atk') or 1)),
                    'xp': max(1, int(e.get('xp') or 1)),
                    'gold': max(0, int(e.get('gold') or 0)),
                    'kind': str(e.get('kind') or 'slime')[:20],
                    'element': str(e.get('element') or '')[:20],
                    'facing': str(e.get('facing') or 'down')[:10],
                }
            )
        except Exception:
            continue
    state['enemies'] = enemies

    if z != 'dungeon' or fl <= 0:
        state['next_respawn_ts'] = float(time.time() + 6.0)
        return state

    target = _instance_target_count(z, fl)
    now_ts = float(time.time())
    next_respawn_ts = float(state.get('next_respawn_ts') or 0.0)
    if len(enemies) < target and now_ts >= next_respawn_ts:
        w, h = _instance_map_size(fl)
        rid = secrets.token_hex(4)
        rng = random.Random(int.from_bytes(sha256(f'{z}:{fl}:{rid}'.encode('utf-8')).digest()[:8], 'big', signed=False))
        new_enemy = _instance_enemy(rng, fl, len(enemies) + 1, w, h)
        new_enemy['id'] = f'dng:{fl}:r{rid}'
        enemies.append(new_enemy)
        density = len(enemies) / max(1, target)
        interval = 8.5
        if density < 0.55:
            interval = 3.4
        if density < 0.35:
            interval = 2.1
        if density < 0.20:
            interval = 1.2
        state['next_respawn_ts'] = float(now_ts + interval)
    else:
        state['next_respawn_ts'] = float(max(now_ts + 0.6, next_respawn_ts or (now_ts + 2.0)))

    state['enemies'] = enemies[:240]
    return state


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


@app.get('/wiki/items')
def game_wiki_items_page():
    return FileResponse(str(STATIC_DIR / 'game-wiki-items.html'))


@app.get('/wiki/enemies')
def game_wiki_enemies_page():
    return FileResponse(str(STATIC_DIR / 'game-wiki-enemies.html'))


@app.get('/wiki/formulas')
def game_wiki_formulas_page():
    return FileResponse(str(STATIC_DIR / 'game-wiki-formulas.html'))


@app.get('/wiki/progression')
def game_wiki_progression_page():
    return FileResponse(str(STATIC_DIR / 'game-wiki-progression.html'))


@app.get('/wiki/changelog')
def game_wiki_changelog_page():
    return FileResponse(str(STATIC_DIR / 'game-wiki-changelog.html'))


@app.get('/wiki/blog')
def game_blog_page():
    return FileResponse(str(STATIC_DIR / 'game-blog.html'))


@app.get('/wiki/blog/{slug}')
def game_blog_article_page(slug: str):
    s = re.sub(r'[^a-z0-9-]', '', str(slug or '').lower())[:120]
    fname = BLOG_ARTICLE_FILES.get(s)
    if not fname:
        raise HTTPException(status_code=404, detail='article not found')
    path = STATIC_DIR / fname
    if not path.exists():
        raise HTTPException(status_code=404, detail='article file missing')
    return FileResponse(str(path))


@app.get('/sitemap.xml')
def sitemap_xml():
    return FileResponse(str(STATIC_DIR / 'sitemap.xml'), media_type='application/xml')


@app.get('/robots.txt')
def robots_txt():
    return FileResponse(str(STATIC_DIR / 'robots.txt'), media_type='text/plain; charset=utf-8')


@app.get('/api/game/me')
def game_me(request: Request, guest_id: str = ''):
    user_key, display_name, logged = _game_identity(request, guest_id=guest_id, strict_guest=True)
    email = ''
    if logged:
        sid = request.cookies.get(_auth_cookie_name(), '')
        user = _auth_session_user(sid)
        email = str((user or {}).get('email') or '')
    return {'ok': True, 'logged_in': bool(logged), 'user_key': user_key, 'display_name': display_name, 'email': email}


@app.get('/api/game/version')
def game_version():
    return {'ok': True, 'version': GAME_VERSION}


@app.get(GAME_ADMIN_SECRET_PATH)
def game_admin_page(request: Request):
    _require_game_admin(request)
    return FileResponse(str(STATIC_DIR / 'game-admin.html'))


@app.get(f'{GAME_ADMIN_SECRET_PATH}/stats')
def game_admin_stats(request: Request):
    _require_game_admin(request)
    now = datetime.now(timezone.utc)
    active_cutoff = (now - timedelta(minutes=5)).isoformat()
    day_cutoff = (now - timedelta(days=1)).isoformat()
    con = _sec_db()
    cur = con.cursor()
    out: dict[str, int | str | float] = {
        'version': GAME_VERSION,
        'generated_at': now.isoformat(),
        'active_users_5m': 0,
        'active_sessions_2m': 0,
        'profiles_total': 0,
        'users_total': 0,
        'guests_total': 0,
        'chat_24h': 0,
        'daily_kills_total': 0,
        'highest_level': 0,
        'highest_floor': 0,
    }
    try:
        cur.execute('SELECT COUNT(1) AS c FROM game_presence WHERE updated_at >= ?', (active_cutoff,))
        out['active_users_5m'] = int((cur.fetchone() or {'c': 0})['c'] or 0)
        cur.execute('SELECT COUNT(1) AS c FROM game_active_sessions')
        out['active_sessions_2m'] = int((cur.fetchone() or {'c': 0})['c'] or 0)
        cur.execute("SELECT COUNT(1) AS c FROM game_profiles WHERE user_key <> 'guest:guest'")
        out['profiles_total'] = int((cur.fetchone() or {'c': 0})['c'] or 0)
        cur.execute("SELECT COUNT(1) AS c FROM game_profiles WHERE user_key LIKE 'user:%'")
        out['users_total'] = int((cur.fetchone() or {'c': 0})['c'] or 0)
        cur.execute(
            """
            SELECT COUNT(1) AS c
            FROM game_profiles p
            LEFT JOIN game_retired_guests rg ON rg.guest_key = p.user_key
            WHERE p.user_key LIKE 'guest:%'
              AND p.user_key <> 'guest:guest'
              AND NOT (rg.guest_key IS NOT NULL)
            """
        )
        out['guests_total'] = int((cur.fetchone() or {'c': 0})['c'] or 0)
        cur.execute('SELECT COUNT(1) AS c FROM game_chat_messages WHERE created_at >= ?', (day_cutoff,))
        out['chat_24h'] = int((cur.fetchone() or {'c': 0})['c'] or 0)
        cur.execute('SELECT COALESCE(SUM(kills),0) AS c FROM game_daily_kills WHERE day_key=?', (_game_today_key(),))
        out['daily_kills_total'] = int((cur.fetchone() or {'c': 0})['c'] or 0)
        cur.execute('SELECT COALESCE(MAX(level),0) AS c, COALESCE(MAX(max_floor),0) AS f FROM game_profiles')
        row = cur.fetchone() or {'c': 0, 'f': 0}
        out['highest_level'] = int(row['c'] or 0)
        out['highest_floor'] = int(row['f'] or 0)
    finally:
        con.close()
    return {'ok': True, 'stats': out}


@app.post('/api/game/profile/name')
def game_profile_name_set(request: Request, payload: dict = Body(...)):
    sid = request.cookies.get(_auth_cookie_name(), '')
    user = _auth_session_user(sid)
    if not (user and user.get('user_id')):
        raise HTTPException(status_code=401, detail='login required')
    user_key = f"user:{int(user['user_id'])}"
    ok_name, clean_name = _validate_player_name(str(payload.get('name') or ''))
    if not ok_name:
        return {'ok': False, 'reason': 'bad_name'}

    cost = GAME_RENAME_COST
    now_iso = datetime.now(timezone.utc).isoformat()
    con = _sec_db()
    cur = con.cursor()
    cur.execute('SELECT save_json FROM game_saves WHERE user_key=? LIMIT 1', (user_key,))
    row = cur.fetchone()
    try:
        save_obj = json.loads(str(row['save_json'] or '{}')) if row else {}
    except Exception:
        save_obj = {}
    player = save_obj.get('player') if isinstance(save_obj.get('player'), dict) else {}
    current_gold = max(0, int(player.get('gold') or 0))
    if current_gold < cost:
        con.close()
        return {'ok': False, 'reason': 'not_enough_gold', 'required': cost}

    player['gold'] = current_gold - cost
    save_obj['player'] = player
    save_obj['t'] = int(time.time() * 1000)
    save_json = json.dumps(save_obj, ensure_ascii=False)[:300000]
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
    cur.execute(
        '''
        INSERT INTO game_profiles(user_key, display_name, level, gold, max_floor, kills, quests_done, updated_at, created_at)
        VALUES (?, ?, 1, ?, 0, 0, 0, ?, ?)
        ON CONFLICT(user_key) DO UPDATE SET
            display_name=excluded.display_name,
            gold=excluded.gold,
            updated_at=excluded.updated_at
        ''',
        (user_key, clean_name[:120], int(player['gold']), now_iso, now_iso),
    )
    cur.execute('UPDATE game_presence SET display_name=?, updated_at=? WHERE user_key=?', (clean_name[:120], now_iso, user_key))
    cur.execute('UPDATE game_daily_kills SET display_name=?, updated_at=? WHERE user_key=?', (clean_name[:120], now_iso, user_key))
    con.commit()
    con.close()
    return {'ok': True, 'display_name': clean_name[:120], 'gold': int(player['gold']), 'cost': cost}


@app.get('/api/game/save')
def game_save_get(request: Request, guest_id: str = ''):
    user_key, display_name, logged = _game_identity(request, guest_id=guest_id, strict_guest=True)
    con = _sec_db()
    cur = con.cursor()
    if _is_retired_guest(cur, user_key):
        con.close()
        return {'ok': False, 'reason': 'guest_retired'}
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
    user_key, display_name, _logged = _game_identity(request, guest_id=guest_id, strict_guest=True)
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
        # Gold can legitimately go down (buying gear/potions), so it must not block a newer save.
        return (int(p.get('lvl') or 1), int(m.get('max_floor') or 0), int(m.get('kills') or 0))

    con = _sec_db()
    cur = con.cursor()
    if _is_retired_guest(cur, user_key):
        con.close()
        return {'ok': False, 'reason': 'guest_retired'}
    _enforce_guest_identity_quota(request, cur, user_key)
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
        _retire_guest_key(cur, guest_key, user_key, datetime.now(timezone.utc).isoformat())
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
    _retire_guest_key(cur, guest_key, user_key, now_iso)
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

    _retire_guest_key(cur, guest_key, user_key, now_iso)
    cur.execute('DELETE FROM game_saves WHERE user_key=?', (guest_key,))
    cur.execute('DELETE FROM game_profiles WHERE user_key=?', (guest_key,))
    cur.execute('UPDATE game_guest_links SET used_at=?, used_by_user_key=? WHERE code=?', (now_iso, user_key, code))
    con.commit(); con.close()
    return {'ok': True, 'claimed': bool(claimed), 'reason': 'ok'}


@app.post('/api/game/presence/update')
def game_presence_update(request: Request, payload: dict = Body(...)):
    guest_id = str(payload.get('guest_id') or '')
    user_key, display_name, _logged = _game_identity(request, guest_id=guest_id, strict_guest=True)
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
    if _is_retired_guest(cur, user_key):
        con.close()
        return {'ok': False, 'reason': 'guest_retired'}
    _enforce_guest_identity_quota(request, cur, user_key)
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


@app.post('/api/game/presence/clear')
def game_presence_clear(request: Request, payload: dict = Body(...)):
    guest_id = str(payload.get('guest_id') or '')
    user_key, _display_name, _logged = _game_identity(request, guest_id=guest_id, strict_guest=True)
    client_session = _game_client_session(payload)
    con = _sec_db(); cur = con.cursor()
    cur.execute('DELETE FROM game_presence WHERE user_key=?', (user_key,))
    cur.execute('DELETE FROM game_active_sessions WHERE user_key=? AND client_session=?', (user_key, client_session))
    con.commit(); con.close()
    return {'ok': True}


@app.get('/api/game/presence/list')
def game_presence_list(request: Request, guest_id: str = '', zone: str = 'village', floor: int = 0, limit: int = 60):
    user_key, _display_name, _logged = _game_identity(request, guest_id=guest_id, strict_guest=True)
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


@app.post('/api/game/instance/sync')
def game_instance_sync(request: Request, payload: dict = Body(...)):
    guest_id = str(payload.get('guest_id') or '')
    user_key, _display_name, _logged = _game_identity(request, guest_id=guest_id, strict_guest=True)
    z = str(payload.get('zone') or 'village').strip().lower()[:40]
    fl = max(0, min(int(payload.get('floor') or 0), 999))
    incoming = payload.get('enemies') if isinstance(payload.get('enemies'), list) else []

    con = _sec_db()
    cur = con.cursor()
    if _is_retired_guest(cur, user_key):
        con.close()
        return {'ok': False, 'reason': 'guest_retired'}
    _enforce_guest_identity_quota(request, cur, user_key)
    state = _instance_load(cur, z, fl)
    current = state.get('enemies') if isinstance(state.get('enemies'), list) else []
    cur_by_id: dict[str, dict] = {}
    for e in current:
        if isinstance(e, dict):
            eid = str(e.get('id') or '').strip()
            if eid:
                cur_by_id[eid] = e

    for raw in incoming[:240]:
        if not isinstance(raw, dict):
            continue
        eid = str(raw.get('id') or '').strip()[:120]
        if not eid:
            continue
        e = cur_by_id.get(eid)
        if not e:
            continue
        try:
            in_hp = int(raw.get('hp') or e.get('hp') or 0)
            hp_max = max(1, int(e.get('hpMax') or 1))
            e['hp'] = max(0, min(hp_max, min(int(e.get('hp') or hp_max), in_hp)))
            e['x'] = float(raw.get('x') or e.get('x') or 0.0)
            e['y'] = float(raw.get('y') or e.get('y') or 0.0)
            e['facing'] = str(raw.get('facing') or e.get('facing') or 'down')[:10]
        except Exception:
            continue

    state['enemies'] = list(cur_by_id.values())
    state = _instance_clean_and_respawn(state)
    _instance_save(cur, z, fl, state)
    con.commit()
    con.close()
    return {
        'ok': True,
        'zone': z,
        'floor': fl,
        'enemies': state.get('enemies', []),
        'server_ts': int(time.time() * 1000),
    }


@app.post('/api/game/chat/send')
def game_chat_send(request: Request, payload: dict = Body(...)):
    guest_id = str(payload.get('guest_id') or '')
    user_key, display_name, _logged = _game_identity(request, guest_id=guest_id, strict_guest=True)
    msg = str(payload.get('message') or '').strip()
    if not msg:
        raise HTTPException(status_code=400, detail='message required')
    msg = re.sub(r'\s+', ' ', msg)[:280]
    for w in GAME_BAD_WORDS:
        msg = re.sub(re.escape(w), '*' * len(w), msg, flags=re.IGNORECASE)
    now_iso = datetime.now(timezone.utc).isoformat()
    con = _sec_db(); cur = con.cursor()
    if _is_retired_guest(cur, user_key):
        con.close()
        return {'ok': False, 'reason': 'guest_retired'}
    _enforce_guest_identity_quota(request, cur, user_key)
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
        SELECT p.user_key, p.display_name, p.level, p.gold, p.max_floor, p.kills, p.quests_done, p.updated_at
        FROM game_profiles p
        LEFT JOIN game_retired_guests rg ON rg.guest_key = p.user_key
        WHERE p.user_key <> 'guest:guest'
          AND lower(p.display_name) <> 'guest-guest'
          AND NOT (p.user_key LIKE 'guest:%' AND rg.guest_key IS NOT NULL)
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
