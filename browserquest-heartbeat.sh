#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="/srv/openclaw-lab/.env"
STATE_FILE="/var/tmp/browserquest-heartbeat.state"
PUBLIC_URL="https://browserquest.online/api/game/version"
LOCAL_URL="http://127.0.0.1:8010/api/game/version"
TIMEOUT_SEC=12

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
CHAT_ID="${TELEGRAM_CHAT_ID:-}"

send_telegram(){
  local text="$1"
  if [[ -z "$BOT_TOKEN" || -z "$CHAT_ID" ]]; then
    return 0
  fi
  curl -fsS --max-time 15 -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
    -d "chat_id=${CHAT_ID}" \
    --data-urlencode "text=${text}" >/dev/null || true
}

check_url(){
  local url="$1"
  curl -fsS --max-time "$TIMEOUT_SEC" "$url" >/dev/null
}

status="ok"
reason=""
if ! check_url "$LOCAL_URL"; then
  status="fail"
  reason="local_api_down"
elif ! check_url "$PUBLIC_URL"; then
  status="fail"
  reason="public_game_down"
fi

prev="unknown"
if [[ -f "$STATE_FILE" ]]; then
  prev="$(cat "$STATE_FILE" 2>/dev/null || echo unknown)"
fi

echo "$status" > "$STATE_FILE"
now="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"

if [[ "$status" == "fail" && "$prev" != "fail" ]]; then
  send_telegram "🚨 BrowserQuest heartbeat FAIL\nTime: ${now}\nReason: ${reason}\nPublic: ${PUBLIC_URL}\nLocal: ${LOCAL_URL}"
elif [[ "$status" == "ok" && "$prev" == "fail" ]]; then
  send_telegram "✅ BrowserQuest heartbeat RECOVERED\nTime: ${now}\nBrowserQuest is reachable again."
fi
