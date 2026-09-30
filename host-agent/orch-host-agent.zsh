#!/bin/zsh
# Reports host idle time and memory pressure to the orchestrator so it can
# hold background work while you are using the Mac. Runs on the macOS host
# (not in a container) — see host-agent/com.example.orch-host-agent.plist.
#
#   ORCH_URL         default http://127.0.0.1:8780
#   ORCH_TOKEN_FILE  file containing the API bearer token (recommended)
#   ORCH_TOKEN       token value (fallback)
#   INTERVAL         seconds between reports, default 15
set -u

ORCH_URL=${ORCH_URL:-http://127.0.0.1:8780}
INTERVAL=${INTERVAL:-15}

token() {
  if [[ -n ${ORCH_TOKEN_FILE:-} && -r $ORCH_TOKEN_FILE ]]; then
    tr -d '[:space:]' < "$ORCH_TOKEN_FILE"
  else
    print -r -- "${ORCH_TOKEN:-}"
  fi
}

idle_secs() {
  # HIDIdleTime is nanoseconds since the last keyboard/mouse event.
  ioreg -c IOHIDSystem | awk '/HIDIdleTime/ { printf "%d\n", $NF / 1000000000; exit }'
}

pressure() {
  # 1 = normal, 2 = warn, 4 = critical
  case $(sysctl -n kern.memorystatus_vm_pressure_level 2>/dev/null) in
    1) print normal ;;
    2) print warn ;;
    4) print critical ;;
    *) print warn ;;  # unknown: fail safe
  esac
}

while true; do
  body=$(printf '{"user_idle_secs":%d,"memory_pressure":"%s"}' "$(idle_secs)" "$(pressure)")
  curl -fsS -m 5 -X PUT "$ORCH_URL/v1/host/status" \
    -H "Authorization: Bearer $(token)" \
    -H 'Content-Type: application/json' \
    -d "$body" >/dev/null || print -u2 "orch-host-agent: report failed"
  sleep "$INTERVAL"
done
