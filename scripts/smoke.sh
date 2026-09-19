#!/usr/bin/env bash
# 端到端冒烟：等 bootstrap(8081)/deviceapi(8083)/alarm-svc(8085) 就绪 → 50 台模拟器跑 40s（10s 时一条 FLAME_DETECTED）
# → 影子 / desired / 指令 / 指令结果 / 告警 五步 + v1.1 开关三态 / 遥测查询 两步逐项 PASS/FAIL。
# 依赖：curl。可选：jq（无则用 grep 兜底）。需先起 emqx/nats/redis/pg/td 与各服务。
set -u
BOOT=${BOOTSTRAP_URL:-http://127.0.0.1:8081}
API=${DEVICEAPI_URL:-http://127.0.0.1:8083}
ALARM=${ALARM_URL:-http://127.0.0.1:8085}
MQTT=${IOT_SIM_MQTT:-tcp://127.0.0.1:1884}
SN=${SMOKE_SN:-SIM00001}
SIM_N=${SIM_N:-50}
SIM_SECONDS=${SIM_SECONDS:-40}
SETTLE=${SETTLE:-20}           # 起模拟器后等多久再查（事件在 10s 触发，留管道攒批/告警消费时间）

pass=0; fail=0
ok()   { echo "PASS  $1"; pass=$((pass+1)); }
bad()  { echo "FAIL  $1${2:+ -- $2}"; fail=$((fail+1)); }
jget() { # jget <json> <jq-path> ; fallback: grep first "key":value
  if command -v jq >/dev/null 2>&1; then printf '%s' "$1" | jq -r "$2" 2>/dev/null
  else local k=${2##*.}; printf '%s' "$1" | grep -o "\"$k\":[^,}]*" | head -1 | sed -E 's/^"[^"]*":"?//; s/"$//'; fi
}

wait_healthz() {
  local url=$1 name=$2 i
  for i in $(seq 1 60); do
    if curl -fsS -m 2 "$url/healthz" >/dev/null 2>&1; then ok "$name healthz ($url)"; return 0; fi
    sleep 1
  done
  bad "$name healthz ($url)" "not ready after 60s"; return 1
}

echo "== smoke: waiting for services"
wait_healthz "$BOOT"  bootstrap-svc || exit 1
wait_healthz "$API"   deviceapi     || exit 1
wait_healthz "$ALARM" alarm-svc     || exit 1

echo "== smoke: launching $SIM_N simulators for ${SIM_SECONDS}s (FLAME_DETECTED@10s)"
cd "$(dirname "$0")/.." || exit 1
go run ./cmd/device-simulator -n "$SIM_N" -event FLAME_DETECTED@10s -bootstrap "$BOOT" -mqtt "$MQTT" -work 5s -hb 60s -bad-firmware 0 -job-optin=true -module LM40 \
  >/tmp/smoke-sim.out 2>/tmp/smoke-sim.err &
SIM_PID=$!
START=$(date +%s)
# go run 不向子进程转发 kill 发来的 SIGINT，先停它的子进程再停它自己，否则 wait 永远不返回
stop_sim() { pkill -INT -P "$SIM_PID" 2>/dev/null; kill -INT "$SIM_PID" 2>/dev/null; wait "$SIM_PID" 2>/dev/null; }
trap stop_sim EXIT
sleep "$SETTLE"

echo "== smoke: checks against $SN"
# 1. shadow
resp=$(curl -sS -m 5 "$API/api/v1/devices/$SN/shadow")
if [ "$(jget "$resp" .code)" = "0" ] && printf '%s' "$resp" | grep -q '"reported"'; then ok "GET shadow ($SN)"; else bad "GET shadow" "$resp"; fi

# 2. desired PATCH
resp=$(curl -sS -m 5 -X PATCH -H 'Content-Type: application/json' -d '{"power_limit":80,"smoke":true}' "$API/api/v1/devices/$SN/desired")
ver=$(jget "$resp" .data.desired_version); [ -z "$ver" ] || [ "$ver" = null ] && ver=$(jget "$resp" .data.version)
if [ "$(jget "$resp" .code)" = "0" ]; then ok "PATCH desired (version=${ver:-?})"; else bad "PATCH desired" "$resp"; fi

# 3. cmd pause
resp=$(curl -sS -m 5 -X POST -H 'Content-Type: application/json' -d '{"action":"pause","params":{}}' "$API/api/v1/devices/$SN/cmd")
cmd_id=$(jget "$resp" .data.cmd_id)
if [ "$(jget "$resp" .code)" = "0" ] && [ -n "$cmd_id" ] && [ "$cmd_id" != null ]; then ok "POST cmd pause (cmd_id=$cmd_id)"; else bad "POST cmd pause" "$resp"; cmd_id=""; fi

# 3b. remote_restart must be 403/10003
resp=$(curl -sS -m 5 -X POST -H 'Content-Type: application/json' -d '{"action":"remote_restart"}' "$API/api/v1/devices/$SN/cmd")
if [ "$(jget "$resp" .code)" = "10003" ]; then ok "POST cmd remote_restart denied (10003)"; else bad "POST cmd remote_restart should be denied" "$resp"; fi

# 4. cmd result (poll up to 15s for the device's cmd_ack to flow back)
if [ -n "$cmd_id" ]; then
  got=""
  for i in $(seq 1 15); do
    resp=$(curl -sS -m 5 "$API/api/v1/cmds/$cmd_id")
    if [ "$(jget "$resp" .code)" = "0" ] && printf '%s' "$resp" | grep -Eq '"result":"(ok|fail)"'; then got=1; break; fi
    sleep 1
  done
  if [ -n "$got" ]; then ok "GET cmds/$cmd_id ($(jget "$resp" .data.result))"; else bad "GET cmds/$cmd_id" "no ack within 15s: $resp"; fi
fi

# 5. alarms should include FLAME_DETECTED：推送成功后状态机已从 open 迁到 notified，两种状态都算命中
found=""
for st in notified open; do
  resp=$(curl -sS -m 5 "$ALARM/api/v1/alarms?status=$st")
  if [ "$(jget "$resp" .code)" = "0" ] && printf '%s' "$resp" | grep -q 'FLAME_DETECTED'; then found=$st; break; fi
done
if [ -n "$found" ]; then ok "GET alarms?status=$found contains FLAME_DETECTED"; else bad "GET alarms?status=notified|open" "$resp"; fi

# 6. v1.1 开关三态：desired 下发 job_feedback_optin=true → 设备下一帧回报 → switches 状态应收敛为 applied（≤15s）
resp=$(curl -sS -m 5 -X PATCH -H 'Content-Type: application/json' -d '{"job_feedback_optin":true}' "$API/api/v1/devices/$SN/desired")
if [ "$(jget "$resp" .code)" != "0" ]; then bad "PATCH desired job_feedback_optin" "$resp"; fi
state=""
for i in $(seq 1 15); do
  resp=$(curl -sS -m 5 "$API/api/v1/devices/$SN/shadow")
  if command -v jq >/dev/null 2>&1; then state=$(printf '%s' "$resp" | jq -r '.data.switches.job_feedback_optin.state' 2>/dev/null)
  else state=$(printf '%s' "$resp" | grep -o '"job_feedback_optin":{[^}]*}' | grep -o '"state":"[a-z]*"' | head -1 | sed -E 's/.*:"//; s/"$//'); fi
  [ "$state" = "applied" ] && break
  sleep 1
done
if [ "$state" = "applied" ]; then ok "GET shadow switches.job_feedback_optin.state=applied"; else bad "GET shadow switches.job_feedback_optin" "state=$state resp=$resp"; fi

# 7. 遥测查询（TDengine 落库）
resp=$(curl -sS -m 5 "$API/api/v1/devices/$SN/telemetry?limit=1")
if [ "$(jget "$resp" .code)" = "0" ] && printf '%s' "$resp" | grep -q '"work_state"'; then ok "GET telemetry?limit=1 ($SN)"; else bad "GET telemetry?limit=1" "$resp"; fi

# let the simulator finish its 40s window, then stop it
NOW=$(date +%s); LEFT=$((START + SIM_SECONDS - NOW)); [ "$LEFT" -gt 0 ] && sleep "$LEFT"
stop_sim; trap - EXIT
echo "== simulator last status: $(tail -1 /tmp/smoke-sim.out 2>/dev/null)"
echo "== smoke: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
