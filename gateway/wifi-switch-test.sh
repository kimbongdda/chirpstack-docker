#!/bin/bash
# 회선 전환 실험 — WiFi 를 대체망으로 바꿨다가 자동으로 원복하고, 그 사이 시각을 기록한다.
#
# 설치: /usr/local/bin/wifi-switch-test.sh
# 사용: sudo wifi-switch-test.sh [--hold 120] [--alt TP-Link_0430_2.4G] [--home starlink] [--dry-run]
#       sudo wifi-switch-test.sh --restore        # 수동 원복 (안전망 타이머도 이걸 부른다)
# 기록: /var/lib/lora-probe/export/switch_<UTC시각>.log  (각 줄: 유닉스ns  ISO시각  이벤트  내용)
#
# 세션이 끊겨도 원복되도록 두 겹으로 지킨다.
#   1. 스크립트는 systemd-run 으로 스스로 떼어내 실행한다 (SSH/Tailscale 세션과 무관).
#   2. 전환 전에 hold+180초 뒤 --restore 를 부르는 독립 타이머를 걸어 둔다.
#      본 스크립트가 죽어도 그 타이머가 원복한다. 정상 종료하면 타이머를 지운다.
# wpa_supplicant 설정 파일은 건드리지 않는다 (select/enable 만, save_config 안 함).
# lora-probe·ttn-gateway·tailscaled 는 재시작하지 않는다. 자가복구를 재려는 실험이므로.
set -u

IFACE=wlan0
HOLD=120
ALT_SSID="TP-Link_0430_2.4G"
HOME_SSID="starlink"
AFTER=120                      # 원복 후 관찰 시간(초)
DRY=0
MODE=run
EXPORT=/var/lib/lora-probe/export
SELF=/usr/local/bin/wifi-switch-test.sh
SERVER_IP=100.84.186.95

while [ $# -gt 0 ]; do
    case "$1" in
        --hold) HOLD="$2"; shift ;;
        --alt) ALT_SSID="$2"; shift ;;
        --home) HOME_SSID="$2"; shift ;;
        --after) AFTER="$2"; shift ;;
        --dry-run) DRY=1 ;;
        --restore) MODE=restore ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
    shift
done

wpa() { wpa_cli -i "$IFACE" "$@"; }
net_id() { wpa list_networks | awk -F'\t' -v s="$1" 'NR>1 && $2==s {print $1; exit}'; }
cur_ssid() { wpa status | sed -n 's/^ssid=//p'; }

restore() {
    local home_id; home_id=$(net_id "$HOME_SSID")
    [ -n "$home_id" ] || { echo "home SSID '$HOME_SSID' not configured" >&2; return 1; }
    wpa enable_network all >/dev/null
    wpa select_network "$home_id" >/dev/null
    # select_network 은 나머지를 비활성화하므로 다시 켜서 priority 폴백을 살려 둔다
    wpa enable_network all >/dev/null
}

if [ "$MODE" = restore ]; then
    restore && echo "restored to $HOME_SSID (runtime only)"
    exit $?
fi

[ "$(id -u)" = 0 ] || { echo "run as root" >&2; exit 1; }

# ---- 사전 점검 ----
ALT_ID=$(net_id "$ALT_SSID"); HOME_ID=$(net_id "$HOME_SSID")
[ -n "$ALT_ID" ] || { echo "alt SSID '$ALT_SSID' not in wpa_supplicant networks" >&2; exit 1; }
[ -n "$HOME_ID" ] || { echo "home SSID '$HOME_SSID' not in wpa_supplicant networks" >&2; exit 1; }
[ "$(cur_ssid)" = "$HOME_SSID" ] || { echo "not on $HOME_SSID now (on '$(cur_ssid)'); refusing" >&2; exit 1; }
wpa scan >/dev/null; sleep 4
if ! wpa scan_results | awk -F'\t' '{print $5}' | grep -qx "$ALT_SSID"; then
    echo "alt SSID '$ALT_SSID' not visible in scan; refusing" >&2; exit 1
fi

if [ "$DRY" = 1 ]; then
    echo "dry-run OK: $HOME_SSID(id $HOME_ID) -> $ALT_SSID(id $ALT_ID) for ${HOLD}s, observe ${AFTER}s after restore"
    echo "safety restore timer would fire after $((HOLD + 180))s"
    exit 0
fi

# ---- 세션과 분리: systemd-run 으로 다시 실행 ----
if [ -z "${WIFI_SWITCH_DETACHED:-}" ]; then
    unit="wifi-switch-test-$(date -u +%Y%m%dT%H%M%SZ)"
    systemd-run --unit="$unit" --collect -E WIFI_SWITCH_DETACHED=1 \
        "$SELF" --hold "$HOLD" --alt "$ALT_SSID" --home "$HOME_SSID" --after "$AFTER"
    echo "started as $unit — follow: journalctl -u $unit -f   (log in $EXPORT/switch_*.log)"
    exit 0
fi

# ---- 실험 본체 ----
mkdir -p "$EXPORT"
LOG="$EXPORT/switch_$(date -u +%Y%m%dT%H%M%SZ).log"
ev() { local l; l="$(date +%s%N) $(date -u +%Y-%m-%dT%H:%M:%S.%3NZ) $1 ${2:-}"; echo "$l" >> "$LOG"; echo "$l"; }

snapshot() {
    ev STATE "ssid=$(cur_ssid) wpa=$(wpa status | sed -n 's/^wpa_state=//p') ip=$(ip -4 -o addr show "$IFACE" | awk '{print $4}') gw=$(ip -4 route show default | awk '{print $3; exit}')"
    ev TS_PEER "$(tailscale status --json 2>/dev/null | python3 -c '
import json,sys
j=json.load(sys.stdin)
for p in (j.get("Peer") or {}).values():
    if "'"$SERVER_IP"'" in (p.get("TailscaleIPs") or []):
        print("curaddr=%s relay=%s" % (p.get("CurAddr") or "-", p.get("Relay") or "-"))' 2>/dev/null)"
}

SWITCHED=0
SAFETY="wifi-switch-restore-$(date -u +%Y%m%dT%H%M%SZ)"
finish() {
    if [ "$SWITCHED" = 1 ]; then ev RESTORE_TRAP "restoring on exit"; restore; fi
    systemctl stop "$SAFETY.timer" 2>/dev/null
}
trap finish EXIT INT TERM

# 상태 변화만 기록하는 감시 루프 (0.2초 간격, 공유기 ping·Tailscale 경로는 2초 간격)
watch_for() {
    local until=$(( $(date +%s) + $1 )) last="" i=0 s gw tsp
    while [ "$(date +%s)" -lt "$until" ]; do
        s="ssid=$(cur_ssid) wpa=$(wpa status | sed -n 's/^wpa_state=//p') ip=$(ip -4 -o addr show "$IFACE" | awk '{print $4}')"
        [ "$s" != "$last" ] && { ev LINK "$s"; last="$s"; }
        if [ $((i % 10)) = 0 ]; then
            gw=$(ip -4 route show default | awk '{print $3; exit}')
            if [ -n "$gw" ] && ping -c1 -W1 -I "$IFACE" "$gw" >/dev/null 2>&1; then r=ok; else r=fail; fi
            [ "$r" != "${lastping:-}" ] && { ev GW_PING "$r gw=${gw:--}"; lastping=$r; }
            tsp=$(tailscale status --json 2>/dev/null | python3 -c '
import json,sys
j=json.load(sys.stdin)
for p in (j.get("Peer") or {}).values():
    if "'"$SERVER_IP"'" in (p.get("TailscaleIPs") or []):
        print("direct" if p.get("CurAddr") else "derp:" + (p.get("Relay") or "-"))' 2>/dev/null)
            [ "$tsp" != "${lasttsp:-}" ] && { ev TS_PATH "${tsp:-unknown}"; lasttsp=$tsp; }
        fi
        i=$((i + 1)); sleep 0.2
    done
}

ev START "home=$HOME_SSID($HOME_ID) alt=$ALT_SSID($ALT_ID) hold=${HOLD}s after=${AFTER}s"
snapshot
watch_for 10                                         # 전환 전 기준선

systemd-run --unit="$SAFETY" --on-active=$((HOLD + 180)) --collect "$SELF" --restore >/dev/null 2>&1 \
    && ev SAFETY "restore timer $SAFETY in $((HOLD + 180))s"

SWITCHED=1
ev SWITCH "select_network $ALT_ID ($ALT_SSID)"
wpa select_network "$ALT_ID" >/dev/null
watch_for "$HOLD"
snapshot

ev RESTORE "select_network $HOME_ID ($HOME_SSID)"
restore
SWITCHED=0
watch_for "$AFTER"
snapshot
ev END "done"
