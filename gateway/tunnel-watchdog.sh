#!/bin/bash
# Tailscale 터널 소켓 감시자 — docs/latency-probe/findings.md §3 의 장애를 자동 복구한다.
#
# 감시하는 장애:
#   UDP 소켓은 connect() 시점에 경로를 정하고 출발지 주소를 고정한다. 회선이 바뀌거나
#   tailscale0 이 잠깐 내려가면, 재조회가 기본 경로(물리 인터페이스)로 떨어져 거기에
#   박힌다. 목적지는 Tailscale 주소인데 출발지가 물리 주소라 패킷은 공유기로 나가
#   버려진다. 프로세스는 살아 있으므로 systemd Restart=always 는 발동하지 않는다.
#
# 판정 방법:
#   목적지가 서버의 Tailscale 주소인 소켓의 출발지가 100.x 대역이 아니면 잘못된 것이다.
#   증상(ackr=0)이 아니라 원인을 직접 보므로 오탐이 적고 빠르다.
#
# 스타링크처럼 자주 끊기는 회선에서는 이 장애가 반복되므로 자동 복구가 필수다.

set -uo pipefail

SERVER_TS_IP="${SERVER_TS_IP:-100.84.186.95}"
COOLDOWN_SEC="${COOLDOWN_SEC:-180}"     # 재시작 후 이 시간 동안은 다시 건드리지 않는다
STATE_DIR="/var/lib/lora-probe"
TAG="tunnel-watchdog"

log() { logger -t "$TAG" -- "$1"; echo "$(date -Is) $1"; }

# tailscale0 이 아직 안 올라왔으면 판정 자체가 무의미하다
if ! ip -4 addr show tailscale0 2>/dev/null | grep -q 'inet '; then
    log "tailscale0 미준비 — 이번 주기는 건너뜀"
    exit 0
fi

# 서비스별로 검사한다. (프로세스명, systemd 유닛)
check_and_fix() {
    local proc="$1" unit="$2" stamp="$STATE_DIR/${unit}.last_restart"

    # 목적지가 서버 Tailscale 주소인 소켓들의 '출발지'를 뽑는다
    local bad
    bad=$(ss -unpH 2>/dev/null \
          | grep "$SERVER_TS_IP" \
          | grep "\"$proc\"" \
          | awk '{print $5}' \
          | cut -d: -f1 \
          | grep -v '^100\.' \
          | sort -u)

    [ -z "$bad" ] && return 0

    # 쿨다운 확인 — 재시작 루프 방지
    if [ -f "$stamp" ]; then
        local age=$(( $(date +%s) - $(stat -c %Y "$stamp") ))
        if [ "$age" -lt "$COOLDOWN_SEC" ]; then
            log "$unit: 잘못된 출발지($bad) 감지했으나 쿨다운 ${age}s/${COOLDOWN_SEC}s — 대기"
            return 0
        fi
    fi

    log "$unit: 소켓 출발지가 Tailscale 대역이 아님 ($bad) — 재시작"
    systemctl restart "$unit"
    mkdir -p "$STATE_DIR"
    touch "$stamp"
}

check_and_fix "lora_pkt_fwd" "ttn-gateway"
check_and_fix "python3"      "lora-probe"
