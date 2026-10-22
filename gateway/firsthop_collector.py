"""첫 홉 수집기 — 끊김이 어느 구간에서 났는지 가르기 위한 게이트웨이 측 보조 측정

설치: /opt/lora-probe/firsthop_collector.py
서비스: firsthop-collector.service
DB: /var/lib/lora-probe/firsthop.db

lora-probe 는 게이트웨이 -> 서버 전 구간(WiFi + Starlink + 인터넷 + Tailscale)을 한 번에
본다. 여기서는 그 앞쪽 두 구간을 따로 잰다.

  1. router_ping : 기본 게이트웨이(공유기)까지 1초 ping  -> WiFi 구간
  2. hop_ping    : 위성 너머 첫 응답 홉까지 10 Hz (TTL 제한 ICMP) -> WiFi + 위성 구간
       위성 다음 홉인 100.64.0.1 은 Tailscale 방화벽(ts-input: 100.64.0.0/10 이 tailscale0
       밖에서 오면 DROP)에 응답이 막혀 못 쓴다. 그래서 1.1.1.1 로 TTL=3 패킷을 보내고 돌아오는
       Time Exceeded(현재 172.16.251.56, Starlink 측)로 지연을 잰다. 오버레이를 타지 않도록
       기본 경로 인터페이스(wlan0)에 묶는다. 라우터가 ICMP 생성 속도를 제한하면 유실처럼
       보일 수 있으니, 단발 유실보다 연속 유실 구간을 볼 것.
  3. ts_path     : Tailscale 서버 피어 경로 1초 기록 (tailscaled 로컬 API)
       direct 이면 CurAddr 가 채워지고, 비면 DERP(TCP) 경유. Relay 는 direct 일 때도 홈 DERP
       이름이 들어 있으므로 경로 판정에 쓰면 안 된다. Tx/Rx 바이트를 같이 남겨, CurAddr 가 빈
       동안 Tx 가 늘면 그 패킷이 DERP 로 나갔다는 직접 증거가 된다.
  4. dish_*      : Starlink 접시 gRPC (192.168.100.1:9200) -> 위성 구간
       - dish_history : 접시가 1초마다 남기는 PoP ping 지연·드롭률 (최근 900초 링버퍼)
       - dish_event   : 접시가 판정한 끊김 원인 (NO_DOWNLINK, OBSTRUCTED, ...)
       - dish_status  : 10초마다 상태 스냅샷 (가림률, 접시 가동시간 등)

프로브가 끊겼는데 공유기 ping 도 끊겼으면 WiFi, 공유기는 살아 있고 접시 드롭률이
올랐으면 위성, 둘 다 멀쩡하면 그 너머(인터넷·Tailscale·서버) 문제로 본다.

접시 gRPC 는 grpcurl 로 부른다 (/usr/local/bin/grpcurl, fullstorydev/grpcurl 릴리스).
python grpcio 는 bullseye 에 reflection 지원이 없어 쓰지 않는다.
"""
import http.client
import json
import os
import queue
import re
import socket
import sqlite3
import struct
import subprocess
import sys
import threading
import time

DB_PATH = os.getenv("FIRSTHOP_DB", "/var/lib/lora-probe/firsthop.db")
GRPCURL = os.getenv("FIRSTHOP_GRPCURL", "/usr/local/bin/grpcurl")
DISH = os.getenv("FIRSTHOP_DISH", "192.168.100.1:9200")
HISTORY_EVERY = float(os.getenv("FIRSTHOP_HISTORY_EVERY", "60"))   # 링버퍼가 900초라 넉넉함
STATUS_EVERY = float(os.getenv("FIRSTHOP_STATUS_EVERY", "10"))
HOP_TARGET = os.getenv("FIRSTHOP_HOP_TARGET", "1.1.1.1")
HOP_TTL = int(os.getenv("FIRSTHOP_HOP_TTL", "3"))
HOP_HZ = float(os.getenv("FIRSTHOP_HOP_HZ", "10"))
HOP_TIMEOUT = 1.0
TS_SOCK = os.getenv("FIRSTHOP_TS_SOCK", "/var/run/tailscale/tailscaled.sock")
TS_PEER_IP = os.getenv("FIRSTHOP_TS_PEER", "100.84.186.95")       # 서버 노드는 Tailscale IP 로 식별

# 접시 history 의 outages 는 GPS 시각이라 쓰지 않고, eventLog(유닉스 ns)를 쓴다.
RING = 900

logq = queue.Queue(maxsize=200000)

SCHEMA = [
    """CREATE TABLE IF NOT EXISTS router_ping(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        t_ns INTEGER, target TEXT, seq INTEGER, rtt_ms REAL, lost INTEGER)""",
    "CREATE INDEX IF NOT EXISTS ix_router_t ON router_ping(t_ns)",
    """CREATE TABLE IF NOT EXISTS hop_ping(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        t_ns INTEGER, iface TEXT, ttl INTEGER, seq INTEGER,
        responder TEXT, rtt_ms REAL, lost INTEGER)""",
    "CREATE INDEX IF NOT EXISTS ix_hop_t ON hop_ping(t_ns)",
    """CREATE TABLE IF NOT EXISTS ts_path(
        t_ns INTEGER PRIMARY KEY, cur_addr TEXT, relay TEXT, peer_relay TEXT,
        rx_bytes INTEGER, tx_bytes INTEGER, last_handshake TEXT, online INTEGER, active INTEGER)""",
    # counter 는 접시 부팅 후 초 단위 일련번호. 재부팅하면 0부터 다시 세므로 boot 로 구분한다.
    """CREATE TABLE IF NOT EXISTS dish_history(
        boot INTEGER, counter INTEGER, t_ns INTEGER,
        pop_ping_ms REAL, pop_ping_drop REAL, down_bps REAL, up_bps REAL,
        PRIMARY KEY(boot, counter))""",
    "CREATE INDEX IF NOT EXISTS ix_hist_t ON dish_history(t_ns)",
    """CREATE TABLE IF NOT EXISTS dish_event(
        start_ns INTEGER, reason TEXT, duration_ns INTEGER, severity TEXT,
        PRIMARY KEY(start_ns, reason))""",
    """CREATE TABLE IF NOT EXISTS dish_status(
        t_ns INTEGER PRIMARY KEY, uptime_s INTEGER, pop_ping_ms REAL,
        frac_obstructed REAL, outage_cause TEXT, sw_update TEXT)""",
]


def put(sql, rows):
    try:
        logq.put_nowait((sql, rows))
    except queue.Full:
        pass


def writer():
    """기록 스레드. 세 수집 스레드의 쓰기를 한 연결로 모은다."""
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.execute("PRAGMA busy_timeout=30000")
    for s in SCHEMA:
        con.execute(s)
    con.commit()
    while True:
        sql, rows = logq.get()
        while True:
            try:
                con.executemany(sql, rows)
                con.commit()
                break
            except sqlite3.Error as e:
                print(f"[firsthop] write retry: {e}", flush=True)
                try:
                    con.rollback()
                except sqlite3.Error:
                    pass
                time.sleep(1)


# ===================== 공유기 ping =====================

PING_OK = re.compile(r"^\[(\d+)\.(\d+)\] .* icmp_seq=(\d+) .*time=([\d.]+) ms")
PING_LOST = re.compile(r"^\[(\d+)\.(\d+)\] no answer yet for icmp_seq=(\d+)")


def default_gateway():
    try:
        out = subprocess.run(["ip", "-4", "route", "show", "default"],
                             capture_output=True, text=True, timeout=5).stdout.split()
        return out[out.index("via") + 1]
    except (ValueError, IndexError, OSError, subprocess.SubprocessError):
        return None


def ts_ns(sec, frac):
    return int(sec) * 1_000_000_000 + int(frac.ljust(9, "0")[:9])


def router_pinger():
    """기본 게이트웨이에 1초 ping. 회선이 바뀌어 게이트웨이가 달라지면 대상도 바꾼다."""
    while True:
        target = default_gateway()
        if not target:
            time.sleep(1)
            continue
        # -D: 유닉스 시각 표시, -O: 응답 없는 seq 도 출력, -W 1: 1초 안에 안 오면 유실
        p = subprocess.Popen(["ping", "-D", "-O", "-n", "-i", "1", "-W", "1", target],
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        print(f"[firsthop] router ping -> {target}", flush=True)
        last_check = time.monotonic()
        for line in p.stdout:
            m = PING_OK.match(line)
            if m:
                put("INSERT INTO router_ping(t_ns,target,seq,rtt_ms,lost) VALUES(?,?,?,?,0)",
                    [(ts_ns(m[1], m[2]), target, int(m[3]), float(m[4]))])
            else:
                m = PING_LOST.match(line)
                if m:
                    put("INSERT INTO router_ping(t_ns,target,seq,rtt_ms,lost) VALUES(?,?,?,NULL,1)",
                        [(ts_ns(m[1], m[2]), target, int(m[3]))])
            # 10초마다 기본 게이트웨이가 바뀌었는지 본다 (WiFi 전환 대비)
            if time.monotonic() - last_check > 10:
                last_check = time.monotonic()
                if default_gateway() != target:
                    break
        p.kill()
        p.wait()
        time.sleep(1)


# ===================== 위성 너머 첫 홉 (TTL 제한 ICMP) =====================

def default_iface():
    try:
        out = subprocess.run(["ip", "-4", "route", "show", "default"],
                             capture_output=True, text=True, timeout=5).stdout.split()
        return out[out.index("dev") + 1]
    except (ValueError, IndexError, OSError, subprocess.SubprocessError):
        return None


def icmp_checksum(b):
    if len(b) % 2:
        b += b"\0"
    s = sum(struct.unpack(f"!{len(b) // 2}H", b))
    s = (s >> 16) + (s & 0xFFFF)
    s += s >> 16
    return ~s & 0xFFFF


def hop_prober():
    """HOP_HZ 로 TTL=HOP_TTL 에코를 보내고 Time Exceeded 응답까지의 지연을 잰다.

    응답 대기는 별도 스레드가 하고, 1초 안에 안 온 seq 는 유실로 적는다.
    10 Hz 를 행마다 커밋하면 SD 카드에 부담이라 1초 단위로 묶어 쓴다.
    """
    ident = os.getpid() & 0xFFFF
    pending = {}                       # seq -> (t_ns, mono, iface)
    lock = threading.Lock()
    batch = []

    def receiver(sock):
        while True:
            try:
                data, addr = sock.recvfrom(1500)
            except OSError:
                return                 # 인터페이스가 바뀌어 소켓을 닫았다
            mono = time.monotonic()
            ihl = (data[0] & 0x0F) * 4
            typ = data[ihl]
            if typ == 11:              # Time Exceeded: 원래 보낸 IP+ICMP 헤더가 안에 들어 있다
                inner = data[ihl + 8:]
                iihl = (inner[0] & 0x0F) * 4
                icmp = inner[iihl:iihl + 8]
            elif typ == 0:             # TTL 이 충분하면 목적지가 직접 답한다
                icmp = data[ihl:ihl + 8]
            else:
                continue
            if len(icmp) < 8:
                continue
            rid, rseq = struct.unpack("!HH", icmp[4:8])
            if rid != ident:
                continue
            with lock:
                p = pending.pop(rseq, None)
                if p:
                    batch.append((p[0], p[2], HOP_TTL, rseq, addr[0],
                                  round((mono - p[1]) * 1000, 3), 0))

    while True:
        iface = default_iface()
        if not iface:
            time.sleep(1)
            continue
        sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_ICMP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BINDTODEVICE, iface.encode() + b"\0")
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_TTL, HOP_TTL)
        threading.Thread(target=receiver, args=(sock,), daemon=True).start()
        print(f"[firsthop] hop ping -> {HOP_TARGET} ttl={HOP_TTL} via {iface} at {HOP_HZ} Hz", flush=True)

        seq = 0
        next_send = time.monotonic()
        last_flush = last_check = time.monotonic()
        while True:
            seq = (seq + 1) & 0xFFFF
            hdr = struct.pack("!BBHHH", 8, 0, 0, ident, seq)
            payload = b"firsthop" * 2
            hdr = struct.pack("!BBHHH", 8, 0, icmp_checksum(hdr + payload), ident, seq)
            with lock:
                pending[seq] = (time.time_ns(), time.monotonic(), iface)
            try:
                sock.sendto(hdr + payload, (HOP_TARGET, 0))
            except OSError:
                pass                   # 망이 없으면 응답이 안 와서 유실로 남는다

            now = time.monotonic()
            with lock:
                for s, p in list(pending.items()):
                    if now - p[1] > HOP_TIMEOUT:
                        del pending[s]
                        batch.append((p[0], p[2], HOP_TTL, s, None, None, 1))
                if now - last_flush >= 1 and batch:
                    put("INSERT INTO hop_ping(t_ns,iface,ttl,seq,responder,rtt_ms,lost)"
                        " VALUES(?,?,?,?,?,?,?)", sorted(batch))
                    batch.clear()
                    last_flush = now
            if now - last_check > 10:
                last_check = now
                if default_iface() != iface:
                    break
            next_send += 1.0 / HOP_HZ
            time.sleep(max(0.0, next_send - time.monotonic()))
        sock.close()


# ===================== Tailscale 서버 피어 경로 =====================

class _UnixHTTP(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("local-tailscaled.sock", timeout=3)
        self._path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(3)
        self.sock.connect(self._path)


def ts_path_poller():
    """1초마다 tailscaled 로컬 API 로 서버 피어 상태를 읽는다.

    `tailscale status --json` 을 매초 띄우면 무거우므로 소켓으로 직접 묻는다 (수 ms).
    매초 커밋하지 않고 10초씩 묶어 쓴다.
    """
    batch, last_flush, warned = [], time.monotonic(), False
    next_t = time.monotonic()
    while True:
        try:
            c = _UnixHTTP(TS_SOCK)
            c.request("GET", "/localapi/v0/status?peers=true")
            st = json.loads(c.getresponse().read())
            c.close()
            for p in (st.get("Peer") or {}).values():
                if TS_PEER_IP in (p.get("TailscaleIPs") or []):
                    batch.append((time.time_ns(), p.get("CurAddr") or "", p.get("Relay") or "",
                                  p.get("PeerRelay") or "", p.get("RxBytes"), p.get("TxBytes"),
                                  p.get("LastHandshake"), int(bool(p.get("Online"))),
                                  int(bool(p.get("Active")))))
                    break
            warned = False
        except (OSError, ValueError, http.client.HTTPException) as e:
            if not warned:
                print(f"[firsthop] tailscaled 로컬 API 실패: {e}", flush=True)
                warned = True
        if batch and time.monotonic() - last_flush >= 10:
            put("INSERT OR IGNORE INTO ts_path VALUES(?,?,?,?,?,?,?,?,?)", batch)
            batch, last_flush = [], time.monotonic()
        next_t += 1.0
        time.sleep(max(0.0, next_t - time.monotonic()))


# ===================== Starlink 접시 =====================

def dish_call(req):
    try:
        out = subprocess.run(
            [GRPCURL, "-plaintext", "-max-time", "5", "-d", json.dumps(req),
             DISH, "SpaceX.API.Device.Device/Handle"],
            capture_output=True, text=True, timeout=10)
        if out.returncode != 0:
            return None
        return json.loads(out.stdout)
    except (OSError, subprocess.SubprocessError, ValueError):
        return None


def dish_history_poller():
    last = {}            # boot -> 마지막으로 저장한 counter
    warned = False
    while True:
        d = dish_call({"get_history": {}})
        h = (d or {}).get("dishGetHistory")
        if not h:
            if not warned:
                print("[firsthop] 접시 gRPC 응답 없음 (Starlink 가 아닌 회선이거나 접시 재부팅 중)", flush=True)
                warned = True
            time.sleep(HISTORY_EVERY)
            continue
        warned = False

        ev = h.get("eventLog") or {}
        now_ns = int(ev.get("currentTimestampNs") or time.time_ns())
        boot = int(ev.get("startTimestampNs") or 0) // 1_000_000_000   # 접시 부팅 시각(초)
        current = int(h.get("current") or 0)

        # 링버퍼: counter c 의 표본은 배열 [c % 900] 에 있다. 최신이 current-1.
        start = max(last.get(boot, -1) + 1, current - RING, 0)
        cols = [h.get(k) or [] for k in ("popPingLatencyMs", "popPingDropRate",
                                         "downlinkThroughputBps", "uplinkThroughputBps")]
        rows = []
        for c in range(start, current):
            i = c % RING
            vals = [col[i] if i < len(col) else None for col in cols]
            # current-1 을 '지금'으로 보고 1초 간격으로 거꾸로 시각을 매긴다 (±1초 정도)
            rows.append((boot, c, now_ns - (current - 1 - c) * 1_000_000_000, *vals))
        if rows:
            put("INSERT OR IGNORE INTO dish_history VALUES(?,?,?,?,?,?,?)", rows)
            last[boot] = current - 1

        events = [(int(e["startTimestampNs"]), e.get("reason", ""),
                   int(e.get("durationNs") or 0), e.get("severity", ""))
                  for e in ev.get("events") or [] if e.get("startTimestampNs")]
        if events:
            # 진행 중이던 끊김은 다음 조회 때 길이가 늘어나므로 덮어쓴다
            put("INSERT OR REPLACE INTO dish_event VALUES(?,?,?,?)", events)
        time.sleep(HISTORY_EVERY)


def dish_status_poller():
    while True:
        d = dish_call({"get_status": {}})
        s = (d or {}).get("dishGetStatus")
        if s:
            ob = s.get("obstructionStats") or {}
            put("INSERT OR REPLACE INTO dish_status VALUES(?,?,?,?,?,?)", [(
                time.time_ns(),
                int((s.get("deviceState") or {}).get("uptimeS") or 0),
                s.get("popPingLatencyMs"),
                ob.get("fractionObstructed"),
                (s.get("outage") or {}).get("cause"),
                s.get("softwareUpdateState"),
            )])
        time.sleep(STATUS_EVERY)


def main():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    threading.Thread(target=writer, daemon=True).start()
    for fn in (router_pinger, hop_prober, ts_path_poller, dish_history_poller, dish_status_poller):
        threading.Thread(target=fn, daemon=True).start()
    print(f"[firsthop] -> {DB_PATH} (dish {DISH}, history {HISTORY_EVERY}s, status {STATUS_EVERY}s)",
          flush=True)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    sys.exit(main())
