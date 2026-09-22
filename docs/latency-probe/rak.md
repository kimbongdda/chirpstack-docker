# 게이트웨이 측 구축 지시문 — RAK7248 프로브 송신기

대상 장비: RAK7248 (Raspberry Pi 기반) / Tailscale 노드 `rak7248-gw` (`100.112.121.22`)
Gateway EUI: `2ccf67fffe405e3c`

먼저 [protocol.md](protocol.md)를 읽을 것. 아래 구현은 그 규격을 따른다.

## ⚠️ 절대 건드리지 말 것

이 장비는 **운영 중인 게이트웨이**다. 아래를 변경하면 데이터 수집이 끊긴다.

- `/opt/ttn-gateway/packet_forwarder/lora_pkt_fwd/global_conf.json` 의
  **`gateway_ID`(EUI), 주파수·채널·region 설정, `server_address`**
- `ttn-gateway` 서비스 — 이 작업에서는 **재시작할 일이 없다**
- 패킷포워더가 점유 중인 **GPS 시리얼 포트** (§2 참고)

프로브는 LoRa 데이터 경로(`1700/udp`)와 완전히 분리된 별도 채널(`1800/udp`)을 쓴다.
기존 동작에 영향을 주지 않아야 한다.

## 1. 사전 확인

### 1.1 터널 확인

```bash
tailscale status
ping -c 3 100.84.186.95
```

서버(`aeron-sever`, `100.84.186.95`)가 보이고 응답해야 한다. 안 되면 여기서 멈추고 보고할 것.

### 1.2 서버 응답기 동작 확인

서버 측 작업([server.md](server.md))이 먼저 끝나 있어야 한다.

```bash
python3 - <<'EOF'
import socket, json, time
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.settimeout(3)
req = {"v":1,"typ":"req","run":1,"seq":1,"src":"pretest","t1":time.time_ns(),
       "tsrc":"sys","path":"direct"}
s.sendto(json.dumps(req).encode(), ("100.84.186.95", 1800))
print(s.recv(2048).decode())
EOF
```

`t2`, `t3`가 담긴 `"typ":"rsp"` JSON이 오면 정상이다.

## 2. GPS 상태 조사 — 변경하지 말고 "조사만" 할 것

RAK2287 모듈은 GPS 탑재/미탑재 버전이 있다. 먼저 현황을 파악해 **보고**한다.

```bash
ls -l /dev/ttyAMA0 /dev/serial0 /dev/ttyUSB* 2>/dev/null
ls -l /dev/pps* 2>/dev/null
grep -iE '"gps|gps_tty_path|fake_gps' /opt/ttn-gateway/packet_forwarder/lora_pkt_fwd/global_conf.json
systemctl is-active gpsd chrony chronyd ntp 2>/dev/null
chronyc tracking 2>/dev/null || timedatectl
```

### 2.1 중요 — GPS 시리얼 포트 쟁탈 금지

SX1302 패킷포워더는 GPS NMEA 시리얼 포트를 **직접 점유**한다.
여기에 `gpsd`를 같은 포트로 붙이면 충돌해서 **패킷포워더의 GPS 기능이 깨진다.**
운영 중인 게이트웨이이므로 이 시도는 하지 말 것.

권장 구성은 다음과 같다.

| 상황 | 시스템 시계 규율 방법 | `tsrc` 값 |
|---|---|---|
| `/dev/pps0` 존재 | chrony의 **PPS refclock** 사용 (NMEA 포트와 충돌 없음) + NTP 보조 | `"gps"` |
| PPS 없음 | chrony + 양질의 NTP (`kr.pool.ntp.org` 등) | `"sys"` |

PPS는 커널이 별도 디바이스(`/dev/pps0`)로 노출하므로,
패킷포워더가 NMEA 시리얼을 계속 쓰면서도 chrony가 PPS만 가져다 쓸 수 있다.

**판단이 애매하면 무리하지 말고 `tsrc="sys"`로 시작할 것.**
[protocol.md](protocol.md) §2에서 설명했듯 **RTT 측정은 시계 정확도와 무관하다.**
시계 품질은 오프셋 추정에만 영향을 주며, 그건 나중에 개선해도 된다.

## 3. 송신기 스크립트

`/opt/lora-probe/probe_client.py` 로 저장한다.

```python
"""프로브 송신기 — docs/latency-probe/protocol.md v1 구현"""
import socket, json, time, sqlite3, threading, queue, os, subprocess, sys

SERVER   = ("100.84.186.95", 1800)
NODE     = "rak7248-gw"
DB_PATH  = "/var/lib/lora-probe/probe_client.db"
INTERVAL = 10.0          # 초
TIMEOUT  = 2.0           # 초
TSRC     = "sys"         # §2 결과에 따라 "gps" 로 변경

logq  = queue.Queue(maxsize=200000)
_path = {"path": "unknown", "relay": ""}

def path_watcher():
    """Tailscale 경로(direct/derp)를 60초마다 갱신. 매 프로브마다 호출하면 부하가 크다."""
    while True:
        try:
            out = subprocess.run(["tailscale", "status", "--json"],
                                 capture_output=True, timeout=10).stdout
            js = json.loads(out)
            for p in (js.get("Peer") or {}).values():
                if p.get("HostName", "").startswith("aeron"):
                    if p.get("CurAddr"):
                        _path.update(path="direct", relay="")
                    else:
                        _path.update(path="derp", relay=p.get("Relay") or "")
                    break
        except Exception:
            pass
        time.sleep(60)

def writer():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS probe(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run INTEGER, seq INTEGER,
        t1 INTEGER, t2 INTEGER, t3 INTEGER, t4 INTEGER,
        rtt_ns INTEGER, offset_ns INTEGER, mono_rtt_ns INTEGER,
        lost INTEGER, tsrc TEXT, path TEXT, relay TEXT)""")
    con.execute("CREATE INDEX IF NOT EXISTS ix_run_seq ON probe(run, seq)")
    con.commit()
    buf = []
    while True:
        buf.append(logq.get())
        if len(buf) >= 10 or logq.empty():
            con.executemany(
                "INSERT INTO probe(run,seq,t1,t2,t3,t4,rtt_ns,offset_ns,"
                "mono_rtt_ns,lost,tsrc,path,relay)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", buf)
            con.commit()
            buf.clear()

def main():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    threading.Thread(target=writer, daemon=True).start()
    threading.Thread(target=path_watcher, daemon=True).start()

    run = int(time.time())
    seq = 0
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(TIMEOUT)
    s.connect(SERVER)
    print(f"[probe] run={run} -> {SERVER} every {INTERVAL}s", flush=True)

    while True:
        cycle_start = time.perf_counter()
        seq += 1
        p, r = _path["path"], _path["relay"]
        req = {"v":1, "typ":"req", "run":run, "seq":seq, "src":NODE,
               "t1":0, "tsrc":TSRC, "path":p}
        body = json.dumps(req, separators=(",", ":"))
        mono1 = time.perf_counter_ns()
        t1 = time.time_ns()                                   # ← 송신 직전
        body = body.replace('"t1":0', f'"t1":{t1}', 1)

        row = None
        try:
            s.send(body.encode())
            deadline = time.perf_counter() + TIMEOUT
            while True:                                        # 오래된 응답은 버린다
                data = s.recv(2048)
                t4 = time.time_ns()                            # ← 수신 직후
                mono4 = time.perf_counter_ns()
                m = json.loads(data)
                if m.get("typ") == "rsp" and m.get("seq") == seq and m.get("run") == run:
                    t2, t3 = m["t2"], m["t3"]
                    rtt = (t4 - t1) - (t3 - t2)
                    off = ((t2 - t1) + (t3 - t4)) // 2
                    row = (run, seq, t1, t2, t3, t4, rtt, off,
                           mono4 - mono1, 0, TSRC, p, r)
                    break
                if time.perf_counter() > deadline:
                    raise socket.timeout()
        except (socket.timeout, OSError, ValueError, KeyError):
            row = (run, seq, t1, None, None, None, None, None, None, 1, TSRC, p, r)

        try:
            logq.put_nowait(row)
        except queue.Full:
            pass

        slept = time.perf_counter() - cycle_start
        time.sleep(max(0.0, INTERVAL - slept))                 # 주기 드리프트 방지

if __name__ == "__main__":
    sys.exit(main())
```

구현상 주의:
- `t1`은 JSON 직렬화 **후** 치환한다. 직렬화 비용이 RTT에 섞이지 않게 하기 위함
- 응답 `seq`/`run`이 일치하지 않으면 버린다. 타임아웃 후 늦게 도착한 응답이
  다음 측정에 섞이는 것을 막는다
- `mono_rtt_ns`는 단조시계 기준 왕복시간이다. 벽시계 `t4-t1`과 크게 어긋나면
  그 구간에 시계 스텝이 있었다는 뜻이다 (protocol.md §6)

## 4. 서비스 등록

```bash
sudo tee /etc/systemd/system/lora-probe.service >/dev/null <<'EOF'
[Unit]
Description=LoRa gateway latency probe client
After=network-online.target tailscaled.service
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/python3 /opt/lora-probe/probe_client.py
Restart=always
RestartSec=10
Nice=-5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now lora-probe
sudo systemctl status lora-probe --no-pager
```

`Nice=-5`는 프로브가 다른 프로세스에 밀려 스케줄링 지연이 측정값에 섞이는 것을 줄인다.
단, 패킷포워더보다 높은 우선순위를 주지는 말 것.

## 5. 검증

```bash
# 로그
sudo journalctl -u lora-probe -n 30 --no-pager

# 최근 측정값 (ms 단위로 환산)
sudo python3 - <<'EOF'
import sqlite3
c = sqlite3.connect("/var/lib/lora-probe/probe_client.db")
rows = list(c.execute(
  "SELECT seq, rtt_ns/1e6, offset_ns/1e6, mono_rtt_ns/1e6, lost, path"
  " FROM probe ORDER BY id DESC LIMIT 10"))
print(f"{'seq':>6} {'rtt_ms':>9} {'offset_ms':>12} {'mono_ms':>9} {'lost':>4}  path")
for r in rows:
    f = lambda v: f"{v:9.3f}" if v is not None else "        -"
    print(f"{r[0]:>6} {f(r[1])} {f(r[2]):>12} {f(r[3])} {r[4]:>4}  {r[5]}")
print("손실률:", list(c.execute("SELECT AVG(lost)*100 FROM probe"))[0][0], "%")
EOF
```

### 기대값 (2026-09-22 기준)

- `path` = `direct`, `rtt_ms` ≈ **6ms** — 현재 게이트웨이와 서버가 같은 LAN(`203.247.41.x`)에 있음
- `offset_ms` — 서버 시계가 NTP 교정 전이면 **수백~수천 ms** 나올 수 있다. 이건 **고장이 아니라
  측정 결과물**이다 (protocol.md §2)
- `lost` — 정상이면 0에 가까움

## 6. 마지막에 보고할 것

1. §2 GPS 조사 결과 (GPS 유무, `/dev/pps0` 유무, 패킷포워더의 GPS 설정, 최종 `TSRC` 값)
2. §5 측정 샘플 10행과 손실률
3. `ttn-gateway` 서비스가 **건드려지지 않고 그대로 동작 중**인지 확인 결과
   (`systemctl status ttn-gateway`, 업링크가 계속 올라가는지)
