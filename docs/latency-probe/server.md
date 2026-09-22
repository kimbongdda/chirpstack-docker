# 서버 측 구축 지시문 — 프로브 응답기

대상 장비: `WIN-OB5FI29VAFC` / Windows Server 2025 / Tailscale 노드 `aeron-sever` (`100.84.186.95`)

먼저 [protocol.md](protocol.md)를 읽을 것. 아래 구현은 그 규격을 따른다.

## 0. 설계 결정 — 왜 Docker가 아니라 네이티브인가

이 서버의 LoRa 경로는 Docker Desktop의 포트 프록시를 거친다
(Windows → WSL2 VM → 컨테이너). 프록시는 그 자체로 지연과 지터를 추가하므로,
컨테이너 안에서 프로브에 응답하면 **네트워크 경로가 아니라 Docker 프록시를 측정하게 된다.**

따라서 응답기는 **Windows에서 네이티브 Python으로 직접 실행**한다.
확인된 환경: Python 3.14.5, `time.time_ns()`는 `GetSystemTimePreciseAsFileTime()` 기반
해상도 100ns — 측정에 충분하다.

> 참고: 실제 LoRa 패킷은 여기에 더해 Docker 프록시 구간을 한 번 더 지난다.
> 즉 이 프로브가 재는 값은 **네트워크 경로의 하한**이며, LoRa 종단 지연은 이보다 크다.
> 둘을 구분해서 해석할 것.

## 1. 디렉터리 준비

```powershell
New-Item -ItemType Directory -Force C:\Users\Administrator\chirpstack-docker\probe\data
```

## 2. 응답기 스크립트

`C:\Users\Administrator\chirpstack-docker\probe\probe_server.py` 로 저장한다.

```python
"""프로브 응답기 — docs/latency-probe/protocol.md v1 구현"""
import socket, json, time, sqlite3, threading, queue, os, sys

BIND_ADDR = "0.0.0.0"
BIND_PORT = 1800
SRV_NAME  = "aeron-sever"
DB_PATH   = r"C:\Users\Administrator\chirpstack-docker\probe\data\probe_server.db"
ALLOW_NET = "100."           # tailnet(100.64.0.0/10)에서 온 것만 응답

logq = queue.Queue(maxsize=200000)

def writer():
    """디스크 I/O를 수신 경로에서 분리한다 (t3-t2 오염 방지)"""
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS probe_rx(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run INTEGER, seq INTEGER, src TEXT, peer TEXT,
        t1 INTEGER, t2 INTEGER, t3 INTEGER,
        tsrc TEXT, path TEXT)""")
    con.execute("CREATE INDEX IF NOT EXISTS ix_run_seq ON probe_rx(run, seq)")
    con.commit()
    buf = []
    while True:
        buf.append(logq.get())
        if len(buf) >= 20 or logq.empty():
            con.executemany(
                "INSERT INTO probe_rx(run,seq,src,peer,t1,t2,t3,tsrc,path)"
                " VALUES(?,?,?,?,?,?,?,?,?)", buf)
            con.commit()
            buf.clear()

def main():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    threading.Thread(target=writer, daemon=True).start()

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
    s.bind((BIND_ADDR, BIND_PORT))
    print(f"[probe] listening {BIND_ADDR}:{BIND_PORT} as {SRV_NAME}", flush=True)

    while True:
        try:
            data, addr = s.recvfrom(2048)
            t2 = time.time_ns()                      # ← 수신 직후
        except OSError:
            continue
        if not addr[0].startswith(ALLOW_NET):
            continue
        try:
            m = json.loads(data)
            if m.get("v") != 1 or m.get("typ") != "req":
                continue
            resp = {"v":1, "typ":"rsp", "run":m.get("run"), "seq":m.get("seq"),
                    "t1":m.get("t1"), "t2":t2, "t3":0, "srv":SRV_NAME}
            body = json.dumps(resp, separators=(",", ":"))
            t3 = time.time_ns()                      # ← 송신 직전
            body = body.replace('"t3":0', f'"t3":{t3}', 1)
            s.sendto(body.encode(), addr)
            try:
                logq.put_nowait((m.get("run"), m.get("seq"), m.get("src"), addr[0],
                                 m.get("t1"), t2, t3, m.get("tsrc"), m.get("path")))
            except queue.Full:
                pass
        except Exception:
            continue                                  # 어떤 입력에도 죽지 않는다

if __name__ == "__main__":
    sys.exit(main())
```

구현상 주의: `t3`는 JSON 직렬화 **후** 문자열 치환으로 넣는다.
직렬화 비용(수 µs)이 `t3`와 실제 송신 사이에 끼지 않게 하기 위함이다.

## 3. 방화벽 규칙

현재 Public/Private 프로파일이 꺼져 있어 당장은 규칙 없이도 통과하지만,
방화벽을 켤 것을 대비해 미리 등록해둔다. 기존 `LoRaWAN Semtech UDP 1700` 규칙과 같은
`Profile=Any` 방식을 따른다.

```powershell
New-NetFirewallRule -DisplayName "LoRa Latency Probe UDP 1800" `
  -Direction Inbound -Protocol UDP -LocalPort 1800 -Action Allow -Profile Any
```

## 4. 상시 실행 등록

측정은 수일~수주 이어지므로 재부팅 후에도 살아나야 한다. 작업 스케줄러에 등록한다.

```powershell
$py = (Get-Command python).Source
$act = New-ScheduledTaskAction -Execute $py `
  -Argument "C:\Users\Administrator\chirpstack-docker\probe\probe_server.py"
$trg = New-ScheduledTaskTrigger -AtStartup
$set = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) `
  -ExecutionTimeLimit ([TimeSpan]::Zero)
Register-ScheduledTask -TaskName "LoRaProbeResponder" -Action $act -Trigger $trg `
  -Settings $set -User "SYSTEM" -RunLevel Highest
Start-ScheduledTask -TaskName "LoRaProbeResponder"
```

## 5. 검증

### 5.1 리스닝 확인

```powershell
netstat -an | Select-String ":1800"
```
`UDP 0.0.0.0:1800` 이 보여야 한다.

### 5.2 로컬 왕복 테스트

```powershell
$u = New-Object System.Net.Sockets.UdpClient
$u.Client.ReceiveTimeout = 3000
$u.Connect("100.84.186.95", 1800)
$t1 = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() * 1000000
$req = '{"v":1,"typ":"req","run":1,"seq":1,"src":"selftest","t1":' + $t1 + ',"tsrc":"sys","path":"direct"}'
$b = [Text.Encoding]::UTF8.GetBytes($req)
[void]$u.Send($b, $b.Length)
$ep = New-Object System.Net.IPEndPoint([Net.IPAddress]::Any, 0)
[Text.Encoding]::UTF8.GetString($u.Receive([ref]$ep))
$u.Close()
```
`t2`, `t3`가 채워진 `"typ":"rsp"` JSON이 돌아오면 정상이다.

### 5.3 기록 확인

```powershell
python -c "import sqlite3;c=sqlite3.connect(r'C:\Users\Administrator\chirpstack-docker\probe\data\probe_server.db');print(list(c.execute('SELECT COUNT(*),MAX(seq) FROM probe_rx')))"
```

## 6. 운영 중 점검 항목

- DB 증가량: 10초 주기면 하루 약 8,640행. 장기 측정 시 용량과 `VACUUM` 고려
- `probe_rx`에 `seq` 구멍이 있으면 **상행(게이트웨이→서버) 유실**이다 (protocol.md §5.3)
- 서버 시계를 나중에 교정하면 그 시점 전후로 `offset_ns` 계열이 불연속이 된다.
  교정 작업 시각을 반드시 기록해둘 것

## 7. 선택 작업 — 서버 시계 교정

측정 자체는 시계에 의존하지 않지만(protocol.md §2), 로그 상관분석에는 영향을 준다.
현재 `time.windows.com` / 폴링 1024초 / 오차 한계 약 1.8초다.

```powershell
w32tm /config /manualpeerlist:"kr.pool.ntp.org,0x8 time.google.com,0x8" /syncfromflags:manual /update
Restart-Service w32time
w32tm /resync
w32tm /query /status
```

교정 후 루트 분산이 얼마나 줄었는지 확인하고, **작업 시각을 측정 로그에 주석으로 남긴다.**
