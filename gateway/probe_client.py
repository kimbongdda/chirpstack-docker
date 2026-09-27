"""프로브 송신기 — docs/latency-probe/protocol.md v1 구현 (게이트웨이 측)

설치: /opt/lora-probe/probe_client.py
서비스: lora-probe.service

rak.md 의 초기 버전에서 두 가지를 고쳤다.
  - 서버 노드 식별을 이름이 아니라 Tailscale IP 로 한다 (findings.md §8)
  - 첫 프로브가 path="unknown" 으로 기록되지 않도록 선행 조회한다

그리고 스타링크처럼 자주 끊기는 회선을 위해 소켓 자가복구를 넣었다 (findings.md §3).
"""
import json
import os
import queue
import socket
import sqlite3
import subprocess
import sys
import threading
import time

SERVER   = ("100.84.186.95", 1800)
NODE     = "rak7248-gw"
DB_PATH  = "/var/lib/lora-probe/probe_client.db"
INTERVAL = float(os.getenv("PROBE_INTERVAL", "10"))
TIMEOUT  = float(os.getenv("PROBE_TIMEOUT", "2"))
TSRC     = os.getenv("PROBE_TSRC", "sys")     # PPS 없음 -> sys (rak.md §2.1)

# 연속으로 이만큼 실패하면 소켓을 다시 만든다.
# 회선이 바뀌면 connect() 된 소켓이 옛 인터페이스에 박혀 조용히 죽는데,
# 프로세스는 살아 있어서 systemd 가 못 살린다. 스스로 복구해야 한다.
RECREATE_AFTER_LOSSES = int(os.getenv("PROBE_RECREATE_AFTER", "3"))

logq  = queue.Queue(maxsize=200000)
_path = {"path": "unknown", "relay": ""}


def resolve_path():
    """서버 노드의 Tailscale 경로를 1회 조회한다. 실패하면 None."""
    try:
        out = subprocess.run(["tailscale", "status", "--json"],
                             capture_output=True, timeout=10).stdout
        js = json.loads(out)
        for p in (js.get("Peer") or {}).values():
            # 노드 식별은 반드시 Tailscale IP 로 한다.
            # HostName 은 OS 호스트명, DNSName 은 콘솔에서 바꾼 이름이라 서로 다르다.
            # 이름으로 매칭하면 노드 이름을 바꾸는 순간 조용히 깨진다.
            if SERVER[0] in (p.get("TailscaleIPs") or []):
                # CurAddr 가 채워져 있으면 direct. Relay 는 direct 일 때도
                # 폴백 후보가 들어 있으므로 그것만 보고 판정하면 안 된다.
                if p.get("CurAddr"):
                    return ("direct", "")
                return ("derp", p.get("Relay") or "")
    except Exception:
        pass
    return None


def path_watcher():
    """매 프로브마다 tailscale status 를 부르면 부하가 크므로 60초마다 갱신."""
    while True:
        r = resolve_path()
        if r:
            _path.update(path=r[0], relay=r[1])
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


def new_socket():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(TIMEOUT)
    s.connect(SERVER)
    return s


def main():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    threading.Thread(target=writer, daemon=True).start()

    # 첫 프로브가 unknown 으로 기록되지 않도록 먼저 한 번 조회한다
    r0 = resolve_path()
    if r0:
        _path.update(path=r0[0], relay=r0[1])
    threading.Thread(target=path_watcher, daemon=True).start()

    run = int(time.time())
    seq = 0
    consecutive_losses = 0
    s = new_socket()
    print(f"[probe] run={run} -> {SERVER} every {INTERVAL}s", flush=True)

    while True:
        cycle_start = time.perf_counter()
        seq += 1
        p, r = _path["path"], _path["relay"]
        req = {"v": 1, "typ": "req", "run": run, "seq": seq, "src": NODE,
               "t1": 0, "tsrc": TSRC, "path": p}
        body = json.dumps(req, separators=(",", ":"))
        mono1 = time.perf_counter_ns()
        t1 = time.time_ns()                                   # <- 송신 직전
        body = body.replace('"t1":0', f'"t1":{t1}', 1)

        row = None
        try:
            s.send(body.encode())
            deadline = time.perf_counter() + TIMEOUT
            while True:                                        # 오래된 응답은 버린다
                data = s.recv(2048)
                t4 = time.time_ns()                            # <- 수신 직후
                mono4 = time.perf_counter_ns()
                m = json.loads(data)
                if m.get("typ") == "rsp" and m.get("seq") == seq and m.get("run") == run:
                    t2, t3 = m["t2"], m["t3"]
                    rtt = (t4 - t1) - (t3 - t2)
                    off = ((t2 - t1) + (t3 - t4)) // 2
                    row = (run, seq, t1, t2, t3, t4, rtt, off,
                           mono4 - mono1, 0, TSRC, p, r)
                    consecutive_losses = 0
                    break
                if time.perf_counter() > deadline:
                    raise socket.timeout()
        except (socket.timeout, OSError, ValueError, KeyError):
            row = (run, seq, t1, None, None, None, None, None, None, 1, TSRC, p, r)
            consecutive_losses += 1

        try:
            logq.put_nowait(row)
        except queue.Full:
            pass

        # 소켓 자가복구. 회선이 바뀌면 옛 인터페이스에 박힌 소켓은 영원히 실패하므로,
        # 연속 실패가 이어지면 경로를 다시 잡아 새로 연결한다.
        if consecutive_losses >= RECREATE_AFTER_LOSSES:
            print(f"[probe] {consecutive_losses}회 연속 실패 — 소켓 재생성", flush=True)
            try:
                s.close()
            except OSError:
                pass
            try:
                s = new_socket()
            except OSError as e:
                print(f"[probe] 소켓 재생성 실패: {e}", flush=True)
            consecutive_losses = 0
            r2 = resolve_path()                                # 경로 상태도 갱신
            if r2:
                _path.update(path=r2[0], relay=r2[1])

        slept = time.perf_counter() - cycle_start
        time.sleep(max(0.0, INTERVAL - slept))                 # 주기 드리프트 방지


if __name__ == "__main__":
    sys.exit(main())
