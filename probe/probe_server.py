"""프로브 응답기 — docs/latency-probe/protocol.md v1 구현

게이트웨이(rak7248-gw)가 보내는 지연 측정 프로브에 t2/t3 타임스탬프를 붙여 응답하고,
수신 기록을 SQLite에 남긴다. Docker 프록시 지연이 측정에 섞이지 않도록
컨테이너가 아닌 Windows 네이티브로 실행한다.
"""
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
            t2 = time.time_ns()                      # <- 수신 직후
        except OSError:
            continue
        if not addr[0].startswith(ALLOW_NET):
            continue
        try:
            m = json.loads(data)
            if m.get("v") != 1 or m.get("typ") != "req":
                continue
            resp = {"v": 1, "typ": "rsp", "run": m.get("run"), "seq": m.get("seq"),
                    "t1": m.get("t1"), "t2": t2, "t3": 0, "srv": SRV_NAME}
            body = json.dumps(resp, separators=(",", ":"))
            t3 = time.time_ns()                      # <- 송신 직전
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
