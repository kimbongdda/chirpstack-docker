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

# ===== 제어 명령 채널 (프로토콜 v2) =====
#
# 게이트웨이 기록 초기화를 위해 응답에 명령을 실어 보낸다. 이미 5초마다 오가는
# 채널을 그대로 쓰므로 새 포트·서비스·SSH 가 필요 없다.
#
# 1회 실행 보장: 명령에 cmd_id 를 붙이고, 게이트웨이가 실행 후 ack_id 로 되돌려
# 보낸다. ack 를 받으면 서버가 명령을 내린다. ack 패킷이 유실되면(손실 3%)
# 같은 cmd_id 가 다시 가지만, 게이트웨이가 이미 실행한 id 를 기억하므로
# 중복 실행되지 않는다.

ackq = queue.Queue(maxsize=10000)
_cmd_lock = threading.Lock()
_pending = {}                # src -> (cmd, cmd_id)


def cmd_poller():
    """pending_cmd 를 주기적으로 읽어 메모리에 캐시한다.

    응답 핫패스(t2~t3)에서 DB 를 읽으면 t3-t2 가 부풀어 RTT 측정이 오염된다.
    따라서 DB 접근은 반드시 이 백그라운드 스레드에서만 한다.
    """
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.execute("PRAGMA busy_timeout=30000")
    con.execute("""CREATE TABLE IF NOT EXISTS pending_cmd(
        src TEXT PRIMARY KEY,
        cmd TEXT,
        cmd_id TEXT,
        created_ns INTEGER,
        acked_ns INTEGER)""")
    con.commit()
    while True:
        try:
            rows = con.execute(
                "SELECT src, cmd, cmd_id FROM pending_cmd"
                " WHERE acked_ns IS NULL").fetchall()
            with _cmd_lock:
                _pending.clear()
                for src, cmd, cid in rows:
                    _pending[src] = (cmd, cid)
        except sqlite3.Error:
            pass
        time.sleep(2)


def ack_worker():
    """게이트웨이가 돌려준 ack 를 기록해 명령을 내린다 (핫패스 밖)."""
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.execute("PRAGMA busy_timeout=30000")
    while True:
        src, ack_id = ackq.get()
        try:
            con.execute(
                "UPDATE pending_cmd SET acked_ns=?"
                " WHERE src=? AND cmd_id=? AND acked_ns IS NULL",
                (time.time_ns(), src, ack_id))
            con.commit()
            print(f"[cmd] {src} acked {ack_id}", flush=True)
        except sqlite3.Error as e:
            print(f"[cmd] ack 기록 실패: {e}", flush=True)


def writer():
    """디스크 I/O를 수신 경로에서 분리한다 (t3-t2 오염 방지)

    대시보드가 기록 초기화(DELETE + VACUUM)를 수행하면 순간적으로 쓰기 잠금이
    걸린다. timeout 과 재시도가 없으면 이 스레드가 예외로 죽어 수집이 조용히
    멈추므로, 잠금은 기다리고 실패해도 버퍼를 보존한 채 재시도한다.
    """
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.execute("PRAGMA busy_timeout=30000")
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
            try:
                con.executemany(
                    "INSERT INTO probe_rx(run,seq,src,peer,t1,t2,t3,tsrc,path)"
                    " VALUES(?,?,?,?,?,?,?,?,?)", buf)
                con.commit()
                buf.clear()
            except sqlite3.Error as e:
                # 버퍼를 비우지 않고 다음 주기에 다시 시도한다.
                # 초기화 직후 테이블이 재생성되는 짧은 구간도 여기서 흡수된다.
                print(f"[probe] write retry: {e}", flush=True)
                try:
                    con.rollback()
                except sqlite3.Error:
                    pass
                time.sleep(1)
                if len(buf) > 50000:          # 무한 적체 방지
                    buf = buf[-20000:]


def main():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    threading.Thread(target=writer, daemon=True).start()
    threading.Thread(target=cmd_poller, daemon=True).start()
    threading.Thread(target=ack_worker, daemon=True).start()

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
            # v1(명령 미지원) 클라이언트도 계속 받는다. v1 은 cmd 필드를 무시한다.
            if m.get("v") not in (1, 2) or m.get("typ") != "req":
                continue
            src = m.get("src")
            resp = {"v": 1, "typ": "rsp", "run": m.get("run"), "seq": m.get("seq"),
                    "t1": m.get("t1"), "t2": t2, "t3": 0, "srv": SRV_NAME}

            # 캐시된 값만 읽는다 (DB 접근 금지 구간)
            with _cmd_lock:
                pend = _pending.get(src)
            if pend:
                resp["cmd"], resp["cmd_id"] = pend
                # 이미 실행했다고 되돌려 보낸 경우 -> 명령 해제를 큐에 넘긴다
                if m.get("ack_id") == pend[1]:
                    try:
                        ackq.put_nowait((src, pend[1]))
                    except queue.Full:
                        pass

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
