"""프로브 송신기 — docs/latency-probe/protocol.md v2 구현 (게이트웨이 측)

설치: /opt/lora-probe/probe_client.py
서비스: lora-probe.service

v2 에서 추가된 것: 서버가 응답에 실어 보내는 제어 명령(기록 초기화) 처리.
대시보드에서 버튼을 누르면 서버가 명령을 올려두고, 다음 응답에 cmd/cmd_id 가
실려 온다. 새 포트·서비스·SSH 가 필요 없다.

1회 실행 보장이 핵심이다. ack 패킷은 유실될 수 있으므로(이 회선 손실 3%)
같은 cmd_id 가 여러 번 도착한다. 실행한 cmd_id 를 파일에 남겨 프로세스가
재시작해도 중복 실행되지 않게 한다.
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
CMD_STATE = "/var/lib/lora-probe/last_cmd_id"     # 실행한 cmd_id 영속화
INTERVAL = float(os.getenv("PROBE_INTERVAL", "5"))
TIMEOUT  = float(os.getenv("PROBE_TIMEOUT", "2"))
TSRC     = os.getenv("PROBE_TSRC", "sys")         # PPS 없음 -> sys

# 연속으로 이만큼 실패하면 소켓을 다시 만든다 (회선 전환 시 소켓 고정 대응)
RECREATE_AFTER_LOSSES = int(os.getenv("PROBE_RECREATE_AFTER", "3"))

logq  = queue.Queue(maxsize=200000)
_path = {"path": "unknown", "relay": ""}
_reset_req = threading.Event()      # 메인 루프 -> writer 로 초기화 요청
_reset_done = threading.Event()     # writer -> 메인 루프 완료 통지


# ===================== 제어 명령 =====================

def load_last_cmd():
    try:
        with open(CMD_STATE) as f:
            return f.read().strip()
    except OSError:
        return ""


def save_last_cmd(cmd_id):
    try:
        os.makedirs(os.path.dirname(CMD_STATE), exist_ok=True)
        with open(CMD_STATE, "w") as f:
            f.write(cmd_id)
    except OSError as e:
        print(f"[probe] cmd_id 저장 실패: {e}", flush=True)


# ===================== 경로 판정 =====================

def resolve_path():
    """서버 노드의 Tailscale 경로를 1회 조회한다. 실패하면 None."""
    try:
        out = subprocess.run(["tailscale", "status", "--json"],
                             capture_output=True, timeout=10).stdout
        js = json.loads(out)
        for p in (js.get("Peer") or {}).values():
            # 노드 식별은 반드시 Tailscale IP 로. HostName 은 OS 호스트명,
            # DNSName 은 콘솔에서 바꾼 이름이라 서로 다르다.
            if SERVER[0] in (p.get("TailscaleIPs") or []):
                # CurAddr 가 있으면 direct. Relay 는 direct 일 때도 폴백
                # 후보가 들어 있어 그것만 보면 오판한다.
                if p.get("CurAddr"):
                    return ("direct", "")
                return ("derp", p.get("Relay") or "")
    except Exception:
        pass
    return None


def path_watcher():
    while True:
        r = resolve_path()
        if r:
            _path.update(path=r[0], relay=r[1])
        time.sleep(60)


# ===================== 기록 =====================

SCHEMA = """CREATE TABLE IF NOT EXISTS probe(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run INTEGER, seq INTEGER,
    t1 INTEGER, t2 INTEGER, t3 INTEGER, t4 INTEGER,
    rtt_ns INTEGER, offset_ns INTEGER, mono_rtt_ns INTEGER,
    lost INTEGER, tsrc TEXT, path TEXT, relay TEXT)"""


def writer():
    """기록 스레드. 초기화도 여기서 수행한다.

    DB 핸들을 가진 스레드가 직접 비워야 한다. 파일을 지우면 이 스레드의
    핸들이 끊긴 채 프로세스는 살아 있어 기록이 조용히 멈춘다.
    """
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.execute("PRAGMA busy_timeout=30000")
    con.execute(SCHEMA)
    con.execute("CREATE INDEX IF NOT EXISTS ix_run_seq ON probe(run, seq)")
    con.commit()
    buf = []
    while True:
        # 초기화 요청이 있으면 먼저 처리한다
        if _reset_req.is_set():
            buf.clear()
            while not logq.empty():          # 이전 run 잔여 행 버림
                try:
                    logq.get_nowait()
                except queue.Empty:
                    break
            try:
                n = con.execute("SELECT COUNT(*) FROM probe").fetchone()[0]
                con.execute("DELETE FROM probe")
                con.execute("DELETE FROM sqlite_sequence WHERE name='probe'")
                con.commit()
                con.execute("VACUUM")
                print(f"[probe] 기록 초기화 완료: {n}행 삭제", flush=True)
            except sqlite3.Error as e:
                print(f"[probe] 초기화 실패: {e}", flush=True)
            _reset_req.clear()
            _reset_done.set()

        try:
            buf.append(logq.get(timeout=1))
        except queue.Empty:
            continue

        if len(buf) >= 10 or logq.empty():
            try:
                con.executemany(
                    "INSERT INTO probe(run,seq,t1,t2,t3,t4,rtt_ns,offset_ns,"
                    "mono_rtt_ns,lost,tsrc,path,relay)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", buf)
                con.commit()
                buf.clear()
            except sqlite3.Error as e:
                print(f"[probe] write retry: {e}", flush=True)
                try:
                    con.rollback()
                except sqlite3.Error:
                    pass
                time.sleep(1)
                if len(buf) > 50000:
                    buf = buf[-20000:]


def new_socket():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(TIMEOUT)
    s.connect(SERVER)
    return s


def main():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    threading.Thread(target=writer, daemon=True).start()

    r0 = resolve_path()                      # 첫 프로브가 unknown 이 되지 않게
    if r0:
        _path.update(path=r0[0], relay=r0[1])
    threading.Thread(target=path_watcher, daemon=True).start()

    run = int(time.time())
    seq = 0
    last_cmd_id = load_last_cmd()
    consecutive_losses = 0
    s = new_socket()
    print(f"[probe] run={run} -> {SERVER} every {INTERVAL}s "
          f"(last_cmd_id={last_cmd_id or 'none'})", flush=True)

    while True:
        cycle_start = time.perf_counter()
        seq += 1
        p, r = _path["path"], _path["relay"]
        req = {"v": 2, "typ": "req", "run": run, "seq": seq, "src": NODE,
               "t1": 0, "tsrc": TSRC, "path": p}
        if last_cmd_id:
            req["ack_id"] = last_cmd_id      # 서버가 명령을 내릴 수 있게 계속 echo
        body = json.dumps(req, separators=(",", ":"))
        mono1 = time.perf_counter_ns()
        t1 = time.time_ns()                                   # <- 송신 직전
        body = body.replace('"t1":0', f'"t1":{t1}', 1)

        row = None
        cmd = None
        try:
            s.send(body.encode())
            deadline = time.perf_counter() + TIMEOUT
            while True:                                        # 오래된 응답 버림
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
                    cmd = (m.get("cmd"), m.get("cmd_id"))
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

        # ---- 제어 명령 처리 ----
        # 같은 cmd_id 가 반복 도착하므로(ack 유실 대비 서버가 계속 보냄)
        # 실행한 id 와 다를 때만 실행한다.
        if cmd and cmd[0] == "reset" and cmd[1] and cmd[1] != last_cmd_id:
            print(f"[probe] 초기화 명령 수신: {cmd[1]}", flush=True)
            last_cmd_id = cmd[1]
            save_last_cmd(last_cmd_id)       # 재시작해도 중복 실행 안 되게 먼저 저장
            _reset_done.clear()
            _reset_req.set()
            if _reset_done.wait(timeout=60):
                run = int(time.time())       # 새 run 으로 구간을 구분
                seq = 0
                print(f"[probe] 새 run={run} 시작", flush=True)
            else:
                print("[probe] 초기화 대기 시간 초과", flush=True)

        # ---- 소켓 자가복구 ----
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
            r2 = resolve_path()
            if r2:
                _path.update(path=r2[0], relay=r2[1])

        slept = time.perf_counter() - cycle_start
        time.sleep(max(0.0, INTERVAL - slept))


if __name__ == "__main__":
    sys.exit(main())
