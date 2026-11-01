"""LoRa 종단 지연 기록기 — docs/latency-probe/protocol.md 의 시계 기준을 공유한다.

게이트웨이 레벨 업링크(protobuf UplinkFrame)를 구독해서, 무선으로 패킷이 잡힌
GPS 시각(gw_time)과 서버가 그것을 받은 시각의 차이를 기록한다.

설계상 중요한 점 두 가지:

1. 반드시 '호스트' 시계로 찍는다.
   probe_server.py 가 재는 시계 오프셋이 호스트 기준이므로, 같은 시계를 써야
   그 보정값을 그대로 적용할 수 있다. 컨테이너 안에서 찍으면 컨테이너 시계가
   호스트와 얼마나 어긋났는지를 따로 알아내야 하는 문제가 생긴다.

2. gw_time 은 GPS 유래 절대시각이다.
   그래서 이 측정은 RTT/2 같은 '경로 대칭' 가정이 필요 없다. 스타링크는 상행이
   하행보다 약 70ms 느린 비대칭 경로라 RTT/2 는 편도를 크게 과소평가한다.
   여기서 나오는 값이 진짜 상행 편도 지연에 해당한다.

기록되는 raw_ms 에는 아직 두 시계의 오프셋이 남아 있다. 분석 시 프로브가 측정한
offset 으로 빼주어야 최종값이 된다 (probe_rx 와 시각으로 조인).
"""
import json
import os
import queue
import sqlite3
import sys
import threading
import time

import paho.mqtt.client as mqtt
from chirpstack_api import gw

MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
# 지역 prefix 를 와일드카드로 둔다.
#
# 토픽은 '<region>/gateway/<eui>/event/up' 형태다. kr920 을 박아두면 베트남
# (as923_2) 이전 시 조용히 아무것도 안 잡힌다 — 에러도 안 나서 알아채기 어렵다.
# prefix 를 '+' 로 받으면 지역이 바뀌어도 설정 변경 없이 그대로 동작하고,
# 어느 지역에서 온 프레임인지는 region 컬럼에 남긴다.
TOPIC     = os.getenv("GW_TOPIC", "+/gateway/+/event/up")
# 라이브 DB 는 9p 마운트(probe/data) 밖에 둔다. 9p 위에서 SQLite 를 호스트가 쓰고
# 컨테이너가 읽으면 잠금·캐싱 문제로 "malformed" 가 난다 (probe_server.py 주석 참고).
# 컨테이너가 읽을 스냅샷만 probe/data 로 원자적으로 내보낸다.
DB_PATH   = os.getenv("E2E_DB_PATH",
                      r"C:\Users\Administrator\chirpstack-docker\probe\live\e2e_latency.db")
SNAP_PATH = r"C:\Users\Administrator\chirpstack-docker\probe\data\e2e_latency.db"
CMD_DIR   = r"C:\Users\Administrator\chirpstack-docker\probe\data\cmd"
SNAP_SEC  = int(os.getenv("E2E_SNAP_SEC", "20"))

logq = queue.Queue(maxsize=200000)


def writer():
    """디스크 I/O 를 수신 경로에서 분리한다 (수신 타임스탬프 오염 방지).

    대시보드의 기록 초기화(DELETE + VACUUM)가 걸리면 쓰기 잠금이 생긴다.
    timeout 과 재시도가 없으면 이 스레드가 죽어 수집이 조용히 멈춘다.
    """
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.execute("PRAGMA busy_timeout=30000")
    con.execute("""CREATE TABLE IF NOT EXISTS e2e(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        recv_ns INTEGER,        -- 서버(호스트) 수신 시각
        gw_time_ns INTEGER,     -- 게이트웨이 무선 수신 시각 (GPS 유래)
        raw_ms REAL,            -- recv - gw_time. 시계 오프셋 미보정
        gateway_id TEXT,
        freq_hz INTEGER,
        sf INTEGER,
        bw_hz INTEGER,
        rssi INTEGER,
        snr REAL,
        crc_ok INTEGER,
        size INTEGER,
        -- tmst: 콘센트레이터 카운터. 배칭 여부 분석용
        -- toa_ms: 계산한 time-on-air. gw_time 이 수신 시작/끝 중
        --         어느 쪽을 가리키는지 판별하는 데 쓴다
        tmst INTEGER,
        toa_ms REAL,
        region TEXT)""")
    # 기존 DB 이행: region 컬럼이 없으면 추가한다
    cols = {r[1] for r in con.execute("PRAGMA table_info(e2e)")}
    if "region" not in cols:
        con.execute("ALTER TABLE e2e ADD COLUMN region TEXT")
    con.execute("CREATE INDEX IF NOT EXISTS ix_recv ON e2e(recv_ns)")
    con.commit()
    buf = []
    while True:
        buf.append(logq.get())
        if len(buf) >= 20 or logq.empty():
            try:
                con.executemany(
                    "INSERT INTO e2e(recv_ns,gw_time_ns,raw_ms,gateway_id,freq_hz,"
                    "sf,bw_hz,rssi,snr,crc_ok,size,tmst,toa_ms,region)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", buf)
                con.commit()
                buf.clear()
            except sqlite3.Error as e:
                # 버퍼를 비우지 않고 다음 주기에 재시도한다
                print(f"[e2e] write retry: {e}", flush=True)
                try:
                    con.rollback()
                except sqlite3.Error:
                    pass
                time.sleep(1)
                if len(buf) > 50000:          # 무한 적체 방지
                    buf = buf[-20000:]


def time_on_air_ms(sf, bw_hz, payload_len, cr=1, preamble=8, crc=True, implicit=False):
    """LoRa 전파 시간(ms). Semtech AN1200.13 의 표준 식.

    gw_time 이 수신 '시작'을 가리키는지 '끝'을 가리키는지 판별하는 데 쓴다.
    raw_ms 가 toa_ms 와 함께 움직이면 시작 기준, 무관하면 끝 기준이다.
    """
    if not sf or not bw_hz:
        return None
    t_sym = (2 ** sf) / bw_hz * 1000.0                    # ms
    # SF11/SF12 @125kHz 는 저속 최적화(DE)가 켜진다
    de = 1 if (t_sym > 16.0) else 0
    num = 8 * payload_len - 4 * sf + 28 + (16 if crc else 0) - (20 if implicit else 0)
    den = 4 * (sf - 2 * de)
    n_payload = 8 + max(0, -(-num // den) * (cr + 4))     # ceil 후 (CR+4) 배
    return (preamble + 4.25) * t_sym + n_payload * t_sym


def on_connect(client, userdata, flags, rc):
    print(f"[e2e] mqtt connected rc={rc}, subscribing {TOPIC}", flush=True)
    client.subscribe(TOPIC)


def on_message(client, userdata, msg):
    recv_ns = time.time_ns()                     # <- 수신 직후, 호스트 시계
    region = msg.topic.split("/", 1)[0] if msg.topic else None
    try:
        up = gw.UplinkFrame()
        up.ParseFromString(msg.payload)
        ri = up.rx_info
        if not ri.HasField("gw_time"):
            return                                # GPS 시각 없는 프레임은 버린다

        ts = ri.gw_time
        gw_ns = ts.seconds * 1_000_000_000 + ts.nanos

        lora = up.tx_info.modulation.lora
        size = len(up.phy_payload)
        # context 는 Semtech tmst(콘센트레이터 카운터) 4바이트 빅엔디언
        tmst = int.from_bytes(ri.context, "big") if ri.context else None

        logq.put_nowait((
            recv_ns,
            gw_ns,
            (recv_ns - gw_ns) / 1e6,
            ri.gateway_id.hex() if isinstance(ri.gateway_id, bytes) else str(ri.gateway_id),
            up.tx_info.frequency,
            lora.spreading_factor,
            lora.bandwidth,
            ri.rssi,
            ri.snr,
            1 if ri.crc_status == gw.CRCStatus.CRC_OK else 0,
            size,
            tmst,
            time_on_air_ms(lora.spreading_factor, lora.bandwidth, size),
            region,
        ))
    except queue.Full:
        pass
    except Exception:
        return                                    # 어떤 프레임에도 죽지 않는다


def snapshot_worker():
    """라이브 DB 의 일관된 사본을 마운트 폴더로 원자적으로 내보낸다."""
    tmp = SNAP_PATH + ".tmp"
    while True:
        time.sleep(SNAP_SEC)
        try:
            os.makedirs(os.path.dirname(SNAP_PATH), exist_ok=True)
            src = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=30)
            dst = sqlite3.connect(tmp)
            src.backup(dst)
            dst.close()
            src.close()
            os.replace(tmp, SNAP_PATH)
        except Exception as e:
            print(f"[snap] e2e 스냅샷 실패: {e}", flush=True)
            try:
                os.remove(tmp)
            except OSError:
                pass


def cmd_file_worker():
    """대시보드가 남긴 e2e 초기화 명령 파일을 처리한다."""
    while True:
        time.sleep(2)
        try:
            if not os.path.isdir(CMD_DIR):
                continue
            for fn in sorted(os.listdir(CMD_DIR)):
                if not (fn.startswith("reset_e2e") and fn.endswith(".json")):
                    continue
                path = os.path.join(CMD_DIR, fn)
                try:
                    con = sqlite3.connect(DB_PATH, timeout=30)
                    con.execute("PRAGMA busy_timeout=30000")
                    n = con.execute("SELECT COUNT(*) FROM e2e").fetchone()[0]
                    con.execute("DELETE FROM e2e")
                    con.execute("DELETE FROM sqlite_sequence WHERE name='e2e'")
                    con.commit()
                    con.execute("VACUUM")
                    con.close()
                    print(f"[cmd] LoRa 종단 기록 {n}행 삭제", flush=True)
                except Exception as e:
                    print(f"[cmd] e2e 초기화 실패: {e}", flush=True)
                os.remove(path)
        except Exception as e:
            print(f"[cmd] 폴링 오류: {e}", flush=True)


def main():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    threading.Thread(target=writer, daemon=True).start()
    threading.Thread(target=snapshot_worker, daemon=True).start()
    threading.Thread(target=cmd_file_worker, daemon=True).start()

    c = mqtt.Client()
    c.on_connect = on_connect
    c.on_message = on_message
    c.connect(MQTT_HOST, MQTT_PORT, 60)
    print(f"[e2e] logging to {DB_PATH}", flush=True)
    c.loop_forever()


if __name__ == "__main__":
    sys.exit(main())
