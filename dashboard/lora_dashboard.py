#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import base64
import os
import sqlite3
import threading
from collections import deque, defaultdict
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))

import grpc
from chirpstack_api import api
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template
from paho.mqtt import client as mqtt_client

# ===================== 설정 (.env 파일에서 읽기) =====================

load_dotenv()

# ===================== SQLite DB =====================

DB_PATH = os.getenv("DB_PATH", "/app/data/lora_history.db")

_db_lock = threading.Lock()

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS uplink_events (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                time          TEXT    NOT NULL,
                dev_eui       TEXT,
                dev_name      TEXT,
                dev_addr      TEXT,
                f_cnt         INTEGER,
                frequency_mhz REAL,
                sf            INTEGER,
                bandwidth     INTEGER,
                code_rate     TEXT,
                dr            TEXT,
                rssi          TEXT,
                snr           TEXT,
                gateways      TEXT,
                payload_hex   TEXT,
                payload_text  TEXT,
                collision_detected  INTEGER DEFAULT 0,
                collision_reasons   TEXT,
                crc_ok_count  INTEGER,
                total_gateways INTEGER,
                topic         TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_time    ON uplink_events(time)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dev_eui ON uplink_events(dev_eui)")
        conn.commit()
    print(f"[DB] 초기화 완료: {DB_PATH}")

def save_event_db(event: dict):
    with _db_lock:
        with get_db() as conn:
            conn.execute("""
                INSERT INTO uplink_events
                    (time, dev_eui, dev_name, dev_addr, f_cnt,
                     frequency_mhz, sf, bandwidth, code_rate, dr,
                     rssi, snr, gateways, payload_hex, payload_text,
                     collision_detected, collision_reasons,
                     crc_ok_count, total_gateways, topic)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                event.get("time"),
                event.get("dev_eui"),
                event.get("dev_name"),
                event.get("dev_addr"),
                event.get("f_cnt"),
                event.get("frequency_mhz"),
                event.get("sf"),
                event.get("bandwidth"),
                event.get("code_rate"),
                event.get("dr"),
                json.dumps(event.get("rssi") or []),
                json.dumps(event.get("snr") or []),
                json.dumps(event.get("gateways") or []),
                event.get("payload_hex"),
                event.get("payload_text"),
                1 if event.get("collision_detected") else 0,
                json.dumps(event.get("collision_reasons") or []),
                event.get("crc_ok_count"),
                event.get("total_gateways"),
                event.get("topic"),
            ))
            conn.commit()

# MQTT 브로커 (Docker: mosquitto 서비스명 / 로컬: IP)
MQTT_BROKER    = os.getenv("MQTT_BROKER", "127.0.0.1")
MQTT_PORT      = int(os.getenv("MQTT_PORT", 1883))
MQTT_USERNAME  = os.getenv("MQTT_USERNAME") or None
MQTT_PASSWORD  = os.getenv("MQTT_PASSWORD") or None

# 구독 토픽 (환경변수로 선택 가능)
# - application/+/device/+/event/up  : 디바이스 uplink 이벤트
# - kr920/gateway/+/event/up          : 게이트웨이 이벤트
SUB_TOPIC  = os.getenv("SUB_TOPIC", "application/+/device/+/event/up")
MAX_EVENTS = int(os.getenv("MAX_EVENTS", 300))

# ChirpStack gRPC (Docker: chirpstack 서비스명 / 로컬: IP)
CHIRPSTACK_HOST    = os.getenv("CHIRPSTACK_HOST", "127.0.0.1")
GRPC_SERVER        = f"{CHIRPSTACK_HOST}:{os.getenv('GRPC_PORT', '8080')}"
API_TOKEN          = os.getenv("API_TOKEN", "")
ADMIN_API_TOKEN    = os.getenv("ADMIN_API_TOKEN", "")
DOWNLINK_FPORT     = int(os.getenv("DOWNLINK_FPORT", 10))
DOWNLINK_CONFIRMED = os.getenv("DOWNLINK_CONFIRMED", "False").lower() == "true"

if not API_TOKEN or not ADMIN_API_TOKEN:
    raise RuntimeError(".env 파일에 API_TOKEN과 ADMIN_API_TOKEN을 설정하세요.")

# gRPC 채널 & 클라이언트 (전역 재사용)
_grpc_channel   = grpc.insecure_channel(GRPC_SERVER)
_device_client  = api.DeviceServiceStub(_grpc_channel)
_auth_metadata  = [("authorization", f"Bearer {API_TOKEN}")]
_admin_metadata = [("authorization", f"Bearer {ADMIN_API_TOKEN}")]

# ===================== 데이터 저장소 =====================

# 최근 uplink 이벤트
events = deque(maxlen=MAX_EVENTS)

# DevEUI별 마지막 fCnt (프레임 카운터)
last_fcnt = {}

# DevEUI별 누락된 프레임(충돌/커버리지 문제 추정) 개수
missed_frames = defaultdict(int)

# deduplicationId 추적 (최근 본 ID들)
seen_dedup_ids = deque(maxlen=1000)

# fCnt별로 여러 게이트웨이에서 수신한 기록 (fCnt 중복)
fcnt_gateway_map = defaultdict(lambda: defaultdict(list))

# ChirpStack에서 조회한 게이트웨이 목록 캐시 {gateway_id: {...}}
chirpstack_gateways = {}

app = Flask(__name__)


# ===================== ChirpStack 게이트웨이 목록 조회 =====================

def fetch_chirpstack_gateways():
    """gRPC로 ChirpStack 게이트웨이 목록을 주기적으로 가져와 캐시에 저장"""
    import time
    _gw_client = api.GatewayServiceStub(_grpc_channel)
    while True:
        try:
            req = api.ListGatewaysRequest()
            req.limit = 1000
            resp = _gw_client.List(req, metadata=_admin_metadata)
            new_cache = {}
            for gw in resp.result:
                last_seen_at = None
                if gw.last_seen_at and gw.last_seen_at.seconds:
                    last_seen_at = datetime.fromtimestamp(gw.last_seen_at.seconds, tz=KST).strftime("%Y-%m-%d %H:%M:%S")

                # GetGateway로 정적 위치 조회
                config_location = None
                try:
                    get_req = api.GetGatewayRequest()
                    get_req.gateway_id = gw.gateway_id
                    get_resp = _gw_client.Get(get_req, metadata=_admin_metadata)
                    loc = get_resp.gateway.location
                    if loc and (loc.latitude != 0 or loc.longitude != 0):
                        config_location = {
                            "latitude": round(loc.latitude, 6),
                            "longitude": round(loc.longitude, 6),
                            "altitude": round(loc.altitude, 1),
                            "source": "CONFIG",
                        }
                except Exception as e:
                    print(f"[WARN] GW location 조회 실패 ({gw.gateway_id}): {e}")

                new_cache[gw.gateway_id] = {
                    "gateway_id": gw.gateway_id,
                    "name": gw.name,
                    "description": gw.description,
                    "state": gw.state,  # 0=NEVER_SEEN, 1=ONLINE, 2=OFFLINE
                    "last_seen_at": last_seen_at,
                    "config_location": config_location,
                }
            chirpstack_gateways.update(new_cache)
            # 새로 없어진 게이트웨이 제거
            for gw_id in list(chirpstack_gateways.keys()):
                if gw_id not in new_cache:
                    del chirpstack_gateways[gw_id]
            print(f"[GW] ChirpStack 게이트웨이 {len(new_cache)}개 조회 완료")
        except Exception as e:
            print(f"[ERR] 게이트웨이 목록 조회 실패: {e}")
        time.sleep(30)  # 30초마다 갱신


# ===================== ACK 다운링크 =====================

def send_ack_downlink(dev_eui: str, dev_name: str):
    """해당 디바이스로 '[이름] ack' 다운링크 보내기"""
    if not dev_eui:
        print("[WARN] dev_eui 없음, 다운링크 생략")
        return

    text = f"{dev_name} ack" if dev_name else "ack"
    payload = text.encode("utf-8")

    req = api.EnqueueDeviceQueueItemRequest()
    req.queue_item.dev_eui = dev_eui
    req.queue_item.f_port = DOWNLINK_FPORT
    req.queue_item.confirmed = DOWNLINK_CONFIRMED
    req.queue_item.data = payload

    resp = _device_client.Enqueue(req, metadata=_auth_metadata)
    print(f"[DOWN] to {dev_name} ({dev_eui}) payload='{text}' id={resp.id}")


# ===================== MQTT 이벤트 파싱 =====================

def parse_event(msg_topic: str, payload: bytes):
    """MQTT 메시지를 파싱해서 우리가 쓸 필드만 뽑는다."""
    try:
        payload_str = payload.decode()
        data = json.loads(payload_str)
    except Exception as e:
        print("[ERR] JSON 파싱 실패:", e)
        print("     raw:", payload)
        return None, None

    # device info
    dev_info = data.get("deviceInfo", {})
    dev_eui = dev_info.get("devEui")
    dev_name = dev_info.get("deviceName")

    # DevAddr / fCnt
    dev_addr = data.get("devAddr")
    fcnt = data.get("fCnt")

    # txInfo -> frequency, modulation(lora -> SF, BW, codeRate)
    tx_info = data.get("txInfo", {}) or {}
    frequency_hz = tx_info.get("frequency")
    frequency_mhz = None
    if isinstance(frequency_hz, (int, float)):
        frequency_mhz = round(frequency_hz / 1_000_000, 3)

    dr = tx_info.get("dr")  # "SF7BW125" 같은 문자열일 수도 있음

    sf = None
    bandwidth = None
    code_rate = None
    modulation = tx_info.get("modulation") or {}
    lora_mod = modulation.get("lora") or modulation.get("LoRa") or {}
    if isinstance(lora_mod, dict):
        sf = lora_mod.get("spreadingFactor")
        bandwidth = lora_mod.get("bandwidth")
        code_rate = lora_mod.get("codeRate")

    # payload (base64 → bytes → hex / text)
    data_b64 = data.get("data")
    payload_hex = None
    payload_text = None
    if data_b64:
        try:
            raw = base64.b64decode(data_b64)
            payload_hex = raw.hex()
            try:
                payload_text = raw.decode("utf-8")
            except Exception:
                payload_text = None
        except Exception:
            pass

    # rxInfo 배열 안에 gatewayId, rssi, snr, location 정보들
    rx_infos = data.get("rxInfo", []) or []
    gateways = [rx.get("gatewayId") for rx in rx_infos if rx.get("gatewayId")]
    rssis = [rx.get("rssi") for rx in rx_infos if rx.get("rssi") is not None]
    snrs = [rx.get("snr") for rx in rx_infos if rx.get("snr") is not None]

    # 게이트웨이별 실시간 GPS (하드웨어 GPS가 있는 경우)
    gw_locations = {}
    for rx in rx_infos:
        gw_id = rx.get("gatewayId")
        loc = rx.get("location") or {}
        lat = loc.get("latitude")
        lon = loc.get("longitude")
        if gw_id and lat is not None and lon is not None and (lat != 0 or lon != 0):
            gw_locations[gw_id] = {
                "latitude": lat,
                "longitude": lon,
                "altitude": loc.get("altitude"),
                "source": "GPS",
            }
    
    # CRC 상태 확인 (CRC_OK = 정상, CRC_BAD = 에러)
    crc_statuses = [rx.get("crcStatus") for rx in rx_infos if rx.get("crcStatus")]
    crc_ok_count = sum(1 for status in crc_statuses if status == "CRC_OK")
    
    # deduplicationId (MQTT 중복 감지용)
    dedup_id = data.get("deduplicationId")

    # 어느 시간에 받은 건지 (서버 시간 기준)
    ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")

    event = {
        "time": ts,
        "topic": msg_topic,
        "dev_eui": dev_eui,
        "dev_name": dev_name,
        "dev_addr": dev_addr,
        "f_cnt": fcnt,
        "gateways": gateways,
        "rssi": rssis,
        "snr": snrs,
        "payload_hex": payload_hex,
        "payload_text": payload_text,
        "frequency_hz": frequency_hz,
        "frequency_mhz": frequency_mhz,
        "sf": sf,
        "bandwidth": bandwidth,
        "code_rate": code_rate,
        "dedup_id": dedup_id,
        "crc_ok_count": crc_ok_count,
        "total_gateways": len(rx_infos),
        "dr": dr,
        "gw_locations": gw_locations,
    }
    return event, dev_eui


# ===================== MQTT 콜백 =====================

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[OK] MQTT 연결 성공")
        print(f"   토픽 구독: {SUB_TOPIC}")
        client.subscribe(SUB_TOPIC)
        print(f"[OK] 토픽 구독 완료, 메시지 대기 중...")
    else:
        print("[ERR] MQTT 연결 실패, 코드:", rc)


def on_disconnect(client, userdata, rc):
    if rc != 0:
        print(f"[!] MQTT 연결 해제: {rc}")
    else:
        print("[*] MQTT 연결 정상 해제")


def on_message(client, userdata, msg):
    global last_fcnt, missed_frames, seen_dedup_ids, fcnt_gateway_map

    event, dev_eui = parse_event(msg.topic, msg.payload)
    if event is None or dev_eui is None:
        return

    # ===== 고급 충돌 감지 로직 =====
    fcnt = event.get("f_cnt")
    dedup_id = event.get("dedup_id")
    collision_reasons = []  # 충돌 이유 기록
    collision_detected = False
    
    # 1. fCnt 갭 감지 (프레임 손실)
    if isinstance(fcnt, int):
        prev = last_fcnt.get(dev_eui)
        if prev is not None and fcnt > prev + 1:
            gap = fcnt - prev - 1
            missed_frames[dev_eui] += gap
            collision_detected = True
            collision_reasons.append(f"fCnt_gap_{gap}")
        last_fcnt[dev_eui] = fcnt
    
    # 2. deduplicationId 중복 감지 (MQTT 중복 전송)
    if dedup_id and dedup_id in seen_dedup_ids:
        collision_detected = True
        collision_reasons.append("dedup_duplicate")
    
    if dedup_id:
        seen_dedup_ids.append(dedup_id)
    
    # 3. 같은 fCnt에 여러 게이트웨이 수신 (확실한 충돌)
    if isinstance(fcnt, int) and dev_eui:
        gateway_key = f"{dev_eui}_{fcnt}"
        gateways = event.get("gateways", [])
        
        for gw in gateways:
            fcnt_gateway_map[gateway_key][gw].append({
                "time": event["time"],
                "rssi": event.get("rssi", [None])[0],
                "snr": event.get("snr", [None])[0]
            })
        
        # 같은 fCnt, 같은 게이트웨이에서 여러 번 수신 = 충돌 가능성
        for gw, records in fcnt_gateway_map[gateway_key].items():
            if len(records) > 1:
                collision_detected = True
                collision_reasons.append(f"multi_gw_{len(records)}x")
    
    # 4. CRC 에러 감지 (신호 문제)
    crc_ok_count = event.get("crc_ok_count", 0)
    total_gateways = event.get("total_gateways", 0)
    if total_gateways > 0 and crc_ok_count < total_gateways:
        crc_err = total_gateways - crc_ok_count
        collision_reasons.append(f"crc_error_{crc_err}")
    
    # 5. SNR 기반 신호 신뢰도 평가 (낮은 SNR + 여러 게이트웨이 = 충돌 가능성)
    snrs = event.get("snr", [])
    if len(snrs) > 0:
        avg_snr = sum(snrs) / len(snrs)
        if avg_snr < -5 and total_gateways > 1:
            collision_reasons.append(f"low_snr_{avg_snr:.1f}")

    # 충돌 정보를 이벤트에 포함
    event["collision_detected"] = collision_detected
    event["collision_reasons"] = collision_reasons

    events.appendleft(event)  # 최신 것을 앞쪽에
    threading.Thread(target=save_event_db, args=(event,), daemon=True).start()

    collision_str = f"[{'COLLISION' if collision_detected else 'OK'}]"
    if collision_reasons:
        collision_str += f" ({', '.join(collision_reasons)})"
    
    print(
        f"[UP] {event['time']} {event['dev_name']}({event['dev_eui']}) "
        f"DevAddr={event['dev_addr']} SF={event['sf']} "
        f"Freq={event['frequency_mhz']}MHz SNR={event['snr']} RSSI={event['rssi']} "
        f"FCnt={event['f_cnt']} Missed={missed_frames.get(dev_eui, 0)} "
        f"{collision_str} "
        f"PAYLOAD={event['payload_hex']}"
    )

    # ACK 다운링크 전송
    try:
        send_ack_downlink(dev_eui, event.get("dev_name"))
    except Exception as e:
        print(f"[ERR] ACK 다운링크 실패: {e}")


def start_mqtt():
    client = mqtt_client.Client()

    if MQTT_USERNAME is not None:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    try:
        print(f"[*] MQTT 브로커 연결 시도: {MQTT_BROKER}:{MQTT_PORT}")
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        print("[OK] MQTT 브로커 연결 성공")
        print(f"[*] 구독 토픽: {SUB_TOPIC}")
        
        print("[*] MQTT 메시지 루프 시작...")
        client.loop_forever()
    except ConnectionRefusedError as e:
        print(f"[ERR] MQTT 브로커에 연결할 수 없음: {MQTT_BROKER}:{MQTT_PORT}")
        print(f"[ERR] 브로커가 실행 중인지 확인하세요: {e}")
        print(f"[*] 5초 후 재시도...")
        import time
        time.sleep(5)
        start_mqtt()  # 재귀 재시도
    except Exception as e:
        print(f"[ERR] MQTT 연결 오류: {e}")
        print(f"[*] 5초 후 재시도...")
        import time
        time.sleep(5)
        start_mqtt()  # 재귀 재시도


# ===================== Flask 웹 (표 + 그래프) =====================

@app.route("/nodes")
def nodes_page():
    return render_template("nodes.html")


@app.route("/")
def index():
    response = render_template("index.html")
    # 캐시 무시 설정
    response = app.make_response(response)
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


def _rows_to_events(rows):
    """DB Row 목록을 이벤트 dict 목록으로 변환"""
    result = []
    for r in rows:
        result.append({
            "time":               r["time"],
            "topic":              r["topic"],
            "dev_eui":            r["dev_eui"],
            "dev_name":           r["dev_name"],
            "dev_addr":           r["dev_addr"],
            "f_cnt":              r["f_cnt"],
            "frequency_mhz":      r["frequency_mhz"],
            "sf":                 r["sf"],
            "bandwidth":          r["bandwidth"],
            "code_rate":          r["code_rate"],
            "dr":                 r["dr"],
            "rssi":               json.loads(r["rssi"] or "[]"),
            "snr":                json.loads(r["snr"] or "[]"),
            "gateways":           json.loads(r["gateways"] or "[]"),
            "payload_hex":        r["payload_hex"],
            "payload_text":       r["payload_text"],
            "collision_detected": bool(r["collision_detected"]),
            "collision_reasons":  json.loads(r["collision_reasons"] or "[]"),
            "crc_ok_count":       r["crc_ok_count"],
            "total_gateways":     r["total_gateways"],
        })
    return result


@app.route("/api/events")
def api_events():
    # DB에서 전체 반환 (제한 없음)
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM uplink_events ORDER BY id DESC"
        ).fetchall()
    return jsonify(_rows_to_events(rows))


@app.route("/api/collisions")
def api_collisions():
    """충돌 감지 정보 조회 API — DB 전체 기준"""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT * FROM uplink_events WHERE collision_detected = 1
               ORDER BY id DESC LIMIT 50"""
        ).fetchall()
        total_count = conn.execute(
            "SELECT COUNT(*) FROM uplink_events WHERE collision_detected = 1"
        ).fetchone()[0]

    collisions = []
    collision_by_reason = defaultdict(int)
    collision_by_device = defaultdict(int)

    for r in rows:
        reasons = json.loads(r["collision_reasons"] or "[]")
        collisions.append({
            "time":           r["time"],
            "dev_eui":        r["dev_eui"],
            "dev_name":       r["dev_name"],
            "f_cnt":          r["f_cnt"],
            "reasons":        reasons,
            "gateways":       json.loads(r["gateways"] or "[]"),
            "rssi":           json.loads(r["rssi"] or "[]"),
            "snr":            json.loads(r["snr"] or "[]"),
            "total_gateways": r["total_gateways"],
            "crc_ok_count":   r["crc_ok_count"],
        })
        for reason in reasons:
            collision_by_reason[reason] += 1
        if r["dev_eui"]:
            collision_by_device[r["dev_eui"]] += 1

    return jsonify({
        "total_collisions": total_count,
        "collisions": collisions,
        "statistics": {
            "by_reason": dict(collision_by_reason),
            "by_device": dict(collision_by_device),
        }
    })


@app.route("/api/summary")
def api_summary():
    # 전체 요약 + 그래프용 데이터 — DB 전체 기준
    with get_db() as conn:
        # 총 이벤트 / 디바이스 / 충돌 수
        total_events  = conn.execute("SELECT COUNT(*) FROM uplink_events").fetchone()[0]
        active_devices = conn.execute("SELECT COUNT(DISTINCT dev_eui) FROM uplink_events WHERE dev_eui IS NOT NULL").fetchone()[0]

        # 평균 SNR / RSSI (JSON 배열 저장이라 Python에서 집계)
        snr_rows  = conn.execute("SELECT snr  FROM uplink_events WHERE snr  != '[]'").fetchall()
        rssi_rows = conn.execute("SELECT rssi FROM uplink_events WHERE rssi != '[]'").fetchall()

        all_snrs, all_rssis = [], []
        for r in snr_rows:
            try: all_snrs.extend(json.loads(r[0]))
            except: pass
        for r in rssi_rows:
            try: all_rssis.extend(json.loads(r[0]))
            except: pass

        avg_snr_all  = round(sum(all_snrs)  / len(all_snrs),  2) if all_snrs  else None
        avg_rssi_all = round(sum(all_rssis) / len(all_rssis), 2) if all_rssis else None

        # SF 분포
        sf_rows = conn.execute(
            "SELECT sf, COUNT(*) FROM uplink_events WHERE sf IS NOT NULL GROUP BY sf ORDER BY sf"
        ).fetchall()
        sfs_sorted     = [str(r[0]) for r in sf_rows]
        sf_counts_list = [r[1] for r in sf_rows]

        # 주파수 분포
        freq_rows = conn.execute(
            "SELECT frequency_mhz, COUNT(*) FROM uplink_events WHERE frequency_mhz IS NOT NULL GROUP BY frequency_mhz ORDER BY frequency_mhz"
        ).fetchall()
        freqs_sorted    = [str(r[0]) for r in freq_rows]
        freq_counts_list = [r[1] for r in freq_rows]

        # SNR 추이 — 최근 300개 (그래프 가독성)
        recent_rows = conn.execute(
            "SELECT time, snr, collision_detected FROM uplink_events ORDER BY id DESC LIMIT 300"
        ).fetchall()

    timestamps, avg_snr_list, collisions_list = [], [], []
    for r in reversed(recent_rows):
        timestamps.append(r[0].split(" ")[-1])
        try:
            snrs = json.loads(r[1] or "[]")
            avg_snr_list.append(round(sum(snrs) / len(snrs), 2) if snrs else None)
        except:
            avg_snr_list.append(None)
        collisions_list.append(bool(r[2]))

    # 활성 게이트웨이 수 (ChirpStack ONLINE 우선)
    online_count = sum(1 for gw in chirpstack_gateways.values() if gw.get("state") == 1)
    if online_count > 0:
        gw_count = online_count
    else:
        events_list = list(events)
        gw_set = set(gw for ev in events_list for gw in (ev.get("gateways") or []))
        gw_count = len(gw_set)

    return jsonify({
        "total_events": total_events,
        "active_devices": active_devices,
        "active_gateways": gw_count,
        "avg_snr_all": avg_snr_all,
        "avg_rssi_all": avg_rssi_all,
        "snr_over_time": {
            "timestamps": timestamps,
            "avg_snr": avg_snr_list,
            "collisions": collisions_list,
        },
        "sf_counts": {
            "sfs": sfs_sorted,
            "counts": sf_counts_list,
        },
        "freq_distribution": {
            "freqs": freqs_sorted,
            "counts": freq_counts_list,
        },
    })


@app.route("/api/nodes")
def api_nodes():
    """노드별 정보 조회 — DB 전체 기준"""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT dev_eui, dev_name, dev_addr, sf, frequency_mhz,
                      MAX(time) as last_seen, rssi, snr, gateways
               FROM uplink_events
               WHERE dev_eui IS NOT NULL
               GROUP BY dev_eui"""
        ).fetchall()

    node_info = []
    for r in rows:
        node_info.append({
            "dev_eui":       r["dev_eui"],
            "dev_name":      r["dev_name"],
            "dev_addr":      r["dev_addr"],
            "sf":            r["sf"],
            "frequency_mhz": r["frequency_mhz"],
            "last_seen":     r["last_seen"],
            "rssi":          json.loads(r["rssi"] or "[]"),
            "snr":           json.loads(r["snr"] or "[]"),
            "gateways":      list(set(json.loads(r["gateways"] or "[]"))),
        })

    return jsonify(node_info)


@app.route("/api/gateways")
def api_gateways():
    """게이트웨이별 정보 조회 (ChirpStack gRPC + MQTT 이벤트 병합)"""
    gateway_info = {}

    # ChirpStack에서 조회한 게이트웨이를 기본값으로 채우기
    state_labels = {0: "NEVER_SEEN", 1: "ONLINE", 2: "OFFLINE"}
    for gw_id, cs_gw in chirpstack_gateways.items():
        gateway_info[gw_id] = {
            "gateway_id": gw_id,
            "name": cs_gw.get("name") or gw_id,
            "description": cs_gw.get("description") or "",
            "state": state_labels.get(cs_gw.get("state", 0), "UNKNOWN"),
            "last_seen_at": cs_gw.get("last_seen_at"),
            "message_count": 0,
            "avg_rssi": [],
            "avg_snr": [],
            "last_seen": cs_gw.get("last_seen_at"),
            "location": cs_gw.get("config_location"),  # 정적 설정 위치
        }

    # MQTT 이벤트에서 RSSI/SNR/메시지 수/실시간 GPS 집계
    for ev in events:
        gw_ids = ev.get("gateways") or []
        rssis = ev.get("rssi") or []
        snrs = ev.get("snr") or []
        ev_gw_locs = ev.get("gw_locations") or {}
        for i, gw_id in enumerate(gw_ids):
            if gw_id not in gateway_info:
                gateway_info[gw_id] = {
                    "gateway_id": gw_id,
                    "name": gw_id,
                    "description": "",
                    "state": "UNKNOWN",
                    "last_seen_at": None,
                    "message_count": 0,
                    "avg_rssi": [],
                    "avg_snr": [],
                    "last_seen": ev.get("time"),
                    "location": None,
                }
            gateway_info[gw_id]["message_count"] += 1
            gateway_info[gw_id]["last_seen"] = ev.get("time")
            if i < len(rssis):
                gateway_info[gw_id]["avg_rssi"].append(rssis[i])
            if i < len(snrs):
                gateway_info[gw_id]["avg_snr"].append(snrs[i])
            # 실시간 GPS가 있으면 덮어쓰기 (정적 설정보다 우선)
            if gw_id in ev_gw_locs:
                gateway_info[gw_id]["location"] = ev_gw_locs[gw_id]

    # 평균값 계산
    for gw_id in gateway_info:
        gw = gateway_info[gw_id]
        gw["avg_rssi"] = round(sum(gw["avg_rssi"]) / len(gw["avg_rssi"]), 1) if gw["avg_rssi"] else None
        gw["avg_snr"] = round(sum(gw["avg_snr"]) / len(gw["avg_snr"]), 1) if gw["avg_snr"] else None

    return jsonify(list(gateway_info.values()))


# ===================== 히스토리 API =====================

@app.route("/history")
def history_page():
    return render_template("history.html")


@app.route("/api/history")
def api_history():
    from flask import request as freq
    date     = freq.args.get("date", "")       # YYYY-MM-DD
    dev_eui  = freq.args.get("dev_eui", "")
    page     = max(1, int(freq.args.get("page", 1)))
    per_page = int(freq.args.get("per_page", 50))
    offset   = (page - 1) * per_page

    where, params = [], []
    if date:
        where.append("time LIKE ?")
        params.append(f"{date}%")
    if dev_eui:
        where.append("dev_eui = ?")
        params.append(dev_eui)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    with get_db() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) FROM uplink_events {where_sql}", params
        ).fetchone()[0]

        rows = conn.execute(
            f"""SELECT * FROM uplink_events {where_sql}
                ORDER BY id DESC LIMIT ? OFFSET ?""",
            params + [per_page, offset]
        ).fetchall()

    events_list = []
    for r in rows:
        events_list.append({
            "id":               r["id"],
            "time":             r["time"],
            "dev_eui":          r["dev_eui"],
            "dev_name":         r["dev_name"],
            "dev_addr":         r["dev_addr"],
            "f_cnt":            r["f_cnt"],
            "frequency_mhz":    r["frequency_mhz"],
            "sf":               r["sf"],
            "bandwidth":        r["bandwidth"],
            "dr":               r["dr"],
            "rssi":             json.loads(r["rssi"] or "[]"),
            "snr":              json.loads(r["snr"] or "[]"),
            "gateways":         json.loads(r["gateways"] or "[]"),
            "payload_hex":      r["payload_hex"],
            "payload_text":     r["payload_text"],
            "collision_detected": bool(r["collision_detected"]),
            "collision_reasons":json.loads(r["collision_reasons"] or "[]"),
            "crc_ok_count":     r["crc_ok_count"],
            "total_gateways":   r["total_gateways"],
        })

    return jsonify({
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page,
        "events": events_list,
    })


@app.route("/api/history/devices")
def api_history_devices():
    """히스토리에 기록된 디바이스 목록"""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT DISTINCT dev_eui, dev_name FROM uplink_events WHERE dev_eui IS NOT NULL ORDER BY dev_name"
        ).fetchall()
    return jsonify([{"dev_eui": r["dev_eui"], "dev_name": r["dev_name"]} for r in rows])


@app.route("/api/history/stats")
def api_history_stats():
    """날짜별 수신 건수 (최근 30일)"""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT substr(time,1,10) as date, COUNT(*) as count
               FROM uplink_events
               GROUP BY date ORDER BY date DESC LIMIT 30"""
        ).fetchall()
    return jsonify([{"date": r["date"], "count": r["count"]} for r in rows])


# ===================== 노드 등록 API =====================

def _get_tenant_id():
    """첫 번째 테넌트 ID 반환 (관리자 토큰 사용)"""
    tenant_client = api.TenantServiceStub(_grpc_channel)
    req = api.ListTenantsRequest()
    req.limit = 1
    resp = tenant_client.List(req, metadata=_admin_metadata)
    if resp.result:
        return resp.result[0].id
    raise RuntimeError("테넌트를 찾을 수 없음")


@app.route("/api/applications")
def api_applications():
    try:
        tenant_id = _get_tenant_id()
        app_client = api.ApplicationServiceStub(_grpc_channel)
        req = api.ListApplicationsRequest()
        req.limit = 100
        req.tenant_id = tenant_id
        resp = app_client.List(req, metadata=_admin_metadata)
        return jsonify([{"id": a.id, "name": a.name} for a in resp.result])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/device-profiles")
def api_device_profiles():
    try:
        tenant_id = _get_tenant_id()
        dp_client = api.DeviceProfileServiceStub(_grpc_channel)
        req = api.ListDeviceProfilesRequest()
        req.limit = 100
        req.tenant_id = tenant_id
        resp = dp_client.List(req, metadata=_admin_metadata)
        return jsonify([{"id": d.id, "name": d.name} for d in resp.result])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/register-node", methods=["POST"])
def api_register_node():
    from flask import request as flask_request
    data = flask_request.get_json()

    dev_eui      = (data.get("dev_eui") or "").replace(" ", "").lower()
    join_eui     = (data.get("join_eui") or "").replace(" ", "").lower()
    name         = data.get("name", "").strip()
    description  = data.get("description", "").strip()
    app_id       = data.get("application_id", "").strip()
    profile_id   = data.get("device_profile_id", "").strip()
    app_key      = (data.get("app_key") or "").replace(" ", "").lower()
    gen_app_key  = (data.get("gen_app_key") or "").replace(" ", "").lower()

    if not all([dev_eui, name, app_id, profile_id]):
        return jsonify({"error": "dev_eui, name, application_id, device_profile_id는 필수입니다"}), 400
    if len(dev_eui) != 16:
        return jsonify({"error": "DevEUI는 16자리 hex여야 합니다"}), 400
    if join_eui and len(join_eui) != 16:
        return jsonify({"error": "JoinEUI는 16자리 hex여야 합니다"}), 400

    try:
        # 디바이스 생성
        create_req = api.CreateDeviceRequest()
        create_req.device.dev_eui = dev_eui
        create_req.device.name = name
        create_req.device.description = description
        create_req.device.application_id = app_id
        create_req.device.device_profile_id = profile_id
        create_req.device.is_disabled = False
        if join_eui:
            create_req.device.join_eui = join_eui
        _device_client.Create(create_req, metadata=_auth_metadata)

        # 키 등록
        keys_req = api.CreateDeviceKeysRequest()
        keys_req.device_keys.dev_eui = dev_eui
        if app_key and len(app_key) == 32:
            keys_req.device_keys.nwk_key = app_key
        if gen_app_key and len(gen_app_key) == 32:
            keys_req.device_keys.gen_app_key = gen_app_key
        if app_key or gen_app_key:
            _device_client.CreateKeys(keys_req, metadata=_auth_metadata)

        print(f"[NODE] 등록 완료: {name} ({dev_eui})")
        return jsonify({"success": True, "dev_eui": dev_eui, "name": name})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/nodes-list")
def api_nodes_list():
    """ChirpStack에 등록된 전체 디바이스 목록"""
    try:
        tenant_id = _get_tenant_id()
        app_client = api.ApplicationServiceStub(_grpc_channel)
        areq = api.ListApplicationsRequest()
        areq.limit = 100
        areq.tenant_id = tenant_id
        aresp = app_client.List(areq, metadata=_admin_metadata)

        devices = []
        for a in aresp.result:
            dreq = api.ListDevicesRequest()
            dreq.limit = 1000
            dreq.application_id = a.id
            dresp = _device_client.List(dreq, metadata=_auth_metadata)
            for d in dresp.result:
                last_seen = None
                if d.last_seen_at and d.last_seen_at.seconds:
                    last_seen = datetime.fromtimestamp(
                        d.last_seen_at.seconds, tz=KST
                    ).strftime("%Y-%m-%d %H:%M:%S")

                # GetDevice로 join_eui, is_disabled 조회
                join_eui = None
                is_disabled = False
                try:
                    get_req = api.GetDeviceRequest()
                    get_req.dev_eui = d.dev_eui
                    get_resp = _device_client.Get(get_req, metadata=_auth_metadata)
                    raw_join_eui = get_resp.device.join_eui
                    # 미설정 시 ChirpStack이 "0000000000000000" 반환하는 경우 무시
                    if raw_join_eui and raw_join_eui != "0000000000000000":
                        join_eui = raw_join_eui
                    is_disabled = get_resp.device.is_disabled
                except Exception as e:
                    print(f"[WARN] GetDevice 실패 ({d.dev_eui}): {e}")

                devices.append({
                    "dev_eui": d.dev_eui,
                    "join_eui": join_eui,
                    "name": d.name,
                    "description": d.description,
                    "application_id": a.id,
                    "application_name": a.name,
                    "device_profile_id": d.device_profile_id,
                    "device_profile_name": d.device_profile_name,
                    "last_seen": last_seen,
                    "is_disabled": is_disabled,
                })
        return jsonify(devices)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/node-keys/<dev_eui>")
def api_node_keys(dev_eui):
    """디바이스 키 조회 (Join EUI + Application Key + Gen App Key)"""
    try:
        # Join EUI는 Device에서 조회
        join_eui = None
        try:
            get_req = api.GetDeviceRequest()
            get_req.dev_eui = dev_eui
            get_resp = _device_client.Get(get_req, metadata=_auth_metadata)
            raw = get_resp.device.join_eui
            if raw and raw != "0000000000000000":
                join_eui = raw
        except Exception as e:
            print(f"[WARN] node-keys GetDevice 실패 ({dev_eui}): {e}")

        # 키는 DeviceKeys에서 조회
        keys_req = api.GetDeviceKeysRequest()
        keys_req.dev_eui = dev_eui
        keys_resp = _device_client.GetKeys(keys_req, metadata=_auth_metadata)
        return jsonify({
            "dev_eui": dev_eui,
            "join_eui": join_eui,
            "nwk_key": keys_resp.device_keys.nwk_key or None,
            "app_key": keys_resp.device_keys.app_key or None,
            "gen_app_key": keys_resp.device_keys.gen_app_key or None,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/edit-node", methods=["PUT"])
def api_edit_node():
    from flask import request as flask_request
    data = flask_request.get_json()

    dev_eui    = (data.get("dev_eui") or "").strip()
    join_eui   = (data.get("join_eui") or "").replace(" ", "").lower()
    name       = data.get("name", "").strip()
    description = data.get("description", "").strip()
    profile_id = data.get("device_profile_id", "").strip()
    app_id     = data.get("application_id", "").strip()

    if not all([dev_eui, name, app_id, profile_id]):
        return jsonify({"error": "필수 항목 누락"}), 400
    try:
        req = api.UpdateDeviceRequest()
        req.device.dev_eui = dev_eui
        req.device.name = name
        req.device.description = description
        req.device.application_id = app_id
        req.device.device_profile_id = profile_id
        if join_eui:
            req.device.join_eui = join_eui
        _device_client.Update(req, metadata=_auth_metadata)
        print(f"[NODE] 수정 완료: {name} ({dev_eui})")
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/delete-node/<dev_eui>", methods=["DELETE"])
def api_delete_node(dev_eui):
    try:
        req = api.DeleteDeviceRequest()
        req.dev_eui = dev_eui
        _device_client.Delete(req, metadata=_auth_metadata)
        print(f"[NODE] 삭제 완료: {dev_eui}")
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ===================== main =====================

if __name__ == "__main__":
    # DB 초기화
    init_db()

    # MQTT를 백그라운드 스레드에서 실행
    print("[*] MQTT 스레드 시작 중...")
    t = threading.Thread(target=start_mqtt, daemon=True)
    t.start()

    # ChirpStack 게이트웨이 목록 주기 조회 스레드
    print("[*] 게이트웨이 조회 스레드 시작 중...")
    gw_thread = threading.Thread(target=fetch_chirpstack_gateways, daemon=True)
    gw_thread.start()
    
    import time
    time.sleep(2)  # MQTT 연결 대기
    print("[OK] MQTT 스레드 준비 완료")
    print("[*] Flask 웹서버 시작 중...")

    # Flask 웹서버 실행 (debug=False로 설정하여 자동 리로드 방지)
    # 브라우저에서 http://localhost:5000 접속
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
