# LoRaWAN 데이터 수집 시스템 구현 보고서

**플랫폼:** ChirpStack v4 on Docker  
**서버 OS:** Windows Server 2025 (Korea)  
**컨테이너 수:** 8 services  
**활성 주파수 대역:** KR920, AS923, AS923-2  
**작성 기준:** docker-compose.yml 및 구성 파일 실측

---

## 목차

1. [시스템 개요](#1-시스템-개요)
2. [인프라 구성 (Docker Compose)](#2-인프라-구성-docker-compose)
3. [주파수 대역 구성](#3-주파수-대역-구성)
4. [데이터 수집 파이프라인](#4-데이터-수집-파이프라인)
5. [MQTT 메시징 구조](#5-mqtt-메시징-구조)
6. [게이트웨이 브리지 구성](#6-게이트웨이-브리지-구성)
7. [대시보드 기능 (Flask :5000)](#7-대시보드-기능-flask-5000)
8. [REST API 엔드포인트 목록](#8-rest-api-엔드포인트-목록)
9. [충돌 감지 알고리즘](#9-충돌-감지-알고리즘)
10. [다운링크 ACK 자동 전송](#10-다운링크-ack-자동-전송)
11. [데이터베이스 구조](#11-데이터베이스-구조)
12. [디바이스 프로파일](#12-디바이스-프로파일)
13. [보안 구성 현황](#13-보안-구성-현황)
14. [운영 정책](#14-운영-정책)

---

## 1. 시스템 개요

본 시스템은 LoRaWAN 무선 센서 네트워크에서 수집된 데이터를 한국 서버에서 집중 관리·모니터링하기 위한 풀스택 구현체입니다. ChirpStack v4를 중심으로 8개의 Docker 컨테이너가 협력하며, 외부 LoRa 게이트웨이 → MQTT 브로커 → 네트워크 서버 → 커스텀 대시보드로 이어지는 완전한 데이터 파이프라인을 구성합니다.

| 항목 | 수치 |
|------|------|
| Docker 서비스 수 | 8개 |
| 외부 노출 포트 | 6개 |
| 활성 LoRa 대역 | 3개 (KR920 / AS923 / AS923-2) |
| 게이트웨이 접속 방식 | 2가지 (UDP + Basics Station) |
| 대시보드 REST API 엔드포인트 | 17개 |
| 충돌 감지 알고리즘 | 5가지 |

**구성 파일 위치:** `c:\Users\Administrator\chirpstack-docker\`

---

## 2. 인프라 구성 (Docker Compose)

모든 서비스는 `docker-compose.yml` 한 파일로 관리됩니다. 컨테이너 간 통신은 Docker 내부 네트워크를 통해 서비스 이름(hostname)으로 이루어집니다.

### 서비스 목록

| 서비스 | 이미지 | 노출 포트 | 프로토콜 | 역할 |
|--------|--------|-----------|----------|------|
| chirpstack | chirpstack/chirpstack:4 | 8080 | HTTP / gRPC | LoRaWAN 네트워크 서버 + 웹 UI + gRPC API |
| chirpstack-gateway-bridge | chirpstack/chirpstack-gateway-bridge:4 | 1700/UDP | UDP | Semtech UDP Packet Forwarder 수신 및 MQTT 변환 |
| chirpstack-gateway-bridge-basicstation | chirpstack/chirpstack-gateway-bridge:4 | 3001/TCP | WebSocket | Basics Station 프로토콜 수신 및 MQTT 변환 |
| chirpstack-rest-api | chirpstack/chirpstack-rest-api:4 | 8090 | HTTP/REST | gRPC → REST JSON 변환 프록시 |
| mosquitto | eclipse-mosquitto:2 | 1883 | MQTT | 메시지 브로커 — 게이트웨이·서버·대시보드 연결 |
| dashboard | 로컬 빌드 (./dashboard) | 5000 | HTTP | 커스텀 Flask 모니터링 대시보드 |
| postgres | postgres:14-alpine | 내부 전용 | TCP | ChirpStack 디바이스·세션 영구 데이터베이스 |
| redis | redis:7-alpine | 내부 전용 | TCP | ChirpStack 세션 캐시 |

### Docker 볼륨 (데이터 영속성)

| 볼륨명 | 마운트 대상 | 저장 내용 |
|--------|------------|----------|
| postgresqldata | postgres:/var/lib/postgresql/data | 디바이스, 세션, 어플리케이션 메타데이터 |
| redisdata | redis:/data | LoRaWAN 세션 캐시 (FCnt, NwkSKey 등) |
| dashboarddata | dashboard:/app/data | SQLite 수신 이력 DB (lora_history.db) |

### 서비스 의존성

```
postgres  ─┐
redis     ─┼──▶ chirpstack ──────────────────────▶ chirpstack-rest-api
mosquitto ─┘        ▲
               ┌────┘
chirpstack-gateway-bridge ──────────────▶ mosquitto
chirpstack-gateway-bridge-basicstation ──▶ mosquitto
dashboard ──▶ mosquitto + chirpstack (gRPC)
```

---

## 3. 주파수 대역 구성

`chirpstack.toml`의 `enabled_regions` 설정 기준.

| Region ID | 공식명 | 상태 | 채널 주파수 (MHz) | MQTT 토픽 접두사 |
|-----------|--------|------|-------------------|-----------------|
| kr920 | KR920 | ✅ 활성 | 922.1 / 922.3 / 922.5 / 922.7 / 922.9 / 923.1 / 923.3 | `kr920/` |
| as923 | AS923 | ✅ 활성 | 923.2 / 923.4 | `as923/` |
| as923_2 | AS923-2 | ✅ 활성 | 921.4 / 921.6 (대역 시프트) | `as923_2/` |
| eu868, us915, au915 외 | 기타 전 세계 대역 | ⬜ 비활성 | — | — |

### KR920 채널 상세 (region_kr920.toml)

| 채널 | 주파수 | 대역폭 | 변조 | 지원 SF | ADR |
|------|--------|--------|------|---------|-----|
| CH0 | 922.1 MHz | 125 kHz | LoRa | SF7–SF12 | 활성 (DR0–DR5) |
| CH1 | 922.3 MHz | 125 kHz | LoRa | SF7–SF12 | 활성 |
| CH2 | 922.5 MHz | 125 kHz | LoRa | SF7–SF12 | 활성 |
| CH3 | 922.7 MHz | 125 kHz | LoRa | SF7–SF12 | 활성 |
| CH4 | 922.9 MHz | 125 kHz | LoRa | SF7–SF12 | 활성 |
| CH5 | 923.1 MHz | 125 kHz | LoRa | SF7–SF12 | 활성 |
| CH6 | 923.3 MHz | 125 kHz | LoRa | SF7–SF12 | 활성 |
| RX2 | 921.9 MHz | — | LoRa | DR0 (SF12) | 수신 전용 |

> **베트남 배포 참고:** 베트남 공식 허가 대역은 AS923-1 (923.2 / 923.4 MHz)입니다. 현재 서버에 `as923` 대역이 활성화되어 있어 베트남 DLOS8N 게이트웨이와 즉시 연동 가능합니다.

---

## 4. 데이터 수집 파이프라인

센서 노드에서 발생한 데이터가 한국 서버 대시보드까지 전달되는 전 과정.

```
[LoRa 센서]
    │  ① LoRa RF (AES-128 암호화 PHY frame)
    │  AS923: 923.2/923.4 MHz  |  KR920: 922.1~923.3 MHz
    ▼
[LoRa Gateway (DLOS8N)]
    │  ② UDP :1700 (기기 내부 — Packet Forwarder → Gateway Bridge)
    │  형식: Semtech UDP JSON (PHYPayload = base64)
    ▼
[Mosquitto :1883]
    │  ③ MQTT (TLS :8883 권장) — Protobuf (UplinkFrame)
    │  토픽: kr920/gateway/{eui}/event/up
    ▼
[ChirpStack NS :8080]
    │  복호화 (AES-128) + 페이로드 디코딩 + JSON 이벤트 생성
    │  ④ MQTT :1883 — JSON (ApplicationUplink)
    │  토픽: application/{id}/device/{eui}/event/up
    ▼
[Mosquitto :1883]
    │  ⑤ MQTT 구독
    ▼
[Dashboard :5000]
    │  충돌 감지 / SQLite 저장 / 실시간 웹 UI
    ▼
[REST API / 외부 연동]
    HTTP :8090 / gRPC :8080 (JWT 인증)

    ↑ 다운링크 ACK (gRPC → MQTT command → Gateway → 노드) ↑
```

### 단계별 프로토콜 상세

| 단계 | 구간 | 프로토콜 | 포트 | 데이터 형식 | 암호화 |
|------|------|----------|------|------------|--------|
| ① | 노드 → 게이트웨이 | LoRa RF | AS923: 923.2/923.4 MHz | LoRaWAN PHY frame (이진) | AES-128 (AppSKey + NwkSKey) |
| ② | Packet Forwarder → Gateway Bridge (기기 내부) | UDP | localhost:1700 | Semtech UDP JSON (PHYPayload = base64) | 없음 (내부 통신) |
| ③ | 게이트웨이 → Mosquitto (인터넷 WAN) | MQTT (TLS) | :8883 / :1883 | Protocol Buffers (UplinkFrame) | TLS 1.2+ |
| ④ | Mosquitto → ChirpStack (Docker 내부) | MQTT | :1883 | Protocol Buffers (UplinkFrame) | 없음 (내부 네트워크) |
| ⑤ | ChirpStack → Mosquitto (앱 이벤트) | MQTT | :1883 | JSON (ApplicationUplink event) | 없음 (내부) |
| ⑥ | Mosquitto → Dashboard | MQTT | :1883 | JSON (복호화 + 코덱 디코딩 완료) | 없음 (내부) |
| ⑦ | 외부 시스템 → ChirpStack API | REST / gRPC | :8090 / :8080 | JSON (REST) / Protobuf (gRPC) | JWT Bearer Token |

### chirpstack.toml 통합 설정

```toml
[integration]
  enabled=["mqtt"]

  [integration.mqtt]
    server="tcp://$MQTT_BROKER_HOST:1883/"
    json=true   # Protobuf 대신 JSON 마샬링 — 앱 레벨 이벤트를 JSON으로 발행
```

> **JSON 마샬링:** `json=true` 설정으로 **앱 레벨 이벤트는 JSON**으로 발행됩니다. 게이트웨이 레벨(Gateway Bridge → Mosquitto 구간)은 여전히 Protobuf를 사용합니다.

---

## 5. MQTT 메시징 구조

Mosquitto 브로커(:1883)가 모든 메시지의 허브 역할을 합니다.

### 게이트웨이 레벨 토픽 (Gateway Bridge 발행)

| 방향 | 토픽 패턴 | 형식 | 발행자 |
|------|----------|------|--------|
| 업링크 | `kr920/gateway/{gateway-eui}/event/up` | Protobuf · UplinkFrame | Gateway Bridge |
| 통계 | `kr920/gateway/{gateway-eui}/event/stats` | Protobuf · GatewayStats | Gateway Bridge |
| ACK | `kr920/gateway/{gateway-eui}/event/ack` | Protobuf · DownlinkTxAck | Gateway Bridge |
| 다운링크 명령 | `kr920/gateway/{gateway-eui}/command/down` | Protobuf · DownlinkFrame | ChirpStack NS |
| 상태 | `kr920/gateway/{gateway-eui}/state/conn` | Protobuf · ConnState | Gateway Bridge |

### 애플리케이션 레벨 토픽 (ChirpStack 발행)

| 이벤트 | 토픽 패턴 | 형식 | 트리거 |
|--------|----------|------|--------|
| **업링크** | `application/{app-id}/device/{dev-eui}/event/up` | JSON | 노드 데이터 수신 |
| 조인 | `application/{app-id}/device/{dev-eui}/event/join` | JSON | OTAA 조인 성공 |
| ACK | `application/{app-id}/device/{dev-eui}/event/ack` | JSON | Confirmed 다운링크 ACK |
| TX ACK | `application/{app-id}/device/{dev-eui}/event/txack` | JSON | 게이트웨이 전송 확인 |
| 오류 | `application/{app-id}/device/{dev-eui}/event/log` | JSON | 에러 / 경고 |
| 배터리/마진 | `application/{app-id}/device/{dev-eui}/event/status` | JSON | DevStatusReq 응답 |

### 대시보드 구독 토픽

```
# dashboard/.env
SUB_TOPIC=application/+/device/+/event/up
# '+' 와일드카드 = 모든 어플리케이션, 모든 디바이스의 업링크 수신
```

### 최종 업링크 JSON 구조 (대시보드 수신 데이터)

```json
{
  "deduplicationId": "5f4a1b2c-...",
  "time": "2026-05-20T09:30:00.000Z",
  "deviceInfo": {
    "deviceName": "zone-a-sensor-01",
    "devEui":     "0102030405060708",
    "tags": { "zone": "A" }
  },
  "object": {
    "temperature_3": 27.2,
    "relative_humidity_5": 50.0
  },
  "rxInfo": [{
    "gatewayId": "aabbccddeeff0011",
    "rssi": -85,
    "snr":  7.5,
    "location": { "latitude": 10.762, "longitude": 106.660 }
  }],
  "txInfo": {
    "frequency":  923200000,
    "modulation": { "lora": { "spreadingFactor": 9, "bandwidth": 125000 } }
  },
  "fCnt": 142,
  "fPort": 1,
  "dr": 3
}
```

---

## 6. 게이트웨이 브리지 구성

ChirpStack Gateway Bridge는 두 가지 모드로 각각 독립 컨테이너로 실행됩니다.

| 모드 | 포트 | 프로토콜 | 설정 파일 | 지원 게이트웨이 |
|------|------|----------|----------|----------------|
| Semtech UDP | 1700/UDP | UDP | chirpstack-gateway-bridge.toml | 대부분의 LoRa 게이트웨이 (DLOS8N 포함) |
| Basics Station | 3001/TCP | WebSocket | chirpstack-gateway-bridge-basicstation-kr920.toml | Basics Station 지원 게이트웨이 |

### MQTT 토픽 설정 (docker-compose.yml 환경변수)

```yaml
INTEGRATION__MQTT__EVENT_TOPIC_TEMPLATE=kr920/gateway/{{ .GatewayID }}/event/{{ .EventType }}
INTEGRATION__MQTT__STATE_TOPIC_TEMPLATE=kr920/gateway/{{ .GatewayID }}/state/{{ .StateType }}
INTEGRATION__MQTT__COMMAND_TOPIC_TEMPLATE=kr920/gateway/{{ .GatewayID }}/command/#
```

### Basics Station KR920 채널 구성

```toml
# chirpstack-gateway-bridge-basicstation-kr920.toml
[backend.basic_station]
  bind=":3001"
  region="KR920"
  frequency_min=920900000   # 920.9 MHz
  frequency_max=923300000   # 923.3 MHz

  [[backend.basic_station.concentrators]]
    [backend.basic_station.concentrators.multi_sf]
    frequencies=[
      922100000, 922300000, 922500000, 922700000,
      922900000, 923100000, 923300000
    ]
```

> **베트남 연동 시 주의:** 현재 Basics Station 설정은 KR920으로 되어 있습니다. 베트남 DLOS8N을 AS923-1로 연결하려면 `as923` Basics Station 설정 파일을 사용해야 합니다. Semtech UDP(:1700) 방식은 대역 자동 감지를 하므로 DLOS8N의 기본 연결 방식으로 권장합니다.

---

## 7. 대시보드 기능 (Flask :5000)

`dashboard/lora_dashboard.py` (1,341줄) — Python Flask 애플리케이션.  
MQTT 구독, gRPC 연동, SQLite 저장, 실시간 웹 UI를 단일 프로세스에서 처리합니다.

### 멀티스레드 구조

| 스레드 | 역할 | 주기 |
|--------|------|------|
| 메인 스레드 | Flask HTTP 서버 (포트 5000) | 상시 |
| MQTT 스레드 | Mosquitto 구독 + 이벤트 처리 | 상시 (이벤트 기반) |
| GW 폴러 스레드 | ChirpStack gRPC로 게이트웨이 목록 갱신 | 30초마다 |
| SQLite 쓰기 스레드 | 업링크 수신 시 DB 저장 (데몬 스레드) | 이벤트 발생 시 |

### 페이지 구성

| URL | 페이지명 | 주요 기능 |
|-----|---------|----------|
| `/` | 실시간 모니터링 | 2초 폴링 업링크 테이블, 게이트웨이 카드, SNR/충돌/SF 차트, Leaflet 지도, 노드 등록 모달 |
| `/history` | 수신 기록 | 30일 일별 패킷 수 차트, 날짜·노드 필터, 페이지네이션 이력 테이블 |
| `/nodes` | 노드 관리 | ChirpStack 디바이스 CRUD(등록/편집/삭제), 키 조회, 패킷 이력, 샘플 코드 생성 |

### JavaScript 폴링 전략

| 함수 | 주기 | 동작 |
|------|------|------|
| pollRecentEvents() | 2초 | `?since_id=N` 증분 요청으로 새 이벤트만 수신 (무 flicker) |
| refreshStaticData() | 60초 | 요약 통계, SF 분포, SNR 추이 갱신 |
| refreshGateways() | 60초 | 게이트웨이 카드 목록 갱신 |

### 의존 라이브러리 (requirements.txt)

```
flask==3.1.1
paho-mqtt==2.1.0
grpcio==1.76.0
chirpstack-api==4.15.0
protobuf==6.33.2
python-dotenv==1.2.2
```

---

## 8. REST API 엔드포인트 목록

대시보드 Flask 서버가 제공하는 17개 엔드포인트 (인증 없음, 내부 Docker 네트워크).

| 메서드 | 경로 | 설명 |
|--------|------|------|
| GET | `/api/events` | 최근 업링크 이벤트 목록 (`?since_id`, `?limit` 지원) |
| GET | `/api/summary` | 집계 통계 — 총 이벤트, 활성 노드/GW, 평균 SNR/RSSI, SF·주파수 분포 (30초 캐시) |
| GET | `/api/collisions` | 최근 50건 충돌 이벤트 + 원인별·디바이스별 통계 |
| GET | `/api/nodes` | 노드별 최근 수신 요약 (RSSI, SNR, SF, 주파수, 연결 GW, last seen) |
| GET | `/api/gateways` | 게이트웨이 정보 — ChirpStack gRPC + MQTT 이벤트 데이터 병합, GPS 포함 |
| GET | `/api/history` | 페이지네이션 이력 (`?date`, `?dev_eui`, `?page`, `?per_page`) |
| GET | `/api/history/devices` | 이력에 등장하는 디바이스 목록 |
| GET | `/api/history/stats` | 최근 30일 일별 패킷 수 (차트용) |
| GET | `/api/node-history/<dev_eui>` | 특정 노드의 최근 30건 패킷 |
| GET | `/api/applications` | ChirpStack 어플리케이션 목록 (gRPC) |
| GET | `/api/device-profiles` | ChirpStack 디바이스 프로파일 목록 (gRPC) |
| GET | `/api/nodes-list` | ChirpStack에 등록된 전체 디바이스 목록 (gRPC) |
| GET | `/api/node-keys/<dev_eui>` | 디바이스 JoinEUI, NwkKey, AppKey, GenAppKey 조회 (gRPC) |
| POST | `/api/register-node` | 새 디바이스 ChirpStack 등록 (gRPC CreateDevice + CreateDeviceKeys) |
| PUT | `/api/edit-node` | 디바이스 정보 수정 (gRPC UpdateDevice) |
| DELETE | `/api/delete-node/<dev_eui>` | ChirpStack에서 디바이스 삭제 (gRPC DeleteDevice) |
| DELETE | `/api/clear-node-events/<dev_eui>` | SQLite 패킷 기록 삭제 (fCnt 기준점 유지) |
| POST | `/api/reset-node-history/<dev_eui>` | 기록 전체 삭제 + fCnt 기준점 0으로 초기화 |

---

## 9. 충돌 감지 알고리즘

수신된 업링크 이벤트마다 5가지 알고리즘을 실행. 결과는 SQLite `collision_detected`, `collision_reasons` 컬럼에 저장.

| # | 알고리즘 | 감지 조건 | 기록 값 예시 |
|---|---------|----------|------------|
| 1 | FCnt 갭 감지 | 프레임 카운터가 1보다 많이 증가 (중간 패킷 유실) | `fCnt_gap_5` |
| 2 | deduplicationId 중복 | 동일한 MQTT dedup ID가 30초 내 재수신 | `dedup_duplicate` |
| 3 | 다중 게이트웨이 동일 FCnt | 동일 게이트웨이에서 같은 FCnt가 30초 내 재수신 | `multi_gw_2x` |
| 4 | CRC 오류 | 수신 게이트웨이 수 대비 CRC OK 수가 적음 | `crc_error_1` |
| 5 | 저 SNR 다중 수신 | 평균 SNR < −5 dB이면서 복수 게이트웨이 수신 | `low_snr_-7.2` |

---

## 10. 다운링크 ACK 자동 전송

업링크 수신 시 대시보드가 자동으로 ACK 다운링크를 전송합니다.

```
업링크 수신 (MQTT)
    │
    ▼
gRPC EnqueueDeviceQueueItem 호출
    ├─ dev_eui   = 송신 노드 EUI
    ├─ f_port    = 10  (DOWNLINK_FPORT 환경변수)
    ├─ data      = "{device_name} ack"  (UTF-8 인코딩)
    └─ confirmed = False  (DOWNLINK_CONFIRMED 환경변수)
    │
    ▼
ChirpStack → RX1/RX2 윈도우에서 게이트웨이로 전달
    │
    ▼
Gateway → LoRa RF 다운링크 → 노드 수신
```

```
# dashboard/.env
DOWNLINK_FPORT=10
DOWNLINK_CONFIRMED=False
```

---

## 11. 데이터베이스 구조

### PostgreSQL — ChirpStack 메타데이터

ChirpStack이 내부적으로 관리합니다. 직접 접근 대신 gRPC/REST API를 사용합니다.

| 저장 데이터 | 접근 방법 |
|------------|----------|
| 어플리케이션, 테넌트 | ChirpStack Web UI / gRPC |
| 디바이스 등록 정보, 프로파일 | gRPC DeviceService |
| 게이트웨이 등록 정보 | gRPC GatewayService |
| OTAA 조인 세션 키 | 내부 자동 관리 |

```
Host: postgres  /  DB: chirpstack  /  User: chirpstack  /  PW: chirpstack
초기화: pg_trgm, hstore 확장 자동 설치
```

### SQLite — 대시보드 수신 이력

경로: `/app/data/lora_history.db` (Docker 볼륨 `dashboarddata` 에 영구 저장)

**테이블: uplink_events**

| 컬럼 | 타입 | 설명 |
|------|------|------|
| id | INTEGER PK | 자동 증가 |
| time | TEXT NOT NULL | 수신 시각 (ISO 8601) |
| dev_eui | TEXT | 디바이스 EUI |
| dev_name | TEXT | 디바이스 이름 |
| dev_addr | TEXT | DevAddr (4바이트 세션 주소) |
| f_cnt | INTEGER | 정규화된 프레임 카운터 |
| frequency_mhz | REAL | 수신 주파수 (MHz) |
| sf | INTEGER | Spreading Factor (7–12) |
| bandwidth | INTEGER | 대역폭 (kHz) |
| code_rate | TEXT | 코드율 (예: "4/5") |
| dr | TEXT | Data Rate 문자열 |
| rssi | TEXT (JSON) | 게이트웨이별 RSSI 배열 |
| snr | TEXT (JSON) | 게이트웨이별 SNR 배열 |
| gateways | TEXT (JSON) | 수신 게이트웨이 ID 목록 |
| payload_hex | TEXT | 복호화된 원시 페이로드 (HEX) |
| payload_text | TEXT | 복호화된 원시 페이로드 (텍스트) |
| collision_detected | INTEGER | 충돌 감지 여부 (0/1) |
| collision_reasons | TEXT (JSON) | 충돌 원인 목록 |
| crc_ok_count | INTEGER | CRC OK 게이트웨이 수 |
| total_gateways | INTEGER | 총 수신 게이트웨이 수 |
| topic | TEXT | 원본 MQTT 토픽 |

인덱스: `time`, `dev_eui`, `collision_detected` 컬럼

**테이블: node_history_state**

| 컬럼 | 타입 | 설명 |
|------|------|------|
| dev_eui | TEXT PK | 디바이스 EUI |
| fcnt_base | INTEGER | FCnt 정규화 기준점 (최초 수신 FCnt) |
| reset_at | TEXT | 기준점 마지막 리셋 시각 |

### Redis — 세션 캐시

```
redis-server --save 300 1 --save 60 100 --appendonly no
# 300초마다 변경 1건 이상이면 스냅샷, 60초마다 100건 이상이면 스냅샷
# appendonly off — AOF 비활성화 (성능 우선)
```

---

## 12. 디바이스 프로파일

현재 ChirpStack에 등록된 테스트 프로파일 (`profile1_mod.json`) 실제 설정값.

| 항목 | 설정값 |
|------|--------|
| 프로파일명 | test |
| 지역 | KR920 |
| LoRaWAN 버전 | 1.0.3 / Revision A |
| 활성화 방식 | OTAA (Over-the-Air Activation) |
| Class B | 비활성 |
| Class C | 비활성 |
| ADR 알고리즘 | default (ChirpStack 기본) |
| 페이로드 코덱 | NONE (별도 JS 코덱 없음) |
| 업링크 인터벌 | 10초 |
| DevStatus 요청 주기 | 1 |
| 활성화 시 큐 삭제 | 활성 (flushQueueOnActivate=true) |
| 릴레이 기능 | 비활성 (isRelay=false) |
| 로밍 | 비활성 |
| 생성일 | 2026-05-20 |
| 최종 수정 | 2026-05-22 |

> **베트남 배포 전 필수 변경:** 현재 프로파일은 KR920으로 설정되어 있습니다. 베트남 AS923-1 노드를 등록하려면 Region을 `AS923`으로 지정한 새 디바이스 프로파일을 생성해야 합니다.

### 디바이스 등록 방법

- **웹 UI**: ChirpStack 대시보드 `:8080` → 디바이스 메뉴
- **커스텀 대시보드**: `:5000/nodes` → 노드 등록 모달 (DevEUI, AppKey 자동 생성)
- **REST API**: `POST /api/register-node`

---

## 13. 보안 구성 현황

| 구간 | 인증/암호화 | 현재 설정 | 비고 |
|------|------------|----------|------|
| LoRa RF (노드 → GW) | AES-128 암호화 | 항상 적용 (LoRaWAN 표준) | AppSKey + NwkSKey 쌍으로 암호화 |
| Mosquitto MQTT | 인증 없음 | `allow_anonymous true` | 내부 Docker 네트워크 한정 — 외부 노출 시 인증 추가 필요 |
| ChirpStack API | JWT Bearer Token | Tenant 키 + Admin 키 분리 발급 | 대시보드 .env에 저장 |
| PostgreSQL | 비밀번호 인증 | chirpstack / chirpstack | 내부 전용 — 외부 포트 미노출 |
| 대시보드 (:5000) | 인증 없음 | — | 현재 내부망 전용 — 외부 공개 시 인증 추가 필요 |
| WAN 구간 (GW → 서버) | MQTT over TLS 권장 | 현재 :1883 (TLS 없음) | 베트남 → 한국 구간은 TLS :8883 설정 필요 |

> **베트남 연결 시 TLS 필수:** Starlink를 통해 베트남 게이트웨이가 한국 서버로 연결할 때는 WAN 구간 암호화가 필요합니다. Mosquitto에 TLS 리스너(:8883) 추가 및 게이트웨이 Bridge 설정에서 `servers=["ssl://서버IP:8883"]`로 변경해야 합니다.

---

## 14. 운영 정책

### 자동 재시작

모든 컨테이너에 `restart: unless-stopped` 정책이 적용되어 있어, 서버 재부팅이나 컨테이너 오류 시 자동으로 재시작됩니다.

### 데이터 백업 / 내보내기

`scripts/` 폴더에 PowerShell 스크립트 제공:

| 스크립트 | 기능 |
|---------|------|
| export-public.ps1 | 공개 가능한 구성 파일만 내보내기 (민감 정보 제외) |
| backup-private.ps1 | API 토큰, .env 등 민감 정보 포함 전체 백업 |
| restore-private.ps1 | 백업에서 복원 |
| common.ps1 | 공통 유틸리티 함수 |

### 디바이스 프로파일 임포트

```bash
# Makefile — 공식 디바이스 프로파일 일괄 등록
make import-device-profiles
# chirpstack-device-profiles GitHub 저장소 클론 후 ChirpStack에 자동 등록
```

### 로그 확인

```bash
docker compose logs -f chirpstack                    # 네트워크 서버 로그
docker compose logs -f chirpstack-gateway-bridge     # UDP 브리지 로그
docker compose logs -f dashboard                     # 대시보드 로그
```

### 서비스 접근 URL

| 서비스 | URL | 용도 |
|--------|-----|------|
| ChirpStack Web UI | `http://[서버IP]:8080` | 네트워크 서버 관리 화면 |
| ChirpStack REST API | `http://[서버IP]:8090` | REST API (Swagger UI 포함) |
| 커스텀 대시보드 | `http://[서버IP]:5000` | 실시간 모니터링 및 노드 관리 |
| MQTT 브로커 | `mqtt://[서버IP]:1883` | 외부 MQTT 클라이언트 연결 |
| 게이트웨이 (UDP) | `udp://[서버IP]:1700` | Semtech UDP Packet Forwarder |
| 게이트웨이 (BS) | `ws://[서버IP]:3001` | Basics Station WebSocket |
