# ChirpStack Docker Example

[English README](README.md)

이 저장소는 [Docker Compose](https://docs.docker.com/compose/) 기반으로
[ChirpStack](https://www.chirpstack.io) LoRaWAN Network Server(v4)를 실행하기 위한
예제 프로젝트입니다.

추가로 [`dashboard`](dashboard) 폴더에는 Flask 기반 커스텀 대시보드도 포함되어
있습니다.

**참고:** 이 `docker-compose.yml`은 테스트 및 개인 환경용 시작점으로 사용하는 것을
권장합니다. 실제 운영 환경에서는 추가 수정이 필요할 수 있습니다.

## 디렉터리 구성

* `docker-compose.yml`: 전체 서비스 실행 구성
* `configuration/chirpstack`: ChirpStack 설정 파일
* `configuration/chirpstack-gateway-bridge`: ChirpStack Gateway Bridge 설정 파일
* `configuration/mosquitto`: Mosquitto(MQTT broker) 설정 파일
* `configuration/postgresql/initdb`: PostgreSQL 초기화 스크립트
* `dashboard`: Flask 기반 LoRa 대시보드 코드

## 패키징/이전 워크플로

공개용 export, 개인용 migration backup/restore 가이드는 아래 문서를 보면 됩니다.

* [docs/package-workflows.ko.md](docs/package-workflows.ko.md)
* [docs/package-workflows.md](docs/package-workflows.md)

## 새 노트북에서 실행하기

1. 저장소를 clone 합니다.
2. `dashboard/.env.example` 파일을 복사해서 `dashboard/.env` 파일을 만듭니다.
3. `dashboard/.env` 안의 MQTT 주소, ChirpStack 주소, API 토큰을 환경에 맞게 수정합니다.
4. 아래 명령으로 컨테이너를 실행합니다.

```bash
docker compose up --build -d
```

실행 후 기본 접속 주소는 아래와 같습니다.

* ChirpStack UI: `http://localhost:8080`
* ChirpStack REST API: `http://localhost:8090`
* Dashboard: `http://localhost:5000`

## Git 공유 시 주의사항

이 저장소는 "개인정보 없는 버전" 기준으로 정리되어 있습니다.

* `dashboard/.env` 는 Git에 포함되지 않습니다.
* Docker named volume 데이터도 Git에 포함되지 않습니다.
* 따라서 다른 PC에서 실행할 때는 `.env` 를 새로 만들고, 데이터는 별도 백업이 필요합니다.

## 설정

이 구성은 여러 지역(region)에 대해 미리 설정되어 있습니다. 다음 두 방식 중 하나로
연동할 수 있습니다.

* ChirpStack Gateway Bridge 인스턴스를 MQTT broker(포트 `1883`)에 연결
* Semtech UDP Packet Forwarder 사용

주의할 점은 아래와 같습니다.

* MQTT topic 은 반드시 지역 prefix 를 포함해야 합니다.
* `configuration/chirpstack` 아래의 지역 설정 파일에서 `topic_prefix` 를 확인할 수 있습니다.
* protobuf marshaler 가 설정되어 있습니다.

이 구성에는 ChirpStack Gateway Bridge 서비스가 두 개 포함되어 있습니다.

* Semtech UDP Packet Forwarder 용 서비스: 포트 `1700`
* Basics Station 용 서비스: 포트 `3001`

기본값은 둘 다 `eu868` topic prefix 를 사용합니다.

### 지역 설정 변경

지역 설정을 바꿀 때는 아래 항목을 같이 확인해야 합니다.

* `configuration/chirpstack/chirpstack.toml` 의 `enabled_regions`
* `docker-compose.yml` 의 `INTEGRATION__MQTT__..._TOPIC_TEMPLATE`
* 사용할 `chirpstack-gateway-bridge-basicstation-*.toml` 파일

#### ChirpStack Gateway Bridge (UDP)

`docker-compose.yml` 안의 `INTEGRATION__..._TOPIC_TEMPLATE` 값에서 `eu868` 부분을
원하는 지역의 `topic_prefix` 로 변경해야 합니다.

#### ChirpStack Gateway Bridge (Basics Station)

`docker-compose.yml` 안에서 Basics Station 서비스가 참조하는 설정 파일을 원하는
지역용 파일로 바꿔야 합니다. 기본값은
`chirpstack-gateway-bridge-basicstation-eu868.toml` 입니다.

## 데이터 유지

PostgreSQL 과 Redis 데이터는 `docker-compose.yml` 에 정의된 Docker named volume 에
저장됩니다. 커스텀 대시보드의 SQLite 이력 데이터도 별도 Docker volume 에 저장됩니다.

## 요구사항

이 프로젝트를 사용하기 전에
[Docker](https://www.docker.com/community-edition) 가 설치되어 있어야 합니다.

## 디바이스 프로필 가져오기

선택 사항으로
[chirpstack-device-profiles](https://github.com/chirpstack/chirpstack-device-profiles)
저장소를 import 하려면 아래 명령을 실행하면 됩니다.

```bash
make import-device-profiles
```

이 명령은 저장소를 clone 한 뒤 ChirpStack import 명령을 실행합니다. 이 단계에서는
`make` 명령이 설치되어 있어야 합니다.

## 사용 방법

전체 스택을 실행하려면:

```bash
docker compose up --build -d
```

초기화가 끝나면 아래 주소로 접속할 수 있습니다.

* ChirpStack UI: `http://localhost:8080`
* ChirpStack REST API: `http://localhost:8090`
* Dashboard: `http://localhost:5000`

**참고:** 이 프로젝트에는
[ChirpStack REST API](https://github.com/chirpstack/chirpstack-rest-api)가 포함되어
있지만, 일반적으로는
[gRPC 인터페이스](https://www.chirpstack.io/docs/chirpstack/api/grpc.html)를
[REST 인터페이스](https://www.chirpstack.io/docs/chirpstack/api/rest.html)보다
우선해서 사용하는 것을 권장합니다.
