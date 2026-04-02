# ChirpStack Docker example

This repository contains a skeleton to setup the [ChirpStack](https://www.chirpstack.io)
open-source LoRaWAN Network Server (v4) using [Docker Compose](https://docs.docker.com/compose/).

**Note:** Please use this `docker-compose.yml` file as a starting point for testing
but keep in mind that for production usage it might need modifications. 

## Korean Guide

이 저장소는 Docker Compose 기반으로 ChirpStack v4, MQTT broker, PostgreSQL, Redis,
REST API, 그리고 커스텀 대시보드를 함께 실행하기 위한 예제 프로젝트입니다.

### What Is Included

* `docker-compose.yml`: 전체 서비스 실행 구성
* `configuration/chirpstack`: ChirpStack 설정 파일
* `configuration/chirpstack-gateway-bridge`: Gateway Bridge 설정 파일
* `configuration/mosquitto`: MQTT broker 설정 파일
* `dashboard`: Flask 기반 LoRa 대시보드 코드

### Quick Start On A New Laptop

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

### Notes For Git Sharing

이 저장소는 "개인정보 없는 버전" 기준으로 정리되어 있습니다.

* `dashboard/.env` 는 Git에 포함되지 않습니다.
* Docker named volume 데이터도 Git에 포함되지 않습니다.
* 따라서 다른 PC에서 실행할 때는 `.env` 를 새로 만들고, 데이터는 별도 백업이 필요합니다.

### Region Configuration

지역 설정을 바꿀 때는 아래 항목을 같이 확인해야 합니다.

* `configuration/chirpstack/chirpstack.toml` 의 `enabled_regions`
* `docker-compose.yml` 의 `INTEGRATION__MQTT__..._TOPIC_TEMPLATE`
* `chirpstack-gateway-bridge-basicstation-*.toml` 중 사용할 지역 파일

## Directory layout

* `docker-compose.yml`: the docker-compose file containing the services
* `configuration/chirpstack`: directory containing the ChirpStack configuration files
* `configuration/chirpstack-gateway-bridge`: directory containing the ChirpStack Gateway Bridge configuration
* `configuration/mosquitto`: directory containing the Mosquitto (MQTT broker) configuration
* `configuration/postgresql/initdb/`: directory containing PostgreSQL initialization scripts

## Configuration

This setup is pre-configured for all regions. You can either connect a ChirpStack Gateway Bridge
instance (v3.14.0+) to the MQTT broker (port 1883) or connect a Semtech UDP Packet Forwarder.
Please note that:

* You must prefix the MQTT topic with the region.
  Please see the region configuration files in the `configuration/chirpstack` for a list
  of topic prefixes (e.g. eu868, us915_0, au915_0, as923_2, ...).
* The protobuf marshaler is configured.

This setup also comes with two instances of the ChirpStack Gateway Bridge. One
is configured to handle the Semtech UDP Packet Forwarder data (port 1700), the
other is configured to handle the Basics Station protocol (port 3001). Both
instances are by default configured for EU868 (using the `eu868` MQTT topic
prefix).

### Reconfigure regions

ChirpStack has at least one configuration of each region enabled. You will find
the list of `enabled_regions` in `configuration/chirpstack/chirpstack.toml`.
Each entry in `enabled_regions` refers to the `id` that can be found in the
`region_XXX.toml` file. This `region_XXX.toml` also contains a `topic_prefix`
configuration which you need to configure the ChirpStack Gateway Bridge
UDP instance (see below).

#### ChirpStack Gateway Bridge (UDP)

Within the `docker-compose.yml` file, you must replace the `eu868` prefix in the
`INTEGRATION__..._TOPIC_TEMPLATE` configuration with the MQTT `topic_prefix` of
the region you would like to use (e.g. `us915_0`, `au915_0`, `in865`, ...).

#### ChirpStack Gateway Bridge (Basics Station)

Within the `docker-compose.yml` file, you must update the configuration file
that the ChirpStack Gateway Bridge instance must used. The default is
`chirpstack-gateway-bridge-basicstation-eu868.toml`. For available
configuration files, please see the `configuration/chirpstack-gateway-bridge`
directory.

# Data persistence

PostgreSQL and Redis data is persisted in Docker volumes, see the `docker-compose.yml`
`volumes` definition.

## Requirements

Before using this `docker-compose.yml` file, make sure you have [Docker](https://www.docker.com/community-edition)
installed.

## Importing device repository

To import the [chirpstack-device-profiles](https://github.com/chirpstack/chirpstack-device-profiles)
repository (optional step), run the following command:

```bash
make import-device-profiles
```

This will clone the `chirpstack-device-profiles` repository and execute the import command of ChirpStack.
Please note that for this step you need to have the `make` command installed.

## Usage

To start the ChirpStack simply run:

```bash
$ docker compose up
```

After all the components have been initialized and started, you should be able
to open http://localhost:8080/ in your browser.

##

The example includes the [ChirpStack REST API](https://github.com/chirpstack/chirpstack-rest-api).
You should be able to access the UI by opening http://localhost:8090 in your browser.

**Note:** It is recommended to use the [gRPC](https://www.chirpstack.io/docs/chirpstack/api/grpc.html)
interface over the [REST](https://www.chirpstack.io/docs/chirpstack/api/rest.html) interface.
