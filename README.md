# ChirpStack Docker Example

[한국어 문서](README.ko.md)

This repository contains a skeleton to set up the
[ChirpStack](https://www.chirpstack.io) open-source LoRaWAN Network Server (v4)
using [Docker Compose](https://docs.docker.com/compose/).

This repository also includes a custom Flask dashboard service under
[`dashboard`](dashboard).

**Note:** Please use this `docker-compose.yml` file as a starting point for
testing, but keep in mind that production usage may require additional changes.

## Directory Layout

* `docker-compose.yml`: Docker Compose file containing the services
* `configuration/chirpstack`: ChirpStack configuration files
* `configuration/chirpstack-gateway-bridge`: ChirpStack Gateway Bridge configuration files
* `configuration/mosquitto`: Mosquitto (MQTT broker) configuration files
* `configuration/postgresql/initdb`: PostgreSQL initialization scripts
* `dashboard`: Flask-based LoRa dashboard code

## Packaging Workflows

If you want ready-made packaging and migration scripts, see:

* [docs/package-workflows.md](docs/package-workflows.md)
* [docs/package-workflows.ko.md](docs/package-workflows.ko.md)

## Configuration

This setup is pre-configured for all regions. You can either connect a
ChirpStack Gateway Bridge instance (v3.14.0+) to the MQTT broker (port 1883) or
connect a Semtech UDP Packet Forwarder.

Please note:

* You must prefix the MQTT topic with the region.
* See the region configuration files in `configuration/chirpstack` for topic
  prefixes such as `eu868`, `us915_0`, `au915_0`, and `as923_2`.
* The protobuf marshaler is configured.

This setup also includes two ChirpStack Gateway Bridge instances:

* One for the Semtech UDP Packet Forwarder protocol on port `1700`
* One for the Basics Station protocol on port `3001`

By default, both use the `eu868` MQTT topic prefix.

### Reconfigure Regions

ChirpStack has at least one configuration of each region enabled. The
`enabled_regions` list is defined in `configuration/chirpstack/chirpstack.toml`.
Each entry refers to the `id` found in the corresponding `region_XXX.toml`
file. That file also defines the `topic_prefix`.

#### ChirpStack Gateway Bridge (UDP)

Within `docker-compose.yml`, replace the `eu868` prefix in the
`INTEGRATION__..._TOPIC_TEMPLATE` values with the MQTT `topic_prefix` of the
region you want to use.

#### ChirpStack Gateway Bridge (Basics Station)

Within `docker-compose.yml`, update the configuration file used by the
Basics Station bridge service. The default is
`chirpstack-gateway-bridge-basicstation-eu868.toml`.

## Data Persistence

PostgreSQL and Redis data are persisted in Docker named volumes defined in
`docker-compose.yml`. The custom dashboard also stores its SQLite history data
in a Docker volume.

## Requirements

Before using this project, make sure
[Docker](https://www.docker.com/community-edition) is installed.

## Importing Device Repository

To import the optional
[chirpstack-device-profiles](https://github.com/chirpstack/chirpstack-device-profiles)
repository, run:

```bash
make import-device-profiles
```

This clones the repository and executes the ChirpStack import command. You need
the `make` command installed for this step.

## Usage

To start the stack:

```bash
docker compose up --build -d
```

After all components are initialized, you should be able to access:

* ChirpStack UI: `http://localhost:8080`
* ChirpStack REST API: `http://localhost:8090`
* Dashboard: `http://localhost:5000`

**Note:** The project includes the
[ChirpStack REST API](https://github.com/chirpstack/chirpstack-rest-api), but
the [gRPC interface](https://www.chirpstack.io/docs/chirpstack/api/grpc.html)
is generally recommended over the
[REST interface](https://www.chirpstack.io/docs/chirpstack/api/rest.html).
