# Kafka Connect Python

The Kafka Connect REST API allows you to manage connectors that move data between Apache Kafka and other systems.

The Kafka Connect command line tool, also known as `kc` or `kafka-connect`, allows users to manage their Kafka Connect cluster and connectors. With this tool, users can retrieve information about the cluster and connectors, create new connectors, update existing connectors, delete connectors, and perform other actions.

This project aims to supported all features of the [Kafka Connect REST API](https://docs.confluent.io/platform/current/connect/references/restapi.html#kconnect-rest-interface).

## Install

```bash
pip install kafka-connect-py
```


## Command Line Usage

This CLI tool is written with [Python Click](https://click.palletsprojects.com/en/latest/). 

#### Group options

Connect to a custom endpoint.

```bash
kc --url https://connect.example.com <sub-command>
```

Connect with basic auth.

```bash
kc --auth="thisismyusername:thisismypass" <sub-command>
```

Connect with insecure SSL certificate.

```bash
kc --no-ssl-verify <sub-command>
```

Change log level.

```bash
kc --log-level=[critical|error|warning|info|debug|notset] <sub-command>
```

Please see [Click: Commands and Groups](https://click.palletsprojects.com/en/8.1.x/commands/#commands-and-groups) for more information.

#### Get Kafka Connect cluster info

```bash
kc info
```

#### Get a list of all connectors

```bash
kc list [--expand=status|info] [--pattern=regex] [--state=running|paused|unassigned|failed]
```

#### Get the details of a single connector

```bash
kc get <connector>
```

#### Get the config of a connector

```bash
kc config <connector>
```

#### Create a new connector with a JSON file

```bash
kc create --config-file <config-file>
```

#### Create a new connector with inline JSON data

```bash
kc create --config-data <config-data>
```

#### Update the configuration for an existing connector with a JSON file

```bash
kc update <connector> --config-file <config_file>
```

#### Update the configuration for an existing connector with inline JSON data

```bash
kc create <connector> --config-data <config-data>
```

#### Restart a connector

```bash
kc restart <connector> [--include-tasks] [--only-failed] | jq
```

#### Restart all connectors

```bash
kc restart --all [--pattern=regex] [--state=running|paused|unassigned|failed] [--include-tasks] [--only-failed]
```
The `state` targets the connector status whereas `--include-tasks` and `--only-failed` target connector tasks.

#### Pause a connector

```bash
kc pause <connector>
```

#### Pause all connectors

```bash
kc pause --all [--pattern=regex] [--state=running|paused|unassigned|failed]
```

#### Resume a connector

```bash
kc resume <connector>
```

#### Resume all connectors

```bash
kc resume --all [--pattern=regex] [--state=running|paused|unassigned|failed]
```

#### Delete a connector

```bash
kc delete <connector>
```

#### Delete all connectors

```bash
kc delete --all [--pattern=regex] [--state=running|paused|unassigned|failed]
```

### Python

```python
# Import the class
from kafka_connect import KafkaConnect

import json

# Instantiate the client
client = KafkaConnect(url="http://localhost:8083")

# Get the version and other details of the Kafka Connect cluster
cluster = client.get_cluster_info()
print(cluster)

# Get a list of active connectors
connectors = client.list_connectors(expand="status")
print(json.dumps(connectors, indent=2))

# Create a new connector
config = {
    "name": "my-connector",
    "config": {
        "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
        "tasks.max": "1",
        "connection.url": "jdbc:postgresql://localhost:5432/mydatabase",
        "connection.user": "myuser",
        "connection.password": "mypassword",
        "table.whitelist": "mytable",
        "mode": "timestamp+incrementing",
        "timestamp.column.name": "modified_at",
        "validate.non.null": "false",
        "incrementing.column.name": "id",
        "topic.prefix": "my-connector-",
    },
}
response = client.create_connector(config)
print(response)

# Update an existing connector
new_config = {
    "config": {
        "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
        "tasks.max": "1",
        "connection.url": "jdbc:postgresql://localhost:5432/mydatabase",
        "connection.user": "myuser",
        "connection.password": "mypassword",
        "table.whitelist": "mytable",
        "mode": "timestamp+incrementing",
        "timestamp.column.name": "modified_at",
        "validate.non.null": "false",
        "incrementing.column.name": "id",
        "topic.prefix": "my-connector-",
    },
}
response = client.update_connector("my-connector", new_config)
print(response)

# Restart a connector
response = client.restart_connector("my-connector")
print(response)

# Delete a connector
response = client.delete_connector("my-connector")
print(response)
```

## Tests

```bash
python3 -m unittest tests/test_kafka_connect.py -v
```
