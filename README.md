# Kafka Connect Python

The Kafka Connect REST API allows you to manage connectors that move data between Apache Kafka and other systems.

The `kafka-connect` or `kc` command line tool provides commands for getting information about the Kafka Connect cluster and its connectors, creating new connectors, updating existing connectors, deleting connectors, etc.

This project aims to supported all features of the [Kafka Connect REST API](https://docs.confluent.io/platform/current/connect/references/restapi.html#kconnect-rest-interface).

## Install

```bash
pip install kafka-connect-py
```

## Command Line Usage

Get the version and other details of the Kafka Connect cluster.

```bash
kc info
```

Get a list of active connectors.

```bash
kc list
kc list [--expand=status|info]
```

Get the details of a single connector.

```bash
kc get <connector>
```

Get the status of a connector.

```bash
kc status <connector>
```

Get the config of a connector.

```bash
kc config <connector>
```

Create a new connector.

```bash
kc create <config-file>
```

Update the configuration for an existing connector.

```bash
kc update <connector> <config_file>
```

Restart a connector.

```bash
kc restart <connector>
```

Pause a connector.

```bash
kc pause <connector>
```

Resume a connector.

```bash
kc resume <connector>
```

Delete a connector.

```
kc delete <connector>
```

### Python

```python
# Import the class
from kafka_connect import KafkaConnect

# Instantiate the client
client = KafkaConnect(endpoint="http://localhost:8083")

# Get the version and other details of the Kafka Connect cluster
cluster = client.get_info()
print(cluster)

# Get a list of active connectors
connectors = client.get_connectors()
print(connectors)

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

```
python3 -m unittest tests/test_kafka_connect.py -v
```