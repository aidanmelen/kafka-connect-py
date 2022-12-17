# Kafka Connect Python

The KafkaConnect class is a client for the Confluent Platform Kafka Connect REST API. 

The Kafka Connect REST API allows you to manage connectors that move data between Apache Kafka and other systems. The KafkaConnect class provides methods for getting information about the Kafka Connect cluster and its connectors, creating new connectors, updating existing connectors, and deleting connectors.

This module aims to supported all features of the [Kafka Connect REST API](https://docs.confluent.io/platform/current/connect/references/restapi.html#kconnect-rest-interface).

## Usage

Get the version and other details of the Kafka Connect cluster.

```bash
$ kc get-cluster
```

Get the details of a single connector.

```bash
$ kc get-connector <connector>
```

Get a list of active connectors. Optionally retrieves additional information about the connectors.

```bash
$ kc get-connectors
$ kc get-connectors --expand status
$ kc get-connectors --expand info
```

Create a new connector, returning the current connector info if successful. Return 409 (Conflict) if rebalance is in process, or if the connector already exists.

```bash
$ kc create-connector <config-file>
```

Create a new connector using the given configuration, or update the configuration for an existing connector. Returns information about the connector after the change has been made. Return 409 (Conflict) if rebalance is in process.

```bash
$ kc put-connector <connector> <config-file>
```

Gets the configuration of a connector.

```bash
$ kc get-connector <connector>
```

Gets the config of a connector.

```bash
$ kc get-connector-config <connector>
```

Gets the status of a connector.

```bash
$ kc get-connector-status <connect
```


### Python

```python
# Import the class
from kafka_connect import KafkaConnect

# Instantiate the client
client = KafkaConnect(endpoint="http://localhost:8083")

# Get the version and other details of the Kafka Connect cluster
cluster = client.get_cluster()
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