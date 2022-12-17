# Kafka Connect Python

The KafkaConnect class is a client for the Confluent Platform Kafka Connect REST API. 

The Kafka Connect REST API allows you to manage connectors that move data between Apache Kafka and other systems. The KafkaConnect class provides methods for getting information about the Kafka Connect cluster and its connectors, creating new connectors, updating existing connectors, and deleting connectors.

This module aims to supported all features of the [Kafka Connect REST API](https://docs.confluent.io/platform/current/connect/references/restapi.html#kconnect-rest-interface).

## Usage

This command retrieves the version and other details of the Kafka Connect cluster.

```bash
$ kc get-cluster
```

This command retrieves the details of a single connector.

```bash
$ kc get-connector <connector>
```

This command retrieves a list of active connectors. The `--expand' option can be used to retrieve additional information about the connectors, such as their status or metadata.

```bash
$ kc get-connectors [--expand=status|info]
```

This command creates a new connector using the configuration specified in the given file. If the connector already exists or a rebalance is in process, this command will return a status code of 409.

```bash
$ kc create-connector <config_file>
```

This command updates the configuration for an existing connector. If a rebalance is in process, this command will return a status code of 409.

```bash
$ kc update-connector <connector> <config_file>
```

This command retrieves the configuration of a connector.

```bash
$ kc get-connector <connector>
```

This command retrieves the config of a connector.

```bash
$ kc get-connector-config <connector>
```

This command retrieves the status of a connector.

```bash
$ kc get-connector-status <connector>
```

This command retrieves the tasks of a connector. The `--include-tasks' option can be used to include task information in the response.

```bash
$ kc get-connector-tasks <connector> [--include-tasks]
```

This command pauses a connector.

```bash
$ kc pause-connector <connector>
```

This command resumes a connector that was previously paused.

```bash
$ kc resume-connector <connector>
```

This command deletes a connector.

```bash
$ kc delete-connector <connector>
```

This command validates the configuration specified in the given file. If the configuration is valid, this command will return a status code of 200.

```bash
$ kc validate-connector-config <config_file>
```

This command retrieves metadata about the specified connector plugin.

```bash
$ kc get-connector-plugin <connector>
```

This command retrieves metadata about all available connector plugins.

```bash
$ kc get-connector-plugins
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