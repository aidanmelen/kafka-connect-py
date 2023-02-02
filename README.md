[![PyPI](https://img.shields.io/pypi/v/kafka_connect_py.svg)](https://pypi.org/project/kafka-connect-py/)
[![codecov](https://codecov.io/gh/aidanmelen/kafka-connect-py/branch/main/graph/badge.svg)](https://codecov.io/gh/aidanmelen/kafka-connect-py)

# Kafka Connect Python

The Kafka Connect REST API allows you to manage connectors that move data between Apache Kafka and other systems.

The Kafka Connect command line tool, also known as `kc` or `kafka-connect`, allows users to manage their Kafka Connect cluster and connectors. With this tool, users can retrieve information about the cluster and connectors, create new connectors, update existing connectors, delete connectors, and perform other actions.

This project aims to supported all features of the [Kafka Connect REST API](https://docs.confluent.io/platform/current/connect/references/restapi.html#kconnect-rest-interface).

## Install

```bash
pip install kafka-connect-py
```

## Command Line Usage

### Getting Basic Connect Cluster Information

To get basic Connect cluster information including the worker version, the commit it’s on, and its Kafka cluster ID, use the following command:

```bash
kc info
```

### Listing Installed Plugins

To list the plugins installed on the worker, use the following command:

```bash
kc plugins
```

To format the result of the installed plugin list for easier readability, pipe the output to the `jq` command:

```bash
kc plugins | jq
```

### Create a Connector Instance

To create a connector instance with JSON data containing the connector’s configuration:

```bash
kc update source-debezium-orders-00 -d '{
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "value.converter": "io.confluent.connect.json.JsonSchemaConverter",
    "value.converter.schemas.enable": "true",
    "value.converter.schema.registry.url": "'$SCHEMA_REGISTRY_URL'",
    "value.converter.basic.auth.credentials.source": "'$BASIC_AUTH_CREDENTIALS_SOURCE'",
    "value.converter.basic.auth.user.info": "'$SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO'",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "dbz",
    "database.server.id": "42",
    "database.server.name": "asgard",
    "table.whitelist": "demo.orders",
    "database.history.kafka.bootstrap.servers": "'$BOOTSTRAP_SERVERS'",
    "database.history.consumer.security.protocol": "SASL_SSL",
    "database.history.consumer.sasl.mechanism": "PLAIN",
    "database.history.consumer.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"'$CLOUD_KEY'\" password=\"'$CLOUD_SECRET'\";",
    "database.history.producer.security.protocol": "SASL_SSL",
    "database.history.producer.sasl.mechanism": "PLAIN",
    "database.history.producer.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"'$CLOUD_KEY'\" password=\"'$CLOUD_SECRET'\";",
    "database.history.kafka.topic": "dbhistory.demo",
    "topic.creation.default.replication.factor": "3",
    "topic.creation.default.partitions": "3",
    "decimal.handling.mode": "double",
    "include.schema.changes": "true",
    "transforms": "unwrap,addTopicPrefix",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.addTopicPrefix.type":"org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.addTopicPrefix.regex":"(.*)",
    "transforms.addTopicPrefix.replacement":"mysql-debezium-$1"
}'
```

Or create/update a connector instance with a JSON file:

```bash
kc update <connector> --config-file <config_file>
```

### Update a Connector

As mentioned above, if there’s a connector to update, you can use the `update` sub-command to amend the configuration (see [Create a Connector Instance](https://github.com/aidanmelen/kafka-connect-py/blob/main/README.md#create-a-connector-instance) above). Because update is used to both create and update connectors, it’s the standard command that you should use most of the time (which also means that you don’t have to completely rewrite your configs).

### List Connector Instances

Use the following command to list of all extant connectors:

```bash
kc list [--expand=info|status] [--pattern=regex] [--state=running|paused|unassigned|failed]
```


### Inspect Config and Status for a Connector

Inspect the config for a given connector as follows:

```bash
kc config sink-elastic-orders-00
```

You can also look at a connector’s status. While the config command shows a connector’s static configuration, the status shows the connector as a runtime entity:

```bash
kc status sink-elastic-orders-00
```

You can also use `list` with the `--expand=status` option to show the status of many connectors at once. We can filter down the response using a regex pattern and/or connector state. 

Use the following to show all connector names prefixed with the word `sink-` and that are in a `FAILED` connector state.

```bash
kc list --expand=status -p sink-.* -s failed
```

### Delete a Connector

If something is wrong in your setup and you don’t think a config change would help, or if you simply don’t need a connector to run anymore, you can delete it by name:

```bash
kc delete sink-elastic-orders-00
```

The `delete` sub-command also supports multiple deletions using the `--all` option. On its own it will apply the sub-command to all connectors.

The following will delete all connector names prefixed with the word `sink-` and that are in a `PAUSED` connector state.

```bash
kc delete --all --pattern sink-.* -s paused
```

The --all option is supported by several sub-commands, including `delete`, `restart`, `resume`, and `pause`. However, for better testing and control over the outcome of your actions, we recommend using the list filtering option before executing any of these sub-commands. This way, you can ensure that your filters are working as intended and avoid unintended consequences. To use list filtering, simply run the `list` sub-command and apply your filters.

### Inspect Task Details

The following command returns the connector status:

```bash
kc status source-debezium-orders-00 | jq
```

If your connector fails, the details of the failure belong to the task. So to inspect the problem, you’ll need to find the stack trace for the task. The task is the entity that is actually running the connector and converter code, so the state for the stack trace lives in it.

```bash
kc task-status source-debezium-orders-00 <task-id> | jq
```

### Restart the Connector and Tasks

If after inspecting a task, you have determined that it has failed and you have fixed the reason for the failure (perhaps restarted a database), you can restart the connector with the following:

```
kc restart source-debezium-orders-00
```

Keep in mind though that restarting the connector doesn’t restart all of its tasks. You will also need to restart the failed task and then get its status again as follows:

```bash
kc task-status source-debezium-orders-00 <task-id> 
```

What's more, you can restart the connector and all its failed tasks with the following:

```bash
kc restart source-debezium-orders-00 --include-tasks --failed-only
```

and check the status again:

```bash
kc status source-debezium-orders-00 | jq
```

### Pause and Resume a Connector

Unlike restarting, pausing a connector does pause its tasks. This happens asynchronously, though, so when you pause a connector, you can’t rely on it pausing all of its tasks at exactly the same time. The tasks are running in a thread pool, so there’s no fancy mechanism to make this happen simultaneously.

A connector and its tasks can be paused as follows:

```bash
kc pause source-debezium-orders-00
```

Just as easily, a connector and its tasks can be resumed:

```bash
kc resume source-debezium-orders-00
```

### Display All of a Connector’s Tasks

A convenient way to display all of a connector’s tasks at once is as follows:

```bash
kc list-tasks source-debezium-orders-00 | jq
```

This information is similar to what you can get from other APIs, but it is broken down by task, and configs for each are shown.
Get a List of Topics Used by a Connector

As of Apache Kafka 2.5, it is possible to get a list of topics used by a connector:

```bash
kc list-topics | jq
```

This shows the topics that a connector is consuming from or producing to. This may not be particularly useful for connectors that are consuming from or producing to a single topic. However, some developers, for example, use regular expressions for topic names in Connect, so this is a major benefit in situations where topic names are derived computationally.

This could also be useful with a source connector that is using SMTs to dynamically change the topic names to which it is producing.

## Python

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

# Get status for a connector
response = client.get_connector_status("my-connector")
print(json.dumps(response, indent=2))

# Restart a connector
response = client.restart_connector("my-connector")
print(response)

# Delete a connector
response = client.delete_connector("my-connector")
print(response)
```

## Tests

```bash
$ python3 -m unittest tests/test_kafka_connect.py -v
..............................
----------------------------------------------------------------------
Ran 30 tests in 0.034s

OK

$ coverage report
Name                                 Stmts   Miss  Cover
--------------------------------------------------------
src/kafka_connect/__init__.py            1      0   100%
src/kafka_connect/kafka_connect.py     206     44    79%
tests/test_kafka_connect.py            254      5    98%
--------------------------------------------------------
TOTAL                                  461     49    89%
```

## License

[Apache 2.0 License - aidanmelen/kafka-connect-py](https://github.com/aidanmelen/kafka-connect-py/blob/main/README.md)

## Credits

The entire [Command Line Usage](https://github.com/aidanmelen/kafka-connect-py/blob/main/README.md#command-line-usage) section was copied directly from the Confluence's [Kafka Connect’s REST API](https://developer.confluent.io/learn-kafka/kafka-connect/rest-api/) course.