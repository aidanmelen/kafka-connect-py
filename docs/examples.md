## Command Line Usage

This CLI tool is written with [Python Click](https://click.palletsprojects.com/en/latest/). 

### Main Command

#### Connect to a custom endpoint

```bash
kc --url https://connect.example.com <sub-command>
```

#### Connect with basic authentication

```bash
kc --auth="username:password" <sub-command>
```

#### Connect with insecure SSL certificate

```bash
kc --no-ssl-verify <sub-command>
```

#### Change log level

```bash
kc --log-level=[critical|error|warning|info|debug|notset] <sub-command>
```

This only impacts `kc` command line logging and has not effect on [Kafka Connect logging](https://docs.confluent.io/platform/current/connect/logging.html#kconnect-long-logging).

### Sub Command

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
```

in the form of this blog post: https://developer.confluent.io/learn-kafka/kafka-connect/rest-api/