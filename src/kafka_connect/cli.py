from .kafka_connect import KafkaConnect

import click
import json

@click.group()
@click.version_option()
@click.option('--endpoint', default='http://localhost:8083', envvar='KAFKA_CONNECT_ENDPOINT', help='The base URL for the Kafka Connect REST API.')
@click.pass_context
def cli(ctx, endpoint):
    """A command-line client for the Confluent Platform Kafka Connect REST API."""
    ctx.obj = KafkaConnect(endpoint)

@cli.command()
@click.pass_obj
def get_cluster(kafka_connect):
    """Get the version and other details of the Kafka Connect cluster.
    
    Examples:
        $ kc get-cluster
    """
    cluster = kafka_connect.get_cluster()
    click.echo(json.dumps(cluster))

@cli.command()
@click.argument('connector')
@click.pass_obj
def get_connector(kafka_connect, connector):
    """Get the details of a single connector."""
    connectors = kafka_connect.get_connectors(connector)
    click.echo(json.dumps(connectors))

@cli.command()
@click.option('--expand', type=click.Choice(['status', 'info']), envvar='KAFKA_CONNECT_EXPAND', help='Optional parameter that retrieves additional information about the connectors.')
@click.pass_obj
def get_connectors(kafka_connect, expand):
    """Get a list of active connectors."""
    connectors = kafka_connect.get_connectors(expand=expand)
    click.echo(json.dumps(connectors))

@cli.command()
@click.argument('config', type=click.File('r'))
@click.pass_obj
def create_connector(kafka_connect, config):
    """Create a new connector, returning the current connector info if successful. Return 409 (Conflict) if rebalance is in process, or if the connector already exists."""
    config_data = config.read()
    response = kafka_connect.create_connector(json.loads(config_data))
    click.echo(json.dumps(response))

@cli.command()
@click.argument('connector')
@click.argument('config', type=click.File('r'))
@click.pass_obj
def put_connector(kafka_connect, connector, config):
    """Create a new connector using the given configuration, or update the configuration for an existing connector. Returns information about the connector after the change has been made. Return 409 (Conflict) if rebalance is in process."""
    config_data = config.read()
    response = kafka_connect.put_connector(connector, json.loads(config_data))
    click.echo(json.dumps(response))

@cli.command()
@click.argument('connector')
@click.pass_obj
def get_connector(kafka_connect, connector):
    """Gets the configuration of a connector."""
    response = kafka_connect.get_connector(connector)
    click.echo(json.dumps(response))

@cli.command()
@click.argument('connector')
@click.pass_obj
def get_connector_config(kafka_connect, connector):
    """Gets the config of a connector."""
    response = kafka_connect.get_connector_config(connector)
    click.echo(json.dumps(response))

@cli.command()
@click.argument('connector')
@click.pass_obj
def get_connector_status(kafka_connect, connector):
    """Gets the status of a connector."""
    response = kafka_connect.get_connector_status(connector)
    click.echo(json.dumps(response))

@cli.command()
@click.argument('connector')
@click.option('--include-tasks', is_flag=True, default=False, envvar='KAFKA_CONNECT_INCLUDE_TASKS', help='Whether to include the Task objects in the restart operation.')
@click.option('--only-failed', is_flag=True, default=False, envvar='KAFKA_CONNECT_ONLY_FAILED', help='Whether to restart only failed Task objects.')
@click.pass_obj
def restart_connector(kafka_connect, connector, include_tasks, only_failed):
    """Restart a connector."""
    response = kafka_connect.restart_connector(connector, include_tasks=include_tasks, only_failed=only_failed)
    click.echo(json.dumps(response))

@cli.command()
@click.argument('connector')
@click.pass_obj
def pause_connector(kafka_connect, connector):
    """Pauses a connector."""
    response = kafka_connect.pause_connector(connector)
    click.echo(json.dumps(response))

@cli.command()
@click.argument('connector')
@click.pass_obj
def resume_connector(kafka_connect, connector):
    """Resumes a connector."""
    response = kafka_connect.resume_connector(connector)
    click.echo

@cli.command()
@click.argument('connector')
@click.pass_obj
def delete_connector(kafka_connect, connector):
    """Deletes a connector."""
    response = kafka_connect.delete_connector(connector)
    click.echo

@cli.command()
@click.argument('connector')
@click.option('--include-tasks', is_flag=True, default=False, envvar='KAFKA_CONNECT_INCLUDE_TASKS', help='Whether to include the Task objects in the restart operation.')
@click.pass_obj
def get_connector_tasks(kafka_connect, connector):
    """Gets the list of tasks associated with a connector."""
    response = kafka_connect.get_connector_tasks(connector)
    click.echo(json.dumps(response))

@cli.command()
@click.argument('connector')
@click.argument('task_id')
@click.pass_obj
def get_connector_task(kafka_connect, connector, task_id):
    """Gets the configuration of a task associated with a connector."""
    response = kafka_connect.get_connector_task(connector, task_id)
    click.echo(json.dumps(response))

@cli.command()
@click.argument('connector')
@click.argument('task_id')
@click.pass_obj
def get_connector_task_status(kafka_connect, connector, task_id):
    """Gets the status of a task associated with a connector."""
    response = kafka_connect.get_connector_task_status(connector, task_id)
    click.echo(json.dumps(response))

@cli.command()
@click.argument('connector')
@click.argument('task_id')
@click.pass_obj
def restart_connector_task(kafka_connect, connector, task_id):
    """Restart a specific task of a connector."""
    response = kafka_connect.restart_connector_task(connector, task_id)
    click.echo(json.dumps(response))

@cli.command()
@click.argument('connector')
@click.pass_obj
def get_connector_topics(kafka_connect, connector):
    """Get the list of topics for a connector."""
    response = kafka_connect.get_connector_topics(connector)
    click.echo

@cli.command()
@click.argument('connector')
@click.pass_obj
def reset_connector_topics(kafka_connect, connector):
    """Reset the list of topics for a connector."""
    response = kafka_connect.reset_connector_topics(connector)
    click.echo

@cli.command()
@click.pass_obj
def get_connector_plugins(kafka_connect):
    """Get the list of connector plugins."""
    response = kafka_connect.get_connector_plugins()
    click.echo

@cli.command()
@click.argument('plugin')
@click.argument('config', type=click.File('r'))
@click.pass_obj
def validate_connector_config(kafka_connect, plugin, config):
    """Validate the configuration for a specific connector plugin."""
    config_data = config.read()
    response = kafka_connect.validate_connector_config(plugin, json.loads(config_data))
    click.echo(json.dumps(response))
