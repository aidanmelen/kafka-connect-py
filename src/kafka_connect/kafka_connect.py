from requests.exceptions import HTTPError, ConnectionError, JSONDecodeError

import json
import logging
import re
import requests


class KafkaConnect():
    """A client for the Confluent Platform Kafka Connect REST API.
    Args:
        endpoint (str): The base URL for the Kafka Connect REST API.
        auth (str): A colon-delimited string of `username` and `password` to use for authenticating with the Kafka Connect REST API.
        ssl_verify (bool): Whether to verify the SSL certificate when making requests to the Kafka Connect REST API. Defaults to True.
        logger (logging.Logger): The logger to be used. If not specified, a new logger will be created.
    """
    def __init__(self, endpoint="http://localhost:8083", auth=None, ssl_verify=True, logger=None):
        self.endpoint = endpoint
        self.headers = {"Content-Type": "application/json"}

        # Split the auth string into username and password and store them as a tuple
        if auth:
            if ':' not in auth:
                raise ValueError("Invalid auth string. Expected a colon-delimited string of `username` and `password`.")
            username, password = auth.split(':')
            self.auth = (username.strip(), password.strip())
        else:
            self.auth = None

        # If ssl_verify is False, disable SSL warnings
        if not ssl_verify:
            import urllib3
            urllib3.disable_warnings()
        self.verify = ssl_verify

        self.logger = logger if logger else logging.getLogger()

    def get_cluster_info(self):
        """Get the version and other details of the Kafka Connect cluster.
        Returns:
            Dict[str, str]: The details of the cluster, including its version, commit ID, and Kafka cluster ID.
        """
        self.logger.info("Getting cluster details")
        url = f"{self.endpoint}"
        response = requests.get(url, auth=self.auth, verify=self.verify)
        response.raise_for_status()
        return response.json()

    def list_connectors(self, expand=None, connector_pattern=None):
        """Get the list of connectors.
        Args:
            expand (str): Optional parameter that retrieves additional information about the connectors.
                Valid values are "status" and "info".
            connector_pattern (str): The regex pattern to match the connector name. If not provided, all connectors will be listed.
        Returns:
            list or dict: The list of connector names or dictionary of connector names and its details.
        """
        self.logger.info("Listing connectors")
        url = f"{self.endpoint}/connectors"
        params = {"expand": expand}
        response = requests.get(url, auth=self.auth, verify=self.verify, params=params)
        response.raise_for_status()
        connectors = response.json()

        if expand:
            filtered_connectors = {}
            for connector, data in connectors.items():
                if connector_pattern:
                    if not re.match(connector_pattern, connector):
                        continue
                filtered_connectors[connector] = data
            return filtered_connectors
        else:
            filtered_connectors = []
            for connector in connectors:
                if connector_pattern:
                    if not re.match(connector_pattern, connector):
                        continue
                filtered_connectors.append(connector)
            return filtered_connectors
    
    def get_connector(self, connector):
        """Get the details of a single connector.
        Args:
            connector (str): The name of the connector.
        Returns:
            Dict[str, Any]: The details of the connector.
        """
        self.logger.info(f"Getting connector: {connector}")
        url = f"{self.endpoint}/connectors/{connector}"
        response = requests.get(url, auth=self.auth, verify=self.verify)
        response.raise_for_status()
        return response.json()

    def get_all_connectors(self, connector_pattern=None):
        """Get the details of all connectors.
        Args:
            connector_pattern (str): The regex pattern to match the connector name. If not provided, all connectors will be retrieved.
        Returns:
            Dict[str, Dict[str, Any]]: A dictionary of responses, where the keys are the connector names and the values are the responses.
        """
        self.logger.info(f"Get the details of all connectors{' matching the pattern: ' + connector_pattern if connector_pattern else ''}")

        responses = {}
        for connector in self.list_connectors():
            self.logger.debug(f"Filtering {connector}")

            if connector_pattern and not re.match(connector_pattern, connector):
                continue

            responses[connector] = self.get_connector(connector)

        return responses

    def create_connector(self, config):
        """Create a new connector.
        Args:
            config (Dict[str, Any]): The configuration for the connector.
        Returns:
            Dict[str, Any]: The response from the REST API, or an empty dictionary if the response is null or if there is a JSONDecodeError.
        Raises:
            HTTPError: If the REST API responds with a non-200 status code.
        """
        self.logger.info(f"Creating connector: {config.get('name')}")
        url = f"{self.endpoint}/connectors"
        response = requests.post(url, auth=self.auth, verify=self.verify, headers=self.headers, data=json.dumps(config))
        
        if response.status_code == 409:
            self.logger.error("Connector already exists or rebalance is in process.")
        
        response.raise_for_status()
        try:
            data = response.json()
        except JSONDecodeError:
            data = None
        return data

    def update_connector(self, connector, config):
        """Update an existing connector.
        Args:
            connector (str): The name of the connector.
            config (Dict[str, Any]): The new configuration for the connector.
        Returns:
            Dict[str, Any]: The response from the REST API, or an empty dictionary if the response is null or if there is a JSONDecodeError.
        """
    
        if "config" in config:
            self.logger.error('The payload is not wrapped in {"config": {}} as in the POST request. The config is directly provided.')

        self.logger.info(f"Updating connector: {connector}")
        url = f"{self.endpoint}/connectors/{connector}/config"
        response = requests.put(url, headers=self.headers, auth=self.auth, verify=self.verify, data=json.dumps(config))

        if response.status_code == 409:
            self.logger.error("Connector rebalance is in process.")
        else:
            return response.json()

        response.raise_for_status()
        try:
            data = response.json()
        except JSONDecodeError:
            data = None
        return data

    def get_connector_config(self, connector):
        """Get the configuration of a single connector.
        Args:
            connector (str): The name of the connector.
        Returns:
            Dict[str, Any]: The configuration of the connector.
        """
        self.logger.info(f"Getting connector config: {connector}")
        url = f"{self.endpoint}/connectors/{connector}/config"
        response = requests.get(url, auth=self.auth, verify=self.verify)
        response.raise_for_status()
        return response.json()
    
    def get_all_connector_configs(self, connector_pattern=None):
        """Get the config of all connectors.
        Args:
            connector_pattern (str): The regex pattern to match the connector name. If not provided, all connector configs will be retrieved.
        Returns:
            Dict[str, Dict[str, Any]]: A dictionary of responses, where the keys are the connector names and the values are the responses.
        """
        self.logger.info(f"Get the config of all connectors{' matching the pattern: ' + connector_pattern if connector_pattern else ''}")

        responses = {}
        for connector in self.list_connectors():
            self.logger.debug(f"Filtering {connector}")

            if connector_pattern and not re.match(connector_pattern, connector):
                continue

            responses[connector] = self.get_connector_config(connector)

        return responses

    def get_connector_status(self, connector):
        """Get the status of a single connector.
        Args:
            connector (str): The name of the connector.
        Returns:
            Dict[str, Any]: The status of the connector.
        """
        self.logger.info(f"Getting connector status: {connector}")
        url = f"{self.endpoint}/connectors/{connector}/status"
        response = requests.get(url, auth=self.auth, verify=self.verify)
        response.raise_for_status()
        return response.json()
    
    def get_all_connector_statuses(self, connector_pattern=None):
        """Get the status of all connectors.
        Args:
            connector_pattern (str): The regex pattern to match the connector name. If not provided, all connector statuses will be retrieved.
        Returns:
            Dict[str, Dict[str, Any]]: A dictionary of responses, where the keys are the connector names and the values are the responses.
        """
        self.logger.info(f"Get the status of all connectors{' matching the pattern: ' + connector_pattern if connector_pattern else ''}")

        responses = {}
        for connector, status in self.list_connectors(expand="status").items():
            self.logger.debug(f"Filtering {connector}")

            if connector_pattern and not re.match(connector_pattern, connector):
                continue

            responses[connector] = status

        return responses

    def restart_connector(self, connector, include_tasks=False, only_failed=False):
        """Restart a single connector.
        Args:
            connector (str): The name of the connector.
            include_tasks (bool): If `True`, the tasks of the connector will also be restarted.
                Defaults to `False`.
            only_failed (bool): Whether to restart only failed Task objects. Defaults to `False`.
        Returns:
            Dict[str, Any]: The response from the REST API, or an empty dictionary if the response is null or if there is a JSONDecodeError.
        """
        self.logger.info(f"Restarting connector: {connector}")
        url = f"{self.endpoint}/connectors/{connector}/restart"
        params = {"includeTasks": include_tasks, "onlyFailed": only_failed}
        response = requests.post(url, auth=self.auth, verify=self.verify, params=params)
        try:
            data = response.json()
        except JSONDecodeError:
            data = None
        
        if response.status_code == 200:
            self.logger.info("Connector restarted successfully.")
            return data
        elif response.status_code == 202:
            self.logger.info("Connector restart request accepted.")
            return data
        elif response.status_code == 204:
            self.logger.info("Connector restart request successful, but no response body returned.")
            return data
        elif response.status_code == 404:
            self.logger.error("Connector not found.")
            raise HTTPError(response.text)
        elif response.status_code == 409:
            self.logger.error("Rebalance needed to restart connector.")
            raise HTTPError(response.text)
        elif response.status_code == 500:
            self.logger.error("Connector restart request timed out.")
            raise HTTPError(response.text)
        
        response.raise_for_status()
        return data
    
    def restart_all_connectors(self, include_tasks=False, only_failed=False, connector_pattern=None):
        """Restart all connectors.
        Args:
            include_tasks (bool): Whether to include tasks when restarting the connector. Default is False.
            only_failed (bool): Whether to only restart failed tasks. Default is False.
            connector_pattern (str): The regex pattern to match the connector name. If not provided, all connectors will be restarted.
        Returns:
            Dict[str, Dict[str, Any]]: A dictionary of responses, where the keys are the connector names and the values are the responses.
        """
        self.logger.info(f"Restarting all connectors{' matching the pattern: ' + connector_pattern if connector_pattern else ''}")

        responses = {}
        for connector, status in self.list_connectors(expand="status").items():
            self.logger.debug(f"Filtering {connector} connector: {status}")

            if connector_pattern and not re.match(connector_pattern, connector):
                continue

            responses[connector] = self.restart_connector(connector, include_tasks, only_failed)

        return responses

    def pause_connector(self, connector):
        """Pause a single connector.
        Args:
            connector (str): The name of the connector.
        Returns:
            Dict[str, Any]: The response from the REST API, or an empty dictionary if the response is null or if there is a JSONDecodeError.
        """
        self.logger.info(f"Pausing connector: {connector}")
        url = f"{self.endpoint}/connectors/{connector}/pause"
        response = requests.put(url, auth=self.auth, verify=self.verify)
        response.raise_for_status()
        try:
            data = response.json()
        except JSONDecodeError:
            data = None
        return data
    
    def pause_all_connectors(self, connector_pattern=None):
        """Pause all connectors.
        Args:
            connector_pattern (str): The regex pattern to match the connector name. If not provided, all running connectors will be paused.
        Returns:
            Dict[str, Dict[str, Any]]: A dictionary of responses, where the keys are the connector names and the values are the responses.
        """
        self.logger.info(f"Pausing all running connectors{' matching the pattern: ' + connector_pattern if connector_pattern else ''}")
        
        responses = {}
        for connector, status in self.list_connectors(expand="status").items():
            self.logger.debug(f"Filtering {connector} connector: {status}")

            if connector_pattern and not re.match(connector_pattern, connector):
                continue

            if status.get("status", {}).get("connector", {}).get("state", None).upper() == "RUNNING":
                responses[connector] = self.pause_connector(connector)
            else:
                self.logger.debug(f"Skipping the {connector} connector because it is not in a RUNNING state.")

        return responses

    def resume_connector(self, connector):
        """Resume a single connector.
        Args:
            connector (str): The name of the connector.
        Returns:
            Dict[str, Any]: The response from the REST API, or an empty dictionary if the response is null or if there is a JSONDecodeError.
        """
        self.logger.info(f"Resuming connector: {connector}")
        url = f"{self.endpoint}/connectors/{connector}/resume"
        response = requests.put(url, auth=self.auth, verify=self.verify)
        response.raise_for_status()
        try:
            data = response.json()
        except JSONDecodeError:
            data = None
        return data

    def resume_all_connectors(self, connector_pattern=None):
        """Resume all connectors.
        Args:
            connector_pattern (str): The regex pattern to match the connector name. If not provided, all paused connectors will be resumed.
        Returns:
            Dict[str, Dict[str, Any]]: A dictionary of responses, where the keys are the connector names and the values are the responses.
        """
        self.logger.info(f"Resuming all paused connectors{' matching the pattern: ' + connector_pattern if connector_pattern else ''}")
        responses = {}
        for connector, status in self.list_connectors(expand="status").items():
            self.logger.debug(f"Filtering {connector} connector: {status}")

            if connector_pattern and not re.match(connector_pattern, connector):
                continue

            if status.get("status", {}).get("connector", {}).get("state", None).upper() == "PAUSED":
                responses[connector] = self.resume_connector(connector)
            else:
                self.logger.debug(f"Skipping the {connector} connector because it is not in a PAUSED state.")

        return responses

    def delete_connector(self, connector):
        """Delete a single connector.
        Args:
            connector (str): The name of the connector.
        Returns:
            Dict[str, Any]: The response from the REST API, or an empty dictionary if the response is null or if there is a JSONDecodeError.
        """
        self.logger.info(f"Deleting connector: {connector}")
        url = f"{self.endpoint}/connectors/{connector}"
        response = requests.delete(url, auth=self.auth, verify=self.verify)
        response.raise_for_status()
        try:
            data = response.json()
        except JSONDecodeError:
            data = None
        return data
    
    def delete_all_connectors(self, connector_pattern=None):
        """Delete all connectors.
        Args:
            connector_pattern (str): The regex pattern to match the connector name. If not provided, all connectors will be deleted.
        Returns:
            Dict[str, Dict[str, Any]]: A dictionary of responses, where the keys are the connector names and the values are the responses.
        """
        self.logger.info(f"Deleting all connectors{' matching the pattern: ' + connector_pattern if connector_pattern else ''}")
        responses = {}
        for connector in self.list_connectors():
            self.logger.debug(f"Filtering connector: {connector}")

            if connector_pattern and not re.match(connector_pattern, connector):
                continue

            responses[connector] = self.delete_connector(connector)

        return responses

    def list_connector_tasks(self, connector):
        """Get the list of tasks for a connector.
        Args:
            connector (str): The name of the connector.
        Returns:
            List[int]: The list of task IDs for the connector.
        """
        self.logger.info(f"Getting tasks for connector: {connector}")
        url = f"{self.endpoint}/connectors/{connector}/tasks"
        response = requests.get(url, auth=self.auth, verify=self.verify)
        response.raise_for_status()
        return response.json()

    def get_connector_task_status(self, connector, task_id):
        """Get the status of a specific task for a connector.
        Args:
            connector (str): The name of the connector.
            task_id (int): The ID of the task.
        Returns:
            Dict[str, Any]: The response from the REST API.
        """
        self.logger.info(f"Getting task status for connector: {connector} and task_id: {task_id}")
        url = f"{self.endpoint}/connectors/{connector}/tasks/{task_id}/status"
        response = requests.get(url, auth=self.auth, verify=self.verify)
        response.raise_for_status()
        return response.json()

    def restart_connector_task(self, connector, task_id):
        """Restart a specific task of a connector.
        Args:
            connector (str): The name of the connector.
            task_id (int): The ID of the task.
        Returns:
            Dict[str, Any]: The response from the REST API, or an empty dictionary if the response is null or if there is a JSONDecodeError.
        """
        self.logger.info(f"Restarting task {task_id} of connector: {connector}")
        url = f"{self.endpoint}/connectors/{connector}/tasks/{task_id}/restart"
        response = requests.post(url, auth=self.auth, verify=self.verify)
        response.raise_for_status()
        try:
            data = response.json()
        except JSONDecodeError:
            data = None
        return data

    def list_connector_topics(self, connector):
        """Get the list of topics for a connector.
        Args:
            connector (str): The name of the connector.
        Returns:
            List[str]: The list of topics for the connector.
        """
        self.logger.info(f"Getting topics for connector: {connector}")
        url = f"{self.endpoint}/connectors/{connector}/topics"
        response = requests.get(url, auth=self.auth, verify=self.verify)
        response.raise_for_status()
        return response.json()

    def reset_connector_topics(self, connector):
        """Reset the list of topics for a connector.
        Args:
            connector (str): The name of the connector.
        Returns:
            Dict[str, Any]: The response from the REST API, or an empty dictionary if the response is null or if there is a JSONDecodeError.
        """
        self.logger.info(f"Resetting topics for connector: {connector}")
        url = f"{self.endpoint}/connectors/{connector}/topics/reset"
        response = requests.put(url, auth=self.auth, verify=self.verify)
        response.raise_for_status()
        try:
            return response.json()
        except JSONDecodeError:
            data = None
        return data

    def list_connector_plugins(self):
        """Get the list of connector plugins.
        Args:
        Returns:
            List[Dict[str, Any]]: The list of connector plugins.
        """
        self.logger.info("Getting connector plugins")
        url = f"{self.endpoint}/connector-plugins"
        response = requests.get(url, auth=self.auth, verify=self.verify)
        response.raise_for_status()
        return response.json()

    def validate_connector_config(self, plugin, config):
        """Validate the configuration for a specific connector plugin.
        Args:
            plugin (str): The name of the plugin.
            config (Dict[str, Any]): The configuration to be validated.
        Returns:
            Dict[str, Any]: The response from the REST API, or an empty dictionary if the response is null or if there is a JSONDecodeError.
        """
        self.logger.info(f"Validating config for plugin: {plugin}")
        url = f"{self.endpoint}/connector-plugins/{plugin}/config/validate"
        response = requests.put(url, auth=self.auth, verify=self.verify, headers=self.headers, data=json.dumps(config))
        response.raise_for_status()
        try:
            data = response.json()
        except JSONDecodeError:
            data = None
        return data
