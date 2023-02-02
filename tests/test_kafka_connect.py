from unittest.mock import patch
from kafka_connect import KafkaConnect

import mock
import logging
import unittest
import json


class TestKafkaConnect(unittest.TestCase):
    def setUp(self):
        self.kafka_connect = KafkaConnect()

    def test_init(self):
        # Test default initialization
        kc = KafkaConnect()
        self.assertEqual(kc.url, "http://localhost:8083")
        self.assertEqual(kc.headers, {"Content-Type": "application/json"})
        self.assertIsNone(kc.auth)
        self.assertTrue(kc.verify)
        self.assertIsNotNone(kc.logger)

        # Test custom initialization
        custom_logger = logging.getLogger("custom_logger")
        kc = KafkaConnect(auth="user:pass", ssl_verify=False, logger=custom_logger)
        self.assertEqual(kc.auth, ("user", "pass"))
        self.assertFalse(kc.verify)
        self.assertEqual(kc.logger, custom_logger)

    def test_init_auth_value_error(self):
        # Test initialization with an invalid auth string
        with self.assertRaises(ValueError) as cm:
            kc = KafkaConnect(auth="invalid_auth")
        self.assertEqual(
            str(cm.exception),
            "Invalid auth string. Expected a colon-delimited string of `username` and `password`.",
        )

    @patch("kafka_connect.kafka_connect.requests")
    def test_get_cluster_info(self, mock_requests):
        mock_response = mock_requests.get.return_value
        mock_response.json.return_value = {
            "version": "1.0.0",
            "commit": "abc123",
            "kafka_cluster_id": "def456",
        }
        result = self.kafka_connect.get_cluster_info()
        mock_requests.get.assert_called_with("http://localhost:8083", auth=None, verify=True)
        mock_response.raise_for_status.assert_called_once()
        self.assertEqual(result, mock_response.json())

    def test_filter_by_pattern_without_expand(self):
        connectors = ["my-jdbc-source", "my-hdfs-sink"]
        pattern = ".*-source$"
        filtered_connectors = self.kafka_connect._KafkaConnect__filter_by_name(connectors, pattern)
        self.assertEqual(filtered_connectors, ["my-jdbc-source"])

    def test_filter_by_pattern_with_expand(self):
        connectors = {"my-jdbc-source": {"type": "source"}, "my-hdfs-sink": {"type": "sink"}}
        pattern = ".*-sink$"
        filtered_connectors = self.kafka_connect._KafkaConnect__filter_by_name(connectors, pattern)
        self.assertEqual(filtered_connectors, {"my-hdfs-sink": {"type": "sink"}})

    def test_filter_by_pattern_without_pattern(self):
        connectors = ["my-jdbc-source", "my-hdfs-sink"]
        pattern = ""
        filtered_connectors = self.kafka_connect._KafkaConnect__filter_by_name(connectors, pattern)
        self.assertEqual(filtered_connectors, ["my-jdbc-source", "my-hdfs-sink"])

    @patch("kafka_connect.kafka_connect.requests")
    def test_filter_by_state_no_state(self, mock_requests):
        connectors = ["my-jdbc-source", "my-hdfs-sink"]
        filtered_connectors = self.kafka_connect._KafkaConnect__filter_by_state(
            connectors, state=None
        )
        self.assertEqual(filtered_connectors, ["my-jdbc-source", "my-hdfs-sink"])

    @patch("kafka_connect.kafka_connect.requests")
    def test_filter_by_state_without_expand(self, mock_requests):
        connectors = ["my-jdbc-source", "my-hdfs-sink"]
        mock_response = mock_requests.get.return_value
        mock_response.json.return_value = {
            "my-jdbc-source": {
                "status": {
                    "name": "my-jdbc-source",
                    "connector": {"state": "RUNNING", "worker_id": "10.0.0.162:8083"},
                    "tasks": [],
                    "type": "sink",
                }
            },
            "my-hdfs-sink": {
                "status": {
                    "name": "my-hdfs-sink",
                    "connector": {"state": "PAUSED", "worker_id": "10.0.0.162:8083"},
                    "tasks": [],
                    "type": "source",
                }
            },
        }

        filtered_connectors = self.kafka_connect._KafkaConnect__filter_by_state(
            connectors, state="RUNNING"
        )

        # ensure the filter function calls list expand=status to get the connector status when not provided.
        mock_requests.get.assert_called_once_with(
            "http://localhost:8083/connectors", auth=None, verify=True, params={"expand": "status"}
        )
        mock_response.raise_for_status.assert_called_once()
        self.assertEqual(filtered_connectors, ["my-jdbc-source"])

    @patch("kafka_connect.kafka_connect.requests")
    def test_filter_by_state_with_expand_status(self, mock_requests):
        connectors = {
            "my-jdbc-source": {
                "status": {
                    "name": "my-jdbc-source",
                    "connector": {"state": "RUNNING", "worker_id": "10.0.0.162:8083"},
                    "tasks": [],
                    "type": "sink",
                }
            },
            "my-hdfs-sink": {
                "status": {
                    "name": "my-hdfs-sink",
                    "connector": {"state": "FAILED", "worker_id": "10.0.0.162:8083"},
                    "tasks": [],
                    "type": "source",
                }
            },
        }

        mock_response = mock_requests.get.return_value
        mock_response.get.return_value.json.return_value = {
            "my-jdbc-source": {
                "status": {
                    "name": "my-jdbc-source",
                    "connector": {"state": "RUNNING", "worker_id": "10.0.0.162:8083"},
                    "tasks": [],
                    "type": "sink",
                }
            }
        }

        filtered_connectors = self.kafka_connect._KafkaConnect__filter_by_state(
            connectors, state="RUNNING"
        )

        # ensure the filter function does not make redundant calls to expand=status when already provided
        mock_requests.get.assert_not_called()
        self.assertEqual(filtered_connectors, {"my-jdbc-source": connectors["my-jdbc-source"]})

    @patch("kafka_connect.kafka_connect.requests")
    def test_filter_by_state_with_expand_info(self, mock_requests):
        connectors = {
            "FileStreamSinkConnectorConnector_0": {
                "info": {
                    "name": "FileStreamSinkConnectorConnector_0",
                    "config": {
                        "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
                        "file": "/Users/smogili/file.txt",
                        "tasks.max": "1",
                        "topics": "datagen",
                        "name": "FileStreamSinkConnectorConnector_0",
                    },
                    "tasks": [],
                    "type": "sink",
                }
            },
            "DatagenConnectorConnector_0": {
                "info": {
                    "name": "DatagenConnectorConnector_0",
                    "config": {
                        "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
                        "quickstart": "clickstream",
                        "tasks.max": "1",
                        "name": "DatagenConnectorConnector_0",
                        "kafka.topic": "datagen",
                    },
                    "tasks": [],
                    "type": "source",
                }
            },
        }
        mock_response = mock_requests.get.return_value
        mock_response.json.return_value = {
            "FileStreamSinkConnectorConnector_0": {
                "status": {
                    "name": "FileStreamSinkConnectorConnector_0",
                    "connector": {"state": "RUNNING", "worker_id": "10.0.0.162:8083"},
                    "tasks": [],
                    "type": "sink",
                }
            },
            "DatagenConnectorConnector_0": {
                "status": {
                    "name": "DatagenConnectorConnector_0",
                    "connector": {"state": "FAILED", "worker_id": "10.0.0.162:8083"},
                    "tasks": [],
                    "type": "source",
                }
            },
        }

        filtered_connectors = self.kafka_connect._KafkaConnect__filter_by_state(
            connectors, state="FAILED"
        )

        # ensure the filter function calls expand=status when expand=info is provided
        mock_requests.get.assert_called_once_with(
            "http://localhost:8083/connectors", auth=None, verify=True, params={"expand": "status"}
        )
        mock_response.raise_for_status.assert_called_once()
        self.assertEqual(
            filtered_connectors,
            {"DatagenConnectorConnector_0": connectors["DatagenConnectorConnector_0"]},
        )

    @patch("kafka_connect.kafka_connect.requests")
    def test_list_connectors_without_expand(self, mock_requests):
        mock_response = mock_requests.get.return_value
        mock_response.json.return_value = json.dumps(["my-jdbc-source", "my-hdfs-sink"])

        result = self.kafka_connect.list_connectors()

        mock_requests.get.assert_called_with(
            "http://localhost:8083/connectors", auth=None, verify=True, params={"expand": None}
        )
        mock_response.raise_for_status.assert_called_once()
        self.assertEqual(result, mock_response.json())

    @patch("kafka_connect.kafka_connect.requests")
    def test_list_connectors_with_expand_status(self, mock_requests):
        mock_response = mock_requests.get.return_value
        mock_response.json.return_value = {
            "FileStreamSinkConnectorConnector_0": {
                "status": {
                    "name": "FileStreamSinkConnectorConnector_0",
                    "connector": {"state": "RUNNING", "worker_id": "10.0.0.162:8083"},
                    "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "10.0.0.162:8083"}],
                    "type": "sink",
                }
            },
            "DatagenConnectorConnector_0": {
                "status": {
                    "name": "DatagenConnectorConnector_0",
                    "connector": {"state": "RUNNING", "worker_id": "10.0.0.162:8083"},
                    "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "10.0.0.162:8083"}],
                    "type": "source",
                }
            },
        }

        result = self.kafka_connect.list_connectors(expand="status")

        mock_requests.get.assert_called_with(
            "http://localhost:8083/connectors", auth=None, verify=True, params={"expand": "status"}
        )
        mock_response.raise_for_status.assert_called_once()
        self.assertEqual(result, mock_response.json())

    @patch("kafka_connect.kafka_connect.requests")
    def test_list_connectors_with_expand_info(self, mock_requests):
        mock_response = mock_requests.get.return_value
        mock_response.json.return_value = {
            "FileStreamSinkConnectorConnector_0": {
                "info": {
                    "name": "FileStreamSinkConnectorConnector_0",
                    "config": {
                        "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
                        "file": "/Users/smogili/file.txt",
                        "tasks.max": "1",
                        "topics": "datagen",
                        "name": "FileStreamSinkConnectorConnector_0",
                    },
                    "tasks": [{"connector": "FileStreamSinkConnectorConnector_0", "task": 0}],
                    "type": "sink",
                }
            },
            "DatagenConnectorConnector_0": {
                "info": {
                    "name": "DatagenConnectorConnector_0",
                    "config": {
                        "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
                        "quickstart": "clickstream",
                        "tasks.max": "1",
                        "name": "DatagenConnectorConnector_0",
                        "kafka.topic": "datagen",
                    },
                    "tasks": [{"connector": "DatagenConnectorConnector_0", "task": 0}],
                    "type": "source",
                }
            },
        }

        result = self.kafka_connect.list_connectors(expand="info")

        mock_requests.get.assert_called_with(
            "http://localhost:8083/connectors", auth=None, verify=True, params={"expand": "info"}
        )
        mock_response.raise_for_status.assert_called_once()
        self.assertEqual(result, mock_response.json())

    @patch("kafka_connect.kafka_connect.requests")
    def test_create_connector(self, mock_requests):
        mock_response = mock_requests.get.return_value
        mock_response.status_code = 201
        mock_response.json.return_value = json.dumps(
            {
                "name": "hdfs-sink-connector",
                "config": {
                    "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
                    "tasks.max": "10",
                    "topics": "test-topic",
                    "hdfs.url": "hdfs://fakehost:9000",
                    "hadoop.conf.dir": "/opt/hadoop/conf",
                    "hadoop.home": "/opt/hadoop",
                    "flush.size": "100",
                    "rotate.interval.ms": "1000",
                },
                "tasks": [
                    {"connector": "hdfs-sink-connector", "task": 1},
                    {"connector": "hdfs-sink-connector", "task": 2},
                    {"connector": "hdfs-sink-connector", "task": 3},
                ],
            }
        )
        mock_requests.post.return_value = mock_response
        self.kafka_connect.requests = mock_requests

        config = {
            "name": "hdfs-sink-connector",
            "config": {
                "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
                "tasks.max": "10",
                "topics": "test-topic",
                "hdfs.url": "hdfs://fakehost:9000",
                "hadoop.conf.dir": "/opt/hadoop/conf",
                "hadoop.home": "/opt/hadoop",
                "flush.size": "100",
                "rotate.interval.ms": "1000",
            },
        }
        result = self.kafka_connect.create_connector(config)

        mock_requests.post.assert_called_with(
            "http://localhost:8083/connectors",
            auth=None,
            verify=True,
            headers={"Content-Type": "application/json"},
            data=json.dumps(config),
        )
        mock_response.raise_for_status.assert_called_once()
        self.assertEqual(result, mock_response.json())

    @patch("kafka_connect.kafka_connect.requests")
    def test_update_connector(self, mock_requests):
        mock_response = mock_requests.get.return_value
        mock_response.status_code = 200
        mock_response.json.return_value = json.dumps(
            {
                "name": "hdfs-sink-connector",
                "config": {
                    "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
                    "tasks.max": "3",
                    "topics": "test-topic",
                    "hdfs.url": "hdfs://fakehost:9000",
                    "hadoop.conf.dir": "/opt/hadoop/conf",
                    "hadoop.home": "/opt/hadoop",
                    "flush.size": "100",
                    "rotate.interval.ms": "1000",
                },
                "tasks": [
                    {"connector": "hdfs-sink-connector", "task": 1},
                    {"connector": "hdfs-sink-connector", "task": 2},
                    {"connector": "hdfs-sink-connector", "task": 3},
                ],
            }
        )
        mock_requests.put.return_value = mock_response
        self.kafka_connect.requests = mock_requests

        config = {
            "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
            "tasks.max": "3",
            "topics": "test-topic",
            "hdfs.url": "hdfs://fakehost:9000",
            "hadoop.conf.dir": "/opt/hadoop/conf",
            "hadoop.home": "/opt/hadoop",
            "flush.size": "100",
            "rotate.interval.ms": "1000",
        }
        result = self.kafka_connect.update_connector("hdfs-sink-connector", config)

        mock_requests.put.assert_called_with(
            "http://localhost:8083/connectors/hdfs-sink-connector/config",
            auth=None,
            verify=True,
            headers={"Content-Type": "application/json"},
            data=json.dumps(config),
        )
        mock_response.raise_for_status.assert_called_once()
        self.assertEqual(result, mock_response.json())

    @patch("kafka_connect.kafka_connect.requests")
    def test_get_connector(self, mock_requests):
        mock_response = mock_requests.get.return_value
        mock_response.json.return_value = {
            "name": "hdfs-sink-connector",
            "config": {
                "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
                "tasks.max": "10",
                "topics": "test-topic",
                "hdfs.url": "hdfs://fakehost:9000",
                "hadoop.conf.dir": "/opt/hadoop/conf",
                "hadoop.home": "/opt/hadoop",
                "flush.size": "100",
                "rotate.interval.ms": "1000",
            },
            "tasks": [
                {"connector": "hdfs-sink-connector", "task": 1},
                {"connector": "hdfs-sink-connector", "task": 2},
                {"connector": "hdfs-sink-connector", "task": 3},
            ],
        }

        result = self.kafka_connect.get_connector("hdfs-sink-connector")

        mock_requests.get.assert_called_with(
            "http://localhost:8083/connectors/hdfs-sink-connector", auth=None, verify=True
        )
        mock_response.raise_for_status.assert_called_once()
        self.assertEqual(result, mock_response.json())

    @patch("kafka_connect.kafka_connect.requests")
    def test_get_connector_config(self, mock_requests):
        mock_response = mock_requests.get.return_value
        mock_response.json.return_value = {
            "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
            "tasks.max": "10",
            "topics": "test-topic",
            "hdfs.url": "hdfs://fakehost:9000",
            "hadoop.conf.dir": "/opt/hadoop/conf",
            "hadoop.home": "/opt/hadoop",
            "flush.size": "100",
            "rotate.interval.ms": "1000",
        }

        result = self.kafka_connect.get_connector_config("hdfs-sink-connector")

        mock_requests.get.assert_called_with(
            "http://localhost:8083/connectors/hdfs-sink-connector/config", auth=None, verify=True
        )
        mock_response.raise_for_status.assert_called_once()
        self.assertEqual(result, mock_response.json())

    @patch("kafka_connect.kafka_connect.requests")
    def test_get_connector_status(self, mock_requests):
        mock_response = mock_requests.get.return_value
        mock_response.json.return_value = {
            "name": "hdfs-sink-connector",
            "connector": {"state": "RUNNING", "worker_id": "fakehost:8083"},
            "tasks": [
                {"id": 0, "state": "RUNNING", "worker_id": "fakehost:8083"},
                {
                    "id": 1,
                    "state": "FAILED",
                    "worker_id": "fakehost:8083",
                    "trace": "org.apache.kafka.common.errors.RecordTooLargeException\n",
                },
            ],
        }

        result = self.kafka_connect.get_connector_status("hdfs-sink-connector")

        mock_requests.get.assert_called_with(
            "http://localhost:8083/connectors/hdfs-sink-connector/status", auth=None, verify=True
        )
        mock_response.raise_for_status.assert_called_once()
        self.assertEqual(result, mock_response.json())

    @patch("kafka_connect.kafka_connect.requests")
    def test_restart_connector(self, mock_requests):
        mock_response = mock_requests.post.return_value
        mock_response.status_code = 200

        result = self.kafka_connect.restart_connector("my-connector")

        mock_requests.post.assert_called_with(
            "http://localhost:8083/connectors/my-connector/restart",
            auth=None,
            verify=True,
            params={"includeTasks": False, "onlyFailed": False},
        )
        self.assertEqual(result, mock_response.json())

    @patch("kafka_connect.kafka_connect.requests")
    def test_restart_connector_with_include_tasks(self, mock_requests):
        mock_response = mock_requests.post.return_value
        mock_response.status_code = 202

        result = self.kafka_connect.restart_connector("my-connector", include_tasks=True)

        mock_requests.post.assert_called_with(
            "http://localhost:8083/connectors/my-connector/restart",
            auth=None,
            verify=True,
            params={"includeTasks": True, "onlyFailed": False},
        )
        self.assertEqual(result, mock_response.json())

    @patch("kafka_connect.kafka_connect.requests")
    def test_restart_connector_with_only_failed(self, mock_requests):
        mock_response = mock_requests.post.return_value
        mock_response.status_code = 202
        mock_response.json.return_value = {
            "name": "my-connector",
            "config": {
                "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
                "tasks.max": "1",
                "file": "/tmp/test.txt",
                "topics": "test",
                "hdfs.url": "hdfs://localhost:8020",
            },
            "tasks": [{"connector": "my-connector", "task": 0, "status": "FAILED"}],
            "type": "sink",
        }

        result = self.kafka_connect.restart_connector("my-connector", only_failed=True)

        mock_requests.post.assert_called_with(
            "http://localhost:8083/connectors/my-connector/restart",
            auth=None,
            verify=True,
            params={"includeTasks": False, "onlyFailed": True},
        )
        self.assertEqual(result, mock_response.json())

    @patch("kafka_connect.kafka_connect.requests")
    def test_pause_connector(self, mock_requests):
        mock_response = mock_requests.put.return_value
        connector_name = "hdfs-sink-connector"

        result = self.kafka_connect.pause_connector(connector_name)

        mock_requests.put.assert_called_with(
            f"http://localhost:8083/connectors/{connector_name}/pause", auth=None, verify=True
        )
        mock_response.raise_for_status.assert_called_once()
        self.assertEqual(result, None)

    @patch("kafka_connect.kafka_connect.requests")
    def test_resume_connector(self, mock_requests):
        connector_name = "hdfs-sink-connector"
        mock_response = mock_requests.put.return_value
        result = self.kafka_connect.resume_connector(connector_name)

        mock_requests.put.assert_called_with(
            f"http://localhost:8083/connectors/{connector_name}/resume", auth=None, verify=True
        )
        mock_response.raise_for_status.assert_called_once()
        self.assertEqual(result, None)

    @patch("kafka_connect.kafka_connect.requests")
    def test_delete_connector(self, mock_requests):
        mock_response = mock_requests.delete.return_value
        connector_name = "hdfs-sink-connector"

        result = self.kafka_connect.delete_connector(connector_name)

        mock_requests.delete.assert_called_with(
            f"http://localhost:8083/connectors/{connector_name}", auth=None, verify=True
        )
        mock_response.raise_for_status.assert_called_with()
        self.assertEqual(result, None)

    @patch("kafka_connect.kafka_connect.requests")
    def test_list_connector_tasks(self, mock_requests):
        connector_name = "hdfs-sink-connector"
        mock_response = mock_requests.get.return_value
        mock_response.json.return_value = [
            {
                "id": {"connector": connector_name, "task": 0},
                "config": {
                    "task.class": "io.confluent.connect.hdfs.HdfsSinkTask",
                    "topics": "test-topic",
                    "hdfs.url": "hdfs://fakehost:9000",
                    "hadoop.conf.dir": "/opt/hadoop/conf",
                    "hadoop.home": "/opt/hadoop",
                    "flush.size": "100",
                    "rotate.interval.ms": "1000",
                },
            }
        ]

        result = self.kafka_connect.list_connector_tasks(connector_name)

        mock_requests.get.assert_called_with(
            f"http://localhost:8083/connectors/{connector_name}/tasks", auth=None, verify=True
        )
        mock_response.raise_for_status.assert_called_with()
        self.assertEqual(result, mock_response.json())

    @patch("kafka_connect.kafka_connect.requests")
    def test_get_connector_task_status(self, mock_requests):
        mock_response = mock_requests.get.return_value
        mock_response.json.return_value = {
            "state": "RUNNING",
            "id": 1,
            "worker_id": "192.168.86.101:8083",
        }

        result = self.kafka_connect.get_connector_task_status("hdfs-sink-connector", 1)

        mock_requests.get.assert_called_with(
            "http://localhost:8083/connectors/hdfs-sink-connector/tasks/1/status",
            auth=None,
            verify=True,
        )
        mock_response.raise_for_status.assert_called_once()
        self.assertEqual(result, mock_response.json())

    @patch("kafka_connect.kafka_connect.requests")
    def test_restart_connector_task(self, mock_requests):
        mock_response = mock_requests.post.return_value

        result = self.kafka_connect.restart_connector_task("hdfs-sink-connector", 1)

        mock_requests.post.assert_called_with(
            "http://localhost:8083/connectors/hdfs-sink-connector/tasks/1/restart",
            auth=None,
            verify=True,
        )
        self.assertEqual(result, None)

    @patch("kafka_connect.kafka_connect.requests")
    def test_list_connector_topics(self, mock_requests):
        mock_response = mock_requests.get.return_value
        mock_response.json.return_value = {
            "hdfs-sink-connector": {"topics": ["test-topic-1", "test-topic-2", "test-topic-3"]}
        }

        result = self.kafka_connect.list_connector_topics("hdfs-sink-connector")

        mock_requests.get.assert_called_with(
            "http://localhost:8083/connectors/hdfs-sink-connector/topics", auth=None, verify=True
        )
        mock_response.raise_for_status.assert_called_once()
        self.assertEqual(result, mock_response.json())

    @patch("kafka_connect.kafka_connect.requests")
    def reset_connector_topics(self, mock_requests):
        mock_response = mock_requests.post.return_value

        result = self.kafka_connect.reset_connector_topics("hdfs-sink-connector", 1)

        mock_requests.post.assert_called_with(
            "http://localhost:8083/connectors/hdfs-sink-connector/topics/reset",
            auth=None,
            verify=True,
        )
        self.assertEqual(result, None)

    @patch("kafka_connect.kafka_connect.requests")
    def test_list_connector_plugins(self, mock_requests):
        mock_response = mock_requests.get.return_value
        mock_response.json.return_value = [
            {"class": "io.confluent.connect.hdfs.HdfsSinkConnector"},
            {"class": "io.confluent.connect.jdbc.JdbcSourceConnector"},
        ]

        result = self.kafka_connect.list_connector_plugins()

        mock_requests.get.assert_called_with(
            "http://localhost:8083/connector-plugins", auth=None, verify=True
        )
        mock_response.raise_for_status.assert_called_once()
        self.assertEqual(result, mock_response.json())

    @patch("kafka_connect.kafka_connect.requests")
    def test_validate_connector_config(self, mock_requests):
        connector_class = "FileStreamSinkConnector"
        config = {
            "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
            "tasks.max": "1",
            "topics": "test-topic",
        }

        mock_response = mock_requests.put.return_value
        mock_response.json.return_value = {
            "name": "FileStreamSinkConnector",
            "error_count": 1,
            "groups": ["Common"],
            "configs": [
                {
                    "definition": {
                        "name": "topics",
                        "type": "LIST",
                        "required": False,
                        "default_value": "",
                        "importance": "HIGH",
                        "documentation": "",
                        "group": "Common",
                        "width": "LONG",
                        "display_name": "Topics",
                        "dependents": [],
                        "order": 4,
                    },
                    "value": {
                        "name": "topics",
                        "value": "test-topic",
                        "recommended_values": [],
                        "errors": [],
                        "visible": True,
                    },
                },
                {
                    "definition": {
                        "name": "file",
                        "type": "STRING",
                        "required": True,
                        "default_value": "",
                        "importance": "HIGH",
                        "documentation": "Destination filename.",
                        "group": None,
                        "width": "NONE",
                        "display_name": "file",
                        "dependents": [],
                        "order": -1,
                    },
                    "value": {},
                },
            ],
        }

        result = self.kafka_connect.validate_connector_config(connector_class, config)

        mock_requests.put.assert_called_with(
            f"http://localhost:8083/connector-plugins/{connector_class}/config/validate",
            auth=None,
            verify=True,
            headers={"Content-Type": "application/json"},
            data=json.dumps(config),
        )
        mock_response.raise_for_status.assert_called_once()
        self.assertEqual(result, mock_response.json())


if __name__ == "__main__":
    unittest.main()
