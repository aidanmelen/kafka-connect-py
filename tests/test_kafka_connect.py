from unittest.mock import patch
from kafka_connect import KafkaConnect

import mock
import unittest
import json


class TestKafkaConnect(unittest.TestCase):
    def setUp(self):
        self.kafka_connect = KafkaConnect()

    @patch('kafka_connect.kafka_connect.requests')
    def test_get_cluster_info(self, mock_requests):
        mock_response = mock_requests.get.return_value
        mock_response.json.return_value = {
            "version": "1.0.0",
            "commit": "abc123",
            "kafka_cluster_id": "def456"
        }
        result = self.kafka_connect.get_cluster_info()
        mock_requests.get.assert_called_with('http://localhost:8083', auth=None, verify=True)
        self.assertEqual(result, mock_response.json())
    
    @patch('kafka_connect.kafka_connect.requests')
    def test_list_connectors_with_no_expand(self, mock_requests):
        mock_response = mock_requests.get.return_value
        mock_response.json.return_value = json.dumps(["my-jdbc-source", "my-hdfs-sink"])

        result = self.kafka_connect.list_connectors()

        mock_requests.get.assert_called_with(
            'http://localhost:8083/connectors',
            auth=None, verify=True, params={'expand': None}
        )

        self.assertEqual(result, mock_response.json())
    
    @patch('kafka_connect.kafka_connect.requests')
    def test_list_connectors_with_expand_status(self, mock_requests):
        mock_response = mock_requests.get.return_value
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "FileStreamSinkConnectorConnector_0": {
                "status": {
                    "name": "FileStreamSinkConnectorConnector_0",
                    "connector": {
                        "state": "RUNNING",
                        "worker_id": "10.0.0.162:8083"
                    },
                    "tasks": [
                        {
                            "id": 0,
                            "state": "RUNNING",
                            "worker_id": "10.0.0.162:8083"
                        }
                    ],
                    "type": "sink"
                }
                },
                "DatagenConnectorConnector_0": {
                    "status": {
                    "name": "DatagenConnectorConnector_0",
                    "connector": {
                        "state": "RUNNING",
                        "worker_id": "10.0.0.162:8083"
                    },
                    "tasks": [
                        {
                            "id": 0,
                            "state": "RUNNING",
                            "worker_id": "10.0.0.162:8083"
                        }
                    ],
                    "type": "source"
                }
            }
        }

        result = self.kafka_connect.list_connectors(expand="status")

        mock_requests.get.assert_called_with(
            'http://localhost:8083/connectors',
            auth=None, verify=True, params={'expand': 'status'}
        )

        self.assertEqual(result, mock_response.json())
    
    @patch('kafka_connect.kafka_connect.requests')
    def test_list_connectors_with_expand_info(self, mock_requests):
        mock_response = mock_requests.get.return_value
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "FileStreamSinkConnectorConnector_0": {
                "info": {
                "name": "FileStreamSinkConnectorConnector_0",
                "config": {
                    "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
                    "file": "/Users/smogili/file.txt",
                    "tasks.max": "1",
                    "topics": "datagen",
                    "name": "FileStreamSinkConnectorConnector_0"
                },
                "tasks": [
                    {
                    "connector": "FileStreamSinkConnectorConnector_0",
                    "task": 0
                    }
                ],
                "type": "sink"
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
                    "kafka.topic": "datagen"
                },
                "tasks": [
                    {
                    "connector": "DatagenConnectorConnector_0",
                    "task": 0
                    }
                ],
                "type": "source"
                }
            }
        }

        result = self.kafka_connect.list_connectors(expand="info")

        mock_requests.get.assert_called_with(
            'http://localhost:8083/connectors',
            auth=None, verify=True, params={'expand': 'info'}
        )

        self.assertEqual(result, mock_response.json())
    
    @patch('kafka_connect.kafka_connect.requests')
    def test_create_connector(self, mock_requests):
        mock_response = mock_requests.get.return_value
        mock_response.status_code = 201
        mock_response.json.return_value = json.dumps({
            "name": "hdfs-sink-connector",
            "config": {
                "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
                "tasks.max": "10",
                "topics": "test-topic",
                "hdfs.url": "hdfs://fakehost:9000",
                "hadoop.conf.dir": "/opt/hadoop/conf",
                "hadoop.home": "/opt/hadoop",
                "flush.size": "100",
                "rotate.interval.ms": "1000"
            },
            "tasks": [
                { "connector": "hdfs-sink-connector", "task": 1 },
                { "connector": "hdfs-sink-connector", "task": 2 },
                { "connector": "hdfs-sink-connector", "task": 3 }
            ]
        })
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
                "rotate.interval.ms": "1000"
            }
        }
        result = self.kafka_connect.create_connector(config)

        mock_requests.post.assert_called_with(
            'http://localhost:8083/connectors',
            auth=None, verify=True,
            headers={'Content-Type': 'application/json'}, data=json.dumps(config)
        )

        self.assertEqual(result, mock_response.json())
    
    @patch('kafka_connect.kafka_connect.requests')
    def test_update_connector(self, mock_requests):
        mock_response = mock_requests.get.return_value
        mock_response.status_code = 200
        mock_response.json.return_value = json.dumps({
            "name": "hdfs-sink-connector",
            "config": {
                "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
                "tasks.max": "3",
                "topics": "test-topic",
                "hdfs.url": "hdfs://fakehost:9000",
                "hadoop.conf.dir": "/opt/hadoop/conf",
                "hadoop.home": "/opt/hadoop",
                "flush.size": "100",
                "rotate.interval.ms": "1000"
            },
            "tasks": [
                { "connector": "hdfs-sink-connector", "task": 1 },
                { "connector": "hdfs-sink-connector", "task": 2 },
                { "connector": "hdfs-sink-connector", "task": 3 }
            ]
        })
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
            "rotate.interval.ms": "1000"
        }
        result = self.kafka_connect.update_connector("hdfs-sink-connector", config)

        mock_requests.put.assert_called_with(
            'http://localhost:8083/connectors/hdfs-sink-connector/config',
            auth=None, verify=True,
            headers={'Content-Type': 'application/json'}, data=json.dumps(config)
        )

        self.assertEqual(result, mock_response.json())
    
    @patch('kafka_connect.kafka_connect.requests')
    def test_get_connector(self, mock_requests):
        mock_response = mock_requests.get.return_value
        mock_response.raise_for_status.return_value = None
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
                "rotate.interval.ms": "1000"
            },
            "tasks": [
                {"connector": "hdfs-sink-connector", "task": 1},
                {"connector": "hdfs-sink-connector", "task": 2},
                {"connector": "hdfs-sink-connector", "task": 3}
            ]
        }

        result = self.kafka_connect.get_connector("hdfs-sink-connector")

        mock_requests.get.assert_called_with(
            'http://localhost:8083/connectors/hdfs-sink-connector',
            auth=None, verify=True
        )

        self.assertEqual(result, mock_response.json())
    
    @patch('kafka_connect.kafka_connect.requests')
    def test_get_connector_config(self, mock_requests):
        mock_response = mock_requests.get.return_value
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
            "tasks.max": "10",
            "topics": "test-topic",
            "hdfs.url": "hdfs://fakehost:9000",
            "hadoop.conf.dir": "/opt/hadoop/conf",
            "hadoop.home": "/opt/hadoop",
            "flush.size": "100",
            "rotate.interval.ms": "1000"
        }

        result = self.kafka_connect.get_connector_config("hdfs-sink-connector")

        mock_requests.get.assert_called_with(
            'http://localhost:8083/connectors/hdfs-sink-connector/config',
            auth=None, verify=True
        )

        self.assertEqual(result, mock_response.json())
    
    @patch('kafka_connect.kafka_connect.requests')
    def test_get_connector_status(self, mock_requests):
        mock_response = mock_requests.get.return_value
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "name": "hdfs-sink-connector",
            "connector": {
                "state": "RUNNING",
                "worker_id": "fakehost:8083"
            },
            "tasks": [
                {
                    "id": 0,
                    "state": "RUNNING",
                    "worker_id": "fakehost:8083"
                },
                {
                    "id": 1,
                    "state": "FAILED",
                    "worker_id": "fakehost:8083",
                    "trace": "org.apache.kafka.common.errors.RecordTooLargeException\n"
                }
            ]
        }

        result = self.kafka_connect.get_connector_status("hdfs-sink-connector")

        mock_requests.get.assert_called_with(
            'http://localhost:8083/connectors/hdfs-sink-connector/status',
            auth=None, verify=True
        )

        self.assertEqual(result, mock_response.json())
    
    @patch('kafka_connect.kafka_connect.requests')
    def test_restart_connector(self, mock_requests):
        mock_response = mock_requests.post.return_value
        mock_response.status_code = 200

        result = self.kafka_connect.restart_connector("my-connector")

        mock_requests.post.assert_called_with(
            'http://localhost:8083/connectors/my-connector/restart',
            auth=None, verify=True, params={'includeTasks': False, 'onlyFailed': False}
        )
        self.assertEqual(result, mock_response.json())
        
    @patch('kafka_connect.kafka_connect.requests')
    def test_restart_connector_with_include_tasks(self, mock_requests):
        mock_response = mock_requests.post.return_value
        mock_response.status_code = 202

        result = self.kafka_connect.restart_connector("my-connector", include_tasks=True)

        mock_requests.post.assert_called_with(
            'http://localhost:8083/connectors/my-connector/restart',
            auth=None, verify=True, params={'includeTasks': True, 'onlyFailed': False}
        )
        self.assertEqual(result, mock_response.json())
    
    @patch('kafka_connect.kafka_connect.requests')
    def test_restart_connector_with_only_failed(self, mock_requests):
        mock_response = mock_requests.post.return_value
        mock_response.raise_for_status.return_value = None
        mock_response.status_code = 202
        mock_response.json.return_value = {
            "name": "my-connector",
            "config": {
                "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
                "tasks.max": "1",
                "file": "/tmp/test.txt",
                "topics": "test",
                "hdfs.url": "hdfs://localhost:8020"
            },
            "tasks": [
                {
                    "connector": "my-connector",
                    "task": 0,
                    "status": "FAILED"
                }
            ],
            "type": "sink"
        }

        result = self.kafka_connect.restart_connector("my-connector", only_failed=True)

        mock_requests.post.assert_called_with(
            'http://localhost:8083/connectors/my-connector/restart',
            auth=None, verify=True, params={'includeTasks': False, 'onlyFailed': True}
        )
        self.assertEqual(result, mock_response.json())
    
    @patch('kafka_connect.kafka_connect.requests')
    def test_pause_connector(self, mock_requests):
        mock_response = mock_requests.put.return_value
        connector_name = 'hdfs-sink-connector'

        self.kafka_connect.pause_connector(connector_name)

        mock_requests.put.assert_called_with(
            f'http://localhost:8083/connectors/{connector_name}/pause',
            auth=None,
            verify=True
        )
        mock_response.raise_for_status.assert_called_once()
    
    @patch('kafka_connect.kafka_connect.requests')
    def test_resume_connector(self, mock_requests):
        connector_name = 'hdfs-sink-connector'
        mock_response = mock_requests.put.return_value
        result = self.kafka_connect.resume_connector(connector_name)

        mock_requests.put.assert_called_with(
            f'http://localhost:8083/connectors/{connector_name}/resume',
            auth=None, verify=True
        )
        mock_response.raise_for_status.assert_called_once()
    
    @patch('kafka_connect.kafka_connect.requests')
    def test_delete_connector(self, mock_requests):
        mock_response = mock_requests.delete.return_value
        connector_name = 'hdfs-sink-connector'

        self.kafka_connect.delete_connector(connector_name)

        mock_requests.delete.assert_called_with(
            f'http://localhost:8083/connectors/{connector_name}',
            auth=None, verify=True
        )
        mock_response.raise_for_status.assert_called_with()

if __name__ == '__main__':
    unittest.main()
