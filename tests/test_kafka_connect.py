from unittest.mock import patch
from kafka_connect import KafkaConnect

import mock
import unittest
import json


class TestKafkaConnect(unittest.TestCase):
    def setUp(self):
        self.kafka_connect = KafkaConnect()

    @patch('kafka_connect.kafka_connect.requests')
    def test_get_info(self, mock_requests):
        # Set up mock response
        mock_response = mock_requests.get.return_value
        mock_response.json.return_value = {
            "version": "1.0.0",
            "commit": "abc123",
            "kafka_cluster_id": "def456"
        }

        # Call the get_info() method
        result = self.kafka_connect.get_info()

        # Assert that the correct URL was called
        mock_requests.get.assert_called_with('http://localhost:8083')

        # Check that the correct result was returned
        self.assertEqual(result, mock_response.json())

    @patch('kafka_connect.kafka_connect.requests')
    def test_create_connector(self, mock_requests):
        # Set up the mock object and expected return value
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

        # Call the method that we want to test
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

        # Check that the requests.post method was called with the correct arguments
        mock_requests.post.assert_called_with(
            'http://localhost:8083/connectors',
            headers={'Content-Type': 'application/json'}, data=json.dumps(config)
        )

        # Check that the correct result was returned
        self.assertEqual(result, mock_response.json())

    @patch('kafka_connect.kafka_connect.requests')
    def test_restart_connector(self, mock_requests):
        # Set up the mock object and expected return value
        mock_response = mock_requests.post.return_value
        mock_response.status_code = 200
        mock_response.json.return_value = None
        mock_requests.post.return_value = mock_response
        self.kafka_connect.requests = mock_requests

        # Call the method that we want to test
        result = self.kafka_connect.restart_connector("my-connector", include_tasks=False, only_failed=False)

        # Check that the requests.post method was called with the correct arguments
        mock_requests.post.assert_called_with(
            'http://localhost:8083/connectors/my-connector/restart',
            params={'includeTasks': False, 'onlyFailed': False}
        )
        # Check that the correct response was returned by the method
        self.assertEqual(result, mock_response.json())

if __name__ == '__main__':
    unittest.main()
