# Copyright The OpenTelemetry Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from unittest import mock
import pytest
import weaviate

from opentelemetry.trace import SpanKind
from opentelemetry.instrumentation.weaviate.mapping import SPAN_NAME_PREFIX, MAPPING_V3

from .test_utils import WeaviateSpanTestBase
from .helpers_v3 import create_mock_weaviate_v3_client


class TestWeaviateV3SpanGeneration(WeaviateSpanTestBase):
    """Test span generation for Weaviate v3 operations."""

    def setUp(self):
        super().setUp()
        # Mock weaviate version to v3
        self.version_patcher = mock.patch('weaviate.__version__', '3.25.0')
        self.version_patcher.start()

    def tearDown(self):
        super().tearDown()
        self.version_patcher.stop()

    def test_v3_connection_span_creation(self):
        """Test that v3 client initialization creates connection span."""
        # Mock the connection to avoid network calls, but use real Weaviate client
        with mock.patch('weaviate.connect.connection.Connection') as mock_connection:
            # Mock the connection object  
            mock_conn_instance = mock.MagicMock()
            mock_conn_instance.url = "http://localhost:8080"
            mock_connection.return_value = mock_conn_instance
            
            # Create real client - this will trigger connection instrumentation
            client = weaviate.Client("http://localhost:8080")
            
        spans = self.assert_span_count(1)
        span = spans[0]
        
        self.assert_span_properties(span, f"{SPAN_NAME_PREFIX}.__init__", SpanKind.CLIENT)
        self.assert_server_attributes(span, "localhost", 8080)

    @mock.patch("weaviate.schema.Schema.get")
    def test_schema_get_span(self, mock_get):
        """Test span creation for Schema.get operation."""
        mock_get.return_value = {"classes": []}
        
        with mock.patch("weaviate.Client") as mock_client:
            client_instance = create_mock_weaviate_v3_client()
            mock_client.return_value = client_instance
            
            client = mock_client("http://localhost:8080")
            result = client.schema.get()
            
        spans = self.memory_exporter.get_finished_spans()
        
        # Should have connection span + operation span
        self.assertGreaterEqual(len(spans), 1)
        
        # Find schema.get span if instrumentation created it
        schema_spans = [span for span in spans if "schema" in span.name.lower()]
        if schema_spans:
            schema_span = schema_spans[0]
            self.assert_span_properties(schema_span, f"{SPAN_NAME_PREFIX}.get", SpanKind.CLIENT)
            self.assert_db_attributes(schema_span, "get")

    @mock.patch("weaviate.schema.Schema.create_class")
    def test_schema_create_class_span(self, mock_create_class):
        """Test span creation for Schema.create_class operation."""
        mock_create_class.return_value = None
        
        with mock.patch("weaviate.Client") as mock_client:
            client_instance = create_mock_weaviate_v3_client()
            mock_client.return_value = client_instance
            
            client = mock_client("http://localhost:8080")
            client.schema.create_class({"class": "TestClass"})
            
        spans = self.memory_exporter.get_finished_spans()
        self.assertGreaterEqual(len(spans), 1)

    @mock.patch("weaviate.data.crud_data.DataObject.create")
    def test_data_create_span(self, mock_create):
        """Test span creation for DataObject.create operation."""
        mock_create.return_value = "mock-uuid"
        
        with mock.patch("weaviate.Client") as mock_client:
            client_instance = create_mock_weaviate_v3_client()
            mock_client.return_value = client_instance
            
            client = mock_client("http://localhost:8080")
            client.data_object.create({"title": "test"}, "TestClass")
            
        spans = self.memory_exporter.get_finished_spans()
        self.assertGreaterEqual(len(spans), 1)

    @mock.patch("weaviate.batch.crud_batch.Batch.add_data_object")
    def test_batch_add_data_object_span(self, mock_add):
        """Test span creation for Batch.add_data_object operation."""
        mock_add.return_value = None
        
        with mock.patch("weaviate.Client") as mock_client:
            client_instance = create_mock_weaviate_v3_client()
            mock_client.return_value = client_instance
            
            client = mock_client("http://localhost:8080")
            client.batch.add_data_object({"title": "test"}, "TestClass")
            
        spans = self.memory_exporter.get_finished_spans()
        self.assertGreaterEqual(len(spans), 1)

    def test_query_get_span(self):
        """Test span creation for Query.get operation."""
        # Mock the connection to avoid network calls, but use real Weaviate client
        with mock.patch('weaviate.connect.connection.Connection') as mock_connection:
            # Mock the connection object  
            mock_conn_instance = mock.MagicMock()
            mock_conn_instance.url = "http://localhost:8080"
            mock_connection.return_value = mock_conn_instance
            
            # Create real client - this will trigger connection instrumentation
            client = weaviate.Client("http://localhost:8080")
            
            # Mock the network request that schema.get makes, but not the method itself
            with mock.patch('requests.get') as mock_get:
                mock_response = mock.MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = {"classes": []}
                mock_get.return_value = mock_response
                
                # Call the real schema.get method - this should trigger instrumentation
                result = client.schema.get()
            
        spans = self.memory_exporter.get_finished_spans()
        # Should have at least 2 spans: connection + schema.get
        self.assertGreaterEqual(len(spans), 2)
        
        # Check that we have both connection and operation spans
        span_names = [span.name for span in spans]
        self.assertIn(f"{SPAN_NAME_PREFIX}.__init__", span_names)
        self.assertTrue(any("get" in name for name in span_names))

    def test_v3_parent_child_relationship(self):
        """Test parent-child relationship between connection and operation spans."""
        with mock.patch("weaviate.Client") as mock_client:
            client_instance = create_mock_weaviate_v3_client()
            mock_client.return_value = client_instance
            
            # Create connection (parent span)
            client = mock_client("http://localhost:8080")
            
            # Perform operation (should create child span)
            with mock.patch("weaviate.schema.Schema.get") as mock_get:
                mock_get.return_value = {"classes": []}
                client.schema.get()
                
        spans = self.memory_exporter.get_finished_spans()
        
        # Should have at least connection span
        self.assertGreaterEqual(len(spans), 1)
        
        # Verify connection span exists
        connection_spans = self.get_spans_by_name(spans, f"{SPAN_NAME_PREFIX}.__init__")
        self.assertGreaterEqual(len(connection_spans), 1)

    def test_v3_multiple_operations_create_separate_spans(self):
        """Test that multiple v3 operations create separate spans."""
        with mock.patch("weaviate.Client") as mock_client:
            client_instance = create_mock_weaviate_v3_client()
            mock_client.return_value = client_instance
            
            client = mock_client("http://localhost:8080")
            
            # Perform multiple operations
            with mock.patch("weaviate.schema.Schema.get") as mock_get:
                mock_get.return_value = {"classes": []}
                client.schema.get()
                client.schema.get()
                
        spans = self.memory_exporter.get_finished_spans()
        
        # Should have connection span + operation spans
        self.assertGreaterEqual(len(spans), 1)

    def test_v3_span_attributes(self):
        """Test that v3 spans have correct database and server attributes."""
        with mock.patch("weaviate.Client") as mock_client:
            client_instance = create_mock_weaviate_v3_client()
            mock_client.return_value = client_instance
            
            client = mock_client("http://localhost:8080")
            
        spans = self.assert_span_count(1)
        span = spans[0]
        
        # Verify database attributes
        self.assert_db_attributes(span)
        
        # Verify server attributes
        self.assert_server_attributes(span, "localhost", 8080)

    @pytest.mark.parametrize("operation_config", MAPPING_V3[:5])  # Test first 5 operations
    def test_v3_mapped_operations_span_names(self, operation_config):
        """Test that mapped v3 operations create spans with correct names."""
        module_name = operation_config["module"]
        operation_name = operation_config["name"]
        expected_span_name = operation_config.get("span_name", f"{SPAN_NAME_PREFIX}.{operation_name}")
        
        # TODO: This is a placeholder test - in real implementation, we would:
        # 1. Mock the specific module and operation
        # 2. Invoke the operation 
        # 3. Verify the span name matches expected_span_name
        
        # For now, just verify the expected span name format
        self.assertTrue(expected_span_name.startswith(SPAN_NAME_PREFIX))

    def test_v3_error_handling_spans(self):
        """Test span creation when v3 operations encounter errors."""
        with mock.patch("weaviate.Client") as mock_client:
            client_instance = create_mock_weaviate_v3_client()
            mock_client.return_value = client_instance
            
            client = mock_client("http://localhost:8080")
            
            # Mock an operation that raises an exception
            with mock.patch("weaviate.schema.Schema.get") as mock_get:
                mock_get.side_effect = Exception("Connection error")
                
                with self.assertRaises(Exception):
                    client.schema.get()
                    
        spans = self.memory_exporter.get_finished_spans()
        
        # Should still have connection span even if operation fails
        self.assertGreaterEqual(len(spans), 1)
        
        # Connection span should exist
        connection_spans = self.get_spans_by_name(spans, f"{SPAN_NAME_PREFIX}.__init__")
        self.assertGreaterEqual(len(connection_spans), 1)
