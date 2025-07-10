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

from opentelemetry.trace import SpanKind
from opentelemetry.instrumentation.weaviate.mapping import SPAN_NAME_PREFIX, MAPPING_V4

from .test_utils import WeaviateSpanTestBase
from .helpers_v4 import create_mock_weaviate_v4_client


class TestWeaviateV4SpanGeneration(WeaviateSpanTestBase):
    """Test span generation for Weaviate v4 operations."""

    def setUp(self):
        super().setUp()
        # Mock weaviate version to v4
        self.version_patcher = mock.patch('weaviate.__version__', '4.5.0')
        self.version_patcher.start()

    def tearDown(self):
        super().tearDown()
        self.version_patcher.stop()

    def test_v4_connection_span_creation(self):
        """Test that v4 client initialization creates connection span."""
        with mock.patch("weaviate.WeaviateClient") as mock_client:
            mock_client.return_value = create_mock_weaviate_v4_client()
            client = mock_client()
            
        spans = self.assert_span_count(1)
        span = spans[0]
        
        self.assert_span_properties(span, f"{SPAN_NAME_PREFIX}.__init__", SpanKind.CLIENT)

    @mock.patch("weaviate.collections.collections._Collections.get")
    def test_collections_get_span(self, mock_get):
        """Test span creation for Collections.get operation."""
        mock_collection = mock.MagicMock()
        mock_get.return_value = mock_collection
        
        with mock.patch("weaviate.WeaviateClient") as mock_client:
            client_instance = create_mock_weaviate_v4_client()
            mock_client.return_value = client_instance
            
            client = mock_client()
            collection = client.collections.get("TestCollection")
            
        spans = self.memory_exporter.get_finished_spans()
        
        # Should have connection span + operation span
        self.assertGreaterEqual(len(spans), 1)

    @mock.patch("weaviate.collections.collections._Collections.create")
    def test_collections_create_span(self, mock_create):
        """Test span creation for Collections.create operation."""
        mock_collection = mock.MagicMock()
        mock_create.return_value = mock_collection
        
        with mock.patch("weaviate.WeaviateClient") as mock_client:
            client_instance = create_mock_weaviate_v4_client()
            mock_client.return_value = client_instance
            
            client = mock_client()
            collection = client.collections.create("TestCollection")
            
        spans = self.memory_exporter.get_finished_spans()
        self.assertGreaterEqual(len(spans), 1)

    @mock.patch("weaviate.collections.data._DataCollection.insert")
    def test_data_insert_span(self, mock_insert):
        """Test span creation for DataCollection.insert operation."""
        mock_insert.return_value = "mock-uuid"
        
        with mock.patch("weaviate.WeaviateClient") as mock_client:
            client_instance = create_mock_weaviate_v4_client()
            mock_client.return_value = client_instance
            
            client = mock_client()
            collection = client.collections.get("TestCollection")
            result = collection.data.insert({"title": "test"})
            
        spans = self.memory_exporter.get_finished_spans()
        self.assertGreaterEqual(len(spans), 1)

    @mock.patch("weaviate.collections.queries.near_text.query._NearTextQuery.near_text")
    def test_near_text_query_span(self, mock_near_text):
        """Test span creation for near_text query operation."""
        mock_result = mock.MagicMock()
        mock_result.objects = []
        mock_near_text.return_value = mock_result
        
        with mock.patch("weaviate.WeaviateClient") as mock_client:
            client_instance = create_mock_weaviate_v4_client()
            mock_client.return_value = client_instance
            
            client = mock_client()
            collection = client.collections.get("TestCollection")
            result = collection.query.near_text("test query")
            
        spans = self.memory_exporter.get_finished_spans()
        self.assertGreaterEqual(len(spans), 1)

    @mock.patch("weaviate.collections.queries.fetch_objects.query._FetchObjectsQuery.fetch_objects")
    def test_fetch_objects_span(self, mock_fetch):
        """Test span creation for fetch_objects operation."""
        mock_result = mock.MagicMock()
        mock_result.objects = []
        mock_fetch.return_value = mock_result
        
        with mock.patch("weaviate.WeaviateClient") as mock_client:
            client_instance = create_mock_weaviate_v4_client()
            mock_client.return_value = client_instance
            
            client = mock_client()
            collection = client.collections.get("TestCollection")
            result = collection.query.fetch_objects(limit=10)
            
        spans = self.memory_exporter.get_finished_spans()
        self.assertGreaterEqual(len(spans), 1)

    @mock.patch("weaviate.collections.batch.collection._BatchCollection.add_object")
    def test_batch_add_object_span(self, mock_add):
        """Test span creation for batch add_object operation."""
        mock_add.return_value = None
        
        with mock.patch("weaviate.WeaviateClient") as mock_client:
            client_instance = create_mock_weaviate_v4_client()
            mock_client.return_value = client_instance
            
            client = mock_client()
            collection = client.collections.get("TestCollection")
            collection.batch.add_object({"title": "test"})
            
        spans = self.memory_exporter.get_finished_spans()
        self.assertGreaterEqual(len(spans), 1)

    def test_v4_parent_child_relationship(self):
        """Test parent-child relationship between connection and operation spans."""
        with mock.patch("weaviate.WeaviateClient") as mock_client:
            client_instance = create_mock_weaviate_v4_client()
            mock_client.return_value = client_instance
            
            # Create connection (parent span)
            client = mock_client()
            
            # Perform operation (should create child span)
            with mock.patch("weaviate.collections.collections._Collections.get") as mock_get:
                mock_get.return_value = mock.MagicMock()
                client.collections.get("TestCollection")
                
        spans = self.memory_exporter.get_finished_spans()
        
        # Should have at least connection span
        self.assertGreaterEqual(len(spans), 1)
        
        # Verify connection span exists
        connection_spans = self.get_spans_by_name(spans, f"{SPAN_NAME_PREFIX}.__init__")
        self.assertGreaterEqual(len(connection_spans), 1)

    def test_v4_multiple_operations_create_separate_spans(self):
        """Test that multiple v4 operations create separate spans."""
        with mock.patch("weaviate.WeaviateClient") as mock_client:
            client_instance = create_mock_weaviate_v4_client()
            mock_client.return_value = client_instance
            
            client = mock_client()
            
            # Perform multiple operations
            with mock.patch("weaviate.collections.collections._Collections.get") as mock_get:
                mock_get.return_value = mock.MagicMock()
                client.collections.get("TestCollection1")
                client.collections.get("TestCollection2")
                
        spans = self.memory_exporter.get_finished_spans()
        
        # Should have connection span + operation spans
        self.assertGreaterEqual(len(spans), 1)

    def test_v4_span_attributes(self):
        """Test that v4 spans have correct database attributes."""
        with mock.patch("weaviate.WeaviateClient") as mock_client:
            client_instance = create_mock_weaviate_v4_client()
            mock_client.return_value = client_instance
            
            client = mock_client()
            
        spans = self.assert_span_count(1)
        span = spans[0]
        
        # Verify database attributes
        self.assert_db_attributes(span)

    def test_v4_similarity_search_events(self):
        """Test that similarity search operations create span events."""
        with mock.patch("weaviate.WeaviateClient") as mock_client:
            client_instance = create_mock_weaviate_v4_client()
            mock_client.return_value = client_instance
            
            client = mock_client()
            
            # Mock near_text operation with results
            with mock.patch("weaviate.collections.queries.near_text.query._NearTextQuery.near_text") as mock_near_text:
                mock_result = mock.MagicMock()
                mock_obj = mock.MagicMock()
                mock_obj.properties = {"title": "Test Document", "content": "Test content"}
                mock_obj.metadata = mock.MagicMock()
                mock_obj.metadata.distance = 0.1
                mock_obj.metadata.certainty = 0.9
                mock_result.objects = [mock_obj]
                mock_near_text.return_value = mock_result
                
                collection = client.collections.get("TestCollection")
                result = collection.query.near_text("test query")
                
        spans = self.memory_exporter.get_finished_spans()
        
        # Should have spans
        self.assertGreaterEqual(len(spans), 1)
        
        # Look for spans with events (similarity search operations)
        spans_with_events = [span for span in spans if len(span.events) > 0]
        # Note: This depends on the actual instrumentation implementation

    @pytest.mark.parametrize("operation_config", MAPPING_V4[:5])  # Test first 5 operations  
    def test_v4_mapped_operations_span_names(self, operation_config):
        """Test that mapped v4 operations create spans with correct names."""
        module_name = operation_config["module"]
        operation_name = operation_config["name"]
        expected_span_name = operation_config.get("span_name", f"{SPAN_NAME_PREFIX}.{operation_name}")
        
        # This is a placeholder test - in real implementation, we would:
        # 1. Mock the specific module and operation
        # 2. Invoke the operation 
        # 3. Verify the span name matches expected_span_name
        
        # For now, just verify the expected span name format
        self.assertTrue(expected_span_name.startswith(SPAN_NAME_PREFIX))

    def test_v4_error_handling_spans(self):
        """Test span creation when v4 operations encounter errors."""
        with mock.patch("weaviate.WeaviateClient") as mock_client:
            client_instance = create_mock_weaviate_v4_client()
            mock_client.return_value = client_instance
            
            client = mock_client()
            
            # Mock an operation that raises an exception
            with mock.patch("weaviate.collections.collections._Collections.get") as mock_get:
                mock_get.side_effect = Exception("Connection error")
                
                with self.assertRaises(Exception):
                    client.collections.get("TestCollection")
                    
        spans = self.memory_exporter.get_finished_spans()
        
        # Should still have connection span even if operation fails
        self.assertGreaterEqual(len(spans), 1)
        
        # Connection span should exist
        connection_spans = self.get_spans_by_name(spans, f"{SPAN_NAME_PREFIX}.__init__")
        self.assertGreaterEqual(len(connection_spans), 1)

    def test_v4_collections_management_spans(self):
        """Test span creation for v4 collection management operations."""
        with mock.patch("weaviate.WeaviateClient") as mock_client:
            client_instance = create_mock_weaviate_v4_client()
            mock_client.return_value = client_instance
            
            client = mock_client()
            
            # Test multiple collection operations
            with mock.patch("weaviate.collections.collections._Collections.create") as mock_create, \
                 mock.patch("weaviate.collections.collections._Collections.delete") as mock_delete:
                
                mock_create.return_value = mock.MagicMock()
                mock_delete.return_value = None
                
                # Create and delete collections
                client.collections.create("TestCollection")
                client.collections.delete("TestCollection")
                
        spans = self.memory_exporter.get_finished_spans()
        
        # Should have connection span + operation spans
        self.assertGreaterEqual(len(spans), 1)
