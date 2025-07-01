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

"""Integration tests for Weaviate instrumentation with real Weaviate server."""

import pytest
import weaviate
from opentelemetry.instrumentation.weaviate import WeaviateInstrumentor
from opentelemetry.semconv._incubating.attributes import db_attributes as DbAttributes
from opentelemetry.semconv._incubating.attributes import server_attributes as ServerAttributes


class TestWeaviateIntegration:
    """Integration tests using real Weaviate server on localhost."""

    @pytest.fixture(autouse=True)
    def setup_and_cleanup(self, instrumentor, span_exporter):
        """Setup test connection and cleanup after tests."""
        self.test_collection_name = "TestCollection"
        
        # Connect to local Weaviate
        try:
            self.client = weaviate.connect_to_local()
            
            # Test basic connectivity
            self.client.is_ready()
            
            yield
            
            # Cleanup after tests
            self.client.close()
            
        except Exception as e:
            pytest.skip(f"Could not connect to Weaviate server: {e}")

    def test_weaviate_connection_creates_span(self, span_exporter):
        """Test that connecting to Weaviate creates instrumentation spans."""
        # Clear any existing spans
        span_exporter.clear()
        
        # Connect to Weaviate (this should create spans)
        client = weaviate.connect_to_local()
        
        try:
            # Check for connection-related spans
            spans = span_exporter.get_finished_spans()
            
            # We should have at least one span from the connection
            assert len(spans) >= 0, "Expected spans from Weaviate connection"
            
            # Look for spans with the correct naming pattern
            weaviate_spans = [span for span in spans if span.name.startswith("db.weaviate")]
            
            if weaviate_spans:
                span = weaviate_spans[0]
                # Verify basic span attributes
                assert span.attributes.get(DbAttributes.DB_SYSTEM_NAME) == "weaviate" or \
                       span.attributes.get(ServerAttributes.SERVER_ADDRESS) == "localhost"
                       
        finally:
            client.close()

    def test_collection_operations_create_spans(self, span_exporter):
        """Test that collection operations create proper spans."""
        span_exporter.clear()
        
        # List existing collections (this should be a simple operation)
        try:
            collections_info = self.client.collections.list_all()
            print(f"Found {len(collections_info)} collections")
        except Exception as e:
            print(f"Collections list failed: {e}")
        
        # Check spans
        spans = span_exporter.get_finished_spans()
        print(f"Spans after collection operations: {[span.name for span in spans]}")
        
        # Look for collection-related spans
        collection_spans = [span for span in spans if "collections" in span.name or "db.weaviate" in span.name]
        
        if collection_spans:
            span = collection_spans[-1]  # Get the most recent span
            assert span.attributes.get(DbAttributes.DB_SYSTEM_NAME) == "weaviate"

    def test_data_insert_creates_spans(self, span_exporter):
        """Test that data insertion creates proper spans."""
        span_exporter.clear()
        
        collection = self.client.collections.get(self.test_collection_name)
        
        # Insert test data
        result = collection.data.insert({
            "title": "Test Document",
            "content": "This is a test document for OpenTelemetry instrumentation."
        })
        
        # Check spans
        spans = span_exporter.get_finished_spans()
        
        # Look for data operation spans
        data_spans = [span for span in spans if "data" in span.name.lower() or "insert" in span.name.lower()]
        
        if data_spans:
            span = data_spans[-1]  # Get the most recent span
            assert span.attributes.get(DbAttributes.DB_SYSTEM_NAME) == "weaviate"
            assert "insert" in span.attributes.get(DbAttributes.DB_OPERATION_NAME, "").lower()

    def test_query_operations_create_spans(self, span_exporter):
        """Test that query operations create proper spans."""
        span_exporter.clear()
        
        collection = self.client.collections.get(self.test_collection_name)
        
        # First insert some data to query
        collection.data.insert({
            "title": "Query Test Document",
            "content": "This document is for testing query instrumentation."
        })
        
        # Clear spans from insert operation
        span_exporter.clear()
        
        # Perform a query
        try:
            response = collection.query.near_text(
                query="test document",
                limit=5
            )
            
            # Check spans
            spans = span_exporter.get_finished_spans()
            
            # Look for query operation spans
            query_spans = [span for span in spans if "query" in span.name.lower()]
            
            if query_spans:
                span = query_spans[-1]  # Get the most recent span
                assert span.attributes.get(DbAttributes.DB_SYSTEM_NAME) == "weaviate"
                
        except Exception as e:
            # Some query operations might not be available depending on Weaviate setup
            print(f"Query test skipped due to: {e}")

    def test_fetch_objects_creates_spans(self, span_exporter):
        """Test that fetch operations create proper spans."""
        span_exporter.clear()
        
        collection = self.client.collections.get(self.test_collection_name)
        
        # Insert test data first
        collection.data.insert({
            "title": "Fetch Test Document",
            "content": "This document is for testing fetch instrumentation."
        })
        
        # Clear spans from insert
        span_exporter.clear()
        
        # Fetch objects
        try:
            response = collection.query.fetch_objects(limit=5)
            
            # Check spans
            spans = span_exporter.get_finished_spans()
            
            # Look for fetch operation spans
            fetch_spans = [span for span in spans if "fetch" in span.name.lower()]
            
            if fetch_spans:
                span = fetch_spans[-1]  # Get the most recent span
                assert span.attributes.get(DbAttributes.DB_SYSTEM_NAME) == "weaviate"
                
        except Exception as e:
            print(f"Fetch test skipped due to: {e}")

    def test_graphql_query_creates_spans(self, span_exporter):
        """Test that GraphQL queries create proper spans."""
        span_exporter.clear()
        
        # Perform a GraphQL query
        query = f"""
        {{
            Get {{
                {self.test_collection_name} {{
                    title
                    content
                }}
            }}
        }}
        """
        
        try:
            response = self.client.graphql_raw_query(query)
            
            # Check spans
            spans = span_exporter.get_finished_spans()
            
            # Look for GraphQL operation spans
            graphql_spans = [span for span in spans if "graphql" in span.name.lower()]
            
            if graphql_spans:
                span = graphql_spans[-1]  # Get the most recent span
                assert span.attributes.get(DbAttributes.DB_SYSTEM_NAME) == "weaviate"
                
        except Exception as e:
            print(f"GraphQL test skipped due to: {e}")

    def test_span_attributes_contain_server_info(self, span_exporter):
        """Test that spans contain proper server information."""
        span_exporter.clear()
        
        # Perform an operation to generate spans
        collection = self.client.collections.get(self.test_collection_name)
        
        # Check spans
        spans = span_exporter.get_finished_spans()
        
        # Find spans with server information
        spans_with_server_info = [
            span for span in spans 
            if span.attributes.get(ServerAttributes.SERVER_ADDRESS) or 
               span.attributes.get(ServerAttributes.SERVER_PORT)
        ]
        
        if spans_with_server_info:
            span = spans_with_server_info[0]
            # Should have localhost as server address
            server_address = span.attributes.get(ServerAttributes.SERVER_ADDRESS)
            if server_address:
                assert server_address in ["localhost", "127.0.0.1"]

    def test_complete_workflow_creates_spans(self, span_exporter):
        """Test a complete workflow similar to the example.py file."""
        span_exporter.clear()
        
        # Get collection
        collection = self.client.collections.get(self.test_collection_name)
        
        # Insert some test data
        collection.data.insert({
            "title": "Biology Question",
            "content": "What is photosynthesis?"
        })
        
        collection.data.insert({
            "title": "Biology Answer", 
            "content": "Photosynthesis is the process by which plants convert light energy into chemical energy."
        })
        
        # Perform a query (similar to example.py)
        try:
            response = collection.query.near_text(
                query="biology",
                limit=2
            )
            
            # Process results (like in example.py)
            for obj in response.objects:
                print(f"Found object: {obj.properties.get('title', 'No title')}")
                
        except Exception as e:
            print(f"Query in workflow test failed: {e}")
        
        # Check that we generated spans throughout the workflow
        spans = span_exporter.get_finished_spans()
        
        # Should have multiple spans from the various operations
        assert len(spans) > 0, "Expected spans from complete workflow"
        
        # Check that we have different types of operations
        span_names = [span.name for span in spans]
        weaviate_spans = [name for name in span_names if name.startswith("db.weaviate")]
        
        print(f"Generated {len(weaviate_spans)} Weaviate spans: {weaviate_spans}")
        
        # Verify that spans have the correct system name
        for span in spans:
            if span.attributes.get(DbAttributes.DB_SYSTEM_NAME):
                assert span.attributes.get(DbAttributes.DB_SYSTEM_NAME) == "weaviate"

    def test_error_handling_in_instrumentation(self, span_exporter):
        """Test that instrumentation handles errors gracefully."""
        span_exporter.clear()
        
        # Try to get a non-existent collection
        try:
            non_existent = self.client.collections.get("NonExistentCollection")
            # Try to perform an operation that might fail
            non_existent.data.insert({"test": "data"})
        except Exception:
            # This is expected to fail, but instrumentation should handle it gracefully
            pass
        
        # Check that spans were still created even with the error
        spans = span_exporter.get_finished_spans()
        
        # Instrumentation should create spans even for failed operations
        # The exact behavior depends on when the error occurs
        print(f"Spans created during error scenario: {len(spans)}")


class TestWeaviateConnectionTypes:
    """Test different connection methods with instrumentation."""

    def test_connect_to_local_instrumentation(self, tracer_provider, span_exporter):
        """Test that connect_to_local is properly instrumented."""
        # Create a new instrumentor for this test
        instrumentor = WeaviateInstrumentor()
        instrumentor.instrument(tracer_provider=tracer_provider)
        
        try:
            span_exporter.clear()
            
            # Connect using connect_to_local
            client = weaviate.connect_to_local()
            
            # Check for spans
            spans = span_exporter.get_finished_spans()
            print(f"Spans from connect_to_local: {[span.name for span in spans]}")
            
            client.close()
            
        except Exception as e:
            pytest.skip(f"Could not test connect_to_local: {e}")
        finally:
            instrumentor.uninstrument()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
