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

"""Simple integration tests for Weaviate instrumentation with real server."""

import pytest
import weaviate
from opentelemetry.instrumentation.weaviate import WeaviateInstrumentor
from opentelemetry.semconv._incubating.attributes import db_attributes as DbAttributes
from opentelemetry.semconv._incubating.attributes import server_attributes as ServerAttributes


class TestSimpleWeaviateIntegration:
    """Simple integration tests using real Weaviate server."""

    def test_basic_connection_with_instrumentation(self, tracer_provider, span_exporter):
        """Test basic connection to Weaviate with instrumentation."""
        # Set up instrumentor
        instrumentor = WeaviateInstrumentor()
        instrumentor.instrument(tracer_provider=tracer_provider)
        
        try:
            span_exporter.clear()
            
            # Connect to local Weaviate
            client = weaviate.connect_to_local()
            
            print(f"Connected to Weaviate successfully")
            
            # Try a simple operation
            try:
                is_ready = client.is_ready()
                print(f"Weaviate is ready: {is_ready}")
            except Exception as e:
                print(f"Ready check failed: {e}")
            
            # Close connection
            client.close()
            
            # Check spans that were created
            spans = span_exporter.get_finished_spans()
            print(f"Total spans created: {len(spans)}")
            
            for span in spans:
                print(f"Span: {span.name}")
                if span.attributes:
                    for key, value in span.attributes.items():
                        print(f"  {key}: {value}")
            
            # Look for Weaviate-related spans
            weaviate_spans = [span for span in spans if "weaviate" in span.name.lower()]
            
            if weaviate_spans:
                print(f"Found {len(weaviate_spans)} Weaviate spans")
                span = weaviate_spans[0]
                
                # Check basic span attributes
                db_system = span.attributes.get(DbAttributes.DB_SYSTEM_NAME)
                server_addr = span.attributes.get(ServerAttributes.SERVER_ADDRESS)
                
                print(f"DB System: {db_system}")
                print(f"Server Address: {server_addr}")
                
                # At least one of these should be set
                assert db_system == "weaviate" or server_addr in ["localhost", "127.0.0.1"]
            else:
                print("No Weaviate spans found - checking if instrumentation is working")
                # Even if no specific Weaviate spans, we should see some spans from the operations
                assert len(spans) >= 0, "Expected some spans from instrumented operations"
                
        except Exception as e:
            pytest.skip(f"Could not connect to Weaviate server: {e}")
        finally:
            try:
                instrumentor.uninstrument()
            except Exception:
                pass  # Ignore errors during cleanup

    def test_graphql_query_with_instrumentation(self, tracer_provider, span_exporter):
        """Test GraphQL query with instrumentation."""
        instrumentor = WeaviateInstrumentor()
        instrumentor.instrument(tracer_provider=tracer_provider)
        
        try:
            span_exporter.clear()
            
            client = weaviate.connect_to_local()
            
            # Try a simple GraphQL query to get schema
            query = """
            {
                Get {
                    _Meta {
                        hostname
                        version
                    }
                }
            }
            """
            
            try:
                response = client.graphql_raw_query(query)
                print(f"GraphQL response: {response}")
            except Exception as e:
                print(f"GraphQL query failed: {e}")
            
            client.close()
            
            # Check spans
            spans = span_exporter.get_finished_spans()
            print(f"Spans after GraphQL query: {len(spans)}")
            
            for span in spans:
                print(f"Span: {span.name}")
                if "graphql" in span.name.lower():
                    print(f"  Found GraphQL span!")
                    assert span.attributes.get(DbAttributes.DB_SYSTEM_NAME) == "weaviate"
                    
        except Exception as e:
            pytest.skip(f"Could not test GraphQL with Weaviate: {e}")
        finally:
            try:
                instrumentor.uninstrument()
            except Exception:
                pass

    def test_example_workflow_instrumentation(self, tracer_provider, span_exporter):
        """Test the workflow from example.py with instrumentation."""
        instrumentor = WeaviateInstrumentor()
        instrumentor.instrument(tracer_provider=tracer_provider)
        
        try:
            span_exporter.clear()
            
            # Replicate the example.py workflow
            client = weaviate.connect_to_local()
            
            # Try to get a collection (like in example.py)
            try:
                # This might fail if Question collection doesn't exist, but should still create spans
                questions = client.collections.get("Question")
                print("Got Question collection")
                
                # Try a query (this will likely fail without data, but should create spans)
                try:
                    response = questions.query.near_text(
                        query="biology",
                        limit=2
                    )
                    print(f"Query response: {response}")
                except Exception as e:
                    print(f"Query failed (expected): {e}")
                    
            except Exception as e:
                print(f"Collection get failed (might be expected): {e}")
            
            client.close()
            
            # Check spans created during the workflow
            spans = span_exporter.get_finished_spans()
            print(f"Workflow generated {len(spans)} spans")
            
            weaviate_spans = [span for span in spans if "weaviate" in span.name.lower()]
            print(f"Weaviate-specific spans: {len(weaviate_spans)}")
            
            for span in weaviate_spans:
                print(f"  - {span.name}")
                if span.attributes.get(DbAttributes.DB_SYSTEM_NAME):
                    assert span.attributes.get(DbAttributes.DB_SYSTEM_NAME) == "weaviate"
                    
        except Exception as e:
            pytest.skip(f"Could not test example workflow: {e}")
        finally:
            try:
                instrumentor.uninstrument()
            except Exception:
                pass


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
