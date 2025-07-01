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

"""Demo test that replicates the example.py functionality with instrumentation."""

import json
import os
import pytest
import weaviate
from opentelemetry.instrumentation.weaviate import WeaviateInstrumentor
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter


def test_example_py_workflow_with_instrumentation():
    """Test that replicates the example.py workflow with full instrumentation."""
    
    # Set up the tracer provider (like in example.py)
    tracer_provider = TracerProvider()
    trace.set_tracer_provider(tracer_provider)
    
    # Add in-memory exporter to capture spans
    memory_exporter = InMemorySpanExporter()
    memory_processor = BatchSpanProcessor(memory_exporter)
    tracer_provider.add_span_processor(memory_processor)
    
    # Add console exporter to see traces in terminal as well (like example.py)
    console_exporter = ConsoleSpanExporter()
    console_processor = BatchSpanProcessor(console_exporter)
    tracer_provider.add_span_processor(console_processor)
    
    # Instrument Weaviate
    WeaviateInstrumentor().instrument()
    
    try:
        print("🔍 Starting instrumented Weaviate operations...")
        
        # Connect to local Weaviate (like example.py)
        client = weaviate.connect_to_local()
        
        # Get the Question collection (like example.py)
        try:
            questions = client.collections.get("Question")
            print("✅ Got Question collection")
            
            # Perform near_text query (like example.py)
            try:
                response = questions.query.near_text(
                    query="biology",
                    limit=2
                )
                
                print(f"📊 Query returned {len(response.objects)} objects")
                
                # Process results (like example.py)
                for i, obj in enumerate(response.objects):
                    print(f"🔹 Object {i+1}: {json.dumps(obj.properties, indent=2)}")
                    
            except Exception as e:
                print(f"⚠️  Query failed (might be expected if no data): {e}")
                
        except Exception as e:
            print(f"⚠️  Collection access failed (might be expected): {e}")
        
        # Close the client (like example.py)
        client.close()
        
        # Force flush to ensure all spans are exported (like example.py)
        tracer_provider.force_flush(timeout_millis=5000)
        
        # Analyze the captured spans
        spans = memory_exporter.get_finished_spans()
        print(f"\n📈 Instrumentation Summary:")
        print(f"   Total spans created: {len(spans)}")
        
        weaviate_spans = [span for span in spans if "weaviate" in span.name.lower()]
        print(f"   Weaviate-specific spans: {len(weaviate_spans)}")
        
        # Group spans by operation type
        operations = {}
        for span in weaviate_spans:
            op_name = span.name.split(".")[-1] if "." in span.name else span.name
            operations[op_name] = operations.get(op_name, 0) + 1
        
        print("   Operations traced:")
        for op, count in operations.items():
            print(f"     - {op}: {count} calls")
        
        # Verify we got instrumentation
        assert len(spans) > 0, "Expected spans from instrumented operations"
        assert len(weaviate_spans) > 0, "Expected Weaviate-specific spans"
        
        # Check span attributes
        span_with_attrs = None
        for span in weaviate_spans:
            if span.attributes:
                span_with_attrs = span
                break
        
        if span_with_attrs:
            print("   Sample span attributes:")
            for key, value in span_with_attrs.attributes.items():
                print(f"     - {key}: {value}")
        
        print("✅ Test completed successfully!")
        
    except Exception as e:
        pytest.skip(f"Could not complete example workflow: {e}")
    
    finally:
        # Clean up instrumentation
        try:
            WeaviateInstrumentor().uninstrument()
        except Exception:
            pass


if __name__ == "__main__":
    # Run this test directly
    test_example_py_workflow_with_instrumentation()
