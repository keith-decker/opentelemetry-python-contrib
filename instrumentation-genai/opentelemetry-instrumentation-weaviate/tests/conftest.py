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

"""Unit tests configuration module."""

import os
from unittest.mock import Mock, MagicMock
from typing import Any

import pytest

from opentelemetry.instrumentation.weaviate import WeaviateInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)


@pytest.fixture(scope="function", name="span_exporter")
def fixture_span_exporter():
    exporter = InMemorySpanExporter()
    yield exporter


@pytest.fixture(scope="function", name="tracer_provider")
def fixture_tracer_provider(span_exporter):
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(span_exporter))
    return provider


@pytest.fixture(scope="function")
def instrumentor(tracer_provider):
    instrumentor = WeaviateInstrumentor()
    instrumentor.instrument(tracer_provider=tracer_provider)
    yield instrumentor
    instrumentor.uninstrument()


@pytest.fixture
def mock_weaviate_client():
    """Create a mock Weaviate client for testing."""
    client = Mock()
    
    # Mock connection attributes
    connection = Mock()
    connection.url = "http://localhost:8080"
    client._connection = connection
    
    # Mock collections
    collections = Mock()
    client.collections = collections
    
    # Mock collection object
    collection = Mock()
    collections.get.return_value = collection
    collections.create.return_value = collection
    collections.delete.return_value = None
    collections.delete_all.return_value = None
    collections.create_from_dict.return_value = collection
    
    # Mock data operations
    data = Mock()
    collection.data = data
    data.insert.return_value = {"id": "test-id"}
    data.replace.return_value = {"id": "test-id"}
    data.update.return_value = {"id": "test-id"}
    
    # Mock query operations
    query = Mock()
    collection.query = query
    
    # Mock query results
    mock_result = Mock()
    mock_result.objects = [
        Mock(properties={"title": "Test Object 1"}),
        Mock(properties={"title": "Test Object 2"})
    ]
    
    query.near_text.return_value = mock_result
    query.fetch_objects.return_value = mock_result
    query.get.return_value = mock_result
    
    # Mock batch operations
    batch = Mock()
    collection.batch = batch
    batch.add_object.return_value = None
    
    # Mock GraphQL operations
    client.graphql_raw_query.return_value = {"data": {"Get": {"Question": []}}}
    
    # Mock close operation
    client.close.return_value = None
    
    return client


@pytest.fixture
def mock_weaviate_module(monkeypatch, mock_weaviate_client):
    """Mock the weaviate module and its functions."""
    
    def mock_connect_to_local(*args, **kwargs):
        return mock_weaviate_client
    
    def mock_connect_to_custom(*args, **kwargs):
        return mock_weaviate_client
    
    def mock_connect_to_weaviate_cloud(*args, **kwargs):
        return mock_weaviate_client
    
    # Create a mock weaviate module
    weaviate_module = Mock()
    weaviate_module.connect_to_local = mock_connect_to_local
    weaviate_module.connect_to_custom = mock_connect_to_custom
    weaviate_module.connect_to_weaviate_cloud = mock_connect_to_weaviate_cloud
    weaviate_module.WeaviateClient = Mock(return_value=mock_weaviate_client)
    
    # Mock the WeaviateClient class
    class MockWeaviateClient:
        def __init__(self, *args, **kwargs):
            self._connection = Mock()
            self._connection.url = "http://localhost:8080"
            self.collections = mock_weaviate_client.collections
            self.graphql_raw_query = mock_weaviate_client.graphql_raw_query
            self.close = mock_weaviate_client.close
    
    weaviate_module.WeaviateClient = MockWeaviateClient
    
    # Patch the module in sys.modules
    import sys
    monkeypatch.setitem(sys.modules, "weaviate", weaviate_module)
    
    # Also patch specific submodules that are used in the mapping
    collections_module = Mock()
    collections_class = Mock()
    collections_class.get = Mock()
    collections_class.create = Mock()
    collections_class.delete = Mock()
    collections_class.delete_all = Mock()
    collections_class.create_from_dict = Mock()
    collections_module._Collections = collections_class
    
    data_module = Mock()
    data_class = Mock()
    data_class.insert = Mock()
    data_class.replace = Mock()
    data_class.update = Mock()
    data_module._DataCollection = data_class
    
    query_module = Mock()
    query_class = Mock()
    query_class.fetch_objects = Mock()
    query_module._FetchObjectsQuery = query_class
    
    grpc_query_module = Mock()
    grpc_query_class = Mock()
    grpc_query_class.get = Mock()
    grpc_query_module._QueryGRPC = grpc_query_class
    
    batch_module = Mock()
    batch_class = Mock()
    batch_class.add_object = Mock()
    batch_module._BatchCollection = batch_class
    
    executor_module = Mock()
    executor_module.execute = Mock()
    
    # Patch submodules
    monkeypatch.setitem(sys.modules, "weaviate.collections.collections", collections_module)
    monkeypatch.setitem(sys.modules, "weaviate.collections.data", data_module)
    monkeypatch.setitem(sys.modules, "weaviate.collections.queries.fetch_objects.query", query_module)
    monkeypatch.setitem(sys.modules, "weaviate.collections.grpc.query", grpc_query_module)
    monkeypatch.setitem(sys.modules, "weaviate.collections.batch.collection", batch_module)
    monkeypatch.setitem(sys.modules, "weaviate.connect.executor", executor_module)
    monkeypatch.setitem(sys.modules, "weaviate.client", weaviate_module)
    
    return weaviate_module


# Real Weaviate fixtures for integration testing
@pytest.fixture(scope="session")
def weaviate_available():
    """Check if Weaviate server is available on localhost."""
    try:
        import weaviate
        client = weaviate.connect_to_local()
        client.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="function")
def real_weaviate_client():
    """Provide a real Weaviate client for integration tests."""
    try:
        import weaviate
        client = weaviate.connect_to_local()
        yield client
        client.close()
    except Exception as e:
        pytest.skip(f"Weaviate server not available: {e}")


@pytest.fixture(scope="function") 
def instrumentor_with_real_weaviate(tracer_provider):
    """Provide an instrumentor configured for real Weaviate testing."""
    instrumentor = WeaviateInstrumentor()
    instrumentor.instrument(tracer_provider=tracer_provider)
    yield instrumentor
    # Note: uninstrument might fail with real modules, so we wrap in try/except
    try:
        instrumentor.uninstrument()
    except Exception:
        pass  # Ignore uninstrumentation errors in tests
