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

"""Tests for Weaviate wrapper classes."""

from unittest.mock import Mock, patch
import pytest

from opentelemetry.instrumentation.weaviate import (
    _WeaviateConnectionInjectionWrapper,
    _WeaviateTraceInjectionWrapper,
)
from opentelemetry.instrumentation.weaviate.mapping import SPAN_NAME_PREFIX
from opentelemetry.semconv._incubating.attributes import db_attributes as DbAttributes
from opentelemetry.semconv._incubating.attributes import server_attributes as ServerAttributes
from opentelemetry.trace import get_tracer


class TestWeaviateConnectionInjectionWrapper:
    """Test the _WeaviateConnectionInjectionWrapper class."""

    def test_wrapper_creation(self):
        """Test that the wrapper can be created."""
        tracer = get_tracer(__name__)
        wrapper = _WeaviateConnectionInjectionWrapper(tracer)
        assert wrapper.tracer == tracer

    def test_wrapper_call_creates_span(self, tracer_provider, span_exporter):
        """Test that calling the wrapper creates a span."""
        tracer = tracer_provider.get_tracer(__name__)
        wrapper = _WeaviateConnectionInjectionWrapper(tracer)
        
        # Mock the wrapped function
        wrapped = Mock(__name__="test_connection")
        instance = Mock()
        
        # Mock the connection
        connection = Mock()
        connection.url = "http://localhost:8080"
        instance._connection = connection
        
        # Call the wrapper
        result = wrapper(wrapped, instance, [], {})
        
        # Verify the wrapped function was called
        wrapped.assert_called_once_with()
        
        # Check spans
        spans = span_exporter.get_finished_spans()
        assert len(spans) == 1
        
        span = spans[0]
        assert span.name == f"{SPAN_NAME_PREFIX}.test_connection"
        assert span.attributes.get(ServerAttributes.SERVER_ADDRESS) == "localhost"
        assert span.attributes.get(ServerAttributes.SERVER_PORT) == 8080

    def test_wrapper_call_without_connection(self, tracer_provider, span_exporter):
        """Test that calling the wrapper works even without connection info."""
        tracer = tracer_provider.get_tracer(__name__)
        wrapper = _WeaviateConnectionInjectionWrapper(tracer)
        
        # Mock the wrapped function
        wrapped = Mock(__name__="test_connection")
        instance = Mock()
        instance._connection = None
        
        # Call the wrapper
        result = wrapper(wrapped, instance, [], {})
        
        # Verify the wrapped function was called
        wrapped.assert_called_once_with()
        
        # Check spans
        spans = span_exporter.get_finished_spans()
        assert len(spans) == 1
        
        span = spans[0]
        assert span.name == f"{SPAN_NAME_PREFIX}.test_connection"
        # Should not have server attributes when connection is None
        assert ServerAttributes.SERVER_ADDRESS not in span.attributes
        assert ServerAttributes.SERVER_PORT not in span.attributes

    def test_wrapper_call_with_invalid_url(self, tracer_provider, span_exporter):
        """Test that calling the wrapper handles invalid URLs gracefully."""
        tracer = tracer_provider.get_tracer(__name__)
        wrapper = _WeaviateConnectionInjectionWrapper(tracer)
        
        # Mock the wrapped function
        wrapped = Mock(__name__="test_connection")
        instance = Mock()
        
        # Mock the connection with invalid URL
        connection = Mock()
        connection.url = "invalid-url"
        instance._connection = connection
        
        # Call the wrapper
        result = wrapper(wrapped, instance, [], {})
        
        # Verify the wrapped function was called
        wrapped.assert_called_once_with()
        
        # Check spans - should still create span even with invalid URL
        spans = span_exporter.get_finished_spans()
        assert len(spans) == 1


class TestWeaviateTraceInjectionWrapper:
    """Test the _WeaviateTraceInjectionWrapper class."""

    def test_wrapper_creation(self):
        """Test that the wrapper can be created."""
        tracer = get_tracer(__name__)
        wrapper = _WeaviateTraceInjectionWrapper(tracer)
        assert wrapper.tracer == tracer
        assert wrapper.wrap_properties == {}

    def test_wrapper_creation_with_properties(self):
        """Test that the wrapper can be created with properties."""
        tracer = get_tracer(__name__)
        properties = {"span_name": "test.operation", "module": "test.module"}
        wrapper = _WeaviateTraceInjectionWrapper(tracer, wrap_properties=properties)
        assert wrapper.tracer == tracer
        assert wrapper.wrap_properties == properties

    def test_wrapper_call_creates_span(self, tracer_provider, span_exporter):
        """Test that calling the wrapper creates a span."""
        tracer = tracer_provider.get_tracer(__name__)
        properties = {"span_name": "collections.get"}
        wrapper = _WeaviateTraceInjectionWrapper(tracer, wrap_properties=properties)
        
        # Mock the wrapped function
        wrapped = Mock(__name__="get")
        wrapped.return_value = "test_result"
        instance = Mock()
        
        # Mock context variables
        with patch('opentelemetry.instrumentation.weaviate._connection_host_context') as mock_host_ctx, \
             patch('opentelemetry.instrumentation.weaviate._connection_port_context') as mock_port_ctx:
            
            mock_host_ctx.get.return_value = "localhost"
            mock_port_ctx.get.return_value = 8080
            
            # Call the wrapper
            result = wrapper(wrapped, instance, [], {})
        
        # Verify the wrapped function was called and result returned
        wrapped.assert_called_once_with()
        assert result == "test_result"
        
        # Check spans
        spans = span_exporter.get_finished_spans()
        assert len(spans) == 1
        
        span = spans[0]
        assert span.name == f"{SPAN_NAME_PREFIX}.get"
        assert span.attributes.get(DbAttributes.DB_SYSTEM_NAME) == "weaviate"
        assert span.attributes.get(DbAttributes.DB_OPERATION_NAME) == "collections.get"
        assert span.attributes.get(DbAttributes.DB_NAME) == "TBD"
        assert span.attributes.get(ServerAttributes.SERVER_ADDRESS) == "localhost"
        assert span.attributes.get(ServerAttributes.SERVER_PORT) == 8080

    def test_wrapper_call_without_context(self, tracer_provider, span_exporter):
        """Test that calling the wrapper works without connection context."""
        tracer = tracer_provider.get_tracer(__name__)
        wrapper = _WeaviateTraceInjectionWrapper(tracer)
        
        # Mock the wrapped function
        wrapped = Mock(__name__="test_operation")
        wrapped.return_value = "test_result"
        instance = Mock()
        
        # Mock context variables to return None
        with patch('opentelemetry.instrumentation.weaviate._connection_host_context') as mock_host_ctx, \
             patch('opentelemetry.instrumentation.weaviate._connection_port_context') as mock_port_ctx:
            
            mock_host_ctx.get.return_value = None
            mock_port_ctx.get.return_value = None
            
            # Call the wrapper
            result = wrapper(wrapped, instance, [], {})
        
        # Verify the wrapped function was called and result returned
        wrapped.assert_called_once_with()
        assert result == "test_result"
        
        # Check spans
        spans = span_exporter.get_finished_spans()
        assert len(spans) == 1
        
        span = spans[0]
        assert span.name == f"{SPAN_NAME_PREFIX}.test_operation"
        assert span.attributes.get(DbAttributes.DB_SYSTEM_NAME) == "weaviate"
        assert span.attributes.get(DbAttributes.DB_OPERATION_NAME) == "test_operation"
        # Should not have server attributes when context is None
        assert ServerAttributes.SERVER_ADDRESS not in span.attributes
        assert ServerAttributes.SERVER_PORT not in span.attributes

    def test_wrapper_call_with_exception(self, tracer_provider, span_exporter):
        """Test that the wrapper handles exceptions properly."""
        tracer = tracer_provider.get_tracer(__name__)
        wrapper = _WeaviateTraceInjectionWrapper(tracer)
        
        # Mock the wrapped function to raise an exception
        wrapped = Mock(__name__="failing_operation")
        wrapped.side_effect = Exception("Test exception")
        instance = Mock()
        
        # Mock context variables
        with patch('opentelemetry.instrumentation.weaviate._connection_host_context') as mock_host_ctx, \
             patch('opentelemetry.instrumentation.weaviate._connection_port_context') as mock_port_ctx:
            
            mock_host_ctx.get.return_value = None
            mock_port_ctx.get.return_value = None
            
            # Call the wrapper - should propagate the exception
            with pytest.raises(Exception, match="Test exception"):
                wrapper(wrapped, instance, [], {})
        
        # Verify the wrapped function was called
        wrapped.assert_called_once_with()
        
        # Check spans - span should still be created even with exception
        spans = span_exporter.get_finished_spans()
        assert len(spans) == 1
        
        span = spans[0]
        assert span.name == f"{SPAN_NAME_PREFIX}.failing_operation"
        assert span.attributes.get(DbAttributes.DB_SYSTEM_NAME) == "weaviate"

    def test_wrapper_call_with_args_and_kwargs(self, tracer_provider, span_exporter):
        """Test that the wrapper passes through args and kwargs correctly."""
        tracer = tracer_provider.get_tracer(__name__)
        wrapper = _WeaviateTraceInjectionWrapper(tracer)
        
        # Mock the wrapped function
        wrapped = Mock(__name__="test_operation")
        wrapped.return_value = "test_result"
        instance = Mock()
        
        args = ("arg1", "arg2")
        kwargs = {"key1": "value1", "key2": "value2"}
        
        # Mock context variables
        with patch('opentelemetry.instrumentation.weaviate._connection_host_context') as mock_host_ctx, \
             patch('opentelemetry.instrumentation.weaviate._connection_port_context') as mock_port_ctx:
            
            mock_host_ctx.get.return_value = None
            mock_port_ctx.get.return_value = None
            
            # Call the wrapper
            result = wrapper(wrapped, instance, args, kwargs)
        
        # Verify the wrapped function was called with correct args and kwargs
        wrapped.assert_called_once_with(*args, **kwargs)
        assert result == "test_result"
        
        # Check spans
        spans = span_exporter.get_finished_spans()
        assert len(spans) == 1

    def test_wrapper_with_unknown_function_name(self, tracer_provider, span_exporter):
        """Test that the wrapper handles functions without __name__ attribute."""
        tracer = tracer_provider.get_tracer(__name__)
        wrapper = _WeaviateTraceInjectionWrapper(tracer)
        
        # Mock a wrapped function without __name__
        wrapped = Mock()
        wrapped.return_value = "test_result"
        delattr(wrapped, '__name__')  # Remove __name__ attribute
        instance = Mock()
        
        # Mock context variables
        with patch('opentelemetry.instrumentation.weaviate._connection_host_context') as mock_host_ctx, \
             patch('opentelemetry.instrumentation.weaviate._connection_port_context') as mock_port_ctx:
            
            mock_host_ctx.get.return_value = None
            mock_port_ctx.get.return_value = None
            
            # Call the wrapper
            result = wrapper(wrapped, instance, [], {})
        
        # Verify the wrapped function was called
        wrapped.assert_called_once_with()
        assert result == "test_result"
        
        # Check spans - should use 'unknown' as function name
        spans = span_exporter.get_finished_spans()
        assert len(spans) == 1
        
        span = spans[0]
        assert span.name == f"{SPAN_NAME_PREFIX}.unknown"
        assert span.attributes.get(DbAttributes.DB_OPERATION_NAME) == "unknown"


if __name__ == "__main__":
    pytest.main([__file__])
