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

"""
Weaviate client instrumentation supporting `weaviate-client`, it can be enabled by
using ``WeaviateInstrumentor``.

.. _weaviate-client: https://pypi.org/project/weaviate-client/

Usage
-----

.. code:: python

    import weaviate
    from opentelemetry.instrumentation.weaviate import WeaviateInstrumentor

    WeaviateInstrumentor().instrument()

    client = weaviate.Client("http://localhost:8080")
    # Your Weaviate operations will now be traced

API
---
"""

from typing import Any, Collection, Dict, Optional
from contextvars import ContextVar

from wrapt import wrap_function_wrapper  # type: ignore

from opentelemetry.instrumentation.weaviate.config import Config
from opentelemetry.instrumentation.weaviate.version import __version__
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.trace import get_tracer, Tracer
# from opentelemetry.metrics import get_meter
# from opentelemetry._events import get_event_logger
from opentelemetry.semconv._incubating.attributes import db_attributes as DbAttributes
from opentelemetry.semconv._incubating.attributes import server_attributes as ServerAttributes


# Potentially not needed.
from opentelemetry.semconv.schemas import Schemas

from .utils import dont_throw, parse_url_to_host_port, extract_db_operation_name
from opentelemetry.instrumentation.utils import unwrap
from .mapping import SPAN_NAME_PREFIX, SPAN_WRAPPING  # type: ignore

_instruments = ("weaviate-client >= 3.0.0, < 5",)


# Context variable for passing connection info within operation call stacks
_connection_host_context: ContextVar[Optional[str]] = ContextVar('weaviate_connection_host', default=None)
_connection_port_context: ContextVar[Optional[int]] = ContextVar('weaviate_connection_port', default=None)


class WeaviateInstrumentor(BaseInstrumentor):
    """An instrumentor for Weaviate's client library."""

    def __init__(self, exception_logger: Optional[Any] = None) -> None:
        super().__init__()
        Config.exception_logger = exception_logger

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    @dont_throw
    def _instrument(self, **kwargs: Any) -> None:
        tracer_provider = kwargs.get("tracer_provider")
        tracer = get_tracer(
            __name__,
            __version__,
            tracer_provider,
            schema_url=Schemas.V1_28_0.value,
        )

        # Overload the init on the client to capture host and port
        # and set them in the context for later use in spans.
        wrap_function_wrapper(
            module="weaviate",
            name="WeaviateClient.__init__",
            wrapper=_WeaviateConnectionInjectionWrapper(tracer),
        )

        for to_wrap in SPAN_WRAPPING:  # type: ignore
            wrap_function_wrapper(
                module=to_wrap["module"],  # type: ignore
                name=to_wrap["name"],  # type: ignore
                wrapper=_WeaviateTraceInjectionWrapper(tracer, wrap_properties=to_wrap),  # type: ignore
            )

    def _uninstrument(self, **kwargs: Any) -> None:
        # Uninstrumenting is not implemented in this example.
        for to_unwrap in SPAN_WRAPPING:  # type: ignore
            unwrap(
                to_unwrap["module"],  # type: ignore
                to_unwrap["name"],  # type: ignore
            )


class _WeaviateConnectionInjectionWrapper:
    """
    A wrapper that intercepts calls to weaviate connection methods to inject tracing headers.
    This is used to create spans for Weaviate connection operations.
    """

    def __init__(self, tracer: Tracer):
        self.tracer = tracer

    def __call__(self, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        name = f"{SPAN_NAME_PREFIX}.{getattr(wrapped, '__name__', 'unknown')}"
        with self.tracer.start_as_current_span(name) as span:
            # Extract connection details from args/kwargs
            connection_host = None
            connection_port = None
      
            return_value = wrapped(*args, **kwargs)
            connection_url = None
            if hasattr(instance, '_connection') and instance._connection is not None:
                connection_url = instance._connection.url
            if connection_url:
                connection_host, connection_port = parse_url_to_host_port(connection_url)
            else:
                connection_host, connection_port = None, None
            _connection_host_context.set(connection_host)
            _connection_port_context.set(connection_port)
            if connection_host is not None:
                span.set_attribute(ServerAttributes.SERVER_ADDRESS, connection_host)
            if connection_port is not None:
                span.set_attribute(ServerAttributes.SERVER_PORT, connection_port)
            return return_value

class _WeaviateTraceInjectionWrapper:
    """
    A wrapper that intercepts calls to weaviate to inject tracing headers.
    This is used to create spans for Weaviate operations.
    """

    def __init__(self, tracer: Tracer, wrap_properties: Optional[Dict[str, str]] = None) -> None:
        self.tracer = tracer
        self.wrap_properties = wrap_properties or {}

    def __call__(self, wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        """
        Wraps the original function to inject tracing headers.
        """
        name = f"{SPAN_NAME_PREFIX}.{getattr(wrapped, '__name__', 'unknown')}"        
        with self.tracer.start_as_current_span(name) as span:
            span.set_attribute(DbAttributes.DB_SYSTEM_NAME, "weaviate")
            
            # Extract operation name dynamically from the function call
            module_name = self.wrap_properties.get("module", "")
            function_name = self.wrap_properties.get("name", "")
            operation_name = extract_db_operation_name(wrapped, module_name, function_name)
            
            span.set_attribute(DbAttributes.DB_OPERATION_NAME, operation_name)
            span.set_attribute(DbAttributes.DB_NAME, "TBD")  # Replace with actual DB name if available
            
            connection_host = _connection_host_context.get()
            connection_port = _connection_port_context.get()
            if connection_host is not None:
                span.set_attribute(ServerAttributes.SERVER_ADDRESS, connection_host)
            if connection_port is not None:
                span.set_attribute(ServerAttributes.SERVER_PORT, connection_port)

            return_value = wrapped(*args, **kwargs)

        return return_value
