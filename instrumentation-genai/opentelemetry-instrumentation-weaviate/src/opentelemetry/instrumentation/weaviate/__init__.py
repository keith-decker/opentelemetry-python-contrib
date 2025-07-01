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

from typing import Collection

from wrapt import wrap_function_wrapper

from opentelemetry.instrumentation.weaviate.config import Config
from opentelemetry.instrumentation.weaviate.version import __version__
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.trace import get_tracer, Tracer
# from opentelemetry.metrics import get_meter
# from opentelemetry._events import get_event_logger
from opentelemetry.semconv._incubating.attributes import db_attributes as DbAttributes

# Potentially not needed.
from opentelemetry.semconv.schemas import Schemas

from .utils import dont_throw
from opentelemetry.instrumentation.utils import unwrap
from .mapping import CONNECTION_WRAPPING, SPAN_NAME_PREFIX, SPAN_WRAPPING

_instruments = ("weaviate-client >= 3.0.0, < 5",)

class WeaviateInstrumentor(BaseInstrumentor):
    """An instrumentor for Weaviate's client library."""

    def __init__(self, exception_logger=None):
        super().__init__()
        Config.exception_logger = exception_logger

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    @dont_throw
    def _instrument(self, **kwargs):
        tracer_provider = kwargs.get("tracer_provider")
        tracer = get_tracer(
            __name__,
            __version__,
            tracer_provider,
            schema_url=Schemas.V1_28_0.value,
        )

        # see if I can overload the connection methods
        
        wrap_function_wrapper(
            module="weaviate",
            name="WeaviateClient.__init__",
            wrapper=_WeaviateConnectionInjectionWrapper(tracer),
        )

        for to_wrap in SPAN_WRAPPING:
            wrap_function_wrapper(
                module=to_wrap["module"],
                name=to_wrap["name"],
                wrapper=_WeaviateTraceInjectionWrapper(tracer),
            )

    def _uninstrument(self, **kwargs):
        # Uninstrumenting is not implemented in this example.
        for to_unwrap in SPAN_WRAPPING:
            unwrap(
                to_unwrap["module"],
                to_unwrap["name"],
            )


class _WeaviateConnectionInjectionWrapper:
    """
    A wrapper that intercepts calls to weaviate connection methods to inject tracing headers.
    This is used to create spans for Weaviate connection operations.
    """

    def __init__(self, tracer: Tracer):
        self.tracer = tracer

    def __call__(self, wrapped, instance, args, kwargs):
        name = f"{SPAN_NAME_PREFIX}.{wrapped.__name__}"
        with self.tracer.start_as_current_span(name) as span:
            # Extract connection details from args/kwargs
            if hasattr(instance, 'url') or 'url' in kwargs:
                url = getattr(instance, 'url', kwargs.get('url', ''))
                span.set_attribute(DbAttributes.DB_CONNECTION_STRING, url)
            
            if hasattr(instance, '_connection') and hasattr(instance._connection, 'url'):
                span.set_attribute(DbAttributes.DB_CONNECTION_STRING, instance._connection.url)
                
            span.set_attribute(DbAttributes.DB_SYSTEM_NAME, "weaviate")
            
            return wrapped(*args, **kwargs)

class _WeaviateTraceInjectionWrapper:
    """
    A wrapper that intercepts calls to weaviate to inject tracing headers.
    This is used to create spans for Weaviate operations.
    """

    def __init__(self, tracer: Tracer):
        self.tracer = tracer

    def __call__(self, wrapped, instance, args, kwargs):
        """
        Wraps the original function to inject tracing headers.
        """
        name = f"{SPAN_NAME_PREFIX}.{wrapped.__name__}"
        with self.tracer.start_as_current_span(name) as span:
            span.set_attribute(DbAttributes.DB_SYSTEM_NAME, "weaviate")
            span.set_attribute(DbAttributes.DB_OPERATION_NAME, "query")
            span.set_attribute(DbAttributes.DB_NAME, "TBD")  # Replace with actual DB name if available

            return_value = wrapped(*args, **kwargs)

        return return_value