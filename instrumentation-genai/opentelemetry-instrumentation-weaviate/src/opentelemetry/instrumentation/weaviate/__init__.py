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
from opentelemetry.semconv.trace import SpanAttributes

# Potentially not needed.
from opentelemetry.semconv.schemas import Schemas

_instruments = ("weaviate-client >= 3.0.0, < 5",)

class WeaviateInstrumentor(BaseInstrumentor):
    """An instrumentor for Weaviate's client library."""

    def __init__(self, exception_logger=None):
        super().__init__()
        Config.exception_logger = exception_logger

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs):
        tracer_provider = kwargs.get("tracer_provider")
        tracer = get_tracer(
            __name__,
            __version__,
            tracer_provider,
            schema_url=Schemas.V1_28_0.value,
        )

        wrap_function_wrapper(
            module="weaviate.client",
            name="WeaviateClient.graphql_raw_query",
            wrapper=_WeaviateTraceInjectionWrapper(tracer),
        )

        wrap_function_wrapper(
            module="weaviate.collections.collections",
            name="_Collections.get",
            wrapper=_WeaviateTraceInjectionWrapper(tracer),
        )

    #     {
    #     "module": "weaviate.collections.collections",
    #     "object": "_Collections",
    #     "method": "get",
    #     "span_name": "db.weaviate.collections.get",
    # },

    def _uninstrument(self, **kwargs):
        # Uninstrumenting is not implemented in this example.
        pass
        

class _WeaviateTraceInjectionWrapper:
    """
    A wrapper that intercepts calls to the underlying LLM code in LangChain
    to inject W3C trace headers into upstream requests (if possible).
    """

    def __init__(self, tracer: Tracer):
        self.tracer = tracer

    def __call__(self, wrapped, instance, args, kwargs):
        """
        Wraps the original function to inject tracing headers.
        """
        name = f"{wrapped.__module__}.{wrapped.__name__}"
        with self.tracer.start_as_current_span(name) as span:
            span.set_attribute(SpanAttributes.DB_SYSTEM, "weaviate")
            span.set_attribute(SpanAttributes.DB_OPERATION, "query")
            span.set_attribute(SpanAttributes.DB_NAME, "TBD")  # Replace with actual DB name if available

            return_value = wrapped(*args, **kwargs)

        return return_value