OpenTelemetry GenAI Tool Call Example
=====================================

This example demonstrates the ``ToolCall`` and ``TelemetryHandler`` APIs from
``opentelemetry-util-genai``. It shows how to create properly instrumented
tool call spans with nested hierarchy, simulating an AI agent that calls tools.

When ``main.py`` runs, it exports traces to an OTLP-compatible endpoint showing:

- Tool call spans with proper semantic convention attributes
- Nested span hierarchy: ``workflow → llm → tool_call``
- Error handling for failed tool executions

Sample Trace Output
-------------------

::

    Span: invoke_agent SimpleAgent
    ├── Attributes:
    │   └── gen_ai.operation.name: invoke_agent
    │
    └── Span: chat gpt-4
        ├── Attributes:
        │   ├── gen_ai.operation.name: chat
        │   ├── gen_ai.request.model: gpt-4
        │   └── gen_ai.provider.name: openai
        │
        ├── Span: execute_tool get_weather
        │   └── Attributes:
        │       ├── gen_ai.operation.name: execute_tool
        │       ├── gen_ai.tool.name: get_weather
        │       ├── gen_ai.tool.call.id: call_100
        │       └── gen_ai.tool.type: function
        │
        └── Span: execute_tool calculate
            └── Attributes:
                ├── gen_ai.operation.name: execute_tool
                ├── gen_ai.tool.name: calculate
                ├── gen_ai.tool.call.id: call_101
                └── gen_ai.tool.type: function

Setup
-----

1. Copy ``.env.example`` to ``.env`` and configure your OTLP endpoint:

   ::

       cp .env.example .env

2. Start an OTLP-compatible collector (e.g., Jaeger):

   ::

       docker run -d --name jaeger \
         -p 16686:16686 \
         -p 4317:4317 \
         jaegertracing/jaeger:2.6

3. Set up a virtual environment and install dependencies:

   ::

       python3 -m venv .venv
       source .venv/bin/activate
       pip install "python-dotenv[cli]"
       pip install -r requirements.txt

       # Install the local util-genai package
       pip install -e ../../

Run
---

::

    dotenv run -- python main.py

You should see console output showing the tool executions, and traces will
appear in your OTLP endpoint (e.g., Jaeger UI at http://localhost:16686).

Content Capturing
-----------------

To capture tool arguments and results in span attributes, set the environment
variable:

::

    export OTEL_SEMCONV_STABILITY_OPT_IN=gen_ai_latest_experimental
    export OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT=SPAN_ONLY

This adds ``gen_ai.tool.call.arguments`` and ``gen_ai.tool.call.result``
attributes to tool call spans.

API Overview
------------

The example demonstrates these key APIs:

``TelemetryHandler.tool_call(ToolCall)``
    Context manager for tool call spans. Automatically handles errors.

``ToolCall``
    Dataclass representing a tool invocation with:
    - ``name``: Tool name (required)
    - ``arguments``: Parameters passed to tool
    - ``id``: Unique call identifier
    - ``tool_type``: "function", "extension", or "datastore"
    - ``tool_description``: Human-readable description
    - ``tool_result``: Set inside context with execution result

``TelemetryHandler.llm(LLMInvocation)``
    Context manager for LLM spans (parent for tool calls).

``tracer.start_as_current_span()``
    Standard OpenTelemetry tracer for root/agent spans.
