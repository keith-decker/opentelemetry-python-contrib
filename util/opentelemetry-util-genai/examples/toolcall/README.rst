OpenTelemetry LangChain Tool Call Example
=========================================

This example demonstrates automatic tool call instrumentation with LangChain.
The ``LangChainInstrumentor`` automatically creates spans for:

- LLM calls (chat completions)
- Tool executions (via ``on_tool_start``/``on_tool_end`` callbacks)

When ``main.py`` runs, it exports traces to an OTLP-compatible endpoint showing:

- Chat span for the LLM invocation
- Tool call span as a child of the chat span
- Proper semantic convention attributes on both spans

Sample Trace Output
-------------------

::

    Span: chat gpt-4o-mini
    ├── Kind: Client
    ├── Attributes:
    │   ├── gen_ai.operation.name: chat
    │   ├── gen_ai.request.model: gpt-4o-mini
    │   └── gen_ai.provider.name: openai
    │
    └── Span: execute_tool get_weather
        ├── Kind: Internal
        └── Attributes:
            ├── gen_ai.operation.name: execute_tool
            ├── gen_ai.tool.name: get_weather
            ├── gen_ai.tool.call.id: <uuid>
            └── gen_ai.tool.type: function

Setup
-----

1. Copy ``.env.example`` to ``.env`` and add your OpenAI API key:

   ::

       cp .env.example .env
       # Edit .env and set OPENAI_API_KEY=sk-...

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

       # Install local packages (from repo root)
       pip install -e ../../                                    # util-genai
       pip install -e ../../../../instrumentation-genai/opentelemetry-instrumentation-langchain

Run
---

::

    dotenv run -- python main.py

You should see console output like:

::

    OpenTelemetry LangChain Tool Call Demo
    ========================================

    Sending query: 'What is the weather in Paris?'
    LLM Response:

    Tool calls requested: 1
      - get_weather({'location': 'Paris'})
        Result: Weather in Paris: 18°C, cloudy

    ========================================
    Demo complete! Check your OTLP endpoint for traces.

Traces will appear in Jaeger UI at http://localhost:16686.

Content Capturing
-----------------

To capture tool arguments and results in span attributes, set the environment
variables (already in ``.env.example``):

::

    export OTEL_SEMCONV_STABILITY_OPT_IN=gen_ai_latest_experimental
    export OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT=SPAN_ONLY

This adds ``gen_ai.tool.call.arguments`` and ``gen_ai.tool.call.result``
attributes to tool call spans.

How It Works
------------

The ``LangChainInstrumentor`` uses callback handlers to intercept LangChain
operations:

1. ``on_chat_model_start`` - Creates a chat span when the LLM is invoked
2. ``on_llm_end`` - Ends the chat span with token usage and response data
3. ``on_tool_start`` - Creates an ``execute_tool`` span when a tool runs
4. ``on_tool_end`` - Ends the tool span with the result

All spans follow the OpenTelemetry GenAI semantic conventions.
