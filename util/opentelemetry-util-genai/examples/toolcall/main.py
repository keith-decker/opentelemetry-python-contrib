"""
Tool Call Demo - OpenTelemetry LangChain Instrumentation Example

Demonstrates automatic tool call instrumentation with LangChain.
The LangChain instrumentor automatically creates spans for:
- LLM calls (chat completions)
- Tool executions (via on_tool_start/on_tool_end callbacks)

Expected trace hierarchy:
    agent_workflow
    ├── chat gpt-4o-mini
    └── execute_tool get_weather

Run with: dotenv run -- python main.py

Requires: OPENAI_API_KEY environment variable
"""

from langchain_core.messages import HumanMessage
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI

from opentelemetry import _logs, metrics, trace
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import (
    OTLPLogExporter,
)
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
    OTLPMetricExporter,
)
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
    OTLPSpanExporter,
)
from opentelemetry.instrumentation.langchain import LangChainInstrumentor
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
)


def setup_telemetry():
    """Configure OpenTelemetry SDK with OTLP exporters."""
    # Create resource with custom service name
    resource = Resource.create({SERVICE_NAME: "toolcall-demo"})

    # Tracing - add ConsoleSpanExporter for debugging
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    trace.set_tracer_provider(provider)

    # Logging
    _logs.set_logger_provider(LoggerProvider())
    _logs.get_logger_provider().add_log_record_processor(
        BatchLogRecordProcessor(OTLPLogExporter())
    )

    # Metrics
    metrics.set_meter_provider(
        MeterProvider(
            metric_readers=[
                PeriodicExportingMetricReader(OTLPMetricExporter()),
            ]
        )
    )


# =============================================================================
# Define Tools using LangChain's @tool decorator
# =============================================================================


@tool
def get_weather(location: str) -> str:
    """Get current weather for a location.

    Args:
        location: The city name to get weather for.

    Returns:
        Weather information as a string.
    """
    # Simulated weather data
    weather_data = {
        "Paris": {"temp": 18, "condition": "cloudy"},
        "Tokyo": {"temp": 24, "condition": "sunny"},
        "New York": {"temp": 15, "condition": "rainy"},
    }
    data = weather_data.get(location, {"temp": 20, "condition": "unknown"})
    return f"Weather in {location}: {data['temp']}°C, {data['condition']}"


@tool
def calculate(expression: str) -> str:
    """Evaluate a mathematical expression safely.

    Args:
        expression: A mathematical expression like "2 + 2" or "10 * 5".

    Returns:
        The result of the calculation.
    """
    # Simple safe evaluation (only digits and basic operators)
    allowed = set("0123456789+-*/(). ")
    if all(c in allowed for c in expression):
        result = eval(expression)  # noqa: S307 - demo only with sanitized input
        return f"Result: {result}"
    return f"Error: Invalid expression '{expression}'"


# =============================================================================
# Main Demo
# =============================================================================


def main():
    print("OpenTelemetry LangChain Tool Call Demo")
    print("=" * 40)

    # Set up OpenTelemetry
    setup_telemetry()

    # Instrument LangChain - pass providers explicitly to avoid singleton issues
    LangChainInstrumentor().instrument(
        tracer_provider=trace.get_tracer_provider(),
    )

    # Create LLM with tools bound
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    tools = [get_weather, calculate]
    llm_with_tools = llm.bind_tools(tools)

    # Create a query that should trigger tool use
    print("\nSending query: 'What is the weather in Paris?'")
    messages = [HumanMessage(content="What is the weather in Paris?")]

    # Get tracer for agent span
    tracer = trace.get_tracer(__name__)

    # Wrap the entire agent loop in a parent span
    # This ensures LLM and tool calls share the same trace
    with tracer.start_as_current_span("agent_workflow") as agent_span:
        agent_span.set_attribute("gen_ai.operation.name", "invoke_agent")

        # Invoke the LLM - this creates a chat span (child of agent_workflow)
        response = llm_with_tools.invoke(messages)
        print(f"LLM Response: {response.content}")

        # Check if the LLM wants to call tools
        if response.tool_calls:
            print(f"\nTool calls requested: {len(response.tool_calls)}")

            for tool_call in response.tool_calls:
                print(f"  - {tool_call['name']}({tool_call['args']})")

                # Execute the tool - creates execute_tool span (child of agent_workflow)
                tool_func = {
                    "get_weather": get_weather,
                    "calculate": calculate,
                }[tool_call["name"]]
                result = tool_func.invoke(tool_call["args"])
                print(f"    Result: {result}")
        else:
            print("\nNo tool calls in response (LLM answered directly)")

    # Clean up
    LangChainInstrumentor().uninstrument()

    # Force flush spans before exit (BatchSpanProcessor buffers them)
    trace.get_tracer_provider().force_flush()

    print("\n" + "=" * 40)
    print("Demo complete! Check your OTLP endpoint for traces.")
    print("\nExpected span hierarchy:")
    print("  agent_workflow")
    print("  ├── chat gpt-4o-mini")
    print("  └── execute_tool get_weather")


if __name__ == "__main__":
    main()
