"""
Tool Call Demo - OpenTelemetry GenAI Utility Example

Demonstrates the ToolCall and TelemetryHandler APIs from opentelemetry-util-genai.
Shows how to create properly instrumented tool call spans with nested hierarchy:

    invoke_agent SimpleAgent
    └── chat gpt-4
        ├── execute_tool get_weather
        └── execute_tool calculate

Run with: dotenv run -- python main.py
"""

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
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.util.genai.handler import TelemetryHandler
from opentelemetry.util.genai.types import (
    LLMInvocation,
    OutputMessage,
    Text,
    ToolCall,
    ToolCallRequest,
)


def setup_telemetry():
    """Configure OpenTelemetry SDK with OTLP exporters."""
    # Tracing
    trace.set_tracer_provider(TracerProvider())
    trace.get_tracer_provider().add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter())
    )

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
# Mock Tools - Simulating real tool implementations
# =============================================================================


def get_weather(location: str) -> dict:
    """Get current weather for a location."""
    # Simulate API call
    return {
        "location": location,
        "temperature": 22,
        "condition": "sunny",
        "humidity": 45,
    }


def calculate(expression: str) -> float:
    """Evaluate a mathematical expression."""
    # Simulate safe calculation (in reality, use a proper parser)
    allowed = set("0123456789+-*/(). ")
    if all(c in allowed for c in expression):
        return eval(expression)  # noqa: S307 - demo only
    raise ValueError(f"Invalid expression: {expression}")


# =============================================================================
# Demo: Tool Call with Context Manager
# =============================================================================


def demo_tool_call_context_manager(handler: TelemetryHandler):
    """Demonstrate the context manager pattern for tool calls.

    This is the recommended approach - clean, handles errors automatically.
    """
    print("\n=== Demo: Tool Call Context Manager ===")

    # Successful tool call
    tool = ToolCall(
        name="get_weather",
        arguments={"location": "Paris"},
        id="call_001",
        tool_type="function",
        tool_description="Get current weather for a location",
    )

    with handler.tool_call(tool) as tc:
        # Execute the actual tool
        result = get_weather(tc.arguments["location"])
        tc.tool_result = result
        print(f"Weather result: {result}")

    # Tool call with error (exception auto-handled)
    print("\nDemonstrating error handling...")
    error_tool = ToolCall(
        name="calculate",
        arguments={"expression": "invalid_expr"},
        id="call_002",
        tool_type="function",
    )

    try:
        with handler.tool_call(error_tool) as tc:
            result = calculate(tc.arguments["expression"])
            tc.tool_result = result
    except ValueError as e:
        print(f"Tool failed (expected): {e}")


# =============================================================================
# Demo: Nested Span Hierarchy (Workflow -> LLM -> Tool)
# =============================================================================


def demo_nested_hierarchy(handler: TelemetryHandler):
    """Demonstrate proper span nesting: agent -> llm -> tool calls.

    This shows how tool calls appear as children of the LLM span that
    triggered them, all within an agent/workflow span.
    """
    print("\n=== Demo: Nested Span Hierarchy ===")

    # Simulated LLM response with tool calls
    mock_tool_calls = [
        ToolCallRequest(
            name="get_weather", arguments={"location": "Tokyo"}, id="call_100"
        ),
        ToolCallRequest(
            name="calculate", arguments={"expression": "25 * 4"}, id="call_101"
        ),
    ]

    # Create a root span to represent the agent/workflow
    # (In a real app, this might come from a workflow handler or framework)
    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span(
        "invoke_agent SimpleAgent"
    ) as agent_span:
        agent_span.set_attribute("gen_ai.operation.name", "invoke_agent")
        print("Started agent: SimpleAgent")

        # Create LLM span
        llm = LLMInvocation(
            request_model="gpt-4",
            provider="openai",
        )

        with handler.llm(llm) as llm_inv:
            print("  LLM call: gpt-4")

            # Simulate LLM deciding to call tools
            llm_inv.output_messages = [
                OutputMessage(
                    role="assistant",
                    parts=[
                        Text("I'll check the weather and do a calculation."),
                    ],
                    finish_reason="tool_calls",
                )
            ]

            # Execute each tool call as child span
            for tool_request in mock_tool_calls:
                tool = ToolCall(
                    name=tool_request.name,
                    arguments=tool_request.arguments,
                    id=tool_request.id,
                    tool_type="function",
                )

                with handler.tool_call(tool) as tc:
                    print(f"    Executing tool: {tc.name}")
                    if tc.name == "get_weather":
                        tc.tool_result = get_weather(tc.arguments["location"])
                    elif tc.name == "calculate":
                        tc.tool_result = calculate(tc.arguments["expression"])
                    print(f"    Result: {tc.tool_result}")

    print("Agent completed")


# =============================================================================
# Main
# =============================================================================


def main():
    print("OpenTelemetry GenAI Tool Call Demo")
    print("=" * 40)

    # Set up OpenTelemetry
    setup_telemetry()

    # Get the telemetry handler
    handler = TelemetryHandler()

    # Run demos
    demo_tool_call_context_manager(handler)
    demo_nested_hierarchy(handler)

    print("\n" + "=" * 40)
    print("Demo complete! Check your OTLP endpoint for traces.")
    print("Expected span hierarchy:")
    print("  invoke_agent SimpleAgent")
    print("  └── chat gpt-4")
    print("      ├── execute_tool get_weather")
    print("      └── execute_tool calculate")


if __name__ == "__main__":
    main()
