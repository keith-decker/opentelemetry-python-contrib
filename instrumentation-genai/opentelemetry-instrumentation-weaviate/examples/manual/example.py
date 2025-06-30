import weaviate
import json
import os
from opentelemetry.instrumentation.weaviate import WeaviateInstrumentor

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Configure OpenTelemetry SDK with both OTLP and console exporters
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# Set up the tracer provider
trace.set_tracer_provider(TracerProvider())

# Add OTLP exporter (reads from OTEL_EXPORTER_OTLP_ENDPOINT env var)
otlp_exporter = OTLPSpanExporter(
    endpoint=os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"),
    headers=()
)
otlp_processor = BatchSpanProcessor(otlp_exporter)
trace.get_tracer_provider().add_span_processor(otlp_processor)

# Add console exporter to see traces in terminal as well
console_exporter = ConsoleSpanExporter()
console_processor = BatchSpanProcessor(console_exporter)
trace.get_tracer_provider().add_span_processor(console_processor)

# Now instrument Weaviate
WeaviateInstrumentor().instrument()

client = weaviate.connect_to_local()

questions = client.collections.get("Question")

response = questions.query.near_text(
    query="biology",
    limit=2
)

for obj in response.objects:
    print(json.dumps(obj.properties, indent=2))

client.close()

# Ensure all spans are exported before exiting
trace.get_tracer_provider().force_flush(timeout_millis=5000)