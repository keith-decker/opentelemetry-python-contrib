import weaviate
import json
import os
from opentelemetry.instrumentation.weaviate import WeaviateInstrumentor
from weaviate.classes.query import MetadataQuery
from weaviate.classes.config import Configure

# Load environment variables
from dotenv import load_dotenv

load_dotenv()

# Configure OpenTelemetry SDK with both OTLP and console exporters
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# Set up the tracer provider
tracer_provider = TracerProvider()
trace.set_tracer_provider(tracer_provider)

# Add OTLP exporter (reads from OTEL_EXPORTER_OTLP_ENDPOINT env var)
otlp_exporter = OTLPSpanExporter(
    endpoint=os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"),
    headers=(),
)
otlp_processor = BatchSpanProcessor(otlp_exporter)
tracer_provider.add_span_processor(otlp_processor)

# Add console exporter to see traces in terminal as well
console_exporter = ConsoleSpanExporter()
console_processor = BatchSpanProcessor(console_exporter)
tracer_provider.add_span_processor(console_processor)

# Now instrument Weaviate
WeaviateInstrumentor().instrument()


def create_collection(client: weaviate.Client):
    """Create a collection of movies with a title and synopsis."""

    vectorizer = weaviate.classes.config.Configure.Vectorizer.text2vec_openai
    generative = weaviate.classes.config.Configure.Generative.openai
    try:
        if client.collections.exists("Movie"):
            print("Collection 'Movie' already exists")
            cleanup_collection(client)

        print("Creating collection 'Movie'")
        movies = client.collections.create(
            name="Movie",
            description="A collection of movies",
            vectorizer_config=vectorizer(base_url="http://host.docker.internal:1234"),
            generative_config=generative(base_url="http://host.docker.internal:1234"),
            properties=[
                weaviate.classes.config.Property(
                    name="title",
                    data_type=weaviate.classes.config.DataType.TEXT,
                    description="The title of the movie",
                ),
                weaviate.classes.config.Property(
                    name="synopsis",
                    data_type=weaviate.classes.config.DataType.TEXT,
                    description="A brief synopsis of the movie",
                ),
                weaviate.classes.config.Property(
                    name="genre",
                    data_type=weaviate.classes.config.DataType.TEXT,
                    description="The genre(s) of the movie",
                ),
            ],
        )
        print("Collection 'Movie' created successfully")
        return movies
    except Exception as e:
        print(f"Error creating collection: {e}")
        return None


def cleanup_collection(client):
    """Clean up by deleting the collection."""
    try:
        if client.collections.exists("Movie"):
            client.collections.delete("Movie")
            print(f'Cleaned up collection: "Movie"')
    except Exception as e:
        print(f"Error cleaning up: {e}")


def insert_single_object(collection):
    """Insert a single object into the collection."""
    try:
        uuid = collection.data.insert(
            {
                "title": "2001: A Space Odyssey",
                "synopsis": "Humanity finds a mysterious object buried beneath the lunar surface and sets off to find its origins with the help of HAL 9000, the world's most advanced super computer.",
                "genre": "Science Fiction",
            }
        )
        print(f"Inserted object with UUID: {uuid}")
        return uuid
    except Exception as e:
        print(f"Error inserting object: {e}")
        return None


def insert_batch_objects(collection):
    """Insert multiple objects using batch operations."""
    try:
        objects = [
            {
                "title": "Legion of Super-Heroes",
                "synopsis": "Kara, devastated by the loss of Krypton, struggles to adjust to her new life on Earth. Her cousin, Superman, mentors her and suggests she leave their space-time to attend the Legion Academy in the 31st century, where she makes new friends and a new enemy: Brainiac 5. Meanwhile, she must contend with a mysterious group called the Dark Circle as it searches for a powerful weapon held in the Academy’s vault.",
                "genre": "Science Fiction",
            },
            {
                "title": "65",
                "synopsis": "After a catastrophic crash on an unknown planet, pilot Mills quickly discovers he's actually stranded on Earth…65 million years ago. Now, with only one chance at rescue, Mills and the only other survivor, Koa, must make their way across an unknown terrain riddled with dangerous prehistoric creatures in an epic fight to survive.",
                "genre": "Science Fiction",
            },
            {
                "title": "Project Gemini",
                "synopsis": "After depleting Earth's resources for centuries, humankind's survival requires an exodus to outer space. An international expedition is quickly formed to find a suitable new planet, but when plans go awry, the crew is suddenly stranded without power on a strange planet, where something unimaginable lies in wait.",
                "genre": "Science Fiction",
            },
            {
                "title": "Interstellar",
                "synopsis": "The adventures of a group of explorers who make use of a newly discovered wormhole to surpass the limitations on human space travel and conquer the vast distances involved in an interstellar voyage.",
                "genre": "Adventure",
            },
            {
                "title": "Die Neue These 1",
                "synopsis": "The story focuses on the exploits of rivals Reinhard von Lohengramm and Yang Wen-li, as they rise to power and fame in the Galactic Empire and the Free Planets Alliance, respectively.",
                "genre": "Animation",
            },
        ]

        with collection.batch.dynamic() as batch:
            for obj in objects:
                batch.add_object(obj)

        print(f"Inserted {len(objects)} objects via batch operation")
    except Exception as e:
        print(f"Error in batch insert: {e}")


def get_object_by_id(collection: weaviate.collections.Collection, uuid):
    """Get a specific object by UUID."""
    try:
        obj = collection.query.fetch_object_by_id(uuid)
        if obj:
            print(f"Retrieved object by ID:")
            print(f"  UUID: {obj.uuid}")
            print(f"  Title: {obj.properties.get('title', 'Unknown')}")
            print(f"  Synopsis: {obj.properties.get('synopsis', '')[:100]}...")
            print(f"  Genre: {obj.properties.get('genre', 'Unknown')}")
        return obj
    except Exception as e:
        print(f"Error getting object by ID: {e}")
        return None


def update_object(collection, uuid):
    """Update an existing object."""
    try:
        collection.data.update(
            uuid=uuid,
            properties={
                "genre": "Updated Science Fiction",
            },
        )
        print(f"Updated object {uuid}")
    except Exception as e:
        print(f"Error updating object: {e}")


def query_near_text(collection):
    """Perform a near_text query."""
    try:
        response = collection.query.near_text(
            query="exploration of space and time",
            limit=3,
            return_metadata=MetadataQuery(distance=True, certainty=True),
        )

        print("Near text query results:")
        for obj in response.objects:
            print(f"  Title: {obj.properties.get('title', 'Unknown')}")
            print(f"  Synopsis: {obj.properties.get('synopsis', '')[:100]}...")
            print(f"  Distance: {obj.metadata.distance}")
            print(f"  Certainty: {obj.metadata.certainty}")
            print("  ---")

        return response
    except Exception as e:
        print(f"Error in near_text query: {e}")
        return None


def query_fetch_objects(collection):
    """Fetch objects with specific creation time."""
    try:
        response = collection.query.fetch_objects(
            limit=5,
            return_metadata=MetadataQuery(
                creation_time=True, distance=True, certainty=True
            ),
        )

        print("Fetch objects results:")
        for obj in response.objects:
            print(f"  Title: {obj.properties.get('title', 'Unknown')}")
            print(f"  Genre: {obj.properties.get('genre', 'Unknown')}")
            print(f"  Creation time: {obj.metadata.creation_time}")
            print("  ---")

        return response
    except Exception as e:
        print(f"Error fetching objects: {e}")
        return None


def query_where_filter(collection):
    """Query with where filter."""
    try:
        from weaviate.classes.query import Filter

        response = collection.query.fetch_objects(
            filters=Filter.by_property("genre").contains_all(["Science Fiction"]),
            limit=10,
        )

        print("Science Fiction Movies:")
        for obj in response.objects:
            print(f"  Title: {obj.properties.get('title', 'Unknown')}")
            print(f"  Genre: {obj.properties.get('genre', 'Unknown')}")
            print("  ---")

        return response
    except Exception as e:
        print(f"Error in where filter query: {e}")
        return None


def aggregate_query(collection):
    """Perform aggregation query."""
    try:
        response = collection.aggregate.over_all(group_by="genre")

        print("Aggregation results by Genre:")
        if hasattr(response, "groups"):
            for group in response.groups:
                print(f"  Genre: {group.grouped_by.value}")
                print(f"  Count: {group.total_count}")
                print("  ---")
        else:
            print(f"  Total count: {response.total_count}")

        return response
    except Exception as e:
        print(f"Error in aggregation query: {e}")
        return None


def delete_object(collection, uuid):
    """Delete an object by UUID."""
    try:
        collection.data.delete_by_id(uuid)
        print(f"Deleted object {uuid}")
    except Exception as e:
        print(f"Error deleting object: {e}")


def main():
    print("OpenTelemetry Weaviate v4 instrumentation initialized")

    # Configure additional headers for API keys
    # This example is connected to local Weaviate instance in docker,
    # with an embedding model on lms using openai's api
    client = weaviate.connect_to_local(headers={"X-OpenAI-Api-Key": "1238457"})

    print("== Client connected to Weaviate ==")

    try:

        # create a collection
        collection = create_collection(client)
        if collection is None:
            print("Failed to create collection, exiting workflow")
            return

        print("== Collection created, starting insert workflows ==")
        # Single object operations
        print("Single object operations")
        uuid = insert_single_object(collection)
        if uuid:
            get_object_by_id(collection, uuid)
            update_object(collection, uuid)
            get_object_by_id(collection, uuid)

        print("Batch operations")
        # Batch operations
        insert_batch_objects(collection)

        # Query operations
        print("== Data inserted, starting query workflows ==")
        query_near_text(collection)
        query_fetch_objects(collection)
        query_where_filter(collection)
        aggregate_query(collection)

        print("Cleanup operations")
        # Cleanup single object
        if uuid:
            delete_object(collection, uuid)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # clean up the connection
        cleanup_collection(client)
        client.close()
        print("Weaviate client connection closed")


if __name__ == "__main__":
    main()
