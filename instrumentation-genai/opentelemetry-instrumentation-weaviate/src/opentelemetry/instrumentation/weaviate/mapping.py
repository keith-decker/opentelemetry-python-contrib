SPAN_NAME_PREFIX: str = "db.weaviate"

CONNECTION_WRAPPING: list[dict[str, str]] = [
    {
        "module": "weaviate",
        "name": "connect_to_local"
    },
    {
        "module": "weaviate",
        "name": "connect_to_weaviate_cloud"
    },
    {
        "module": "weaviate",
        "name": "connect_to_custom"
    }
]

SPAN_WRAPPING: list[dict[str, str]] = [
    #Collections
    {
        "module": "weaviate.collections.collections",
        "name": "_Collections.get",
        "span_name": "collections.get",
    },
    {
        "module": "weaviate.collections.collections",
        "name": "_Collections.create",
        "span_name": "collections.create",
    },
    {
        "module": "weaviate.collections.collections",
        "name": "_Collections.delete",
        "span_name": "collections.delete",
    },
    {
        "module": "weaviate.collections.collections",
        "name": "_Collections.delete_all",
        "span_name": "collections.delete_all",
    },
    {
        "module": "weaviate.collections.collections",
        "name": "_Collections.create_from_dict",
        "span_name": "collections.create_from_dict",
    },

    # Batch
    {
        "module": "weaviate.collections.batch.collection",
        "name": "_BatchCollection.add_object",
        "span_name": "collections.batch.add_object",
    },


    # Data - Collection
    {
        "module": "weaviate.collections.data",
        "name": "_DataCollection.insert",
        "span_name": "collections.data.insert",
    },
    {
        "module": "weaviate.collections.data",
        "name": "_DataCollection.replace",
        "span_name": "collections.data.replace",
    },
    {
        "module": "weaviate.collections.data",
        "name": "_DataCollection.update",
        "span_name": "collections.data.update",
    },

    # queries
    # {
    #     "module": "weaviate.collections.queries.fetch_object_by_id.query",
    #     "name": "_FetchObjectByIDQuery.fetch_object_by_id",
    #     "span_name": "collections.query.fetch_object_by_id",
    # },
    {
        "module": "weaviate.collections.queries.near_text.query",
        "name": "_NearTextQuery.near_text",
        "span_name": "collections.query.near_text",
    },
    {
        "module": "weaviate.collections.queries.fetch_objects.query",
        "name": "_FetchObjectsQuery.fetch_objects",
        "span_name": "collections.query.fetch_objects",
    },
    {
        "module": "weaviate.collections.grpc.query",
        "name": "_QueryGRPC.get",
        "span_name": "collections.query.get",
    },

    # GraphQL
    # {
    #     "module": "weaviate.gql.filter",
    #     "name": "GraphQL.do",
    #     "span_name": "gql.filter.do",
    # },
    # {
    #     "module": "weaviate.gql.aggregate",
    #     "name": "AggregateBuilder.do",
    #     "span_name": "gql.aggregate.do",
    # },
    # {
    #     "module": "weaviate.gql.get",
    #     "name": "GetBuilder.do",
    #     "span_name": "gql.get.do",
    # }, 
    {
        "module": "weaviate.client",
        "name": "WeaviateClient.graphql_raw_query",
        "span_name": "client.graphql_raw_query",
    },

    # internal functions
    {
        "module": "weaviate.connect.executor",
        "name": "execute",
        "span_name": "connect.executor.execute",
    },
]
