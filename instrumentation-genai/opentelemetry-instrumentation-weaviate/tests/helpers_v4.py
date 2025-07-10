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

from unittest import mock
from typing import Any, Dict, List


class MockWeaviateV4Client:
    """Mock Weaviate v4 client for testing."""
    
    def __init__(self, connection_params: Dict[str, Any] = None, **kwargs):
        self._connection = MockConnection()
        self.collections = MockCollections()


class MockConnection:
    """Mock Weaviate v4 connection."""
    
    def __init__(self, url: str = "http://localhost:8080"):
        self.url = url


class MockCollections:
    """Mock Weaviate v4 Collections management."""
    
    def get(self, name: str) -> 'MockCollection':
        return MockCollection(name)
    
    def create(self, name: str, **kwargs) -> 'MockCollection':
        return MockCollection(name)
    
    def delete(self, name: str) -> None:
        return None
    
    def delete_all(self) -> None:
        return None
    
    def create_from_dict(self, config: Dict[str, Any]) -> 'MockCollection':
        return MockCollection(config.get("class", "MockClass"))


class MockCollection:
    """Mock Weaviate v4 Collection."""
    
    def __init__(self, name: str):
        self.name = name
        self.data = MockDataCollection()
        self.query = MockQueryCollection()
        self.batch = MockBatchCollection()
    
    def insert(self, properties: Dict[str, Any], **kwargs) -> str:
        return "mock-uuid"
    
    def replace(self, uuid: str, properties: Dict[str, Any], **kwargs) -> None:
        return None
    
    def update(self, uuid: str, properties: Dict[str, Any], **kwargs) -> None:
        return None


class MockDataCollection:
    """Mock Weaviate v4 DataCollection operations."""
    
    def insert(self, properties: Dict[str, Any], **kwargs) -> str:
        return "mock-uuid"
    
    def replace(self, uuid: str, properties: Dict[str, Any], **kwargs) -> None:
        return None
    
    def update(self, uuid: str, properties: Dict[str, Any], **kwargs) -> None:
        return None


class MockQueryCollection:
    """Mock Weaviate v4 Query operations."""
    
    def near_text(self, query: str, **kwargs) -> 'MockQueryResult':
        return MockQueryResult()
    
    def fetch_objects(self, limit: int = None, **kwargs) -> 'MockQueryResult':
        return MockQueryResult()
    
    def get(self, uuid: str = None, **kwargs) -> Dict[str, Any]:
        return {"properties": {}, "uuid": uuid or "mock-uuid"}


class MockBatchCollection:
    """Mock Weaviate v4 Batch operations."""
    
    def add_object(self, properties: Dict[str, Any], **kwargs) -> None:
        return None


class MockQueryResult:
    """Mock Weaviate v4 query result."""
    
    def __init__(self):
        self.objects = [MockQueryObject()]


class MockQueryObject:
    """Mock Weaviate v4 query result object."""
    
    def __init__(self):
        self.properties = {"title": "Mock document", "content": "Mock content"}
        self.metadata = MockMetadata()


class MockMetadata:
    """Mock Weaviate v4 object metadata."""
    
    def __init__(self):
        self.distance = 0.1
        self.certainty = 0.9
        self.score = 0.95


def create_mock_weaviate_v4_client() -> MockWeaviateV4Client:
    """Create a mock Weaviate v4 client."""
    return MockWeaviateV4Client()


def patch_weaviate_v4():
    """Context manager to patch weaviate v4 modules."""
    return mock.patch.dict(
        'sys.modules', {
            'weaviate': mock.MagicMock(),
            'weaviate.collections.collections': mock.MagicMock(),
            'weaviate.collections.data': mock.MagicMock(),
            'weaviate.collections.queries.near_text.query': mock.MagicMock(),
            'weaviate.collections.queries.fetch_objects.query': mock.MagicMock(),
            'weaviate.collections.grpc.query': mock.MagicMock(),
            'weaviate.collections.batch.collection': mock.MagicMock(),
        }
    )
