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
from typing import Any, Dict


class MockWeaviateV3Client:
    """Mock Weaviate v3 client for testing."""
    
    def __init__(self, url: str = "http://localhost:8080", **kwargs):
        self.url = url
        self.schema = MockSchema()
        self.data_object = MockDataObject()
        self.batch = MockBatch()
        self.query = MockQuery()


class MockSchema:
    """Mock Weaviate v3 Schema operations."""
    
    def get(self, class_name: str = None) -> Dict[str, Any]:
        return {"classes": []}
    
    def create_class(self, schema_class: Dict[str, Any]) -> None:
        return None
    
    def create(self, schema: Dict[str, Any]) -> None:
        return None
    
    def delete_class(self, class_name: str) -> None:
        return None
    
    def delete_all(self) -> None:
        return None


class MockDataObject:
    """Mock Weaviate v3 DataObject operations."""
    
    def create(self, data_object: Dict[str, Any], class_name: str, **kwargs) -> str:
        return "mock-uuid"
    
    def validate(self, data_object: Dict[str, Any], class_name: str, **kwargs) -> Dict[str, Any]:
        return {"valid": True}
    
    def get(self, uuid: str = None, class_name: str = None, **kwargs) -> Dict[str, Any]:
        return {"properties": {}, "id": uuid or "mock-uuid"}


class MockBatch:
    """Mock Weaviate v3 Batch operations."""
    
    def add_data_object(self, data_object: Dict[str, Any], class_name: str, **kwargs) -> None:
        return None
    
    def flush(self) -> None:
        return None


class MockQuery:
    """Mock Weaviate v3 Query operations."""
    
    def get(self, class_name: str = None, properties: list = None) -> 'MockGetBuilder':
        return MockGetBuilder()
    
    def aggregate(self, class_name: str = None) -> Dict[str, Any]:
        return {"data": {"Aggregate": {}}}
    
    def raw(self, gql_query: str) -> Dict[str, Any]:
        return {"data": {}}


class MockGetBuilder:
    """Mock Weaviate v3 GetBuilder for query chaining."""
    
    def do(self) -> Dict[str, Any]:
        return {"data": {"Get": {}}}


def create_mock_weaviate_v3_client(url: str = "http://localhost:8080") -> MockWeaviateV3Client:
    """Create a mock Weaviate v3 client."""
    return MockWeaviateV3Client(url)


def patch_weaviate_v3():
    """Context manager to patch weaviate v3 modules."""
    return mock.patch.dict(
        'sys.modules', {
            'weaviate': mock.MagicMock(),
            'weaviate.schema': mock.MagicMock(),
            'weaviate.data.crud_data': mock.MagicMock(),
            'weaviate.batch.crud_batch': mock.MagicMock(),
            'weaviate.gql.query': mock.MagicMock(),
            'weaviate.gql.get': mock.MagicMock(),
        }
    )
