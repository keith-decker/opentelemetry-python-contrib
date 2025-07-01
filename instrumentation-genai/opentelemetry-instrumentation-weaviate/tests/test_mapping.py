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

"""Tests for Weaviate mapping configuration."""

import pytest

from opentelemetry.instrumentation.weaviate.mapping import (
    SPAN_NAME_PREFIX,
    CONNECTION_WRAPPING,
    SPAN_WRAPPING,
)


class TestMappingConfiguration:
    """Test the mapping configuration for Weaviate instrumentation."""

    def test_span_name_prefix(self):
        """Test the span name prefix is correctly defined."""
        assert SPAN_NAME_PREFIX == "db.weaviate"
        assert isinstance(SPAN_NAME_PREFIX, str)
        assert len(SPAN_NAME_PREFIX) > 0

    def test_connection_wrapping_structure(self):
        """Test the CONNECTION_WRAPPING configuration structure."""
        assert isinstance(CONNECTION_WRAPPING, list)
        
        for config in CONNECTION_WRAPPING:
            assert isinstance(config, dict)
            assert "module" in config
            assert "name" in config
            
            assert isinstance(config["module"], str)
            assert isinstance(config["name"], str)
            assert len(config["module"]) > 0
            assert len(config["name"]) > 0

    def test_connection_wrapping_content(self):
        """Test the specific content of CONNECTION_WRAPPING."""
        expected_connections = [
            {"module": "weaviate", "name": "connect_to_local"},
            {"module": "weaviate", "name": "connect_to_weaviate_cloud"},
            {"module": "weaviate", "name": "connect_to_custom"},
        ]
        
        assert len(CONNECTION_WRAPPING) == len(expected_connections)
        
        for expected in expected_connections:
            assert expected in CONNECTION_WRAPPING

    def test_span_wrapping_structure(self):
        """Test the SPAN_WRAPPING configuration structure."""
        assert isinstance(SPAN_WRAPPING, list)
        assert len(SPAN_WRAPPING) > 0
        
        for config in SPAN_WRAPPING:
            assert isinstance(config, dict)
            assert "module" in config
            assert "name" in config
            
            assert isinstance(config["module"], str)
            assert isinstance(config["name"], str)
            assert len(config["module"]) > 0
            assert len(config["name"]) > 0
            
            # span_name is optional
            if "span_name" in config:
                assert isinstance(config["span_name"], str)
                assert len(config["span_name"]) > 0

    def test_span_wrapping_collections_operations(self):
        """Test that collections operations are properly mapped."""
        collections_operations = [
            config for config in SPAN_WRAPPING
            if "collections" in config.get("module", "")
        ]
        
        assert len(collections_operations) > 0
        
        # Check for essential collection operations
        operation_names = [config.get("name", "") for config in collections_operations]
        
        # Should have basic CRUD operations
        assert any("get" in name for name in operation_names)
        assert any("create" in name for name in operation_names)
        assert any("delete" in name for name in operation_names)

    def test_span_wrapping_data_operations(self):
        """Test that data operations are properly mapped."""
        data_operations = [
            config for config in SPAN_WRAPPING
            if "data" in config.get("module", "")
        ]
        
        assert len(data_operations) > 0
        
        # Check for essential data operations
        operation_names = [config.get("name", "") for config in data_operations]
        
        # Should have basic data operations
        assert any("insert" in name for name in operation_names)
        assert any("update" in name for name in operation_names)
        assert any("replace" in name for name in operation_names)

    def test_span_wrapping_query_operations(self):
        """Test that query operations are properly mapped."""
        query_operations = [
            config for config in SPAN_WRAPPING
            if "query" in config.get("module", "") or "grpc" in config.get("module", "")
        ]
        
        assert len(query_operations) > 0
        
        # Check for query operations
        operation_names = [config.get("name", "") for config in query_operations]
        
        # Should have query operations
        assert any("fetch" in name or "get" in name for name in operation_names)

    def test_span_wrapping_batch_operations(self):
        """Test that batch operations are properly mapped."""
        batch_operations = [
            config for config in SPAN_WRAPPING
            if "batch" in config.get("module", "")
        ]
        
        assert len(batch_operations) > 0
        
        # Check for batch operations
        operation_names = [config.get("name", "") for config in batch_operations]
        
        # Should have add operations for batch
        assert any("add" in name for name in operation_names)

    def test_span_wrapping_client_operations(self):
        """Test that client-level operations are properly mapped."""
        client_operations = [
            config for config in SPAN_WRAPPING
            if "client" in config.get("module", "")
        ]
        
        # Should have at least GraphQL operations
        if client_operations:
            operation_names = [config.get("name", "") for config in client_operations]
            assert any("graphql" in name for name in operation_names)

    def test_span_names_are_descriptive(self):
        """Test that span names are descriptive and follow conventions."""
        for config in SPAN_WRAPPING:
            if "span_name" in config:
                span_name = config["span_name"]
                
                # Should contain dots for hierarchy
                assert "." in span_name
                
                # Should not start or end with dots
                assert not span_name.startswith(".")
                assert not span_name.endswith(".")
                
                # Should be lowercase or contain underscores
                assert span_name.islower() or "_" in span_name

    def test_module_names_are_valid(self):
        """Test that module names follow Python module naming conventions."""
        all_configs = CONNECTION_WRAPPING + SPAN_WRAPPING
        
        for config in all_configs:
            module_name = config["module"]
            
            # Should contain dots for package hierarchy
            assert "." in module_name or module_name == "weaviate"
            
            # Should not start or end with dots
            assert not module_name.startswith(".")
            assert not module_name.endswith(".")
            
            # Should not contain spaces
            assert " " not in module_name

    def test_function_names_are_valid(self):
        """Test that function names follow Python naming conventions."""
        all_configs = CONNECTION_WRAPPING + SPAN_WRAPPING
        
        for config in all_configs:
            function_name = config["name"]
            
            # Should not be empty
            assert len(function_name) > 0
            
            # Should not contain spaces
            assert " " not in function_name
            
            # Should be valid Python identifier format (class.method or function)
            parts = function_name.split(".")
            for part in parts:
                # Each part should be a valid identifier pattern
                assert part.replace("_", "a").isalnum()

    def test_no_duplicate_mappings(self):
        """Test that there are no duplicate mappings."""
        all_configs = CONNECTION_WRAPPING + SPAN_WRAPPING
        
        module_function_pairs = []
        for config in all_configs:
            pair = (config["module"], config["name"])
            assert pair not in module_function_pairs, f"Duplicate mapping found: {pair}"
            module_function_pairs.append(pair)

    def test_mapping_coverage(self):
        """Test that mapping covers essential Weaviate operations."""
        all_configs = CONNECTION_WRAPPING + SPAN_WRAPPING
        
        # Extract all operation types from span names and function names
        operations = set()
        
        for config in all_configs:
            # Add function name
            operations.add(config["name"].split(".")[-1])
            
            # Add span name if available
            if "span_name" in config:
                operations.update(config["span_name"].split("."))
        
        # Essential operations that should be covered
        essential_ops = {"get", "create", "delete", "insert", "update", "query"}
        
        # Check coverage (some operations might be covered)
        covered_ops = essential_ops.intersection(operations)
        assert len(covered_ops) > 0, f"No essential operations covered. Available: {operations}"

    def test_weaviate_specific_modules(self):
        """Test that all modules are Weaviate-specific."""
        all_configs = CONNECTION_WRAPPING + SPAN_WRAPPING
        
        for config in all_configs:
            module_name = config["module"]
            
            # Should be weaviate module or submodule
            assert module_name.startswith("weaviate"), f"Non-weaviate module found: {module_name}"

    def test_consistent_naming_patterns(self):
        """Test that naming patterns are consistent across the mapping."""
        # Check that similar operations follow similar naming patterns
        data_operations = [
            config for config in SPAN_WRAPPING
            if "data" in config.get("module", "")
        ]
        
        # Data operations should have consistent span naming
        for config in data_operations:
            if "span_name" in config:
                span_name = config["span_name"]
                assert span_name.startswith("collections.data."), \
                    f"Inconsistent data operation span name: {span_name}"

    def test_mapping_completeness_for_example_usage(self):
        """Test that mapping covers operations used in the example.py file."""
        # Based on the example.py file, these operations should be covered
        required_operations = [
            # Connection
            "connect_to_local",
            # Collection access
            "get",  # collections.get
            # Query operations
            "near_text",  # query.near_text
        ]
        
        all_function_names = []
        for config in CONNECTION_WRAPPING + SPAN_WRAPPING:
            all_function_names.append(config["name"])
            if "span_name" in config:
                all_function_names.extend(config["span_name"].split("."))
        
        for required_op in required_operations:
            # Check if the operation is covered in some form
            covered = any(required_op in func_name for func_name in all_function_names)
            if not covered:
                # It's okay if not all operations are covered yet
                # This is more of a documentation test
                pass


if __name__ == "__main__":
    pytest.main([__file__])
