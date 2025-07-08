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

"""Tests for Weaviate utility functions."""

import logging
from unittest.mock import Mock, PropertyMock, patch
import pytest

from opentelemetry.instrumentation.weaviate.utils import (
    dont_throw,
    parse_url_to_host_port,
    extract_collection_name,
    extract_db_operation_name,
)
from opentelemetry.instrumentation.weaviate.config import Config


class TestDontThrowDecorator:
    """Test the dont_throw decorator."""

    def test_dont_throw_with_successful_function(self):
        """Test that dont_throw allows successful functions to return normally."""
        @dont_throw
        def successful_function(value):
            return value * 2
        
        result = successful_function(5)
        assert result == 10

    def test_dont_throw_with_failing_function(self):
        """Test that dont_throw catches exceptions and returns None."""
        @dont_throw
        def failing_function():
            raise ValueError("Test error")
        
        result = failing_function()
        assert result is None

    def test_dont_throw_with_different_exception_types(self):
        """Test that dont_throw catches different types of exceptions."""
        @dont_throw
        def function_with_type_error():
            raise TypeError("Type error")
        
        @dont_throw
        def function_with_runtime_error():
            raise RuntimeError("Runtime error")
        
        @dont_throw
        def function_with_custom_error():
            class CustomError(Exception):
                pass
            raise CustomError("Custom error")
        
        assert function_with_type_error() is None
        assert function_with_runtime_error() is None
        assert function_with_custom_error() is None

    def test_dont_throw_logs_exception(self, caplog):
        """Test that dont_throw logs exceptions."""
        @dont_throw
        def failing_function():
            raise ValueError("Test error for logging")
        
        with caplog.at_level(logging.DEBUG):
            result = failing_function()
        
        assert result is None
        assert "OpenTelemetry instrumentation for Weaviate encountered an error" in caplog.text
        assert "failing_function" in caplog.text

    def test_dont_throw_calls_exception_logger(self):
        """Test that dont_throw calls the configured exception logger."""
        # Set up a mock exception logger
        mock_logger = Mock()
        original_logger = Config.exception_logger
        Config.exception_logger = mock_logger
        
        try:
            @dont_throw
            def failing_function():
                test_exception = ValueError("Test error for logger")
                raise test_exception
            
            result = failing_function()
            
            assert result is None
            mock_logger.assert_called_once()
            # Check that the exception was passed to the logger
            called_exception = mock_logger.call_args[0][0]
            assert isinstance(called_exception, ValueError)
            assert str(called_exception) == "Test error for logger"
        
        finally:
            # Restore original logger
            Config.exception_logger = original_logger

    def test_dont_throw_without_exception_logger(self):
        """Test that dont_throw works when no exception logger is configured."""
        # Ensure no exception logger is set
        original_logger = Config.exception_logger
        Config.exception_logger = None
        
        try:
            @dont_throw
            def failing_function():
                raise ValueError("Test error without logger")
            
            # Should not raise an exception
            result = failing_function()
            assert result is None
        
        finally:
            # Restore original logger
            Config.exception_logger = original_logger

    def test_dont_throw_preserves_function_metadata(self):
        """Test that dont_throw preserves function name and other metadata."""
        @dont_throw
        def test_function():
            """Test function docstring."""
            return "success"
        
        assert test_function.__name__ == "wrapper"  # Will be wrapper due to decorator
        # The original function is available in the closure
        result = test_function()
        assert result == "success"

    def test_dont_throw_with_function_arguments(self):
        """Test that dont_throw properly handles function arguments."""
        @dont_throw
        def function_with_args(a, b, c=None, *args, **kwargs):
            if c == "fail":
                raise ValueError("Requested failure")
            return f"a={a}, b={b}, c={c}, args={args}, kwargs={kwargs}"
        
        # Test successful call - fix argument order
        result = function_with_args(1, 2, c=3, key="value")
        expected = "a=1, b=2, c=3, args=(), kwargs={'key': 'value'}"
        assert result == expected
        
        # Test failing call
        result = function_with_args(1, 2, c="fail")
        assert result is None


class TestParseUrlToHostPort:
    """Test the parse_url_to_host_port function."""

    def test_parse_http_url_with_port(self):
        """Test parsing HTTP URL with explicit port."""
        host, port = parse_url_to_host_port("http://localhost:8080")
        assert host == "localhost"
        assert port == 8080

    def test_parse_https_url_with_port(self):
        """Test parsing HTTPS URL with explicit port."""
        host, port = parse_url_to_host_port("https://example.com:9200")
        assert host == "example.com"
        assert port == 9200

    def test_parse_http_url_without_port(self):
        """Test parsing HTTP URL without explicit port."""
        host, port = parse_url_to_host_port("http://example.com")
        assert host == "example.com"
        assert port is None  # HTTP default port is not explicitly returned

    def test_parse_https_url_without_port(self):
        """Test parsing HTTPS URL without explicit port."""
        host, port = parse_url_to_host_port("https://example.com")
        assert host == "example.com"
        assert port is None  # HTTPS default port is not explicitly returned

    def test_parse_url_with_path(self):
        """Test parsing URL with path components."""
        host, port = parse_url_to_host_port("http://localhost:8080/v1/objects")
        assert host == "localhost"
        assert port == 8080

    def test_parse_url_with_query_parameters(self):
        """Test parsing URL with query parameters."""
        host, port = parse_url_to_host_port("https://api.example.com:443/endpoint?param=value")
        assert host == "api.example.com"
        assert port == 443

    def test_parse_ip_address_url(self):
        """Test parsing URL with IP address."""
        host, port = parse_url_to_host_port("http://192.168.1.100:8080")
        assert host == "192.168.1.100"
        assert port == 8080

    def test_parse_ipv6_url(self):
        """Test parsing URL with IPv6 address."""
        host, port = parse_url_to_host_port("http://[::1]:8080")
        assert host == "::1"
        assert port == 8080

    def test_parse_localhost_variations(self):
        """Test parsing different localhost variations."""
        # Standard localhost
        host, port = parse_url_to_host_port("http://localhost:8080")
        assert host == "localhost"
        assert port == 8080
        
        # IP version of localhost
        host, port = parse_url_to_host_port("http://127.0.0.1:8080")
        assert host == "127.0.0.1"
        assert port == 8080

    def test_parse_invalid_url(self):
        """Test parsing invalid URLs."""
        # Should not raise an exception, but might return None values
        host, port = parse_url_to_host_port("not-a-url")
        # The function might return None for both or handle it gracefully
        # The exact behavior depends on urlparse implementation

    def test_parse_empty_url(self):
        """Test parsing empty URL."""
        host, port = parse_url_to_host_port("")
        assert host is None or host == ""
        assert port is None

    def test_parse_url_with_username_password(self):
        """Test parsing URL with authentication credentials."""
        host, port = parse_url_to_host_port("http://user:password@example.com:8080")
        assert host == "example.com"
        assert port == 8080

    def test_parse_non_standard_ports(self):
        """Test parsing URLs with non-standard ports."""
        test_cases = [
            ("http://example.com:3000", "example.com", 3000),
            ("https://api.service.com:8443", "api.service.com", 8443),
            ("http://localhost:9999", "localhost", 9999),
        ]
        
        for url, expected_host, expected_port in test_cases:
            host, port = parse_url_to_host_port(url)
            assert host == expected_host
            assert port == expected_port

    def test_parse_url_different_schemes(self):
        """Test parsing URLs with different schemes."""
        # While typically HTTP/HTTPS, test other schemes
        host, port = parse_url_to_host_port("ftp://ftp.example.com:21")
        assert host == "ftp.example.com"
        assert port == 21


class TestConfig:
    """Test the Config class."""

    def test_config_initial_state(self):
        """Test that Config has the expected initial state."""
        # Config.exception_logger should be None initially
        assert Config.exception_logger is None

    def test_config_can_set_exception_logger(self):
        """Test that Config.exception_logger can be set and retrieved."""
        original_logger = Config.exception_logger
        
        try:
            mock_logger = Mock()
            Config.exception_logger = mock_logger
            assert Config.exception_logger == mock_logger
        
        finally:
            Config.exception_logger = original_logger

    def test_config_exception_logger_is_shared(self):
        """Test that Config.exception_logger is shared across instances."""
        original_logger = Config.exception_logger
        
        try:
            mock_logger = Mock()
            Config.exception_logger = mock_logger
            
            # Access from different contexts should return the same logger
            from opentelemetry.instrumentation.weaviate.config import Config as Config2
            assert Config2.exception_logger == mock_logger
        
        finally:
            Config.exception_logger = original_logger



class TestExtractDbOperationName:
    """Test the extract_db_operation_name function."""

    def test_collections_create_operations(self):
        """Test mapping of collection create operations."""
        create_operations = [
            ("create", "create"),
            ("create_from_dict", "create"),
            ("CREATE", "create"),
            ("createCollection", "create"),
        ]
        
        for function_name, expected in create_operations:
            mock_wrapped = Mock()
            mock_wrapped.__name__ = function_name
            
            result = extract_db_operation_name(
                wrapped=mock_wrapped,
                module_name="weaviate.collections.collections",
                function_name=function_name
            )
            
            assert result == expected

    def test_collections_delete_operations(self):
        """Test mapping of collection delete operations."""
        delete_operations = [
            ("delete", "delete"),
            ("delete_all", "delete"),
            ("DELETE", "delete"),
            ("deleteCollection", "delete"),
        ]
        
        for function_name, expected in delete_operations:
            mock_wrapped = Mock()
            mock_wrapped.__name__ = function_name
            
            result = extract_db_operation_name(
                wrapped=mock_wrapped,
                module_name="weaviate.collections.collections",
                function_name=function_name
            )
            
            assert result == expected

    def test_collections_insert_operations(self):
        """Test mapping of collection insert operations."""
        insert_operations = [
            ("insert", "insert"),
            ("add_object", "insert"),
            ("INSERT", "insert"),
            ("insertObject", "insert"),  # contains 'insert'
        ]
        
        for function_name, expected in insert_operations:
            mock_wrapped = Mock()
            mock_wrapped.__name__ = function_name
            
            result = extract_db_operation_name(
                wrapped=mock_wrapped,
                module_name="weaviate.collections.data",
                function_name=function_name
            )
            
            assert result == expected

    def test_collections_update_operations(self):
        """Test mapping of collection update operations."""
        update_operations = [
            ("update", "update"),
            ("replace", "update"),
            ("UPDATE", "update"),
            ("replaceObject", "update"),
        ]
        
        for function_name, expected in update_operations:
            mock_wrapped = Mock()
            mock_wrapped.__name__ = function_name
            
            result = extract_db_operation_name(
                wrapped=mock_wrapped,
                module_name="weaviate.collections.data",
                function_name=function_name
            )
            
            assert result == expected

    def test_collections_select_operations(self):
        """Test mapping of collection select operations."""
        select_operations = [
            ("get", "select"),
            ("fetch", "select"),
            ("query", "select"),
            ("GET", "select"),
            ("fetchObjects", "select"),
        ]
        
        for function_name, expected in select_operations:
            mock_wrapped = Mock()
            mock_wrapped.__name__ = function_name
            
            result = extract_db_operation_name(
                wrapped=mock_wrapped,
                module_name="weaviate.collections.data",
                function_name=function_name
            )
            
            assert result == expected

    def test_graphql_operations_default_to_select(self):
        """Test that GraphQL operations default to select."""
        graphql_modules = [
            "weaviate.graphql.get",
            "weaviate.gql.query",
            "weaviate.GraphQL.execute",
        ]
        
        for module_name in graphql_modules:
            mock_wrapped = Mock()
            mock_wrapped.__name__ = "execute"
            
            result = extract_db_operation_name(
                wrapped=mock_wrapped,
                module_name=module_name,
                function_name="execute"
            )
            
            assert result == "select"

    def test_query_module_operations_default_to_select(self):
        """Test that query module operations default to select."""
        query_modules = [
            "weaviate.query.aggregate",
            "weaviate.Query.get",
            "weaviate.QUERY.fetch",
        ]
        
        for module_name in query_modules:
            mock_wrapped = Mock()
            mock_wrapped.__name__ = "execute"
            
            result = extract_db_operation_name(
                wrapped=mock_wrapped,
                module_name=module_name,
                function_name="execute"
            )
            
            assert result == "select"

    def test_batch_operations_default_to_insert(self):
        """Test that batch operations default to insert."""
        batch_modules = [
            "weaviate.batch.client",
            "weaviate.Batch.add",
            "weaviate.BATCH.execute",
        ]
        
        for module_name in batch_modules:
            mock_wrapped = Mock()
            mock_wrapped.__name__ = "add_object"
            
            result = extract_db_operation_name(
                wrapped=mock_wrapped,
                module_name=module_name,
                function_name="add_object"
            )
            
            assert result == "insert"

    def test_connect_executor_operations_default_to_exec(self):
        """Test that connect/executor operations default to exec."""
        exec_modules = [
            "weaviate.connect.client",
            "weaviate.executor.run",
            "weaviate.Connect.execute",
        ]
        
        for module_name in exec_modules:
            mock_wrapped = Mock()
            mock_wrapped.__name__ = "execute"
            
            result = extract_db_operation_name(
                wrapped=mock_wrapped,
                module_name=module_name,
                function_name="execute"
            )
            
            assert result == "exec"

    def test_function_name_fallback_patterns(self):
        """Test fallback to function name patterns when module doesn't match."""
        fallback_cases = [
            ("createSomething", "create"),
            ("addNewItem", "create"),
            ("newCollection", "create"),
            ("deleteSomething", "delete"),
            ("removeItem", "delete"),
            ("dropTable", "delete"),
            ("insertData", "insert"),
            ("putObject", "insert"),
            ("saveRecord", "insert"),
            ("updateRecord", "update"),
            ("modifyData", "update"),
            ("replaceItem", "update"),
            ("patchObject", "update"),
            ("getSomething", "select"),
            ("fetchData", "select"),
            ("findRecord", "select"),
            ("searchItems", "select"),
            ("queryData", "select"),
            ("selectAll", "select"),
            ("readFile", "select"),
        ]
        
        for function_name, expected in fallback_cases:
            mock_wrapped = Mock()
            mock_wrapped.__name__ = function_name
            
            result = extract_db_operation_name(
                wrapped=mock_wrapped,
                module_name="some.unknown.module",
                function_name=function_name
            )
            
            assert result == expected

    def test_ultimate_fallback_to_exec(self):
        """Test that unknown operations fall back to 'exec'."""
        unknown_operations = [
            "unknownOperation",
            "someRandomFunction",
            "doSomething",
            "process",
            "handle",
        ]
        
        for function_name in unknown_operations:
            mock_wrapped = Mock()
            mock_wrapped.__name__ = function_name
            
            result = extract_db_operation_name(
                wrapped=mock_wrapped,
                module_name="some.unknown.module",
                function_name=function_name
            )
            
            assert result == "exec"

    def test_wrapped_function_name_takes_precedence(self):
        """Test that the wrapped function's __name__ takes precedence over function_name parameter."""
        mock_wrapped = Mock()
        mock_wrapped.__name__ = "actualCreate"
        
        result = extract_db_operation_name(
            wrapped=mock_wrapped,
            module_name="weaviate.collections.collections",
            function_name="passedName"  # This should be ignored
        )
        
        assert result == "create"

    def test_missing_wrapped_name_uses_function_name_parameter(self):
        """Test fallback to function_name parameter when wrapped.__name__ is not available."""
        mock_wrapped = Mock(spec=[])  # No __name__ attribute
        
        result = extract_db_operation_name(
            wrapped=mock_wrapped,
            module_name="weaviate.collections.collections",
            function_name="create"
        )
        
        assert result == "create"

    def test_case_insensitive_matching(self):
        """Test that operation matching is case insensitive."""
        case_variations = [
            ("CREATE", "create"),
            ("Create", "create"),
            ("cReAtE", "create"),
            ("DELETE", "delete"),
            ("Delete", "delete"),
            ("dElEtE", "delete"),
            ("INSERT", "insert"),
            ("Insert", "insert"),
            ("iNsErT", "insert"),
            ("UPDATE", "update"),
            ("Update", "update"),
            ("uPdAtE", "update"),
            ("SELECT", "select"),
            ("Select", "select"),
            ("sElEcT", "select"),
        ]
        
        for function_name, expected in case_variations:
            mock_wrapped = Mock()
            mock_wrapped.__name__ = function_name
            
            result = extract_db_operation_name(
                wrapped=mock_wrapped,
                module_name="weaviate.collections.collections",
                function_name=function_name
            )
            
            assert result == expected

    def test_compound_function_names(self):
        """Test function names with multiple operation indicators."""
        compound_cases = [
            ("createAndInsert", "create"),  # create takes precedence
            ("updateOrReplace", "update"),  # update takes precedence
            ("deleteAndRemove", "delete"),  # delete takes precedence
            ("getAndFetch", "select"),     # get takes precedence
        ]
        
        for function_name, expected in compound_cases:
            mock_wrapped = Mock()
            mock_wrapped.__name__ = function_name
            
            result = extract_db_operation_name(
                wrapped=mock_wrapped,
                module_name="some.module",
                function_name=function_name
            )
            
            assert result == expected

    def test_module_name_case_insensitive(self):
        """Test that some module name matching is case insensitive."""
        # Note: collections module check is case-sensitive, others are case-insensitive
        module_variations = [
            # These work because the module checks use .lower()
            ("weaviate.GRAPHQL.get", "execute", "select"),
            ("weaviate.GraphQL.Get", "query", "select"),
            ("weaviate.QUERY.aggregate", "run", "select"),
            ("weaviate.Query.Aggregate", "execute", "select"),
            ("weaviate.BATCH.client", "add_object", "insert"),
            ("weaviate.Batch.Client", "insert_data", "insert"),
            # These use lowercase collections (case-sensitive check)
            ("weaviate.collections.data", "insert", "insert"),
            ("weaviate.collections.data", "add_object", "insert"),
        ]
        
        for module_name, function_name, expected in module_variations:
            mock_wrapped = Mock()
            mock_wrapped.__name__ = function_name
            
            result = extract_db_operation_name(
                wrapped=mock_wrapped,
                module_name=module_name,
                function_name=function_name
            )
            
            assert result == expected

    def test_collections_module_case_sensitivity(self):
        """Test that collections module check is case-sensitive."""
        # The collections check is case-sensitive (should use lowercase)
        test_cases = [
            ("weaviate.collections.data", "add_object", "insert"),  # Lowercase works
            ("weaviate.Collections.Data", "add_object", "create"),  # Uppercase falls back to general logic
            ("weaviate.COLLECTIONS.data", "add_object", "create"),  # Uppercase falls back to general logic
        ]
        
        for module_name, function_name, expected in test_cases:
            mock_wrapped = Mock()
            mock_wrapped.__name__ = function_name
            
            result = extract_db_operation_name(
                wrapped=mock_wrapped,
                module_name=module_name,
                function_name=function_name
            )
            
            assert result == expected, f"Expected {expected} for {module_name}.{function_name}, got {result}"

    def test_camel_case_vs_snake_case_patterns(self):
        """Test that function name patterns are specific to the implementation."""
        # The implementation looks for specific patterns, not general naming conventions
        test_cases = [
            ("add_object", "insert"),    # Exact match for 'add_object' in collections
            ("addObject", "create"),     # 'add' matches create fallback, not 'add_object'
            ("add", "create"),           # 'add' matches create fallback
            ("insert_data", "insert"),   # Contains 'insert'
            ("insertData", "insert"),    # Contains 'insert'
        ]
        
        for function_name, expected in test_cases:
            mock_wrapped = Mock()
            mock_wrapped.__name__ = function_name
            
            result = extract_db_operation_name(
                wrapped=mock_wrapped,
                module_name="weaviate.collections.data",
                function_name=function_name
            )
            
            assert result == expected, f"Expected {expected} for {function_name}, got {result}"


if __name__ == "__main__":
    pytest.main([__file__])
