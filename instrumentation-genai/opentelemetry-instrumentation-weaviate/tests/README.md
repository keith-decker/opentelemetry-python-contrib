# Weaviate Instrumentation Tests

This directory contains comprehensive tests for the OpenTelemetry Weaviate instrumentation package.

## Test Structure

### Test Files

1. **`conftest.py`** - Test configuration and fixtures
   - Provides mock Weaviate client and modules
   - Sets up OpenTelemetry test infrastructure (tracers, span exporters)
   - Contains shared fixtures used across all test files

2. **`test_instrumentor.py`** - Main instrumentor tests
   - Tests `WeaviateInstrumentor` class functionality
   - Tests instrumentation and uninstrumentation
   - Tests connection operations and span creation
   - Tests various Weaviate operations (collections, data, queries)
   - Tests complete workflows and error handling

3. **`test_wrappers.py`** - Wrapper class tests
   - Tests `_WeaviateConnectionInjectionWrapper`
   - Tests `_WeaviateTraceInjectionWrapper`
   - Tests span creation and attribute setting
   - Tests error handling in wrappers

4. **`test_utils.py`** - Utility function tests
   - Tests `dont_throw` decorator
   - Tests `parse_url_to_host_port` function
   - Tests `Config` class
   - Tests error handling and edge cases

5. **`test_integration.py`** - Integration tests
   - Tests complete Weaviate workflows
   - Tests various operation combinations
   - Tests concurrent operations
   - Tests error scenarios
   - Tests performance considerations

6. **`test_mapping.py`** - Configuration mapping tests
   - Tests `SPAN_WRAPPING` and `CONNECTION_WRAPPING` configurations
   - Tests mapping structure and content validation
   - Tests naming conventions and consistency
   - Tests coverage of essential operations

7. **`run_tests.py`** - Test runner script
   - Convenient script to run all tests
   - Provides summary of test results

## Test Features

### Mocking Strategy
- Uses extensive mocking to avoid requiring a real Weaviate instance
- Mocks Weaviate client, collections, and all operations
- Provides realistic mock responses for testing

### Coverage Areas
- **Instrumentation lifecycle**: instrument/uninstrument
- **Connection operations**: various connection methods
- **Collection operations**: CRUD operations on collections
- **Data operations**: insert, update, replace, delete
- **Query operations**: near_text, fetch_objects, GraphQL
- **Batch operations**: bulk data operations
- **Error handling**: exception scenarios and edge cases
- **Performance**: basic performance considerations
- **Configuration**: mapping validation and consistency

### Test Types
- **Unit tests**: Individual component testing
- **Integration tests**: End-to-end workflow testing
- **Configuration tests**: Validation of mapping configurations
- **Error tests**: Exception handling and edge cases

## Running Tests

### Run All Tests
```bash
python run_tests.py
```

### Run Individual Test Files
```bash
# Run instrumentor tests
python -m pytest test_instrumentor.py -v

# Run wrapper tests
python -m pytest test_wrappers.py -v

# Run utility tests
python -m pytest test_utils.py -v

# Run integration tests
python -m pytest test_integration.py -v

# Run mapping tests
python -m pytest test_mapping.py -v
```

### Run Specific Test Classes or Methods
```bash
# Run specific test class
python -m pytest test_instrumentor.py::TestWeaviateInstrumentor -v

# Run specific test method
python -m pytest test_instrumentor.py::TestWeaviateInstrumentor::test_instrumentation_dependencies -v
```

## Test Requirements

### Dependencies
The tests require the following packages:
- `pytest` - Test framework
- `unittest.mock` - Mocking framework (built-in)
- `opentelemetry-api` - OpenTelemetry API
- `opentelemetry-sdk` - OpenTelemetry SDK
- `opentelemetry-instrumentation` - Base instrumentation

### Optional Dependencies
- `weaviate-client` - For more realistic testing (not required due to mocking)

## Test Configuration

### Fixtures
- `span_exporter` - In-memory span exporter for capturing spans
- `tracer_provider` - Configured tracer provider for testing
- `instrumentor` - Configured and instrumented WeaviateInstrumentor
- `mock_weaviate_client` - Mock Weaviate client with realistic behavior
- `mock_weaviate_module` - Mock Weaviate module for import testing

### Environment
Tests are designed to run in isolation without external dependencies:
- No real Weaviate server required
- No network connections made
- All operations are mocked

## Expected Test Behavior

### Successful Tests
When tests pass, you should see:
- Spans created for instrumented operations
- Proper span attributes (DB system, operation names, server info)
- Correct span naming following `db.weaviate.*` pattern
- Proper error handling without crashes

### Common Issues
- **Import errors**: Ensure OpenTelemetry packages are installed
- **Mock failures**: Check that mock setup matches actual Weaviate API
- **Attribute assertions**: Verify span attributes match expected values

## Extending Tests

### Adding New Operation Tests
1. Add the operation to the relevant mock objects in `conftest.py`
2. Create test methods in appropriate test files
3. Verify span creation and attributes
4. Test error scenarios

### Adding New Integration Tests
1. Add test methods to `test_integration.py`
2. Create realistic workflows using mock operations
3. Verify end-to-end span creation

### Updating Mapping Tests
1. Update `test_mapping.py` when adding new operations to mapping
2. Verify configuration structure and naming conventions
3. Test coverage of new operations

## Notes

- Tests use mocking extensively to avoid external dependencies
- Real Weaviate integration tests would require a separate test suite
- Performance tests are basic due to mocking overhead
- Tests follow OpenTelemetry instrumentation testing patterns used in other packages
