# Simple test runner to verify our span generation tests
# This is a standalone test that doesn't require the full test infrastructure

import sys
import os

# Add the source directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from unittest import mock
from opentelemetry.instrumentation.weaviate.mapping import SPAN_NAME_PREFIX

def test_mapping_constants():
    """Test that our mapping constants are correct."""
    print(f"SPAN_NAME_PREFIX: {SPAN_NAME_PREFIX}")
    assert SPAN_NAME_PREFIX == "db.weaviate"
    print("✓ SPAN_NAME_PREFIX is correct")

def test_import_instrumentation():
    """Test that we can import the instrumentation."""
    from opentelemetry.instrumentation.weaviate import WeaviateInstrumentor
    print("✓ Successfully imported WeaviateInstrumentor")
    
    # Test that we can create an instance
    instrumentor = WeaviateInstrumentor()
    print("✓ Successfully created WeaviateInstrumentor instance")
    
    # Test dependencies
    deps = instrumentor.instrumentation_dependencies()
    print(f"✓ Instrumentation dependencies: {deps}")
    assert isinstance(instrumentor, WeaviateInstrumentor)

def test_helpers():
    """Test that our test helpers work."""
    # Import relative to current directory
    from .helpers_v3 import create_mock_weaviate_v3_client
    from .helpers_v4 import create_mock_weaviate_v4_client
    
    # Test v3 mock
    v3_client = create_mock_weaviate_v3_client()
    print("✓ Successfully created mock v3 client")
    
    # Test v4 mock  
    v4_client = create_mock_weaviate_v4_client()
    print("✓ Successfully created mock v4 client")
    assert v3_client is not None
    assert v4_client is not None

def main():
    """Run all tests."""
    print("Running basic tests for Weaviate instrumentation...")
    
    tests = [
        test_mapping_constants,
        test_import_instrumentation, 
        test_helpers
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            result = test()
            if result is not False:
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"✗ {test.__name__} failed with exception: {e}")
            failed += 1
    
    print(f"\nResults: {passed} passed, {failed} failed")
    return failed == 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
