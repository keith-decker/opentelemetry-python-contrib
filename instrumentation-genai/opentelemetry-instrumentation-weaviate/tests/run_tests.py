#!/usr/bin/env python3
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

"""Test runner for Weaviate instrumentation tests."""

import sys
import subprocess
from pathlib import Path


def run_tests():
    """Run all Weaviate instrumentation tests."""
    test_dir = Path(__file__).parent
    
    test_files = [
        "test_instrumentor.py",
        "test_wrappers.py", 
        "test_utils.py",
        "test_integration.py",
        "test_mapping.py"
    ]
    
    print("Running Weaviate instrumentation tests...")
    print("=" * 50)
    
    overall_success = True
    
    for test_file in test_files:
        test_path = test_dir / test_file
        if test_path.exists():
            print(f"\nRunning {test_file}...")
            try:
                result = subprocess.run(
                    [sys.executable, "-m", "pytest", str(test_path), "-v"],
                    capture_output=True,
                    text=True,
                    cwd=test_dir
                )
                
                if result.returncode == 0:
                    print(f"✅ {test_file} passed")
                else:
                    print(f"❌ {test_file} failed")
                    print("STDOUT:", result.stdout)
                    print("STDERR:", result.stderr)
                    overall_success = False
                    
            except Exception as e:
                print(f"❌ Error running {test_file}: {e}")
                overall_success = False
        else:
            print(f"⚠️  {test_file} not found")
    
    print("\n" + "=" * 50)
    if overall_success:
        print("🎉 All tests passed!")
        return 0
    else:
        print("💥 Some tests failed!")
        return 1


if __name__ == "__main__":
    exit_code = run_tests()
    sys.exit(exit_code)
