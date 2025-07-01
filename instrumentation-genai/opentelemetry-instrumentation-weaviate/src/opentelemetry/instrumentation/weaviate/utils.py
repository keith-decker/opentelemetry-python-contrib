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

import logging
import traceback
from typing import Any, Callable, Optional, Tuple
from urllib.parse import urlparse
# TODO: get semconv for vector databases
# from opentelemetry.semconv._incubating.attributes import gen_ai_attributes as GenAI

logger = logging.getLogger(__name__)

def dont_throw(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator that catches and logs exceptions, rather than re-raising them,
    to avoid interfering with user code if instrumentation fails.
    """
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.debug(
                "OpenTelemetry instrumentation for Weaviate encountered an error in %s: %s",
                func.__name__,
                traceback.format_exc(),
            )
            from opentelemetry.instrumentation.weaviate.config import Config
            if Config.exception_logger:
                Config.exception_logger(e)
            return None
    return wrapper


def parse_url_to_host_port(url: str) -> Tuple[Optional[str], Optional[int]]:
    parsed = urlparse(url)
    host: Optional[str] = parsed.hostname
    port: Optional[int] = parsed.port
    return host, port