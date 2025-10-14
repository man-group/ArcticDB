"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import inspect
import logging
import os
import re
from typing import Any, Dict, Union


class GitHubSanitizingHandler(logging.StreamHandler):
    """
    The handler sanitizes messages only when execution is in GitHub
    """

    def emit(self, record: logging.LogRecord):
        # Sanitize the message here
        record.msg = self.sanitize_message(record.msg)
        super().emit(record)

    @staticmethod
    def sanitize_message(message: str) -> str:
        if (os.getenv("GITHUB_ACTIONS") == "true") and isinstance(message, str):
            # Use regex to find and replace sensitive access keys
            sanitized_message = message
            for regexp in [
                r"(secret=)[^\s&]+",
                r"(access=)[^\s&]+",
                r"(.*SECRET_KEY=).*$",
                r"(.*ACCESS_KEY=).*$",
                r"(.*AZURE_CONNECTION_STRING=).*$",
                r"(AccountKey=)([^;]+)",
            ]:
                sanitized_message = re.sub(regexp, r"\1***", sanitized_message, flags=re.IGNORECASE)
            return sanitized_message
        return message


loggers: Dict[str, logging.Logger] = {}


def get_logger(bencmhark_cls: Union[str, Any] = None):
    """
    Creates logger instance with associated console handler.
    The logger name can be either passed as string or class,
    or if not automatically will assume the caller module name
    """
    logLevel = logging.INFO
    if bencmhark_cls:
        if isinstance(bencmhark_cls, str):
            value = bencmhark_cls
        else:
            value = type(bencmhark_cls).__name__
        name = value
    else:
        frame = inspect.stack()[1]
        module = inspect.getmodule(frame[0])
        name = module.__name__

    logger = loggers.get(name, None)
    if logger:
        return logger
    logger = logging.getLogger(name)
    logger.setLevel(logLevel)
    console_handler = GitHubSanitizingHandler()
    console_handler.setLevel(logLevel)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    loggers[name] = logger
    return logger


class GitHubSanitizingException(Exception):
    def __init__(self, message: str):
        # Sanitize the message
        sanitized_message = GitHubSanitizingHandler.sanitize_message(message)
        super().__init__(sanitized_message)
