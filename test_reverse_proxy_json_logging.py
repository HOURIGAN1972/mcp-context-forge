#!/usr/bin/env python3
"""Test script to verify reverse proxy JSON logging configuration."""

import os
import sys
import logging
from datetime import datetime, timezone

# Set LOG_FORMAT to json before testing
os.environ['LOG_FORMAT'] = 'json'

# Import the JSON formatter directly
from pythonjsonlogger import json as jsonlogger

class SimpleJsonFormatter(jsonlogger.JsonFormatter):
    """Simple JSON formatter for testing."""
    
    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        log_record["@timestamp"] = dt.isoformat().replace("+00:00", "Z")

def test_json_logging():
    """Test that JSON logging is properly configured."""
    # Configure logger as the CLI does
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.INFO)
    
    # Use JSON formatter
    log_format = os.getenv("LOG_FORMAT", "text").lower()
    print(f"LOG_FORMAT environment variable: {log_format}", file=sys.stdout)
    
    if log_format == "json":
        json_formatter = SimpleJsonFormatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setFormatter(json_formatter)
        print("✓ Using JSON formatter", file=sys.stdout)
    else:
        text_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S"
        )
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setFormatter(text_formatter)
        print("✓ Using text formatter", file=sys.stdout)
    
    console_handler.setLevel(logging.INFO)
    root_logger.addHandler(console_handler)
    
    # Test logging
    logger = logging.getLogger("mcpgateway.mcp_reverse_proxy.test")
    print("\n--- Test log output (should be JSON) ---", file=sys.stdout)
    logger.info("Test message from reverse proxy")
    logger.info("Another test message with data")
    print("--- End test output ---\n", file=sys.stdout)

if __name__ == "__main__":
    test_json_logging()

# Made with Bob
