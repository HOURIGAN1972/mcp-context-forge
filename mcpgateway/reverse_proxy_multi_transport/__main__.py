# -*- coding: utf-8 -*-
"""Location: ./mcpgateway/reverse_proxy_multi_transport/__main__.py
Copyright 2025
SPDX-License-Identifier: Apache-2.0
Authors: Mihai Criveti

Entry point for running reverse proxy as a module.
Allows: python -m mcpgateway.reverse_proxy
"""

# First-Party
from mcpgateway.reverse_proxy_multi_transport.cli import run

if __name__ == "__main__":
    run()

# Made with Bob
