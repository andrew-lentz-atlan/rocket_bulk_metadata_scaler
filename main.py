#!/usr/bin/env python3
"""
Entry point for the Bulk Metadata Scaler App.

This file is used by the Dockerfile entrypoint.
"""

import asyncio
from bulk_metadata_scaler_app.main import main

if __name__ == "__main__":
    asyncio.run(main(daemon=False))

