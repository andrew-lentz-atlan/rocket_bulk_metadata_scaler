#!/usr/bin/env python3
"""
Local test script for the bulk metadata scaler app.

This script tests the core functionality without the full SDK infrastructure.

Usage:
    # Set environment variables
    export ATLAN_API_KEY="your-api-key"
    export ATLAN_BASE_URL="https://your-tenant.atlan.com"

    # Run the test
    python run_local.py
"""

import asyncio
import base64
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bulk_metadata_scaler_app.activities import BulkMetadataActivities
from bulk_metadata_scaler_app.models import WorkflowResult, ProcessingStatus


async def test_parse_file():
    """Test file parsing activity."""
    print("=" * 60)
    print("TEST: File Parsing")
    print("=" * 60)

    activities = BulkMetadataActivities()

    # Read the sample file
    sample_file = os.path.join(
        os.path.dirname(__file__), "sample_reference.csv"
    )

    with open(sample_file, "rb") as f:
        content = f.read()

    config = {
        "file_content": base64.b64encode(content).decode("utf-8"),
        "file_name": "sample_reference.csv",
        "search_column": "name",
        "custom_metadata_delimiter": "::",
    }

    # Call the activity directly (without Temporal)
    result = await activities.parse_file(config)

    print(f"Total rows: {result['total_rows']}")
    print(f"Column mapping:")
    print(f"  Search column: {result['column_mapping']['search_column']}")
    print(f"  Standard fields: {list(result['column_mapping']['standard_fields'].keys())}")
    print(f"  Custom metadata: {list(result['column_mapping']['custom_metadata'].keys())}")
    print(f"\nRecords found: {len(result['records'])}")

    for record in result["records"]:
        print(f"\n  Row {record['row_index']}: {record['asset_name']}")
        if record["standard_values"]:
            print(f"    Standard: {list(record['standard_values'].keys())}")
        if record["custom_metadata_values"]:
            for cm_set in record["custom_metadata_values"]:
                print(f"    CM [{cm_set}]: {list(record['custom_metadata_values'][cm_set].keys())}")

    return result


async def test_find_assets(asset_name: str = "test"):
    """Test asset search activity."""
    print("\n" + "=" * 60)
    print(f"TEST: Find Assets by Name '{asset_name}'")
    print("=" * 60)

    # Check for credentials
    if not os.environ.get("ATLAN_API_KEY"):
        print("⚠️  ATLAN_API_KEY not set, skipping asset search test")
        return []

    if not os.environ.get("ATLAN_BASE_URL"):
        print("⚠️  ATLAN_BASE_URL not set, skipping asset search test")
        return []

    activities = BulkMetadataActivities()

    try:
        assets = await activities.find_assets_by_name(asset_name, ["Column"])
        print(f"Found {len(assets)} assets matching '{asset_name}'")

        for asset in assets[:5]:  # Show first 5
            print(f"  - {asset['name']} ({asset['type_name']})")
            print(f"    GUID: {asset['guid']}")

        return assets

    except Exception as e:
        print(f"❌ Error: {e}")
        return []


async def test_full_workflow(dry_run: bool = True):
    """Test the full workflow (without Temporal)."""
    print("\n" + "=" * 60)
    print(f"TEST: Full Workflow (dry_run={dry_run})")
    print("=" * 60)

    # Check for credentials
    if not os.environ.get("ATLAN_API_KEY"):
        print("⚠️  ATLAN_API_KEY not set, skipping full workflow test")
        return

    activities = BulkMetadataActivities()

    # Parse file
    sample_file = os.path.join(
        os.path.dirname(__file__), "sample_reference.csv"
    )

    with open(sample_file, "rb") as f:
        content = f.read()

    config = {
        "file_content": base64.b64encode(content).decode("utf-8"),
        "file_name": "sample_reference.csv",
        "search_column": "name",
        "custom_metadata_delimiter": "::",
    }

    parse_result = await activities.parse_file(config)
    records = parse_result["records"]

    print(f"Processing {len(records)} records...")

    # Process each record
    result = WorkflowResult()
    result.total_rows = parse_result["total_rows"]

    for record in records:
        try:
            row_result = await activities.process_single_row(
                record, ["Column"], dry_run
            )

            status = row_result.get("status")
            result.total_assets_found += row_result.get("assets_found", 0)
            result.total_assets_updated += row_result.get("assets_updated", 0)

            if status == ProcessingStatus.SUCCESS.value:
                result.successful_rows += 1
            elif status == ProcessingStatus.NOT_FOUND.value:
                result.not_found_rows += 1
            elif status == ProcessingStatus.SKIPPED.value:
                result.skipped_rows += 1
            elif status == ProcessingStatus.FAILED.value:
                result.failed_rows += 1

            print(f"  Row {record['row_index']} ({record['asset_name']}): {status}")

        except Exception as e:
            result.failed_rows += 1
            print(f"  Row {record['row_index']}: ERROR - {e}")

    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total rows: {result.total_rows}")
    print(f"  ✅ Successful: {result.successful_rows}")
    print(f"  ⚠️  Partial: {result.partial_rows}")
    print(f"  ❌ Failed: {result.failed_rows}")
    print(f"  🔍 Not found: {result.not_found_rows}")
    print(f"  ⏭️  Skipped: {result.skipped_rows}")
    print(f"\nAssets found: {result.total_assets_found}")
    print(f"Assets updated: {result.total_assets_updated}")


async def main():
    """Run all tests."""
    print("\n🚀 Bulk Metadata Scaler - Local Test\n")

    # Test 1: File parsing (no API needed)
    await test_parse_file()

    # Test 2: Asset search (needs API)
    await test_find_assets("test")

    # Test 3: Full workflow (needs API)
    await test_full_workflow(dry_run=False)

    print("\n✅ Tests complete!\n")


if __name__ == "__main__":
    asyncio.run(main())

