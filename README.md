# Bulk Metadata Scaler App

An Atlan Application SDK app for bulk-updating asset metadata based on reference files.

## Features

- Upload Excel/CSV reference files via API
- Dynamically maps columns to Atlan properties
- Finds all assets matching column names
- Bulk-updates description, owners, tags, and custom metadata
- Supports dry-run mode for preview

## Installation

```bash
# Install dependencies
pip install -e .

# Or with uv
uv sync
```

## Configuration

Set environment variables:

```bash
export ATLAN_API_KEY="your-api-key"
export ATLAN_BASE_URL="https://your-tenant.atlan.com"
```

## Usage

### Start the Application

```bash
# Start dependencies (Dapr + Temporal)
uv run poe start-deps

# Run the app
python -m bulk_metadata_scaler_app.main
```

### API Endpoints

#### Upload and Process File

```bash
POST /api/v1/enrich
Content-Type: multipart/form-data

# Form fields:
# - file: The reference file (CSV or Excel)
# - asset_types: Comma-separated asset types (default: Column)
# - dry_run: true/false (default: false)
```

#### Example with curl

```bash
curl -X POST "http://localhost:8000/api/v1/enrich" \
  -F "file=@reference_data.xlsx" \
  -F "asset_types=Column" \
  -F "dry_run=true"
```

## Reference File Format

| name | description | user_owners | certificate | CustomMetadata::FieldName |
|------|-------------|-------------|-------------|---------------------------|
| customer_id | Customer identifier | john@example.com | VERIFIED | Some Value |

### Column Types

1. **name** (required): Asset name to search for
2. **Standard fields**: `description`, `user_owners`, `group_owners`, `certificate`
3. **Custom metadata**: Format as `CustomMetadataSetName::FieldName`

## Architecture

Built on the Atlan Application SDK using:

- **Temporal**: Workflow orchestration
- **Dapr**: State management and service mesh
- **FastAPI**: REST API endpoints
- **pyatlan**: Atlan Python SDK for asset operations

## Development

```bash
# Run tests
pytest tests/

# Format code
ruff format .
```

