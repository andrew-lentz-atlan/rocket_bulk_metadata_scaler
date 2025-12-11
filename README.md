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

### Quick Start (Full SDK Mode)

```bash
# Terminal 1: Start dependencies (Dapr + Temporal) from application-sdk directory
cd /path/to/application-sdk
uv run poe start-deps

# Terminal 2: Run the app
cd /path/to/bulk_metadata_scaler_app
python -m bulk_metadata_scaler_app.main

# Terminal 3: Run the Web UI
streamlit run ui.py
```

### Web UI

The app includes a **Streamlit web interface** for easy file uploads:

```bash
streamlit run ui.py
```

Then open http://localhost:8501 in your browser.

Features:
- 📁 Drag-and-drop file upload
- ⚙️ Configure asset types and dry-run mode
- 📊 Real-time workflow progress
- 📋 Results summary

### API Endpoints

#### Trigger Workflow (via JSON)

```bash
POST /workflows/v1/workflow/enrich
Content-Type: application/json

{
  "file_content": "<base64-encoded-file>",
  "file_name": "reference.csv",
  "asset_types": ["Column"],
  "dry_run": true
}
```

#### Example with Python

```python
import base64
import requests

with open('reference.csv', 'rb') as f:
    content = base64.b64encode(f.read()).decode('utf-8')

response = requests.post(
    'http://localhost:8000/workflows/v1/workflow/enrich',
    json={
        'file_content': content,
        'file_name': 'reference.csv',
        'asset_types': ['Column'],
        'dry_run': True
    }
)
print(response.json())

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

## Deployment Options

### Local Development (Streamlit UI)
```bash
streamlit run ui.py
```
Perfect for testing and demos. Opens at http://localhost:8501

### Atlan Production Deployment
The app includes a `configmap.json` that defines the UI forms for Atlan's platform:
- Atlan renders the UI automatically based on the configmap
- No custom frontend needed
- Integrates with Atlan's authentication and asset browser

The configmap is served via `/workflows/v1/configmap/{id}` endpoint.

## Development

```bash
# Run tests
pytest tests/

# Format code
ruff format .
```

