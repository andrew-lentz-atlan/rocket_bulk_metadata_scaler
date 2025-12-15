# Bulk Metadata Scaler App

An Atlan Application SDK app for bulk-updating asset metadata based on reference files.

## Features

- 📁 Upload Excel/CSV reference files
- 🔍 Find all assets matching names in your reference file
- 📝 Bulk-update description, owners, certificates, and custom metadata
- 🔄 Dry-run mode for preview before applying changes
- 🎯 Filter by asset types (Column, Table, View, etc.)

## Installation

```bash
# Clone and enter the directory
cd bulk_metadata_scaler_app

# Install dependencies with uv (recommended)
uv sync

# Or with pip
pip install -e .
```

## Configuration

Set environment variables in a `.env` file:

```bash
ATLAN_API_KEY="your-api-key"
ATLAN_BASE_URL="https://your-tenant.atlan.com"
```

## Quick Start

### Terminal 1: Start SDK Dependencies

```bash
# From the application-sdk directory
cd /path/to/application-sdk
uv run poe start-deps
```

### Terminal 2: Run the App

```bash
cd /path/to/bulk_metadata_scaler_app
source /path/to/venv/bin/activate
python -m bulk_metadata_scaler_app.main
```

### Terminal 3 (Optional): Streamlit UI

```bash
streamlit run ui.py
```

## Access Points

| Interface | URL | Description |
|-----------|-----|-------------|
| **App Playground** | http://localhost:8000 | Atlan-style UI for testing |
| **Streamlit UI** | http://localhost:8501 | Alternative UI with drag-drop upload |
| **Temporal Dashboard** | http://localhost:8233 | Monitor workflow execution |
| **API Docs** | http://localhost:8000/docs | OpenAPI documentation |

## Reference File Format

Your CSV/Excel file should have a `name` column and any metadata columns you want to update:

| name | description | certificate | user_owners | Data Governance::Business Owner |
|------|-------------|-------------|-------------|--------------------------------|
| customer_id | Customer identifier | VERIFIED | john@example.com | Jane Smith |
| order_date | Date order placed | DRAFT | | |

### Supported Columns

1. **name** (required): Asset name to search for
2. **Standard fields**: 
   - `description` - Asset description
   - `certificate` - VERIFIED, DRAFT, or DEPRECATED
   - `user_owners` - Comma-separated email addresses
   - `group_owners` - Comma-separated group names
3. **Custom metadata**: `CustomMetadataSetName::FieldName` format

## API Usage

### Trigger Workflow

```bash
POST /workflows/v1/start
Content-Type: application/json

{
  "file_content": "<base64-encoded-file>",
  "file_name": "reference.csv",
  "asset_types": ["Column"],
  "dry_run": true
}
```

### Python Example

```python
import base64
import requests

with open('reference.csv', 'rb') as f:
    content = base64.b64encode(f.read()).decode('utf-8')

response = requests.post(
    'http://localhost:8000/workflows/v1/start',
    json={
        'file_content': content,
        'file_name': 'reference.csv',
        'asset_types': ['Column'],
        'dry_run': True
    }
)
print(response.json())
```

## Architecture

Built on the Atlan Application SDK:

- **Temporal**: Workflow orchestration and reliability
- **Dapr**: State management and service mesh
- **FastAPI**: REST API endpoints
- **pyatlan**: Atlan Python SDK for asset operations

## Deployment

### Local Development

Use Streamlit UI or App Playground for testing:
```bash
streamlit run ui.py  # Full file upload support
# or
# Visit http://localhost:8000 for App Playground
```

### Atlan Production

When deployed to Atlan:
1. The platform reads `frontend/config.json` to render the UI
2. File uploads work via drag-and-drop
3. Integrates with Atlan authentication

## Project Structure

```
bulk_metadata_scaler_app/
├── bulk_metadata_scaler_app/   # Main package
│   ├── activities.py           # Temporal activities (business logic)
│   ├── workflow.py             # Temporal workflow definition
│   ├── models.py               # Data models
│   └── main.py                 # FastAPI server & entry point
├── frontend/
│   └── config.json             # UI configuration for Atlan
├── ui.py                       # Streamlit UI
├── sample_reference.csv        # Example input file
├── pyproject.toml              # Dependencies
└── README.md
```

## Development

```bash
# Run tests
pytest tests/

# Format code
ruff format .
```
