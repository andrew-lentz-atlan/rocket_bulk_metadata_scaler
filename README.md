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

## Quick Start (Local Development)

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
# or visit http://localhost:8000 for App Playground
```

### Docker Build

Build the Docker image locally:
```bash
docker build -t bulk-metadata-scaler:latest .
```

### Atlan Production Deployment

The app follows the [Atlan Apps deployment pattern](https://github.com/atlanhq/atlan-sample-apps):

1. **GitHub Actions** builds and pushes the image to GHCR on push to `main`
2. **Helm charts** deploy the app to the tenant's Kubernetes cluster
3. **Dapr** provides state management and object store access
4. **Temporal** orchestrates workflow execution

#### Deployment Steps

1. Push code to GitHub (triggers image build)
2. Deploy via Helm chart to internal tenant for testing
3. Create marketplace package for production release

See [Atlan Apps Deployment Via Helm Chart](https://github.com/atlanhq/atlan-sample-apps) for details.

### File Input Modes

| Mode | Config Key | Environment |
|------|------------|-------------|
| **Object Store** | `object_store_key` | Production (presigned URL upload) |
| **Base64** | `file_content` | API / Streamlit |
| **Local Path** | `file_upload` | Playground testing |

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
├── components/                 # Dapr components (local dev)
├── .github/workflows/          # CI/CD
│   └── build-image.yaml        # Docker image build
├── Dockerfile                  # Production container
├── ui.py                       # Streamlit UI
├── main.py                     # Entry point for Docker
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

# Build Docker image
docker build -t bulk-metadata-scaler .

# Run in Docker (local test)
docker run -p 8000:8000 -e ATLAN_API_KEY=xxx -e ATLAN_BASE_URL=https://tenant.atlan.com bulk-metadata-scaler
```
