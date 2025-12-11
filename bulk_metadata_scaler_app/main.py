"""
Bulk Metadata Scaler App - Main Entry Point

An Atlan Application SDK app for bulk-updating asset metadata based on reference files.

Usage:
    # Start dependencies first (in application-sdk directory):
    uv run poe start-deps

    # Then run the app:
    python -m bulk_metadata_scaler_app.main
"""

import asyncio
import base64
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, File, Form, UploadFile, HTTPException
from pydantic import BaseModel

from application_sdk.application import BaseApplication
from application_sdk.clients.utils import get_workflow_client
from application_sdk.handlers import HandlerInterface
from application_sdk.observability.logger_adaptor import get_logger
from application_sdk.server.fastapi import APIServer, HttpWorkflowTrigger
from application_sdk.worker import Worker

from .activities import BulkMetadataActivities
from .workflow import BulkMetadataEnrichmentWorkflow

APPLICATION_NAME = "bulk-metadata-scaler"

logger = get_logger(__name__)


class EnrichmentRequest(BaseModel):
    """Request model for the enrichment endpoint."""
    file_content: str  # Base64 encoded
    file_name: str
    asset_types: List[str] = ["Column"]
    dry_run: bool = False


class EnrichmentResponse(BaseModel):
    """Response model for the enrichment endpoint."""
    workflow_id: str
    status: str
    message: str


class BulkMetadataHandler(HandlerInterface):
    """Handler for the bulk metadata scaler application."""

    def __init__(self):
        """Initialize the handler and load configmap."""
        self._configmap: Optional[Dict[str, Any]] = None
        self._load_configmap()

    def _load_configmap(self) -> None:
        """Load the configmap from file."""
        # Look for configmap.json in the package directory or parent
        possible_paths = [
            Path(__file__).parent.parent / "configmap.json",
            Path(__file__).parent / "configmap.json",
            Path("configmap.json"),
        ]
        
        for config_path in possible_paths:
            if config_path.exists():
                with open(config_path, "r") as f:
                    self._configmap = json.load(f)
                logger.info(f"Loaded configmap from {config_path}")
                return
        
        logger.warning("No configmap.json found")

    async def load(self, **kwargs: Any) -> None:
        """Load handler resources."""
        pass

    async def test_auth(self, **kwargs: Any) -> bool:
        """Test authentication."""
        # Auth is handled by the Atlan client
        return True

    async def fetch_metadata(self, **kwargs: Any) -> Any:
        """Fetch metadata - not used in this app."""
        return {}

    async def preflight_check(self, **kwargs: Any) -> Any:
        """Preflight check - not used in this app."""
        return {"success": True}

    async def get_configmap(self, config_map_id: str) -> Dict[str, Any]:
        """Return the configmap for UI rendering."""
        if self._configmap:
            return self._configmap
        return {
            "title": "Bulk Metadata Scaler",
            "description": "Upload a reference file to enrich asset metadata",
            "type": "object",
            "properties": {}
        }


class BulkMetadataScalerApp(APIServer):
    """Custom FastAPI server with file upload endpoint."""

    custom_router: APIRouter = APIRouter()

    def register_routers(self):
        """Register custom routers."""
        self.app.include_router(self.custom_router, prefix="/api/v1")
        super().register_routers()

    def register_ui_routes(self):
        """Override to disable static file mounting (we don't have a frontend)."""
        # Skip the default UI routes that require frontend/static directory
        pass

    def register_routes(self):
        """Register custom routes."""
        self.custom_router.add_api_route(
            "/enrich",
            self.enrich_from_file,
            methods=["POST"],
            response_model=EnrichmentResponse,
            summary="Upload and process reference file",
            description="Upload a CSV or Excel file to enrich asset metadata",
        )

        self.custom_router.add_api_route(
            "/health",
            self.health_check,
            methods=["GET"],
            summary="Health check",
        )

        super().register_routes()

    async def health_check(self) -> Dict[str, str]:
        """Health check endpoint."""
        return {"status": "healthy", "app": APPLICATION_NAME}

    async def enrich_from_file(
        self,
        file: UploadFile = File(..., description="Reference file (CSV or Excel)"),
        asset_types: str = Form(
            default="Column",
            description="Comma-separated asset types to search (e.g., Column,Table)"
        ),
        dry_run: bool = Form(
            default=False,
            description="If true, preview changes without applying"
        ),
    ) -> EnrichmentResponse:
        """
        Upload a reference file and trigger the enrichment workflow.

        The file should contain:
        - A 'name' column with asset names to search
        - Standard columns: description, user_owners, group_owners, certificate
        - Custom metadata columns: 'CustomMetadataSet::FieldName' format
        """
        # Validate file type
        if not file.filename:
            raise HTTPException(status_code=400, detail="No filename provided")

        valid_extensions = [".csv", ".xlsx", ".xls"]
        if not any(file.filename.lower().endswith(ext) for ext in valid_extensions):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid file type. Supported: {valid_extensions}"
            )

        # Read file content
        try:
            content = await file.read()
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to read file: {e}")

        # Parse asset types
        asset_type_list = [t.strip() for t in asset_types.split(",") if t.strip()]

        logger.info(
            f"Received file: {file.filename}, "
            f"size: {len(content)} bytes, "
            f"asset_types: {asset_type_list}, "
            f"dry_run: {dry_run}"
        )

        # Prepare workflow config
        # Encode file content as base64 for serialization
        workflow_config = {
            "file_content": base64.b64encode(content).decode("utf-8"),
            "file_name": file.filename,
            "asset_types": asset_type_list,
            "dry_run": dry_run,
            "search_column": "name",
            "custom_metadata_delimiter": "::",
        }

        # Start the workflow
        try:
            # Use the registered workflow trigger
            workflow_response = await self._start_workflow(
                workflow_class=BulkMetadataEnrichmentWorkflow,
                workflow_args=workflow_config,
            )

            return EnrichmentResponse(
                workflow_id=workflow_response.get("workflow_id", "unknown"),
                status="started",
                message=f"Processing {file.filename} with {len(content)} bytes",
            )

        except Exception as e:
            logger.error(f"Failed to start workflow: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to start workflow: {e}")

    async def _start_workflow(
        self,
        workflow_class: Any,
        workflow_args: Dict[str, Any],
    ) -> Dict[str, str]:
        """Start a workflow and return the response."""
        # This will be implemented by the base class or overridden
        # For now, return a placeholder
        import uuid
        return {
            "workflow_id": str(uuid.uuid4()),
            "run_id": str(uuid.uuid4()),
        }


async def main(daemon: bool = True) -> Dict[str, Any]:
    """Main entry point for the application."""
    logger.info(f"Starting {APPLICATION_NAME}")

    # Initialize the Temporal workflow client
    logger.info("Initializing Temporal workflow client...")
    workflow_client = get_workflow_client(application_name=APPLICATION_NAME)
    await workflow_client.load()
    logger.info("Temporal workflow client connected successfully")

    # Create the activities instance and get activity methods
    activities_instance = BulkMetadataActivities()
    activity_methods = [
        activities_instance.get_workflow_args,  # SDK activity to retrieve config from state store
        activities_instance.parse_file,
        activities_instance.find_assets_by_name,
        activities_instance.update_asset_metadata,
        activities_instance.process_single_row,
    ]

    # Create and start the worker (daemon mode so it runs in background)
    logger.info("Starting Temporal worker...")
    worker = Worker(
        workflow_client=workflow_client,
        workflow_activities=activity_methods,
        workflow_classes=[BulkMetadataEnrichmentWorkflow],
        passthrough_modules=["pyatlan", "pandas"],
    )
    await worker.start(daemon=True)
    logger.info("Temporal worker started in background")

    # Initialize the custom FastAPI app with the workflow client
    app = BulkMetadataScalerApp(
        handler=BulkMetadataHandler(),
        workflow_client=workflow_client,
        has_configmap=True,  # Enable configmap-based UI for Atlan platform
    )

    # Register the workflow
    app.register_workflow(
        BulkMetadataEnrichmentWorkflow,
        [
            HttpWorkflowTrigger(
                endpoint="/workflow/enrich",
                methods=["POST"],
                workflow_class=BulkMetadataEnrichmentWorkflow,
            )
        ],
    )

    # Start the server
    await app.start()

    return {"status": "started", "app": APPLICATION_NAME}


async def run_standalone():
    """Run as standalone application (for testing without full SDK infrastructure)."""
    logger.info(f"Starting {APPLICATION_NAME} in standalone mode")

    # Initialize base application
    app = BaseApplication(name=APPLICATION_NAME)

    # Setup workflow
    await app.setup_workflow(
        workflow_and_activities_classes=[
            (BulkMetadataEnrichmentWorkflow, BulkMetadataActivities)
        ]
    )

    # Start worker
    await app.start_worker(daemon=False)


if __name__ == "__main__":
    # Run the FastAPI application
    asyncio.run(main(daemon=False))

