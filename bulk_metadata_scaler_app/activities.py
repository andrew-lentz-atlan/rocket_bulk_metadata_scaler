"""Activities for the bulk metadata scaler workflow."""

import base64
import io
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Type

import pandas as pd
from temporalio import activity

from application_sdk.activities import ActivitiesInterface
from application_sdk.activities.common.utils import auto_heartbeater
from application_sdk.observability.logger_adaptor import get_logger

from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Asset, Column, Table, View
from pyatlan.model.fluent_search import FluentSearch
from pyatlan.model.enums import CertificateStatus

from .models import (
    ColumnMapping,
    EnrichmentRecord,
    AssetUpdateResult,
    RowProcessingResult,
    ProcessingStatus,
    WorkflowConfig,
    WorkflowResult,
)

logger = get_logger(__name__)

# Mapping of asset type names to pyatlan classes
ASSET_TYPE_MAP: Dict[str, Type[Asset]] = {
    "Column": Column,
    "Table": Table,
    "View": View,
}

# Certificate status mapping
CERTIFICATE_STATUS_MAP = {
    "verified": CertificateStatus.VERIFIED,
    "draft": CertificateStatus.DRAFT,
    "deprecated": CertificateStatus.DEPRECATED,
}

# Standard fields that can be updated
STANDARD_FIELDS = {
    "description",
    "user_owners",
    "group_owners",
    "certificate",
    "certificate_message",
}


class BulkMetadataActivities(ActivitiesInterface):
    """Activities for bulk metadata enrichment."""

    def __init__(self):
        """Initialize the activities."""
        self._client: Optional[AtlanClient] = None

    def _get_client(self) -> AtlanClient:
        """Get or create the Atlan client."""
        if self._client is None:
            self._client = AtlanClient()
        return self._client

    @activity.defn
    @auto_heartbeater
    async def parse_file(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse the uploaded file and extract records.

        Args:
            config: Workflow configuration containing file_content and file_name.

        Returns:
            Dictionary with column_mapping and records.
        """
        logger.info(f"Parsing file: {config.get('file_name', 'unknown')}")

        file_content = config.get("file_content", "")
        file_name = config.get("file_name", "data.csv")
        search_column = config.get("search_column", "name")
        cm_delimiter = config.get("custom_metadata_delimiter", "::")

        # Decode base64 content if it's a string
        if isinstance(file_content, str):
            try:
                file_content = base64.b64decode(file_content)
            except Exception:
                file_content = file_content.encode()

        # Determine file type and load
        suffix = Path(file_name).suffix.lower()

        if suffix in [".xlsx", ".xls"]:
            df = pd.read_excel(io.BytesIO(file_content), dtype=str)
        else:
            df = pd.read_csv(io.BytesIO(file_content), dtype=str)

        # Clean column names
        df.columns = df.columns.str.strip()

        logger.info(f"Loaded {len(df)} rows with {len(df.columns)} columns")

        # Classify columns
        column_mapping = self._classify_columns(
            list(df.columns), search_column, cm_delimiter
        )

        if not column_mapping.search_column:
            raise ValueError(f"No search column '{search_column}' found in file")

        # Extract records
        records = self._extract_records(df, column_mapping)

        logger.info(f"Extracted {len(records)} valid records")

        return {
            "column_mapping": {
                "search_column": column_mapping.search_column,
                "standard_fields": column_mapping.standard_fields,
                "custom_metadata": column_mapping.custom_metadata,
            },
            "records": [
                {
                    "row_index": r.row_index,
                    "asset_name": r.asset_name,
                    "standard_values": r.standard_values,
                    "custom_metadata_values": r.custom_metadata_values,
                }
                for r in records
            ],
            "total_rows": len(df),
        }

    def _classify_columns(
        self, columns: List[str], search_column: str, cm_delimiter: str
    ) -> ColumnMapping:
        """Classify columns into search, standard, and custom metadata."""
        mapping = ColumnMapping(search_column="")
        search_col_lower = search_column.lower()

        for col in columns:
            col_lower = col.lower().strip()

            if col_lower == search_col_lower:
                mapping.search_column = col
            elif col_lower in STANDARD_FIELDS:
                mapping.standard_fields[col_lower] = col
            elif cm_delimiter in col:
                parts = col.split(cm_delimiter, 1)
                if len(parts) == 2:
                    cm_set = parts[0].strip()
                    field = parts[1].strip()
                    if cm_set not in mapping.custom_metadata:
                        mapping.custom_metadata[cm_set] = {}
                    mapping.custom_metadata[cm_set][field] = col
            else:
                mapping.unrecognized_columns.append(col)

        return mapping

    def _extract_records(
        self, df: pd.DataFrame, mapping: ColumnMapping
    ) -> List[EnrichmentRecord]:
        """Extract enrichment records from the dataframe."""
        records = []

        for idx, row in df.iterrows():
            asset_name = row.get(mapping.search_column)

            if pd.isna(asset_name) or not str(asset_name).strip():
                continue

            asset_name = str(asset_name).strip()

            # Extract standard values
            standard_values = {}
            for field_name, col_name in mapping.standard_fields.items():
                value = row.get(col_name)
                if not pd.isna(value) and str(value).strip():
                    standard_values[field_name] = str(value).strip()

            # Extract custom metadata values
            cm_values = {}
            for cm_set, fields in mapping.custom_metadata.items():
                cm_field_values = {}
                for field_name, col_name in fields.items():
                    value = row.get(col_name)
                    if not pd.isna(value) and str(value).strip():
                        cm_field_values[field_name] = str(value).strip()
                if cm_field_values:
                    cm_values[cm_set] = cm_field_values

            records.append(
                EnrichmentRecord(
                    row_index=int(idx),
                    asset_name=asset_name,
                    standard_values=standard_values,
                    custom_metadata_values=cm_values,
                )
            )

        return records

    @activity.defn
    @auto_heartbeater
    async def find_assets_by_name(
        self, asset_name: str, asset_types: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Find all assets matching the given name.

        Args:
            asset_name: The asset name to search for.
            asset_types: List of asset type names to search.

        Returns:
            List of asset dictionaries with guid, qualified_name, and name.
        """
        client = self._get_client()
        all_assets = []

        for type_name in asset_types:
            asset_class = ASSET_TYPE_MAP.get(type_name)
            if not asset_class:
                logger.warning(f"Unknown asset type: {type_name}")
                continue

            try:
                search = (
                    FluentSearch()
                    .where(FluentSearch.asset_type(asset_class))
                    .where(FluentSearch.active_assets())
                    .where(Asset.NAME.eq(asset_name))
                    .page_size(100)
                    .include_on_results(Asset.NAME)
                    .include_on_results(Asset.QUALIFIED_NAME)
                )

                search_request = search.to_request()
                results = client.asset.search(search_request)

                for asset in results:
                    all_assets.append({
                        "guid": asset.guid,
                        "qualified_name": asset.qualified_name,
                        "name": asset.name,
                        "type_name": type_name,
                    })

            except Exception as e:
                logger.error(f"Error searching for {type_name} '{asset_name}': {e}")

        logger.info(f"Found {len(all_assets)} assets matching '{asset_name}'")
        return all_assets

    @activity.defn
    @auto_heartbeater
    async def update_asset_metadata(
        self,
        asset_info: Dict[str, Any],
        record: Dict[str, Any],
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Update metadata on a single asset.

        Args:
            asset_info: Asset information (guid, qualified_name, name, type_name).
            record: The enrichment record with values to apply.
            dry_run: If True, don't actually update.

        Returns:
            AssetUpdateResult as a dictionary.
        """
        client = self._get_client()
        guid = asset_info["guid"]
        asset_type_name = asset_info.get("type_name", "Column")
        asset_class = ASSET_TYPE_MAP.get(asset_type_name, Column)

        result = {
            "guid": guid,
            "qualified_name": asset_info.get("qualified_name", ""),
            "name": asset_info.get("name", ""),
            "success": False,
            "error": None,
            "updated_fields": [],
        }

        try:
            # Fetch the asset
            asset = client.asset.get_by_guid(
                guid=guid, asset_type=asset_class, ignore_relationships=True
            )

            if not asset:
                result["error"] = "Asset not found"
                return result

            # Create updater
            to_update = asset.trim_to_required()
            updated_fields = []

            # Apply standard fields
            standard_values = record.get("standard_values", {})

            if "description" in standard_values:
                to_update.description = standard_values["description"]
                updated_fields.append("description")

            if "user_owners" in standard_values:
                owners = [
                    o.strip()
                    for o in standard_values["user_owners"].split(",")
                    if o.strip()
                ]
                if owners:
                    to_update.owner_users = set(owners)
                    updated_fields.append("user_owners")

            if "group_owners" in standard_values:
                owners = [
                    o.strip()
                    for o in standard_values["group_owners"].split(",")
                    if o.strip()
                ]
                if owners:
                    to_update.owner_groups = set(owners)
                    updated_fields.append("group_owners")

            if "certificate" in standard_values:
                cert_value = standard_values["certificate"].lower().strip()
                if cert_value in CERTIFICATE_STATUS_MAP:
                    to_update.certificate_status = CERTIFICATE_STATUS_MAP[cert_value]
                    updated_fields.append("certificate")

            # Apply custom metadata
            cm_values = record.get("custom_metadata_values", {})
            for cm_set_name, fields in cm_values.items():
                try:
                    cma = asset.get_custom_metadata(client=client, name=cm_set_name)
                    for field_name, value in fields.items():
                        try:
                            cma[field_name] = value
                            updated_fields.append(f"{cm_set_name}::{field_name}")
                        except KeyError:
                            logger.warning(
                                f"Invalid CM field '{field_name}' for '{cm_set_name}'"
                            )
                    to_update.set_custom_metadata(custom_metadata=cma, client=client)
                except Exception as e:
                    logger.error(f"Error with custom metadata '{cm_set_name}': {e}")

            if not updated_fields:
                result["success"] = True
                return result

            # Save the asset
            if dry_run:
                logger.info(
                    f"[DRY RUN] Would update '{asset.name}' with: {updated_fields}"
                )
            else:
                if cm_values:
                    client.asset.save_merging_cm(to_update)
                else:
                    client.asset.save(to_update)
                logger.info(f"Updated '{asset.name}' with: {updated_fields}")

            result["success"] = True
            result["updated_fields"] = updated_fields

        except Exception as e:
            result["error"] = str(e)
            logger.error(f"Error updating asset {guid}: {e}")

        return result

    @activity.defn
    @auto_heartbeater
    async def process_single_row(
        self,
        record: Dict[str, Any],
        asset_types: List[str],
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Process a single row: find assets and update metadata.

        Args:
            record: The enrichment record.
            asset_types: Asset types to search.
            dry_run: If True, don't actually update.

        Returns:
            RowProcessingResult as a dictionary.
        """
        row_index = record["row_index"]
        asset_name = record["asset_name"]

        result = {
            "row_index": row_index,
            "asset_name": asset_name,
            "status": ProcessingStatus.PENDING.value,
            "assets_found": 0,
            "assets_updated": 0,
            "error": None,
        }

        # Check if record has values
        has_values = bool(record.get("standard_values")) or bool(
            record.get("custom_metadata_values")
        )
        if not has_values:
            result["status"] = ProcessingStatus.SKIPPED.value
            return result

        # Find assets
        try:
            assets = await self.find_assets_by_name(asset_name, asset_types)
            result["assets_found"] = len(assets)
        except Exception as e:
            result["status"] = ProcessingStatus.FAILED.value
            result["error"] = f"Search failed: {str(e)}"
            return result

        if not assets:
            result["status"] = ProcessingStatus.NOT_FOUND.value
            return result

        # Update each asset
        success_count = 0
        fail_count = 0

        for asset_info in assets:
            try:
                update_result = await self.update_asset_metadata(
                    asset_info, record, dry_run
                )
                if update_result["success"]:
                    success_count += 1
                else:
                    fail_count += 1
            except Exception as e:
                fail_count += 1
                logger.error(f"Error updating asset: {e}")

        result["assets_updated"] = success_count

        if fail_count == 0:
            result["status"] = ProcessingStatus.SUCCESS.value
        elif success_count == 0:
            result["status"] = ProcessingStatus.FAILED.value
            result["error"] = f"All {fail_count} updates failed"
        else:
            result["status"] = ProcessingStatus.PARTIAL.value
            result["error"] = f"{fail_count}/{len(assets)} updates failed"

        return result

