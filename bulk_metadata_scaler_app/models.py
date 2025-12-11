"""Data models for the bulk metadata scaler app."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class ProcessingStatus(Enum):
    """Status of a processing operation."""
    PENDING = "pending"
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"
    SKIPPED = "skipped"
    NOT_FOUND = "not_found"


@dataclass
class ColumnMapping:
    """Mapping of columns from the input file to Atlan properties."""
    search_column: str
    standard_fields: Dict[str, str] = field(default_factory=dict)
    custom_metadata: Dict[str, Dict[str, str]] = field(default_factory=dict)
    unrecognized_columns: List[str] = field(default_factory=list)

    def has_updates(self) -> bool:
        """Check if there are any fields to update."""
        return bool(self.standard_fields or self.custom_metadata)


@dataclass
class EnrichmentRecord:
    """A single record from the input file to be processed."""
    row_index: int
    asset_name: str
    standard_values: Dict[str, Any] = field(default_factory=dict)
    custom_metadata_values: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def has_values_to_update(self) -> bool:
        """Check if this record has any non-empty values to update."""
        for value in self.standard_values.values():
            if value is not None and str(value).strip():
                return True
        for cm_fields in self.custom_metadata_values.values():
            for value in cm_fields.values():
                if value is not None and str(value).strip():
                    return True
        return False


@dataclass
class AssetUpdateResult:
    """Result of updating a single asset."""
    guid: str
    qualified_name: str
    name: str
    success: bool
    error: Optional[str] = None
    updated_fields: List[str] = field(default_factory=list)


@dataclass
class RowProcessingResult:
    """Result of processing a single row."""
    row_index: int
    asset_name: str
    status: ProcessingStatus
    assets_found: int = 0
    assets_updated: int = 0
    error: Optional[str] = None


@dataclass
class WorkflowResult:
    """Final result of the enrichment workflow."""
    total_rows: int = 0
    successful_rows: int = 0
    partial_rows: int = 0
    failed_rows: int = 0
    not_found_rows: int = 0
    skipped_rows: int = 0
    total_assets_found: int = 0
    total_assets_updated: int = 0
    row_results: List[RowProcessingResult] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "total_rows": self.total_rows,
            "successful_rows": self.successful_rows,
            "partial_rows": self.partial_rows,
            "failed_rows": self.failed_rows,
            "not_found_rows": self.not_found_rows,
            "skipped_rows": self.skipped_rows,
            "total_assets_found": self.total_assets_found,
            "total_assets_updated": self.total_assets_updated,
            "errors": self.errors[:10],  # Limit errors in response
        }


@dataclass
class WorkflowConfig:
    """Configuration for the enrichment workflow."""
    file_content: bytes
    file_name: str
    asset_types: List[str] = field(default_factory=lambda: ["Column"])
    search_column: str = "name"
    dry_run: bool = False
    custom_metadata_delimiter: str = "::"
    tag_delimiter: str = ","
    owner_delimiter: str = ","

