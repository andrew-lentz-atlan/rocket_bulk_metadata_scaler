"""Configuration and setup forms for the bulk metadata scaler app."""

from typing import Any, Dict, List

# Setup form configuration for the Atlan UI
# This follows the Atlan App Framework setup forms pattern

SETUP_FORM_CONFIG = {
    "name": "Bulk Metadata Scaler",
    "description": "Bulk-update asset metadata from a reference file",
    "version": "0.1.0",
    "icon": "file-spreadsheet",
    "category": "data-governance",
    "steps": [
        {
            "id": "file-upload",
            "title": "Upload Reference File",
            "description": "Upload a CSV or Excel file with asset metadata",
            "fields": [
                {
                    "id": "reference_file",
                    "type": "file",
                    "label": "Reference File",
                    "description": "CSV or Excel file with asset names and metadata",
                    "required": True,
                    "accept": [".csv", ".xlsx", ".xls"],
                    "maxSize": "10MB",
                },
            ],
        },
        {
            "id": "configuration",
            "title": "Configuration",
            "description": "Configure how assets are matched and updated",
            "fields": [
                {
                    "id": "asset_types",
                    "type": "multiselect",
                    "label": "Asset Types",
                    "description": "Types of assets to search for",
                    "required": True,
                    "default": ["Column"],
                    "options": [
                        {"value": "Column", "label": "Column"},
                        {"value": "Table", "label": "Table"},
                        {"value": "View", "label": "View"},
                    ],
                },
                {
                    "id": "search_column",
                    "type": "text",
                    "label": "Search Column",
                    "description": "Column name in the file that contains asset names",
                    "required": True,
                    "default": "name",
                },
                {
                    "id": "dry_run",
                    "type": "checkbox",
                    "label": "Dry Run",
                    "description": "Preview changes without applying them",
                    "default": False,
                },
            ],
        },
        {
            "id": "confirmation",
            "title": "Review & Confirm",
            "description": "Review your configuration and start the workflow",
            "fields": [
                {
                    "id": "confirmation",
                    "type": "summary",
                    "label": "Configuration Summary",
                },
            ],
        },
    ],
}


# Reference file format documentation
REFERENCE_FILE_FORMAT = {
    "required_columns": ["name"],
    "standard_columns": [
        {
            "name": "name",
            "description": "Asset name to search for (required)",
            "example": "customer_id",
        },
        {
            "name": "description",
            "description": "Description to set on the asset",
            "example": "Unique identifier for customers",
        },
        {
            "name": "user_owners",
            "description": "Comma-separated list of user emails",
            "example": "john@example.com,jane@example.com",
        },
        {
            "name": "group_owners",
            "description": "Comma-separated list of group names",
            "example": "data-stewards,analytics-team",
        },
        {
            "name": "certificate",
            "description": "Certification status (VERIFIED, DRAFT, or DEPRECATED)",
            "example": "VERIFIED",
        },
    ],
    "custom_metadata_format": {
        "pattern": "CustomMetadataSetName::FieldName",
        "description": "Use double colons to specify custom metadata fields",
        "example": "Data Governance::Data Steward",
    },
}


def get_setup_form() -> Dict[str, Any]:
    """Get the setup form configuration."""
    return SETUP_FORM_CONFIG


def get_file_format_help() -> Dict[str, Any]:
    """Get help documentation for the reference file format."""
    return REFERENCE_FILE_FORMAT


def validate_form_input(form_data: Dict[str, Any]) -> List[str]:
    """
    Validate form input and return list of errors.

    Args:
        form_data: The form data to validate.

    Returns:
        List of error messages (empty if valid).
    """
    errors = []

    if not form_data.get("reference_file"):
        errors.append("Reference file is required")

    if not form_data.get("asset_types"):
        errors.append("At least one asset type must be selected")

    if not form_data.get("search_column"):
        errors.append("Search column name is required")

    return errors

