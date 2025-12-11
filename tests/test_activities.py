"""Tests for bulk metadata scaler activities."""

import pytest
from bulk_metadata_scaler_app.activities import BulkMetadataActivities
from bulk_metadata_scaler_app.models import ColumnMapping


class TestBulkMetadataActivities:
    """Test suite for BulkMetadataActivities."""

    @pytest.fixture
    def activities(self):
        """Create a BulkMetadataActivities instance for testing."""
        return BulkMetadataActivities()

    def test_classify_columns_search_column(self, activities):
        """Test that search column is correctly identified."""
        columns = ["name", "description", "other"]
        mapping = activities._classify_columns(columns, "name", "::")

        assert mapping.search_column == "name"

    def test_classify_columns_standard_fields(self, activities):
        """Test that standard fields are correctly identified."""
        columns = ["name", "description", "user_owners", "certificate"]
        mapping = activities._classify_columns(columns, "name", "::")

        assert "description" in mapping.standard_fields
        assert "user_owners" in mapping.standard_fields
        assert "certificate" in mapping.standard_fields

    def test_classify_columns_custom_metadata(self, activities):
        """Test that custom metadata columns are correctly identified."""
        columns = ["name", "Data Governance::Data Steward", "Data Governance::Review Date"]
        mapping = activities._classify_columns(columns, "name", "::")

        assert "Data Governance" in mapping.custom_metadata
        assert "Data Steward" in mapping.custom_metadata["Data Governance"]
        assert "Review Date" in mapping.custom_metadata["Data Governance"]

    def test_classify_columns_unrecognized(self, activities):
        """Test that unrecognized columns are tracked."""
        columns = ["name", "random_column", "another_one"]
        mapping = activities._classify_columns(columns, "name", "::")

        assert "random_column" in mapping.unrecognized_columns
        assert "another_one" in mapping.unrecognized_columns

    def test_column_mapping_has_updates(self):
        """Test ColumnMapping.has_updates method."""
        # Empty mapping
        empty_mapping = ColumnMapping(search_column="name")
        assert not empty_mapping.has_updates()

        # Mapping with standard fields
        with_standard = ColumnMapping(
            search_column="name",
            standard_fields={"description": "description"}
        )
        assert with_standard.has_updates()

        # Mapping with custom metadata
        with_cm = ColumnMapping(
            search_column="name",
            custom_metadata={"CM": {"field": "CM::field"}}
        )
        assert with_cm.has_updates()

