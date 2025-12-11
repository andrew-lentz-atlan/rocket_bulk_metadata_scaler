"""Workflow for bulk metadata enrichment."""

from datetime import timedelta
from typing import Any, Callable, Dict, Sequence, cast

from temporalio import workflow

from application_sdk.activities import ActivitiesInterface
from application_sdk.observability.logger_adaptor import get_logger
from application_sdk.workflows import WorkflowInterface

from .activities import BulkMetadataActivities
from .models import ProcessingStatus, WorkflowResult

logger = get_logger(__name__)


@workflow.defn
class BulkMetadataEnrichmentWorkflow(WorkflowInterface):
    """Workflow for processing bulk metadata enrichment from a reference file."""

    @workflow.run
    async def run(self, workflow_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the bulk metadata enrichment workflow.

        Args:
            workflow_config: Configuration containing file_content, file_name,
                           asset_types, dry_run, etc.

        Returns:
            WorkflowResult as a dictionary.
        """
        activities = BulkMetadataActivities()
        
        asset_types = workflow_config.get("asset_types", ["Column"])
        dry_run = workflow_config.get("dry_run", False)

        # Initialize result
        result = WorkflowResult()

        # Step 1: Parse the file
        try:
            parse_result = await workflow.execute_activity_method(
                activities.parse_file,
                args=[workflow_config],
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(seconds=30),
            )
        except Exception as e:
            result.errors.append(f"Failed to parse file: {str(e)}")
            return result.to_dict()

        records = parse_result.get("records", [])
        result.total_rows = parse_result.get("total_rows", len(records))

        if not records:
            result.errors.append("No valid records found in file")
            return result.to_dict()

        logger.info(f"Processing {len(records)} records, dry_run={dry_run}")

        # Step 2: Process each record
        for record in records:
            try:
                row_result = await workflow.execute_activity_method(
                    activities.process_single_row,
                    args=[record, asset_types, dry_run],
                    start_to_close_timeout=timedelta(minutes=10),
                    heartbeat_timeout=timedelta(seconds=60),
                )

                # Update counters based on status
                status = row_result.get("status")
                result.total_assets_found += row_result.get("assets_found", 0)
                result.total_assets_updated += row_result.get("assets_updated", 0)

                if status == ProcessingStatus.SUCCESS.value:
                    result.successful_rows += 1
                elif status == ProcessingStatus.PARTIAL.value:
                    result.partial_rows += 1
                elif status == ProcessingStatus.FAILED.value:
                    result.failed_rows += 1
                    if row_result.get("error"):
                        result.errors.append(
                            f"Row {row_result['row_index']}: {row_result['error']}"
                        )
                elif status == ProcessingStatus.NOT_FOUND.value:
                    result.not_found_rows += 1
                elif status == ProcessingStatus.SKIPPED.value:
                    result.skipped_rows += 1

                # Add to results
                result.row_results.append(row_result)

            except Exception as e:
                result.failed_rows += 1
                result.errors.append(
                    f"Row {record.get('row_index', '?')}: {str(e)}"
                )

        logger.info(
            f"Workflow complete: {result.successful_rows} successful, "
            f"{result.failed_rows} failed, {result.not_found_rows} not found"
        )

        return result.to_dict()

    @staticmethod
    def get_activities(activities: ActivitiesInterface) -> Sequence[Callable[..., Any]]:
        """Return the list of activities used by this workflow."""
        activities = cast(BulkMetadataActivities, activities)
        return [
            activities.parse_file,
            activities.find_assets_by_name,
            activities.update_asset_metadata,
            activities.process_single_row,
        ]

