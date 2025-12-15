"""Workflow for bulk metadata enrichment."""

from datetime import timedelta
from typing import Any, Callable, Dict, Sequence, cast

from temporalio import workflow
from temporalio.common import RetryPolicy

from application_sdk.activities import ActivitiesInterface
from application_sdk.observability.logger_adaptor import get_logger
from application_sdk.workflows import WorkflowInterface

from .activities import BulkMetadataActivities
from .models import ProcessingStatus, WorkflowResult

logger = get_logger(__name__)

# Retry policy: max 3 attempts, then fail
DEFAULT_RETRY_POLICY = RetryPolicy(
    maximum_attempts=3,
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=30),
    backoff_coefficient=2.0,
)


@workflow.defn
class BulkMetadataEnrichmentWorkflow(WorkflowInterface):
    """Workflow for processing bulk metadata enrichment from a reference file."""

    @workflow.run
    async def run(self, workflow_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the bulk metadata enrichment workflow.

        Args:
            workflow_config: Configuration containing workflow_id (full config is in state store).

        Returns:
            WorkflowResult as a dictionary.
        """
        activities = BulkMetadataActivities()
        
        # SDK stores full config in state store - retrieve it using get_workflow_args
        full_config: Dict[str, Any] = await workflow.execute_activity_method(
            activities.get_workflow_args,
            args=[workflow_config],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=DEFAULT_RETRY_POLICY,
        )
        
        asset_types = full_config.get("asset_types", ["Column"])
        
        # Handle run_type (playground) or dry_run (API/Streamlit)
        # run_type: "dry" or "live" from playground
        # dry_run: bool from API
        run_type = full_config.get("run_type", "")
        dry_run_value = full_config.get("dry_run", None)
        
        if run_type:
            # From playground - "dry" means dry run, "live" means apply changes
            dry_run = run_type.lower() == "dry"
        elif dry_run_value is not None:
            # From API/Streamlit - boolean value
            dry_run = bool(dry_run_value)
        else:
            # Default to dry run for safety
            dry_run = True

        # Initialize result
        result = WorkflowResult()

        # Step 1: Parse the file
        try:
            parse_result = await workflow.execute_activity_method(
                activities.parse_file,
                args=[full_config],  # Use full_config which has file_content
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(seconds=30),
                retry_policy=DEFAULT_RETRY_POLICY,
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
                    retry_policy=DEFAULT_RETRY_POLICY,
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

