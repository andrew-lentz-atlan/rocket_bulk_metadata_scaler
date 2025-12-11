"""
Bulk Metadata Scaler - Streamlit UI

A simple web interface for uploading reference files and triggering
the bulk metadata enrichment workflow.

Usage:
    streamlit run ui.py
"""

import base64
import time
from typing import Optional

import requests
import streamlit as st

# Configuration
API_BASE_URL = "http://localhost:8000"

# Page config
st.set_page_config(
    page_title="Bulk Metadata Scaler",
    page_icon="🚀",
    layout="centered",
)

# Custom CSS for better styling
st.markdown("""
<style>
    .stApp {
        max-width: 800px;
        margin: 0 auto;
    }
    .success-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
    }
    .error-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        color: #721c24;
    }
    .info-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #cce5ff;
        border: 1px solid #b8daff;
        color: #004085;
    }
</style>
""", unsafe_allow_html=True)


def check_api_health() -> bool:
    """Check if the API server is running."""
    try:
        response = requests.get(f"{API_BASE_URL}/server/health", timeout=5)
        return response.status_code == 200
    except Exception:
        return False


def trigger_workflow(
    file_content: bytes,
    file_name: str,
    asset_types: list[str],
    dry_run: bool,
) -> dict:
    """Trigger the enrichment workflow via API."""
    encoded_content = base64.b64encode(file_content).decode("utf-8")
    
    payload = {
        "file_content": encoded_content,
        "file_name": file_name,
        "asset_types": asset_types,
        "dry_run": dry_run,
    }
    
    response = requests.post(
        f"{API_BASE_URL}/workflows/v1/workflow/enrich",
        json=payload,
        timeout=30,
    )
    return response.json()


def get_workflow_status(workflow_id: str, run_id: str) -> Optional[dict]:
    """Get the status of a workflow run."""
    try:
        response = requests.get(
            f"{API_BASE_URL}/workflows/v1/status/{workflow_id}/{run_id}",
            timeout=10,
        )
        if response.status_code == 200:
            return response.json()
    except Exception:
        pass
    return None


# Main UI
st.title("🚀 Bulk Metadata Scaler")
st.markdown("Upload a reference file to enrich asset metadata in Atlan.")

# Check API health
api_healthy = check_api_health()
if not api_healthy:
    st.error("⚠️ **API server is not running.** Please start the app first:")
    st.code("python -m bulk_metadata_scaler_app.main", language="bash")
    st.stop()

st.success("✅ Connected to API server")

# Divider
st.divider()

# File upload section
st.subheader("📁 Upload Reference File")
st.markdown("""
Your file should contain:
- A `name` column with asset names to search for
- Standard columns: `description`, `certificate`, `user_owners`, `group_owners`
- Custom metadata: `CustomMetadataSet::FieldName` format
""")

uploaded_file = st.file_uploader(
    "Choose a CSV or Excel file",
    type=["csv", "xlsx", "xls"],
    help="Upload a reference file containing asset metadata",
)

if uploaded_file:
    st.success(f"📄 **{uploaded_file.name}** ({uploaded_file.size:,} bytes)")

# Configuration section
st.divider()
st.subheader("⚙️ Configuration")

col1, col2 = st.columns(2)

with col1:
    asset_types = st.multiselect(
        "Asset Types to Search",
        options=["Column", "Table", "View", "Database", "Schema"],
        default=["Column"],
        help="Select which asset types to search for matching names",
    )

with col2:
    dry_run = st.checkbox(
        "🔍 Dry Run (Preview Only)",
        value=True,
        help="If checked, shows what would change without actually updating assets",
    )

# Execute section
st.divider()

if st.button(
    "🚀 Start Enrichment" if not dry_run else "🔍 Preview Changes",
    type="primary",
    disabled=not uploaded_file or not asset_types,
    use_container_width=True,
):
    if not uploaded_file:
        st.error("Please upload a file first")
    elif not asset_types:
        st.error("Please select at least one asset type")
    else:
        with st.spinner("Starting workflow..."):
            try:
                # Read file content
                file_content = uploaded_file.read()
                
                # Trigger workflow
                result = trigger_workflow(
                    file_content=file_content,
                    file_name=uploaded_file.name,
                    asset_types=asset_types,
                    dry_run=dry_run,
                )
                
                if result.get("success"):
                    workflow_id = result["data"]["workflow_id"]
                    run_id = result["data"]["run_id"]
                    
                    st.success("✅ Workflow started successfully!")
                    
                    # Show workflow info
                    st.markdown(f"""
                    **Workflow ID:** `{workflow_id}`  
                    **Run ID:** `{run_id}`
                    """)
                    
                    # Poll for status
                    st.markdown("---")
                    st.markdown("### 📊 Workflow Progress")
                    
                    status_placeholder = st.empty()
                    progress_bar = st.progress(0)
                    
                    max_polls = 60  # Max 60 polls (5 minutes at 5s interval)
                    for i in range(max_polls):
                        status = get_workflow_status(workflow_id, run_id)
                        
                        if status and status.get("success"):
                            data = status.get("data", {})
                            workflow_status = data.get("status", "UNKNOWN")
                            
                            if workflow_status == "COMPLETED":
                                progress_bar.progress(100)
                                status_placeholder.success("✅ Workflow completed!")
                                
                                # Show results if available
                                if "result" in data:
                                    result_data = data["result"]
                                    st.markdown("### 📋 Results")
                                    
                                    col1, col2, col3 = st.columns(3)
                                    col1.metric("Successful", result_data.get("successful_rows", 0))
                                    col2.metric("Failed", result_data.get("failed_rows", 0))
                                    col3.metric("Not Found", result_data.get("not_found_rows", 0))
                                    
                                    col4, col5 = st.columns(2)
                                    col4.metric("Assets Found", result_data.get("total_assets_found", 0))
                                    col5.metric("Assets Updated", result_data.get("total_assets_updated", 0))
                                    
                                    if result_data.get("errors"):
                                        with st.expander("⚠️ Errors"):
                                            for error in result_data["errors"]:
                                                st.error(error)
                                break
                                
                            elif workflow_status == "FAILED":
                                progress_bar.progress(100)
                                status_placeholder.error("❌ Workflow failed")
                                if "error" in data:
                                    st.error(data["error"])
                                break
                                
                            elif workflow_status == "RUNNING":
                                progress_bar.progress(min(95, (i + 1) * 100 // max_polls))
                                status_placeholder.info(f"⏳ Running... ({i * 5}s)")
                            else:
                                status_placeholder.info(f"Status: {workflow_status}")
                        
                        time.sleep(5)
                    else:
                        status_placeholder.warning("⏱️ Timeout - check Temporal UI for status")
                        st.markdown(f"[Open Temporal UI](http://localhost:8233/namespaces/default/workflows/{workflow_id}/{run_id})")
                        
                else:
                    st.error(f"❌ Failed to start workflow: {result.get('message', 'Unknown error')}")
                    
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")

# Footer
st.divider()
st.markdown("""
<div style="text-align: center; color: #888; font-size: 0.85rem;">
    <p>Bulk Metadata Scaler | Built with Atlan Application SDK</p>
    <p>
        <a href="http://localhost:8233" target="_blank">Temporal UI</a> |
        <a href="http://localhost:8000/docs" target="_blank">API Docs</a>
    </p>
</div>
""", unsafe_allow_html=True)

