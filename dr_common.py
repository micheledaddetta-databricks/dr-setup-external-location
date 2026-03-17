"""
Databricks Disaster Recovery - Shared Utilities
================================================
Region mapping, helpers, and SDK client factories shared across DR scripts.
"""

import re

from databricks.sdk import AccountClient, WorkspaceClient


# ---------------------------------------------------------------------------
# Region mapping
# ---------------------------------------------------------------------------
REGION_REPLACEMENTS = {
    # storage account suffix
    "tstne": "tstwe",
    # access connector & resource group region tags
    "tst-ne": "tst-we",
}


def convert_to_secondary(value: str) -> str:
    """Convert a primary (west) resource reference to its secondary (north) equivalent."""
    result = value
    for primary_pattern, secondary_pattern in REGION_REPLACEMENTS.items():
        result = result.replace(primary_pattern, secondary_pattern)
    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def extract_storage_account_from_url(url: str) -> str | None:
    """Extract storage account name from an abfss:// URL.

    Example: abfss://unity@adledwdeltatstwe.dfs.core.windows.net/... -> adledwdeltatstwe
    """
    m = re.search(r"@([^.]+)\.dfs\.core\.windows\.net", url)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# SDK client factories
# ---------------------------------------------------------------------------
def create_workspace_client(profile: str) -> WorkspaceClient:
    """Create a WorkspaceClient and validate connectivity."""
    w = WorkspaceClient(profile=profile)
    w.current_user.me()
    return w


def create_account_client(profile: str) -> AccountClient:
    """Create an AccountClient and validate connectivity."""
    a = AccountClient(profile=profile)
    # Trigger a lightweight call to verify the profile works
    list(a.metastores.list())
    return a
