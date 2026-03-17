# Databricks Disaster Recovery - Unity Catalog Replication

## Overview

Replicate Unity Catalog objects (storage credentials, external locations) from a **primary workspace (West Europe)** to a **secondary workspace (North Europe)** on Azure for Eurobank.

## Architecture

- **Primary region:** West Europe
- **Secondary region:** North Europe
- **Metastore:** Unity Catalog with one catalog containing multiple schemas
- **Each schema** has its own storage credential and external location

## Naming Conventions

| Resource | West Europe (primary) | North Europe (secondary) |
|---|---|---|
| Storage account | `adledwdeltatstwe` | `adledwdeltatstne` |
| Access connector | `dbac-tst-we` | `dbac-tst-ne` |
| Resource group | `rg-dmlz-databricks-access-tst-we-01` | `rg-dmlz-databricks-access-tst-ne-01` |
| Schema storage URL | `abfss://unity@adledwdeltatstwe.dfs.core.windows.net/__unitystorage/schemas/<uuid>` | `abfss://unity@adledwdeltatstne.dfs.core.windows.net/__unitystorage/schemas/<uuid>` |

**Access connector (full ARM ID):**
- West: `/subscriptions/e160a181-3aba-4c87-b109-68b27f2d77bf/resourceGroups/rg-dmlz-databricks-access-tst-we-01/providers/Microsoft.Databricks/accessConnectors/dbac-tst-we`
- North: `/subscriptions/e160a181-3aba-4c87-b109-68b27f2d77bf/resourceGroups/rg-dmlz-databricks-access-tst-ne-01/providers/Microsoft.Databricks/accessConnectors/dbac-tst-ne`

## Implementation Plan

### Phase 1: Discovery & Replication of UC Objects (DONE)
- [x] `dr_uc_replication.py` - Python script that:
  1. Prompts for workspace URLs, catalog name, and PATs at runtime
  2. Connects to primary workspace, lists all schemas in the catalog
  3. For each schema, resolves the external location and storage credential
  4. Checks if each storage credential exists in secondary; creates if missing (with region-mapped access connector)
  5. Checks if each external location exists in secondary; creates if missing (with region-mapped storage URL)
  6. Supports dry-run mode (default) for safe previewing

### Phase 1.5: Network Connectivity Configuration (NCC) Setup (TODO)
- [ ] Create one NCC per region (West Europe and North Europe)
- [ ] Discover all unique storage accounts from external locations in both primary and secondary workspaces
- [ ] For each NCC, add private endpoint rules for **every** storage account (both primary and secondary region storages):
  - Blob endpoint (`blob.core.windows.net`) per storage account
  - DFS endpoint (`dfs.core.windows.net`) per storage account
- [ ] Example: each NCC gets rules for both `adledwdeltatstwe` and `adledwdeltatstne` (blob + dfs = 4 rules per storage pair)
- [ ] Support dry-run mode for previewing NCC and rule creation
- [ ] Skip creation if NCC or rules already exist


## Prerequisites

- Azure storage accounts and managed identities already provisioned in both regions
- Access connectors already created in both regions
- Databricks CLI authentication with Unity Catalog admin permissions on both workspaces
- Python 3.12+ with dependencies from `requirements.txt`

## Setup & Running

```bash
pip install -r requirements.txt
python dr_uc_replication.py
```

Prompts for all inputs at runtime. Defaults to dry-run mode.