"""
Databricks Disaster Recovery - Phase 1: Unity Catalog Replication
=================================================================
Replicates storage credentials and external locations from a primary
workspace (West Europe) to a secondary workspace (North Europe) on Azure.

Usage:  python dr_uc_replication.py
"""

import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    AzureManagedIdentityRequest,
    ExternalLocationInfo,
    SchemaInfo,
    StorageCredentialInfo,
)

from dr_common import convert_to_secondary, create_workspace_client


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------
def collect_primary_info(w: WorkspaceClient, catalog_name: str) -> dict:
    """Gather schemas, storage credentials and external locations from primary."""
    print(f"\n{'='*70}")
    print(f"Reading catalog '{catalog_name}' from PRIMARY workspace")
    print(f"{'='*70}")

    # 1. List schemas
    schemas: list[SchemaInfo] = list(w.schemas.list(catalog_name=catalog_name))
    print(f"\nFound {len(schemas)} schema(s) in catalog '{catalog_name}':")
    for s in schemas:
        print(f"  - {s.name}  (storage_root: {s.storage_root or 'N/A'})")

    # 2. List all storage credentials
    all_creds: list[StorageCredentialInfo] = list(w.storage_credentials.list())
    creds_by_name = {c.name: c for c in all_creds}
    print(f"\nFound {len(all_creds)} storage credential(s) in primary workspace:")
    for c in all_creds:
        print(f"  - {c.name}")

    # 3. List all external locations
    all_extlocs: list[ExternalLocationInfo] = list(w.external_locations.list())
    print(f"\nFound {len(all_extlocs)} external location(s) in primary workspace:")
    for e in all_extlocs:
        print(f"  - {e.name}  -> {e.url or 'N/A'}  (credential: {e.credential_name or 'N/A'})")

    # 4. For each schema, find which external location covers its storage_root
    schema_mapping = []
    for s in schemas:
        storage_root = s.storage_root or ""
        matched_extloc = None
        matched_cred = None

        for e in all_extlocs:
            extloc_url = e.url or ""
            if storage_root and extloc_url and storage_root.startswith(extloc_url):
                matched_extloc = e
                matched_cred = creds_by_name.get(e.credential_name)
                break

        schema_mapping.append({
            "schema": s,
            "external_location": matched_extloc,
            "storage_credential": matched_cred,
        })

    return {
        "schemas": schemas,
        "storage_credentials": all_creds,
        "external_locations": all_extlocs,
        "schema_mapping": schema_mapping,
    }


def replicate_to_secondary(
    primary_info: dict,
    w_secondary: WorkspaceClient,
    dry_run: bool = False,
):
    """Ensure storage credentials and external locations exist in secondary workspace."""
    print(f"\n{'='*70}")
    print(f"Replicating to SECONDARY workspace")
    print(f"{'='*70}")

    # Collect unique storage credentials and external locations to replicate
    creds_to_replicate: dict[str, StorageCredentialInfo] = {}
    extlocs_to_replicate: dict[str, ExternalLocationInfo] = {}

    for mapping in primary_info["schema_mapping"]:
        cred: StorageCredentialInfo | None = mapping.get("storage_credential")
        extloc: ExternalLocationInfo | None = mapping.get("external_location")
        if cred and cred.name not in creds_to_replicate:
            creds_to_replicate[cred.name] = cred
        if extloc and extloc.name not in extlocs_to_replicate:
            extlocs_to_replicate[extloc.name] = extloc

    # Also replicate all credentials referenced by external locations
    creds_by_name_primary = {c.name: c for c in primary_info["storage_credentials"]}
    for extloc in extlocs_to_replicate.values():
        if extloc.credential_name and extloc.credential_name not in creds_to_replicate:
            cred = creds_by_name_primary.get(extloc.credential_name)
            if cred:
                creds_to_replicate[cred.name] = cred

    # ---- Replicate storage credentials ------------------------------------
    print(f"\n--- Storage Credentials ({len(creds_to_replicate)} to check) ---")
    secondary_creds = list(w_secondary.storage_credentials.list())
    secondary_cred_names = {c.name for c in secondary_creds}

    for cred_name, cred in creds_to_replicate.items():
        if cred_name in secondary_cred_names:
            print(f"  [EXISTS]  Storage credential '{cred_name}' already exists in secondary.")
            continue

        if cred.azure_managed_identity:
            ami = cred.azure_managed_identity
            primary_connector = ami.access_connector_id or ""
            secondary_connector = convert_to_secondary(primary_connector)
            managed_id = convert_to_secondary(ami.managed_identity_id) if ami.managed_identity_id else None

            if dry_run:
                print(f"  [DRY-RUN] Would create storage credential '{cred_name}':")
                print(f"            access_connector_id: {secondary_connector}")
                if managed_id:
                    print(f"            managed_identity_id: {managed_id}")
            else:
                print(f"  [CREATE]  Creating storage credential '{cred_name}' in secondary...")
                try:
                    result = w_secondary.storage_credentials.create(
                        name=cred_name,
                        azure_managed_identity=AzureManagedIdentityRequest(
                            access_connector_id=secondary_connector,
                            managed_identity_id=managed_id,
                        ),
                        comment=cred.comment or "",
                    )
                    print(f"            Created successfully (id: {result.id})")
                except Exception as e:
                    print(f"            FAILED: {e}")

        elif cred.azure_service_principal:
            if dry_run:
                print(f"  [DRY-RUN] Would create storage credential '{cred_name}' (service principal)")
            else:
                print(f"  [CREATE]  Creating storage credential '{cred_name}' in secondary...")
                try:
                    result = w_secondary.storage_credentials.create(
                        name=cred_name,
                        azure_service_principal=cred.azure_service_principal,
                        comment=cred.comment or "",
                    )
                    print(f"            Created successfully (id: {result.id})")
                except Exception as e:
                    print(f"            FAILED: {e}")
        else:
            print(f"  [SKIP]    Storage credential '{cred_name}' - unsupported credential type.")

    # ---- Replicate external locations -------------------------------------
    print(f"\n--- External Locations ({len(extlocs_to_replicate)} to check) ---")
    secondary_extlocs = list(w_secondary.external_locations.list())
    secondary_extloc_names = {e.name for e in secondary_extlocs}

    for extloc_name, extloc in extlocs_to_replicate.items():
        if extloc_name in secondary_extloc_names:
            print(f"  [EXISTS]  External location '{extloc_name}' already exists in secondary.")
            continue

        primary_url = extloc.url or ""
        secondary_url = convert_to_secondary(primary_url)

        if dry_run:
            print(f"  [DRY-RUN] Would create external location '{extloc_name}':")
            print(f"            URL: {primary_url} -> {secondary_url}")
            print(f"            credential: {extloc.credential_name}")
        else:
            print(f"  [CREATE]  Creating external location '{extloc_name}' in secondary...")
            print(f"            URL: {primary_url} -> {secondary_url}")
            try:
                result = w_secondary.external_locations.create(
                    name=extloc_name,
                    url=secondary_url,
                    credential_name=extloc.credential_name or "",
                    comment=extloc.comment or "",
                    read_only=extloc.read_only or False,
                )
                print(f"            Created successfully (name: {result.name})")
            except Exception as e:
                print(f"            FAILED: {e}")


# ---------------------------------------------------------------------------
# Schema-level storage location summary
# ---------------------------------------------------------------------------
def print_schema_summary(primary_info: dict):
    """Print a summary of each schema and its associated resources."""
    print(f"\n{'='*70}")
    print("Schema -> External Location -> Storage Credential Mapping")
    print(f"{'='*70}")
    for mapping in primary_info["schema_mapping"]:
        schema: SchemaInfo = mapping["schema"]
        extloc: ExternalLocationInfo | None = mapping.get("external_location")
        cred: StorageCredentialInfo | None = mapping.get("storage_credential")
        print(f"\n  Schema: {schema.full_name or schema.name}")
        print(f"    Storage root:       {schema.storage_root or 'N/A'}")
        print(f"    External location:  {extloc.name if extloc else 'NONE FOUND'}")
        if extloc:
            print(f"      URL:              {extloc.url or 'N/A'}")
            print(f"      Secondary URL:    {convert_to_secondary(extloc.url or '')}")
        print(f"    Storage credential: {cred.name if cred else 'NONE FOUND'}")
        if cred and cred.azure_managed_identity:
            ami = cred.azure_managed_identity
            print(f"      Access connector:           {ami.access_connector_id or 'N/A'}")
            print(f"      Secondary access connector: {convert_to_secondary(ami.access_connector_id or '')}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 70)
    print("  Databricks DR - Phase 1: Unity Catalog Replication (Azure)")
    print("  Authentication: Databricks SDK profiles")
    print("=" * 70)

    # Collect runtime inputs
    primary_profile = "primary-ws" #input("\nDatabricks CLI profile for PRIMARY: ").strip()
    secondary_profile = "secondary-ws" #input("Databricks CLI profile for SECONDARY: ").strip()
    catalog_name = "primary-catalog" #input("Catalog name to setup: ").strip()

    if not all([primary_profile, secondary_profile, catalog_name]):
        print("\n[ERROR] All inputs are required.")
        sys.exit(1)

    dry_run_input = "y" #input("\nDry run? (y/n, default=y): ").strip().lower()
    dry_run = dry_run_input != "n"
    if dry_run:
        print("\n>>> DRY-RUN MODE: No changes will be made to the secondary workspace. <<<")

    # Initialize clients
    print("\nValidating profiles...")
    try:
        w_primary = create_workspace_client(primary_profile)
        print(f"  PRIMARY profile '{primary_profile}' - OK")
    except Exception as e:
        print(f"  [ERROR] Failed to connect to primary workspace: {e}")
        sys.exit(1)

    try:
        w_secondary = create_workspace_client(secondary_profile)
        print(f"  SECONDARY profile '{secondary_profile}' - OK")
    except Exception as e:
        print(f"  [ERROR] Failed to connect to secondary workspace: {e}")
        sys.exit(1)

    # Collect primary info
    primary_info = collect_primary_info(w_primary, catalog_name)

    # Print summary
    print_schema_summary(primary_info)

    # Replicate
    replicate_to_secondary(primary_info, w_secondary, dry_run=dry_run)

    print(f"\n{'='*70}")
    if dry_run:
        print("DRY-RUN complete. Re-run with dry_run=n to apply changes.")
    else:
        print("Replication complete.")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
