"""
Databricks Disaster Recovery - Unity Catalog Replication Script
===============================================================
Replicates storage credentials and external locations from a primary
workspace (West Europe) to a secondary workspace (North Europe) on Azure.

Authentication: Uses Databricks CLI profiles (databricks configure --profile).

Naming conventions:
  Storage accounts:    adledwdeltatst[we|ne]
  Access connectors:   dbac-tst-[we|ne]  in  rg-dmlz-databricks-access-tst-[we|ne]-01
  Schema storage URLs: abfss://unity@<storage_account>.dfs.core.windows.net/__unitystorage/schemas/<uuid>
"""

import json
import subprocess
import sys
from typing import Optional


# ---------------------------------------------------------------------------
# Region mapping
# ---------------------------------------------------------------------------
REGION_REPLACEMENTS = {
    # storage account suffix
    "tstwe": "tstne",
    # access connector & resource group region tags
    "tst-we": "tst-ne",
}


def convert_to_secondary(value: str) -> str:
    """Convert a primary (west) resource reference to its secondary (north) equivalent."""
    result = value
    for primary_pattern, secondary_pattern in REGION_REPLACEMENTS.items():
        result = result.replace(primary_pattern, secondary_pattern)
    return result


# ---------------------------------------------------------------------------
# Databricks CLI wrapper
# ---------------------------------------------------------------------------
class DatabricksCLIClient:
    """Wraps `databricks` CLI calls for Unity Catalog operations."""

    def __init__(self, profile: str, label: str = ""):
        self.profile = profile
        self.label = label
        # Validate profile works
        self._run(["databricks", "current-user", "me"])

    def _run(self, cmd: list[str], check: bool = True) -> dict | list | None:
        full_cmd = cmd + ["--profile", self.profile, "--output", "json"]
        result = subprocess.run(full_cmd, capture_output=True, text=True, timeout=60)
        if check and result.returncode != 0:
            print(f"  [ERROR] Command failed: {' '.join(cmd)}")
            print(f"          stderr: {result.stderr.strip()}")
            if check:
                raise RuntimeError(f"CLI command failed: {result.stderr.strip()}")
        if result.stdout.strip():
            return json.loads(result.stdout)
        return None

    # -- Schemas ------------------------------------------------------------
    def list_schemas(self, catalog_name: str) -> list[dict]:
        data = self._run(["databricks", "schemas", "list", catalog_name])
        return data if isinstance(data, list) else []

    # -- Storage Credentials ------------------------------------------------
    def list_storage_credentials(self) -> list[dict]:
        data = self._run(["databricks", "storage-credentials", "list"])
        return data if isinstance(data, list) else []

    def get_storage_credential(self, name: str) -> Optional[dict]:
        try:
            data = self._run(["databricks", "storage-credentials", "get", name])
            return data
        except RuntimeError:
            return None

    def create_storage_credential(self, payload: dict) -> dict:
        data = self._run([
            "databricks", "storage-credentials", "create",
            "--json", json.dumps(payload),
        ])
        return data or {}

    # -- External Locations -------------------------------------------------
    def list_external_locations(self) -> list[dict]:
        data = self._run(["databricks", "external-locations", "list"])
        return data if isinstance(data, list) else []

    def get_external_location(self, name: str) -> Optional[dict]:
        try:
            data = self._run(["databricks", "external-locations", "get", name])
            return data
        except RuntimeError:
            return None

    def create_external_location(self, payload: dict) -> dict:
        data = self._run([
            "databricks", "external-locations", "create",
            "--json", json.dumps(payload),
        ])
        return data or {}


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------
def collect_primary_info(client: DatabricksCLIClient, catalog_name: str) -> dict:
    """Gather schemas, storage credentials and external locations from primary."""
    print(f"\n{'='*70}")
    print(f"Reading catalog '{catalog_name}' from PRIMARY workspace")
    print(f"{'='*70}")

    # 1. List schemas
    schemas = client.list_schemas(catalog_name)
    print(f"\nFound {len(schemas)} schema(s) in catalog '{catalog_name}':")
    for s in schemas:
        print(f"  - {s['name']}  (storage_root: {s.get('storage_root', 'N/A')})")

    # 2. List all storage credentials
    all_creds = client.list_storage_credentials()
    creds_by_name = {c["name"]: c for c in all_creds}
    print(f"\nFound {len(all_creds)} storage credential(s) in primary workspace:")
    for c in all_creds:
        print(f"  - {c['name']}")

    # 3. List all external locations
    all_extlocs = client.list_external_locations()
    extlocs_by_name = {e["name"]: e for e in all_extlocs}
    print(f"\nFound {len(all_extlocs)} external location(s) in primary workspace:")
    for e in all_extlocs:
        print(f"  - {e['name']}  -> {e.get('url', 'N/A')}  (credential: {e.get('credential_name', 'N/A')})")

    # 4. For each schema, find which external location covers its storage_root
    schema_mapping = []
    for s in schemas:
        storage_root = s.get("storage_root", "")
        matched_extloc = None
        matched_cred = None

        # Find external location whose url is a prefix of the schema storage root
        for e in all_extlocs:
            extloc_url = e.get("url", "")
            if storage_root and extloc_url and storage_root.startswith(extloc_url):
                matched_extloc = e
                matched_cred = creds_by_name.get(e.get("credential_name"))
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
    secondary_client: DatabricksCLIClient,
    dry_run: bool = False,
):
    """Ensure storage credentials and external locations exist in secondary workspace."""
    print(f"\n{'='*70}")
    print(f"Replicating to SECONDARY workspace")
    print(f"{'='*70}")

    # Collect unique storage credentials and external locations to replicate
    creds_to_replicate: dict[str, dict] = {}
    extlocs_to_replicate: dict[str, dict] = {}

    for mapping in primary_info["schema_mapping"]:
        cred = mapping.get("storage_credential")
        extloc = mapping.get("external_location")
        if cred and cred["name"] not in creds_to_replicate:
            creds_to_replicate[cred["name"]] = cred
        if extloc and extloc["name"] not in extlocs_to_replicate:
            extlocs_to_replicate[extloc["name"]] = extloc

    # Also replicate all credentials referenced by external locations
    creds_by_name_primary = {c["name"]: c for c in primary_info["storage_credentials"]}
    for extloc in extlocs_to_replicate.values():
        cred_name = extloc.get("credential_name")
        if cred_name and cred_name not in creds_to_replicate:
            cred = creds_by_name_primary.get(cred_name)
            if cred:
                creds_to_replicate[cred_name] = cred

    # ---- Replicate storage credentials ------------------------------------
    print(f"\n--- Storage Credentials ({len(creds_to_replicate)} to check) ---")
    secondary_creds = secondary_client.list_storage_credentials()
    secondary_cred_names = {c["name"] for c in secondary_creds}

    for cred_name, cred in creds_to_replicate.items():
        if cred_name in secondary_cred_names:
            print(f"  [EXISTS]  Storage credential '{cred_name}' already exists in secondary.")
            continue

        # Build the secondary credential payload
        payload = {"name": cred_name, "comment": cred.get("comment", "")}

        if "azure_managed_identity" in cred:
            ami = cred["azure_managed_identity"]
            primary_connector = ami.get("access_connector_id", "")
            secondary_connector = convert_to_secondary(primary_connector)
            payload["azure_managed_identity"] = {
                "access_connector_id": secondary_connector,
            }
            if ami.get("managed_identity_id"):
                payload["azure_managed_identity"]["managed_identity_id"] = convert_to_secondary(
                    ami["managed_identity_id"]
                )
        elif "azure_service_principal" in cred:
            payload["azure_service_principal"] = cred["azure_service_principal"]
        else:
            print(f"  [SKIP]    Storage credential '{cred_name}' - unsupported credential type.")
            continue

        if dry_run:
            print(f"  [DRY-RUN] Would create storage credential '{cred_name}':")
            print(f"            {json.dumps(payload, indent=2)}")
        else:
            print(f"  [CREATE]  Creating storage credential '{cred_name}' in secondary...")
            try:
                result = secondary_client.create_storage_credential(payload)
                print(f"            Created successfully (id: {result.get('id', 'N/A')})")
            except Exception as e:
                print(f"            FAILED: {e}")

    # ---- Replicate external locations -------------------------------------
    print(f"\n--- External Locations ({len(extlocs_to_replicate)} to check) ---")
    secondary_extlocs = secondary_client.list_external_locations()
    secondary_extloc_names = {e["name"] for e in secondary_extlocs}

    for extloc_name, extloc in extlocs_to_replicate.items():
        if extloc_name in secondary_extloc_names:
            print(f"  [EXISTS]  External location '{extloc_name}' already exists in secondary.")
            continue

        primary_url = extloc.get("url", "")
        secondary_url = convert_to_secondary(primary_url)

        payload = {
            "name": extloc_name,
            "url": secondary_url,
            "credential_name": extloc.get("credential_name", ""),
            "comment": extloc.get("comment", ""),
            "read_only": extloc.get("read_only", False),
        }

        if dry_run:
            print(f"  [DRY-RUN] Would create external location '{extloc_name}':")
            print(f"            URL: {primary_url} -> {secondary_url}")
            print(f"            {json.dumps(payload, indent=2)}")
        else:
            print(f"  [CREATE]  Creating external location '{extloc_name}' in secondary...")
            print(f"            URL: {primary_url} -> {secondary_url}")
            try:
                result = secondary_client.create_external_location(payload)
                print(f"            Created successfully (id: {result.get('id', 'N/A')})")
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
        schema = mapping["schema"]
        extloc = mapping.get("external_location")
        cred = mapping.get("storage_credential")
        print(f"\n  Schema: {schema.get('full_name', schema['name'])}")
        print(f"    Storage root:       {schema.get('storage_root', 'N/A')}")
        print(f"    External location:  {extloc['name'] if extloc else 'NONE FOUND'}")
        if extloc:
            print(f"      URL:              {extloc.get('url', 'N/A')}")
            print(f"      Secondary URL:    {convert_to_secondary(extloc.get('url', ''))}")
        print(f"    Storage credential: {cred['name'] if cred else 'NONE FOUND'}")
        if cred and "azure_managed_identity" in cred:
            ami = cred["azure_managed_identity"]
            print(f"      Access connector:           {ami.get('access_connector_id', 'N/A')}")
            print(f"      Secondary access connector: {convert_to_secondary(ami.get('access_connector_id', ''))}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 70)
    print("  Databricks DR - Unity Catalog Replication (Azure)")
    primary_region = "westeurope" #input("\nDatabricks Primary Region: ").strip()
    secondary_region = "northeurope" #input("\nDatabricks Secondary Region: ").strip()
    print("  Authentication: Databricks CLI profiles")
    print("=" * 70)

    # Collect runtime inputs
    primary_profile = "primary-ws" #input(f"\nDatabricks CLI profile for PRIMARY ({primary_region}): ").strip()
    secondary_profile = "secondary-ws" #input(f"Databricks CLI profile for SECONDARY ({secondary_region}): ").strip()
    catalog_name = "primary-catalog" #input("Catalog name to setup: ").strip()

    if not all([primary_profile, secondary_profile, catalog_name]):
        print("\n[ERROR] All inputs are required.")
        sys.exit(1)

    dry_run_input = input("\nDry run? (y/n, default=y): ").strip().lower()
    dry_run = dry_run_input != "n"
    if dry_run:
        print("\n>>> DRY-RUN MODE: No changes will be made to the secondary workspace. <<<")

    # Initialize clients
    print("\nValidating CLI profiles...")
    try:
        primary_client = DatabricksCLIClient(primary_profile, "PRIMARY")
        print(f"  PRIMARY profile '{primary_profile}' - OK")
    except Exception as e:
        print(f"  [ERROR] Failed to connect to primary workspace: {e}")
        sys.exit(1)

    try:
        secondary_client = DatabricksCLIClient(secondary_profile, "SECONDARY")
        print(f"  SECONDARY profile '{secondary_profile}' - OK")
    except Exception as e:
        print(f"  [ERROR] Failed to connect to secondary workspace: {e}")
        sys.exit(1)

    # Collect primary info
    primary_info = collect_primary_info(primary_client, catalog_name)

    # Print summary
    print_schema_summary(primary_info)

    # Replicate
    replicate_to_secondary(primary_info, secondary_client, dry_run=dry_run)

    print(f"\n{'='*70}")
    if dry_run:
        print("DRY-RUN complete. Re-run with dry_run=n to apply changes.")
    else:
        print("Replication complete.")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
