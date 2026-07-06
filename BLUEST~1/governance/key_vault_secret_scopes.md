# Secrets Management: Azure Key Vault + Databricks Secret Scopes

All credentials (storage keys, Databricks PATs, Oracle passwords, service principal secrets)
are stored in Azure Key Vault (`bsi-prod-kv`) — never in notebook code, ADF pipeline JSON, or
Git. Addresses the JD's "Working knowledge of ... Azure Key Vault for secrets and credential
management" desired skill.

## Setup
```bash
# Create a Databricks secret scope backed by Key Vault (one-time setup per workspace)
databricks secrets create-scope --scope bsi-kv-scope \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id /subscriptions/xxxx/resourceGroups/bsi-prod-rg/providers/Microsoft.KeyVault/vaults/bsi-prod-kv \
  --dns-name https://bsi-prod-kv.vault.azure.net/
```

## Usage in notebooks
```python
adls_key = dbutils.secrets.get(scope="bsi-kv-scope", key="adls-gen2-account-key")
oracle_pwd = dbutils.secrets.get(scope="bsi-kv-scope", key="oracle-crm-password")
```

## Usage in ADF
Every ADF linked service (`LS_ADLS_Gen2`, `LS_Databricks`, `LS_OnPremOracle`) resolves its
credential via an `AzureKeyVaultSecret` reference to `LS_BSI_KeyVault` rather than storing a
plaintext secret in the linked service definition (see `adf/linkedService/*.json`).

## Access control
- ADF's system-assigned managed identity is granted `Get`/`List` secret permissions only
  (no `Set`/`Delete`) via Key Vault access policy / RBAC.
- Databricks workspace service principal has a scoped, time-bound access policy reviewed
  quarterly as part of the audit cycle.
- Secret rotation: Databricks PAT and Oracle password rotate every 90 days; rotation is
  tracked in the `bsi_governance.audit_log` table via `audit_logging.write_audit_event`.
