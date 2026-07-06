# RBAC, Network Security & Compliance Controls

| Control | Implementation |
|---|---|
| Identity | Azure AD groups (`bsi-data-engineers`, `bsi-analysts`, `bsi-readonly`) mapped to Unity Catalog / ADLS Gen2 ACLs — no individual user grants. |
| Least privilege | Analysts get `SELECT` on `bsi_gold.*` only; Silver/Bronze restricted to the data engineering group. |
| Network | ADLS Gen2 and Databricks workspace deployed with Private Link / VNet injection; public network access disabled in prod. |
| Encryption | At rest: Storage Service Encryption (Microsoft-managed keys, CMK optional). In transit: TLS 1.2+ enforced on all linked services. |
| Managed identity | ADF and Databricks use system-assigned managed identities for ADLS access — no long-lived storage keys in most paths. |
| Audit | Azure Monitor diagnostic settings stream ADF pipeline runs, Databricks audit logs, and Key Vault access logs to a central Log Analytics workspace with a 1-year retention policy for compliance. |
| Data classification | PII fields (visitor identifiers, lead/contact email addresses) tagged in Purview and masked/pseudonymized in Gold-layer marts consumed by non-engineering teams. |

This directly maps to the JD's "Working knowledge of data governance, audit trails, metadata
management, and compliance standards" required skill and "Follow internal controls, audit
protocols, and secure data handling procedures" responsibility.
